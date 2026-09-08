# avapuck

A privacy-first data platform, small enough to run on a laptop: pull 100 rows
per run from a BigQuery public dataset, or upload any CSV of any size through
the console, encrypt sensitive fields with AES-256-GCM through an HTTPS
service, publish to Kafka, mask under Apache Ranger policy at the Hive
boundary, load a Postgres warehouse, emit a PDF, and then publish one
**signed verdict** that tells downstream teams whether to proceed.

```
BigQuery/CSV ─▶ card split ─▶ encrypt ─▶ publish ─▶ decrypt+mask ─▶ Hive ─▶ Postgres ─▶ PDF ─▶ verdict
   or upload     6·enc·4    AES-256-GCM  every row   Ranger policy               per run     signed,
   (uncapped)               on executors (envelope)                             slot, UTC   always
```

One Airflow DAG, `trips_pipeline`, eight stages plus a terminal `emit_verdict`
task that runs even when a stage failed. Every four hours, or whenever the
console starts a run. Each stage consumes what the one before it wrote and
streams rows in chunks, so a hundred-row scheduled run and a two-gigabyte
upload take the same path.

**Read `docs/DECISIONS.md` for why it is shaped this way, and
`TRUST-BOUNDARY.md` for what is stubbed.** Nothing in this repository should
be described as production-ready without reading the second one.

---

## Quick start

```bash
git clone https://github.com/mabdullah679/avapuck.git
cd avapuck
./scripts/install-prereqs-ubuntu.sh   # Ubuntu/Debian only; skip if Docker is installed
./setup.sh
```

Docker with the Compose plugin is the only host prerequisite. `setup.sh`:

1. creates `.env.local` from `.env.example` (dev defaults; edit it);
2. generates a local TLS CA and the two service certificates;
3. installs the git hooks (`pre-push` runs `make ci`; `post-commit` pokes Jenkins);
4. builds and starts the stack, creates the Kafka topics and ACLs;
5. runs the pipeline once for the current hour's slot, UTC;
6. prints where the PDF landed and what the watchdog thinks.

```bash
./setup.sh --no-run     # set up, do not run
./setup.sh --ci         # also start local Jenkins
./setup.sh --rebuild    # rebuild the locally-built images
./setup.sh --down       # stop, keep data
./setup.sh --clean      # stop and delete volumes
```

**Sizing.** Give Docker 8 GB, 4 CPUs, 40 GB of disk, more disk for the 2 GB
scale test. On Windows, run inside WSL2 from the Linux filesystem; `setup.sh`
checks both.

### What you get

| Where | What | Login |
|---|---|---|
| <http://localhost:8447> | **Console**: upload a CSV, run it, runs and verdicts, the Kafka log table, the Spark cluster, the traffic audit | `CONSOLE_ADMIN_USER` / `CLIENT_SECRET_CONSOLE_ADMIN` |
| <http://localhost:8085> | Airflow | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| <http://localhost:8090> | Kafka UI (Kafbat), read-only | `KAFKA_UI_LOGIN_USER` / `KAFKA_UI_LOGIN_PASSWORD` |
| <http://localhost:8446/status> | Watchdog: last verdict per dataset, healthy or not, and why | none |
| <http://localhost:8448/status> | Scaler: workers, load, last decision | none |
| <http://localhost:8080> | Spark master | none |
| <http://localhost:8095> | Jenkins, with `--ci` | `JENKINS_ADMIN_*` |
| `pipeline-reports/` | One PDF per run slot, e.g. `trips_report_20260908T0400Z.pdf` | |

All logins come from `.env.local`.

### Upload and run

Open the console. On this machine it signs you in by itself
(`CONSOLE_DEV_AUTOLOGIN=true`, a dev convenience that must be off anywhere
else); otherwise sign in with `CONSOLE_ADMIN_USER` and
`CLIENT_SECRET_CONSOLE_ADMIN`. Drop a CSV or pick one, choose the pipeline
from the dropdown, which lists every DAG Airflow knows with three presets
(all, responding 200, connected), name the dataset if the file name is not
it, press **Run**. That starts one fresh run of that pipeline with the file
as its source, uncapped by default, and the Runs tab shows it progress to a
signed verdict. To try it with generated data:

```bash
python3 scripts/make_test_csv.py --rows 1000 > small.csv       # then upload it
python3 scripts/make_test_csv.py --gigabytes 2 > big.csv       # the scale test; see docs/RUNBOOK.md
```

### Run it by hand

```bash
docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13T04:00:00
.venv/bin/python -m pipeline.run_pipeline --csv path/to/file.csv     # no Airflow, same stages
```

Dates are UTC. The run slot names the PDF and the verdict; after 8pm Eastern
the slot date is already tomorrow, which is correct.

---

## The things that do not move

Numbered in `CLAUDE.md`; every design decision either serves one of these or
is written down in `TRUST-BOUNDARY.md` as a known limit.

1. **Sensitive data is encrypted before it lands anywhere.** The only
   plaintext window is inside the HTTPS crypto service. `assert_no_plaintext`
   refuses every chunk that would break this.
2. **Every job authenticates with a JWT from the IdP**, verified by signature,
   issuer, audience and expiry, never merely decoded. The run verdict on Kafka
   is signed the same way, and the console's login goes through the same IdP.
3. **Masking policy is Apache Ranger policy JSON**, authored once, generated
   for review when absent, never regenerated over an edit.
4. **100 rows per BigQuery run, hard-capped**, because that is a bill.
   Uploads are bounded only by `UPLOAD_MAX_ROWS`, and the data path streams.
5. **Idempotent by run slot.** A re-run replaces its own partition; on Kafka
   the envelope's idempotency key carries the same rule.
6. **The verdict is always emitted and is the only predicate.**
7. **One place for every value.** `.env.local`, no compose defaults.

---

## What is in the box

| Component | Where | Status |
|---|---|---|
| Orchestration | `dags/trips_pipeline_dag.py`, `pipeline/stages.py` | Airflow 3.0.2, one DAG. The plain runner walks the same stage list |
| Console | `services/console/` | Upload, run, runs and verdicts, Kafka log, Spark cluster, traffic. IdP login |
| Identity | `services/idp/` | Local RS256 IdP, per-job clients and audience grants. **Stand-in** for a real IdP |
| Encryption | `services/crypto/` | AES-256-GCM, field name as AAD, keyed blind indexes. **One master key, no KMS** |
| Streaming data path | `pipeline/common/batching.py` | Every stage in chunks; crypto on Spark executors |
| Messaging | `pipeline/messaging/`, `contracts/` | Kafka with SASL and deny-by-default ACLs; platform envelope; three topics |
| Verdict | `pipeline/verdict.py` | Pure function; always emitted; signed by the IdP |
| Consumer | `dags/verdict_listener_dag.py` | Reference DAG, asset-scheduled on the control topic. See `docs/CONSUMING.md` |
| Kafka tap | `services/tap/` | Scrape-only; every flat and control message into `monitor.kafka_log` |
| Watchdog | `services/watchdog/` | Outside Airflow; unhealthy when a verdict is late or negative |
| Scaler | `services/scaler/` | Grows and shrinks Spark workers on load. **Holds the Docker socket** |
| Traffic audit | `pipeline/common/traffic.py`, `config/traffic_profile.json` | Detect and block against a written profile, on the console and the IdP |
| Masking | `config/ranger/`, `pipeline/mask/` | Real Ranger policy JSON, applied by a local engine. **Ranger admin not deployed** |
| Data contracts | `config/datasets/`, `pipeline/metadata/contract.py` | Written once per dataset, gated on drift |
| Warehouse | Hive 4 + Postgres 16 | External tables over masked Parquet; batched upsert keyed on the manifest |
| Report | `pipeline/report/` | One PDF per run, UTC, printing the data window and the verdict's warnings |
| CI | `Jenkinsfile`, `docker/jenkins/` | Local Jenkins, two tiers, both plain `make` targets |
| Secrets | `.env.example` | One committed template; `.env.local` is the only place a value lives |

---

## Kafka

Three topics, one producer, one envelope. Every message is
`{envelope, payload, signature?}`; `contracts/` holds the schemas and
`pipeline/messaging/` is the shared library every producer and consumer uses.

| Topic | Carries | Who may read |
|---|---|---|
| `rpos_encrypted` | `row.encrypted`: sensitive fields **as ciphertext** plus blind indexes | nobody by default |
| `rpos_flat` | `row.flat`: the never-sensitive business columns | `consumer`, `ui`, `tap` |
| `pipeline_runs` | `run.verdict`: one signed yes/no per run, always | `consumer`, `ui`, `watchdog`, `tap` |

No sensitive value is published in the clear. The flat topic carries the
columns that were never sensitive, and the publisher refuses the send
otherwise. Every row of every run is published, uploads included, by
decision (`docs/DECISIONS.md` D16). The `pipeline` principal writes and never
reads. SASL/PLAIN authenticates; it does not encrypt the wire.

---

## Credentials and configuration

**One file.** `.env.local` (gitignored) is the single place every value
lives. `.env.example` (committed) lists every variable with its dev default.
`docker-compose.yml` has no built-in defaults: a variable missing from
`.env.local` fails `up` by name, and `setup.sh` lists all the missing ones at
once. Wire your own secrets manager into `.env.local` however your
organisation does it. `docs/DECISIONS.md` D12.

**BigQuery is not switchable from the console.** `EXTRACT_MODE` is an env var
fixed at stack start, on purpose: it is a billable source and there is no
RBAC yet. When RBAC exists, an admin UI gets an "update BigQuery toggle" API
call. D15.

### Live BigQuery

1. Create a service account with **BigQuery Job User** (`roles/bigquery.jobUser`).
2. Download a JSON key to `secrets/gcp-sa.json` and `chmod 600` it. Never commit it.
3. In `.env.local`, set `GCP_PROJECT_ID=<your project id>` and `EXTRACT_MODE=live`.
4. `docker compose --env-file .env.local --profile core up -d airflow`

About 24 MB scanned per run, about $0.0002. Your own query in `BQ_SQL_FILE`
must use `LIMIT @row_limit`; the extractor refuses one that does not.

---

## Testing and CI

```bash
make test          # the suite, no stack needed
make ci            # exactly what Jenkins and the pre-push hook run
```

`./setup.sh --ci` starts Jenkins at <http://localhost:8095>, seeded from
`docker/jenkins/casc.yaml`. Nothing in this repo uses GitHub Actions.

---

## Repository layout

```
dags/              trips_pipeline_dag.py (the run), verdict_listener_dag.py (reference consumer)
pipeline/
  stages.py        THE stage list, walked by the DAG and by run_pipeline.py
  verdict.py       the run verdict: one pure function, the heaviest tests
  common/          batching.py (streaming), traffic.py (audit), auth.py, trace.py
  messaging/       envelope, contracts loader, dedupe, signing, trigger filter
  metadata/        schema inference, dataset contracts, Ranger policy generation
  extract/ transform/ mask/ load/ report/ publish/
services/          console/ · idp/ · crypto/ · watchdog/ · tap/ · scaler/
contracts/         JSON Schema for every message and stage event, plus examples
config/            bq/ SQL · datasets/ contracts · ranger/ policy · traffic_profile.json
docker/            Dockerfiles, Kafka and Airflow entrypoints, Jenkins seed
scripts/           gen_certs.sh, make_test_csv.py, ci/contracts_check.py, git-hooks/
tests/             run them: make test
docs/              DECISIONS, ORCHESTRATION-SPEC, ARCHITECTURE, EXECUTION-FLOW, RUNBOOK, CONSUMING
```

## For AI coding agents

`AGENTS.md` in the root is the working agreement. Read it, then
`TRUST-BOUNDARY.md`, before changing anything.

## Provenance

Data is the public `bigquery-public-data` datasets named above, an offline
fixture with the same shape, and generated files from `scripts/make_test_csv.py`.
No real customer data is used anywhere in this repository.
