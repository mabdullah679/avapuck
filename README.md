# avapuck

A privacy-first data platform, small enough to run on a laptop: pull 100 rows
per run from a BigQuery public dataset (or any CSV), encrypt sensitive fields
with AES-256-GCM through an HTTPS service, publish to Kafka, mask under Apache
Ranger policy at the Hive boundary, load a Postgres warehouse, emit a PDF, and
then publish one **signed verdict** that tells downstream teams whether to
proceed.

```
BigQuery/CSV ─▶ card split ─▶ encrypt ─▶ publish ─▶ decrypt+mask ─▶ Hive ─▶ Postgres ─▶ PDF ─▶ verdict
   plaintext,      6·enc·4    AES-256-GCM  2 topics    Ranger policy               per run     signed,
   transient                              (envelope)                              slot, UTC   always
```

One Airflow DAG, `trips_pipeline`, eight stages plus a terminal `emit_verdict`
task that runs even when a stage failed. Every four hours. Each stage consumes
what the one before it wrote and logs its own start, status and duration to
the Airflow UI.

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

**Sizing.** Measured working set across a full run is about 4.7 GB; caps total
about 15 GB. Give Docker 8 GB, 4 CPUs, 40 GB of disk. On Windows, run inside
WSL2 from the Linux filesystem; `setup.sh` checks both.

### What you get

| Where | What |
|---|---|
| <http://localhost:8085> | Airflow. Password: `docker exec pl-airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated` |
| <http://localhost:8090> | Kafka UI (Kafbat), read-only. Login from `KAFKA_UI_LOGIN_*` in `.env.local` |
| <http://localhost:8446/status> | Watchdog: last verdict per dataset, healthy or not, and why |
| <http://localhost:8080> | Spark master |
| <http://localhost:8095> | Jenkins, with `--ci` |
| `pipeline-reports/` | One PDF per run slot, e.g. `trips_report_20260906T0400Z.pdf` |

### Run it again

```bash
docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13T04:00:00
```

Dates are UTC. The run slot names the PDF and the verdict; after 8pm Eastern
the slot date is already tomorrow, which is correct.

Against a specific CSV instead of the configured source:

```bash
docker exec -e CSV_SOURCE_PATH=/data/csv/<file>.csv \
  pl-airflow airflow dags test trips_pipeline 2026-09-13T04:00:00
```

Without Airflow at all (same stage list, same functions):

```bash
.venv/bin/python -m pipeline.run_pipeline --slot 2026-09-13T04:00Z
```

---

## The five things that do not move

Numbered in `CLAUDE.md`; every design decision either serves one of these or
is written down in `TRUST-BOUNDARY.md` as a known limit.

1. **Sensitive data is encrypted before it lands anywhere.** The only
   plaintext window is inside the HTTPS crypto service. `assert_no_plaintext`
   refuses every write that would break this.
2. **Every job authenticates with a JWT from the IdP**, verified by signature,
   issuer, audience and expiry, never merely decoded. The run verdict on Kafka
   is signed the same way.
3. **Masking policy is Apache Ranger policy JSON**, authored once, generated
   for review when absent, never regenerated over an edit.
4. **100 rows per run, hard-capped** in the SQL and asserted after.
5. **Idempotent by run slot.** A re-run replaces its own partition; on Kafka
   the envelope's idempotency key carries the same rule.

---

## What is in the box

| Component | Where | Status |
|---|---|---|
| Orchestration | `dags/trips_pipeline_dag.py`, `pipeline/stages.py` | Airflow 3.0.2, one DAG. The plain runner walks the same stage list |
| Identity | `services/idp/` | Local RS256 IdP, per-job clients and audience grants. **Stand-in** for a real IdP |
| Encryption | `services/crypto/` | AES-256-GCM, field name as AAD, keyed blind indexes. **One master key, no KMS** |
| Messaging | `pipeline/messaging/`, `contracts/` | Kafka with SASL and deny-by-default ACLs; platform envelope; three topics |
| Verdict | `pipeline/verdict.py` | Pure function; always emitted; signed by the IdP |
| Consumer | `dags/verdict_listener_dag.py` | Reference DAG, asset-scheduled on the control topic. See `docs/CONSUMING.md` |
| Watchdog | `services/watchdog/` | Outside Airflow; unhealthy when a verdict is late or negative |
| Masking | `config/ranger/`, `pipeline/mask/` | Real Ranger policy JSON, applied by a local engine. **Ranger admin not deployed** |
| Data contracts | `config/datasets/`, `pipeline/metadata/contract.py` | Written once per dataset, gated on drift |
| Warehouse | Hive 4 + Postgres 16 | External tables over masked Parquet; upsert keyed on the manifest |
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
| `rpos_flat` | `row.flat`: the never-sensitive business columns | `consumer`, `ui` |
| `pipeline_runs` | `run.verdict`: one signed yes/no per run, always | `consumer`, `ui`, `watchdog` |

No sensitive value is published in the clear. The requirement said "the
encrypted value to one topic and the unencrypted value to the other"; taken
literally that puts names, emails and card numbers into a durable, replicated
log. The flat topic therefore carries the columns that were never sensitive,
and the publisher refuses the send otherwise. `TRUST-BOUNDARY.md` 2.9.

The `pipeline` principal writes and never reads. SASL/PLAIN authenticates;
it does not encrypt the wire. 2.9a.

---

## Credentials and configuration

**One file.** `.env.local` (gitignored) is the single place every value
lives. `.env.example` (committed) lists every variable with its dev default.
`docker-compose.yml` has no built-in defaults: a variable missing from
`.env.local` fails `up` by name, and `setup.sh` lists all the missing ones at
once. Wire your own secrets manager into `.env.local` however your
organisation does it; nothing else needs to change. `docs/DECISIONS.md` D12.

| What | Where | Required |
|---|---|---|
| Every password, secret, topic name, tuning value | `.env.local` | yes |
| TLS CA and service certs | `certs/`, from `./scripts/gen_certs.sh` | yes, generated |
| GCP service-account key | `secrets/gcp-sa.json` | only for live BigQuery |

### Live BigQuery

1. Create a service account with **BigQuery Job User** (`roles/bigquery.jobUser`).
2. Download a JSON key to `secrets/gcp-sa.json` and `chmod 600` it. Never commit it.
3. In `.env.local`, set `GCP_PROJECT_ID=<your project id>` and `EXTRACT_MODE=live`.
4. `docker compose --env-file .env.local --profile core up -d airflow`

The default live source is retail point-of-sale transactions joined to
customer records (`bigquery-public-data.thelook_ecommerce`): real names,
emails and street addresses, which is what makes it a meaningful test of the
encrypt and mask stages. About 24 MB scanned per run, about $0.0002. Two SQL
files ship in `config/bq/`; `rpos_transactions_cards.sql` adds a `card_info`
column of published Visa/Mastercard test PANs. Your own query must use
`LIMIT @row_limit`; the extractor refuses one that does not.

---

## Testing and CI

```bash
make test          # the suite, no stack needed
make ci            # exactly what Jenkins and the pre-push hook run
make ci-service    # tests + every contract example validates
make ci-platform   # envelope embedding + backward compatibility vs main
```

`./setup.sh --ci` starts Jenkins at <http://localhost:8095>, seeded from
`docker/jenkins/casc.yaml`, polling the repo over a read-only bind mount. It
runs the same two targets. Nothing in this repo uses GitHub Actions.

---

## Repository layout

```
dags/              trips_pipeline_dag.py (the run), verdict_listener_dag.py (reference consumer)
pipeline/
  stages.py        THE stage list, walked by the DAG and by run_pipeline.py
  verdict.py       the run verdict: one pure function, the heaviest tests
  messaging/       envelope, contracts loader, dedupe, signing, trigger filter
  metadata/        schema inference, dataset contracts, Ranger policy generation
  extract/ transform/ mask/ load/ report/ publish/
services/          idp/ · crypto/ · watchdog/
contracts/         JSON Schema for every message and stage event, plus examples
config/            bq/ SQL · datasets/ contracts + overrides · ranger/ policy JSON
docker/            Dockerfiles, Kafka entrypoint and init, Jenkins seed
scripts/           gen_certs.sh, ci/contracts_check.py, git-hooks/
tests/             run them: make test
docs/              DECISIONS, ORCHESTRATION-SPEC, ARCHITECTURE, EXECUTION-FLOW, RUNBOOK, CONSUMING
```

## For AI coding agents

`AGENTS.md` in the root is the working agreement. Read it, then
`TRUST-BOUNDARY.md`, before changing anything. A `pipeline-run` skill in
`.claude/skills/` covers bringing the stack up and verifying it did the work.

## Provenance

Data is the public `bigquery-public-data` datasets named above and an
offline fixture with the same shape. No real customer data is used anywhere
in this repository.
