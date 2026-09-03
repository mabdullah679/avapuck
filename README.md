# Trips Data Pipeline

A daily-batch data pipeline that pulls from a Google BigQuery public dataset,
encrypts sensitive fields with AES-256-GCM through an HTTPS service, masks them
under Apache Ranger policy at the Hive boundary, loads a Postgres warehouse, and
emits a PDF report with charts.

Runs on Kubernetes (minikube) or locally with Docker Compose.

```
BigQuery/CSV ──▶ CSV ──▶ [card split] ──▶ [encrypt] ──▶ Parquet
                          prefix/mid/sfx     AES-256-GCM
                                                            │
                                              [decrypt + mask] ──▶ Hive
                                                                    │
                                          PDF ◀── Postgres ◀────────┘
```

One Airflow DAG, `trips_pipeline`, with eight tasks chained in order:
`extract` → `card_split` → `encrypt` → `publish` → `mask` → `register` →
`load` → `report`. Scheduled every 4 hours; each task consumes what the
previous one wrote, and logs its own start, status and duration to the
Airflow UI.

`publish` sends each row to two Kafka topics from one producer:
`rpos_encrypted` (sensitive fields as ciphertext) and `rpos_flat`
(non-sensitive business columns). **No sensitive value is published in the
clear** — see `TRUST-BOUNDARY.md` §2.9.

If BigQuery fails 3 times (`BQ_ATTEMPTS`), the extract stage falls back to
`CSV_FALLBACK_PATH` rather than failing the run, and says so in the UI.

---

## ⚠️ Read this before you start

**`TRUST-BOUNDARY.md` is not an appendix.** It states exactly what is
production-grade, what is stubbed, and what has never been verified. Two
examples that will matter to you on day one:

- **TLS is enabled locally, with a self-signed dev CA.** The IdP and crypto
  service serve real HTTPS and every client verifies against `certs/ca.crt`.
  But the CA's private key sits in `certs/` beside the certs it signs, so this
  trust root is worth exactly as much as the machine holding it. The Kubernetes
  manifests were **not** updated and are still plain HTTP.
- **Card splitting protects only ~6 digits.** See §2.8 — fine for the published
  test PANs in the sample data, not for live cardholder data.
- **Apache Ranger's admin service is not deployed.** Masking uses genuine Ranger
  policy JSON, applied by a local engine implementing Ranger's semantics. Every
  masked row records `masked_by` so you can always tell which ran.

Do not present anything from this repository as production-ready without
reading that file first.

---

## For AI coding agents

**`AGENTS.md` in the repository root is the working agreement** — invariants
that must not be broken, the things that will surprise you, and how to describe
this system honestly. Read it before changing anything.

A `pipeline-run` skill in `.claude/skills/` covers bringing the stack up,
running the pipeline, and verifying that Spark and Hive actually did the work
rather than silently falling back.



If you are an agent working on this repository, read these in order **before
changing anything**:

| Read | For |
|---|---|
| `AGENTS.md` (root) | **Start here.** Working agreement, invariants, how to describe this honestly |
| `docs/AGENTS.md` | Longer-form background on the same invariants |
| `TRUST-BOUNDARY.md` | What is real vs. stubbed. Never claim a stub works |
| `docs/ARCHITECTURE.md` | Why the data flows this way; the security model |
| `docs/EXECUTION-FLOW.md` | One run end to end, with the correlation-id trace |
| `docs/RUNBOOK.md` | Operating it, and every failure we have actually hit |
| `docs/DEPLOYMENT.md` | Kubernetes/minikube deployment, step by step |

**Five invariants. Breaking any one is a defect, not a trade-off:**

1. Plaintext sensitive data must never reach Parquet, Hive, Postgres, logs, or
   the PDF.
2. Every job authenticates with a JWT that is *verified* — signature, issuer,
   audience, expiry — never merely decoded.
3. Masking rules live in Ranger policy JSON. Never in transformation code.
4. 100 rows per run, capped in SQL *and* by `maximum_bytes_billed`.
5. Re-running a logical slice replaces its data. Never duplicates it.

---

## Quick start — Kubernetes

**Start with Docker Compose below unless you specifically need Kubernetes.**
The Compose path is the one that has TLS enabled, runs encryption on the real
Spark cluster, and registers real Hive tables; the k8s manifests predate all
three (see `TRUST-BOUNDARY.md` §4c).

Full detail in **`docs/DEPLOYMENT.md`**. The short version:

```bash
# 1. Prerequisites: docker, minikube, kubectl. Docker needs ≥ 20GB.
make -f Makefile.k8s preflight

# 2. Your GCP service-account key (this is the only credential you supply)
gcloud iam service-accounts keys create secrets/gcp-sa.json \
  --iam-account=<your-sa>@<your-project>.iam.gserviceaccount.com \
  --project=<your-project>
chmod 600 secrets/gcp-sa.json

# 3. Bring up the cluster, build images, deploy, verify
make -f Makefile.k8s up

# 4. Run the pipeline once, now
make -f Makefile.k8s run-now
make -f Makefile.k8s logs
```

## Quick start — local Docker Compose

This is the path to use if you just want to see the pipeline run. It works
**without a Google Cloud account**: the CSV source path exercises every stage
end to end using sample data in the repo.

### 1. Prerequisites

- Docker Desktop with **≥ 12 GB** allocated to Docker (Settings → Resources).
  The stack runs Spark, Hive, two Postgres instances and Airflow.
- `openssl` on your PATH (macOS and most Linux ship it).
- No Python install needed on the host — everything runs in containers.

### 2. Generate your own TLS certificates

The repo does **not** ship private keys. Generate your own local CA and the
two service certificates:

```bash
./scripts/gen_certs.sh
```

This writes `certs/{ca,idp,crypto}.{crt,key}`. The keys are gitignored. Re-run
with `--force` to regenerate (this invalidates anything trusting the old CA).

### 3. Start the stack

```bash
docker compose --profile core up -d
```

Wait for health: `docker compose ps` should show `pl-airflow`, `pl-idp`,
`pl-crypto`, `pl-postgres` and `pl-spark-master` as `healthy`. First start
pulls and builds images and can take several minutes.

### 4. Run the pipeline end to end

```bash
docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13
```

That is the whole pipeline: one DAG, seven tasks. With no GCP credentials
configured it uses the bundled offline fixture, so this works on a fresh clone.

To run it against a specific CSV instead:

```bash
CSV=/data/csv/BLM_CO_Q2_2026_Oil_and_Gas_Lease_Sale_-2400573660170243848.csv
docker exec -e CSV_SOURCE_PATH=$CSV pl-airflow \
  airflow dags test trips_pipeline 2026-09-13
```

The finished PDF lands in `pipeline-reports/` on your host.

### 5. Open the Airflow UI (optional)

<http://localhost:8085>. The generated admin password:

```bash
docker exec pl-airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

### 6. Verify it actually did the work

Worth running, because several stages fall back silently if misconfigured:

```bash
# Encryption ran on the real Spark cluster (not the pyarrow fallback):
curl -s http://localhost:8080/json/ | grep -o '"name":"pipeline-encrypt"' | head -1

# The Hive table exists and is queryable:
docker exec pl-airflow python -c "
import os; from pathlib import Path; import jaydebeapi
jars=sorted(str(j) for d in os.environ['HIVE_JDBC_CLASSPATH'].split(':') if d
            for j in Path(d).glob('*.jar'))
c=jaydebeapi.connect('org.apache.hive.jdbc.HiveDriver',
                     'jdbc:hive2://hiveserver2:10000',['',''],jars)
cur=c.cursor(); cur.execute('SHOW TABLES IN trips_warehouse'); print(cur.fetchall())"

# Kafka received both topics:
docker exec pl-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic rpos_flat \
  --from-beginning --max-messages 1 --timeout-ms 15000

# No plaintext card number reached the warehouse:
docker exec pl-postgres psql -U pipeline -d analytics -t -c \
  "select card_info_masked from warehouse.blm_co_q2_2026_oil_and_gas_lease_sale limit 3;"
```

---

## Credentials you must supply

**Nothing sensitive is committed to this repository.** Everything below is
generated locally or comes from your own account.

| What | Where it goes | Required? | How to get it |
|---|---|---|---|
| **TLS CA + certs** | `certs/` | **Yes** | `./scripts/gen_certs.sh` — self-signed, local only |
| **GCP service-account key** | `secrets/gcp-sa.json` | Only for live BigQuery | See below |
| **GCP project id** | `GCP_PROJECT_ID` env or `.env` | Only for live BigQuery | The project **ID** string, not the number |
| **Service secrets** | compose defaults | No | Dev fallbacks (`dev-spark-secret`, …) are fine locally, **not** anywhere shared |
| **Postgres password** | compose default | No | `pipeline-dev-password` locally; override via `POSTGRES_PASSWORD` |
| **Airflow admin password** | auto-generated | No | Read it from the container (step 5) |

### If you want live BigQuery

1. In Google Cloud Console → **IAM & Admin → Service Accounts → Create**.
2. Grant **BigQuery Job User** (`roles/bigquery.jobUser`). That is enough — the
   pipeline only runs query jobs, and `bigquery-public-data` is readable by any
   authenticated user. Do not grant broader roles than this.
3. **Keys → Add key → JSON**, download it.
4. Put it at `secrets/gcp-sa.json` and `chmod 600` it. That directory is
   gitignored; **never commit this file** — it bills a real project.
5. Set your project id and switch the extract mode:

```bash
cat > .env <<'ENV'
GCP_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp-sa.json
EXTRACT_MODE=live
ENV
docker compose --profile core up -d airflow
```

The default live source is retail point-of-sale transactions joined to customer
records (`bigquery-public-data.thelook_ecommerce`) — real names, emails and
street addresses, which is what makes it a meaningful test of the encrypt and
mask stages. ~30 MB scanned per run, about **$0.0002**.

Two SQL files ship in `config/bq/`, selected with `BQ_SQL_FILE`:

| File | What it adds |
|---|---|
| `rpos_transactions.sql` | Transactions + customer PII |
| `rpos_transactions_cards.sql` | The same, plus a `card_info` column of **published Visa/Mastercard test PANs** — no BigQuery public dataset carries card numbers, so these are synthesised in SQL and assigned deterministically by transaction id |

To point at a different table entirely, set `BQ_SQL_FILE` to your own query and
move `BQ_DATA_START`/`BQ_DATA_END` to that table's real span — the 4-hour window
maps into that range, and a mismatch lands on an empty day. Your query **must**
use `LIMIT @row_limit`; the extractor refuses one that does not, because the
row cap is enforced in the SQL rather than merely asserted afterwards.

Cost control is already in place — 100 rows per run, capped in SQL and by
`BQ_MAX_BYTES_BILLED` (default 300 MB) — but **you** are paying, so read
`docs/DATASET-CHOICE.md` before pointing it at a large table.

### Rotating or revoking

- TLS: re-run `./scripts/gen_certs.sh --force`, then
  `docker compose --profile core up -d --force-recreate idp crypto airflow`.
- GCP: delete the key in the Cloud Console; the file alone is the credential.

---

## What you must change for your environment

Everything environment-specific is in **one** place. See
`docs/DEPLOYMENT.md#configure-for-your-environment`.

| Setting | Where | Note |
|---|---|---|
| GCP project id | `k8s/base/02-config.yaml` → `GCP_PROJECT_ID` | the ID string, not the number |
| Service-account email | same file | must hold `bigquery.jobUser` + `bigquery.dataViewer` |
| Service-account key | `secrets/gcp-sa.json` | gitignored; never committed |
| BigQuery dataset | same file → `BQ_DATASET` / `BQ_TABLE` | see `docs/DATASET-CHOICE.md` |
| Byte cap | same file → `BQ_MAX_BYTES_BILLED` | **measure first**: `make -f Makefile.k8s cost` |
| Schedule | `k8s/jobs/20-pipeline-cronjob.yaml` → `schedule` | default `0 */4 * * *` |

All passwords and keys are **generated at deploy time** by `k8s/make-secrets.sh`
and applied straight to the cluster. No secret is ever written to a file in this
repository.

---

## Repository layout

```
pipeline/          the stages — extract, transform, mask, load, report
  common/          JWT auth, correlation-id tracing
  extract/         BigQuery client (+ an offline fixture)
  transform/       Spark encrypt; Hive decrypt+mask
  mask/            Ranger policy engine
  load/            Postgres upsert
  report/          PDF with charts
services/          idp/ (JWT issuer) · crypto/ (AES-256-GCM)
config/ranger/     masking policies — real Ranger policy JSON
dags/              Airflow DAGs (asset-driven)
k8s/               Kubernetes manifests
  base/            namespace, quota, config, storage, network policy
  components/      the services
  jobs/            the 4-hourly CronJob
docker/            Dockerfiles
scripts/           run_chain.py, preflight and auth checks
tests/             invariant tests — run these before you ship
docs/              see the table above
```

## Testing

```bash
.venv/bin/python -m pytest tests/ -q          # 27 invariant tests
make -f Makefile.k8s verify                   # in-cluster security checks
```

`tests/test_secret_hygiene.py` fails the suite if a credential is ever
committed, an ignore rule is weakened, or a key file is world-readable.

## Licence and provenance

Data is the public `bigquery-public-data.austin_bikeshare` dataset. No real
customer data is used anywhere in this repository.
