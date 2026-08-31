# Trips Data Pipeline

A daily-batch data pipeline that pulls from a Google BigQuery public dataset,
encrypts sensitive fields with AES-256-GCM through an HTTPS service, masks them
under Apache Ranger policy at the Hive boundary, loads a Postgres warehouse, and
emits a PDF report with charts.

Runs on Kubernetes (minikube) or locally with Docker Compose.

```
BigQuery ──▶ CSV ──▶ [encrypt] ──▶ Parquet ──▶ [decrypt+mask] ──▶ Hive
                                                                    │
                                          PDF ◀── Postgres ◀────────┘
```

---

## ⚠️ Read this before you start

**`TRUST-BOUNDARY.md` is not an appendix.** It states exactly what is
production-grade, what is stubbed, and what has never been verified. Two
examples that will matter to you on day one:

- **TLS is not enabled.** Services speak plain HTTP inside the cluster. The
  "HTTPS crypto service" is HTTP today.
- **Apache Ranger's admin service is not deployed.** Masking uses genuine Ranger
  policy JSON, applied by a local engine implementing Ranger's semantics. Every
  masked row records `masked_by` so you can always tell which ran.

Do not present anything from this repository as production-ready without
reading that file first.

---

## For AI coding agents

If you are an agent working on this repository, read these in order **before
changing anything**:

| Read | For |
|---|---|
| `docs/AGENTS.md` | **Start here.** Working agreement, invariants you must not break, where things live |
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

## Quick start — Kubernetes (recommended)

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

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
docker compose --profile core up -d
.venv/bin/python scripts/run_chain.py --slot 2026-08-31T12:00
open data/reports/*.pdf
```

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
