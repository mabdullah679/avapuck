# Runbook

## First run

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

cp .env.local.example .env.local        # set GCP_PROJECT_ID
docker compose --profile core up -d     # idp, crypto, postgres, spark, hive

.venv/bin/python -m pipeline.run_pipeline --date 2026-08-27
open data/reports/trips_report_2026-08-27.pdf
```

## Running under Airflow

```bash
export AIRFLOW_HOME=$PWD/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
.venv/bin/airflow db migrate
.venv/bin/airflow dags reserialize          # register all five before testing

.venv/bin/airflow dags test trips_01_extract 2026-08-27
```

`dags reserialize` matters: `TriggerDagRunOperator` needs the downstream DAG in
the metadata DB, and `dags test` alone does not register siblings.

For the scheduler-driven chain, set `AIRFLOW_CHAIN_WAIT=1` so each stage blocks
on the next. Without a running scheduler leave it unset, or stages poll forever.

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| `IdP refused token: invalid_client` | `CLIENT_SECRET_*` not exported to the task | export it, or set it in compose |
| `REFUSING TO WRITE: sensitive plaintext` | encryption did not apply to a column | check the crypto service is up; **do not bypass this guard** |
| `no Ranger masking policy for [...]` | a sensitive column has no policy | add one to `config/ranger/hive_masking_policies.json` |
| `Spark write failed ... JavaSparkContext` | host JDK is 26; Spark 4 needs 17/21 | submit inside the container: `./pipeline/transform/spark_submit.sh <date>` |
| `UNIQUE constraint failed: dag_run` | a DAG run already exists for that date | `airflow dags delete <dag_id> -y`, or use a different date |
| `decryption failed for field` | key changed, or ciphertext moved between columns | check `CRYPTO_MASTER_KEY` is stable across restarts |

## The key-rotation hazard

If the crypto service restarts **without** `CRYPTO_MASTER_KEY` set, it
generates a new key and every previously written ciphertext becomes permanently
unreadable. Always set it explicitly outside local dev.

## Re-running a date

Safe by design. Every stage is idempotent:

```bash
.venv/bin/python -m pipeline.run_pipeline --date 2026-08-27
```

Re-running the load reports `0 inserted / 100 updated`.

## Cost check

```bash
.venv/bin/python -c "
from datetime import date
from pipeline.extract.bigquery_extract import extractor_from_env
print(extractor_from_env().dry_run_bytes(date(2026,8,27)) / 1048576, 'MiB')"
```

A dry run is free. If this ever prints something large, the partition predicate
has stopped pruning — investigate before running the real query.
