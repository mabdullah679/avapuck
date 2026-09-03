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

docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13
```

`dags reserialize` matters: `TriggerDagRunOperator` needs the downstream DAG in
the metadata DB. With one DAG this is no longer a concern -- a single
`dags test` runs all seven tasks in order.

For the scheduler-driven chain, set `AIRFLOW_CHAIN_WAIT=1` so each stage blocks
on the next. Without a running scheduler leave it unset, or stages poll forever.

## Ingesting a different CSV

Any CSV works; the pipeline infers its schema, sensitivity and key.

```bash
.venv/bin/python -m pipeline.run_pipeline --date 2026-09-02 --csv path/to/export.csv
```

Or set `CSV_SOURCE_PATH` (the Airflow path uses this too — it takes precedence
over `EXTRACT_MODE=live`, so a named file is never silently replaced by a
billable BigQuery query):

```bash
export CSV_SOURCE_PATH=$PWD/data/incoming/export.csv
```

**Check what it inferred before trusting the run.** The manifest is written
beside the landed CSV and records a reason for every column:

```bash
.venv/bin/python -c "
from pathlib import Path
from pipeline.metadata.schema import load_manifest
m = load_manifest(Path('data/csv/<dataset>_2026-09-02.csv'))
print('key:', m.primary_key, '(synthetic)' if m.key_is_synthetic else '')
for c in m.columns:
    print(f'  {\"SENSITIVE\" if c.sensitive else \"public   \"} {c.name:28} {c.reason}')"
```

Correct a wrong guess in `config/datasets/<dataset>.json` — this is a config
edit, not a code change:

```json
{
  "primary_key": "objectid",
  "columns": {
    "operator_name": {"sensitivity": "direct_identifier", "mask": "MASK_HASH"},
    "county_des":    {"sensitivity": "public"}
  }
}
```

Then re-run. A dataset's Ranger policy is generated once into
`config/ranger/<dataset>_masking_policies.json` and never regenerated, so
**delete that file** after changing sensitivity, or edit its mask types
directly — whichever you edit is what enforces.

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
