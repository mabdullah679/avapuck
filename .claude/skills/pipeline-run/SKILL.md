---
name: pipeline-run
description: Bring up the BigQuery → Spark → Kafka → Hive → Postgres → PDF stack and run the pipeline end to end. Use when asked to start, run, rebuild, verify or demo this pipeline, to check that a change still produces a PDF and a verdict, or to run the local CI.
---

# Running this pipeline

One DAG, `trips_pipeline`, eight stages and a verdict:
`extract → card_split → encrypt → publish → mask → register → load → report → emit_verdict`.
Each step logs its own start, status and duration to the Airflow UI. The
default path costs nothing and needs no cloud account.

Every `docker compose` invocation needs `--env-file .env.local`.

## 1. Bring the stack up

```bash
./setup.sh --no-run      # first time: creates .env.local, certs, hooks; builds; starts
./setup.sh               # the same, then runs the current hour's slot
./setup.sh --ci          # also Jenkins
```

By hand:

```bash
docker compose --env-file .env.local --profile core up -d --build
docker compose --env-file .env.local --profile core ps
```

Wait until `pl-airflow`, `pl-idp`, `pl-crypto`, `pl-postgres`, `pl-kafka`,
`pl-spark-master`, `pl-kafka-ui` and `pl-watchdog` read `healthy`. Hive has no
healthcheck; `setup.sh` probes it with beeline.

## 2. Run it

```bash
docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13T04:00:00
```

UTC. The slot names the PDF in `pipeline-reports/` and the verdict on
`pipeline_runs`. To force a CSV: `-e CSV_SOURCE_PATH=/data/csv/<file>.csv`.
For live BigQuery, set `GCP_PROJECT_ID` and `EXTRACT_MODE=live` in `.env.local`
and put the key at `secrets/gcp-sa.json`.

## 3. Verify it actually did the work

Degradations are reported, not hidden. Read the verdict rather than assuming:

```bash
# The verdict, and whether it arrived on time:
curl -s http://localhost:8446/status | python3 -m json.tool

# The message itself (Kafka UI: http://localhost:8090, topic pipeline_runs), or:
docker exec pl-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic pipeline_runs --from-beginning \
  --max-messages 1 --timeout-ms 15000 --consumer.config /tmp/admin.properties

# The reference consumer recorded it:
docker exec pl-postgres psql -U pipeline -d analytics -c \
  "select dataset, run_slot, proceed, status, replay from consumer.received_verdicts order by received_at desc limit 3"

# Spark really ran (else the verdict says 'encrypt ran on pyarrow'):
curl -s http://localhost:8080/json/ | grep -c pipeline-encrypt

# Nothing sensitive reached the warehouse:
docker exec pl-postgres psql -U pipeline -d analytics -c "\d warehouse.<table>"
```

Every column holding sensitive data must end `_masked` or `_blind_index`.

## 4. Tests and CI

```bash
make test          # the suite; no stack needed
make ci            # what the pre-push hook and Jenkins run
```

`tests/test_secret_hygiene.py` needs `git`, so run on the host.

## The console and the UIs

<http://localhost:8447> is the console: upload a CSV (uncapped), run it,
and watch runs, verdicts, the Kafka log table, the Spark cluster and the
traffic audit. Login: `CONSOLE_ADMIN_USER` / `CLIENT_SECRET_CONSOLE_ADMIN`
from `.env.local`. Generate a test file with
`python3 scripts/make_test_csv.py --rows 1000 > small.csv`; the 2 GB scale
test procedure is in `docs/RUNBOOK.md` and has not been run yet.

<http://localhost:8085> is Airflow: `AIRFLOW_ADMIN_USER` /
`AIRFLOW_ADMIN_PASSWORD` from `.env.local` (written into Airflow's passwords
file by `docker/airflow-entrypoint.sh`).

## When something fails

| Symptom | Cause |
|---|---|
| compose fails naming a variable | `.env.local` lacks it; `setup.sh` lists all missing |
| verdict reason `encrypt ran on pyarrow` | Spark unreachable: `SPARK_MASTER_URL`, worker `/data` mount, executor Python 3.12 |
| verdict reason `hive table not registered` | HiveServer2 down, or the JDBC jars missing |
| verdict reason `fell back to CSV` | BigQuery failed `BQ_ATTEMPTS` times; the run used `CSV_FALLBACK_PATH` |
| `no longer matches its contract` | the dataset changed shape; `python -m pipeline.metadata.contract accept` if intended |
| `CERTIFICATE_VERIFY_FAILED` | `PIPELINE_CA_BUNDLE` unset, or certs regenerated without recreating containers |
| `0 rows for window …` | `BQ_DATA_START`/`END` do not match the source table's real span |
| watchdog unhealthy | no verdict within interval + grace, or the last one said no; `/status` says which |
| DAG triggered from the UI sits in `up_for_retry` | a task reads `ctx["logical_date"]` directly; use `logical_ts(ctx)` |
| `TopicAuthorizationException` | the principal has no ACL for that topic; that is the design |

Failing closed is the designed behaviour in every one of these. Do not work
around them by disabling a check.
