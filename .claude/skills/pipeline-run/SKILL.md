---
name: pipeline-run
description: Bring up the BigQuery → Spark → Hive → Postgres → PDF stack and run the pipeline end to end. Use when asked to start, run, rebuild, verify or demo this pipeline, or to check that a change still produces a PDF.
---

# Running this pipeline

One DAG, `trips_pipeline`, eight tasks:
`extract → card_split → encrypt → publish → mask → register → load → report`.
Each step logs its own start, status and duration to the Airflow UI.

The default path costs nothing and needs no cloud account.

## 1. Certificates (first time only)

No private keys are committed. Generate a local CA and the two service certs:

```bash
./scripts/gen_certs.sh
```

Refuses to overwrite; pass `--force` to regenerate (invalidates anything
trusting the old CA).

## 2. Bring the stack up

```bash
docker compose --profile core up -d
```

Twelve containers (including a Kafka broker and a one-shot topic creator).
First run builds three images and pulls Hive/Spark/Postgres/Kafka —
allow 10–20 minutes and ≥12 GB of Docker memory. Wait for health:

```bash
docker compose ps          # pl-airflow, pl-idp, pl-crypto, pl-postgres,
                           # pl-spark-master should read (healthy)
```

`pl-hive-metastore` and `pl-hiveserver2` have no healthcheck; confirm with
`nc -z 127.0.0.1 10000`.

## 3. Run it

```bash
docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13
```

The PDF lands in `pipeline-reports/` on the host.

**Offline (no GCP account)** — the default when `GCP_PROJECT_ID` is unset.
To force a specific CSV:

```bash
docker exec -e CSV_SOURCE_PATH=/data/csv/<file>.csv \
  pl-airflow airflow dags test trips_pipeline 2026-09-13
```

**Live BigQuery** — put a service-account key (role: BigQuery Job User) at
`secrets/gcp-sa.json`, then:

```bash
printf 'GCP_PROJECT_ID=your-project\nEXTRACT_MODE=live\n' > .env
docker compose --profile core up -d airflow
```

~30 MB scanned per run, about $0.0002. Capped by `BQ_MAX_BYTES_BILLED`.

## 4. Verify it actually did the work

Several stages degrade silently but *report* it. Check, don't assume:

```bash
# Spark really ran (else engine=pyarrow):
curl -s http://localhost:8080/json/ | grep -c pipeline-encrypt

# Hive table is real and queryable:
docker exec pl-airflow python -c "
import os; from pathlib import Path; import jaydebeapi
jars=sorted(str(j) for d in os.environ['HIVE_JDBC_CLASSPATH'].split(':') if d
            for j in Path(d).glob('*.jar'))
c=jaydebeapi.connect('org.apache.hive.jdbc.HiveDriver',
                     'jdbc:hive2://hiveserver2:10000',['',''],jars)
cur=c.cursor(); cur.execute('SHOW TABLES IN trips_warehouse'); print(cur.fetchall())"

# Kafka got both topics (encrypted + flat):
docker exec pl-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic rpos_encrypted

# Nothing sensitive reached the warehouse:
docker exec pl-postgres psql -U pipeline -d analytics -c "\d warehouse.<table>"
```

Every column holding sensitive data must end `_masked` or `_blind_index`.

## 5. Tests

```bash
docker run --rm --entrypoint python -v "$PWD:/w" -w /w \
  -e PIPELINE_DATA_ROOT=/w/data -e PYTHONPATH=/w \
  avapuck-airflow -m pytest tests/ -q
```

63 pass. `tests/test_secret_hygiene.py` needs `git`, so run it on the host.

## Airflow UI

<http://localhost:8085>, user `admin`. Password:

```bash
docker exec pl-airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Regenerated whenever that file is lost — re-read it rather than assuming.

## When something fails

| Symptom | Cause |
|---|---|
| `engine=pyarrow` in the trace | Spark unreachable — check `SPARK_MASTER_URL`, worker `/data` mount, executor Python 3.12 |
| `registered=false` | HiveServer2 down, or the JDBC jars missing |
| `CERTIFICATE_VERIFY_FAILED` | `PIPELINE_CA_BUNDLE` unset, or certs regenerated without recreating containers |
| `0 rows for window …` | `BQ_DATA_START`/`END` do not match the source table's real span |
| `no Ranger masking policy for [...]` | A new sensitive column — policy is appended on the next run; re-run the stage |
| `fell_back_to_csv=True` in the trace | BigQuery failed `BQ_ATTEMPTS` times; the run used `CSV_FALLBACK_PATH` and did **not** read BigQuery |
| `NoBrokersAvailable` | Kafka not up, or `KAFKA_BOOTSTRAP` wrong — topics are not auto-created, so a typo fails rather than making a third topic |

Failing closed is the designed behaviour in every one of these. Do not work
around them by disabling a check.
