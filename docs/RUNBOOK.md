# Runbook

Operating the stack, and every failure actually hit. `docs/DECISIONS.md` says
why things are shaped this way; this file says what to do.

## First run

```bash
./setup.sh
```

That creates `.env.local`, generates certificates, installs the git hooks,
starts everything, creates Kafka topics and ACLs, and runs one slot. See the
README for what it prints and where.

Every `docker compose` command needs the env file:

```bash
docker compose --env-file .env.local --profile core ps
docker compose --env-file .env.local --profile core logs --tail 50 airflow
```

`make up` / `make down` wrap that.

## Running a slot

```bash
docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13T04:00:00
```

All times are UTC. The slot is the start of the four-hour interval; the PDF is
named by it (`trips_report_20260913T0400Z.pdf`) and the verdict on the control
topic carries it. A re-run of the same slot replaces its rows and publishes a
new verdict with the same `idempotency_key` and a new `event_id`.

Without Airflow, walking the same stage list:

```bash
.venv/bin/python -m pipeline.run_pipeline --slot 2026-09-13T04:00Z
.venv/bin/python -m pipeline.run_pipeline --stage encrypt        # one stage
.venv/bin/python -m pipeline.run_pipeline --no-verdict           # skip Kafka
```

## Reading the verdict

Three places show the same answer:

- **Kafka UI** <http://localhost:8090>, topic `pipeline_runs`. One message per
  run; `payload.proceed`, `payload.status`, `payload.reasons`.
- **Watchdog** <http://localhost:8446/status>. Last verdict per dataset and
  whether it is on time.
- **The PDF**, "Run status" section: the same status and reasons as of the
  report stage.

`proceed=true` with `status=degraded` means the data is correct and something
worth knowing happened: BigQuery fell back to CSV, Spark fell back to pyarrow,
the Hive table did not register, the delivery copy failed. Each is a reason.

## Ingesting a different CSV

Any CSV works; the pipeline infers its schema, sensitivity and key.

```bash
docker exec -e CSV_SOURCE_PATH=/data/csv/export.csv \
  pl-airflow airflow dags test trips_pipeline 2026-09-13T04:00:00
```

`CSV_SOURCE_PATH` takes precedence over `EXTRACT_MODE=live`, so a named file
is never silently replaced by a billable BigQuery query.

**The first run writes the dataset's contract** to
`config/datasets/<dataset>.json` under `schema`, marked `_generated`. Review
it: every column's inferred type and sensitivity is there. Correct a wrong
guess in the same file's `columns` overrides, then accept the new shape:

```json
{
  "primary_key": "objectid",
  "event_time_column": "sale_date",
  "columns": {
    "operator_name": {"sensitivity": "direct_identifier", "mask": "MASK_HASH"},
    "county_des":    {"sensitivity": "public"}
  },
  "schema": { "...generated; do not edit by hand unless you mean it..." }
}
```

```bash
.venv/bin/python -m pipeline.metadata.contract accept <dataset> data/csv/<dataset>_<date>.csv
```

That rewrites the `schema` block from current inference, prints what changed,
and marks it accepted. Commit it; the platform CI tier checks it.

A later export whose columns, types, sensitivity or key no longer match
**stops the extract stage** with the differences listed. That is the gate
working. Accept or fix, never bypass.

`event_time_column` is optional. When set, the PDF's data window is the
earliest and latest value of that column in the landed rows.

A dataset's Ranger policy is generated once into
`config/ranger/<dataset>_masking_policies.json` and never regenerated. After
changing sensitivity, edit its mask types, or delete the file to regenerate.

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| `docker compose` fails naming a variable | `.env.local` lacks it | copy the line from `.env.example`; `setup.sh` lists all missing at once |
| `IdP refused token: invalid_client` | a `CLIENT_SECRET_*` differs between the IdP and the caller | both read `.env.local`; recreate the containers after editing it |
| `REFUSING TO WRITE: sensitive plaintext` | encryption did not apply to a column | check the crypto service is up; **do not bypass this guard** |
| `no Ranger masking policy for [...]` | a sensitive column has no policy | policy is appended on the next run; review the mask types |
| `dataset 'x' no longer matches its contract` | the export changed shape | read the differences; `contract accept` if intended |
| `stage event violated its contract` | a stage's returned dict lost a field | fix the stage; `contracts/stage_event.schema.json` says what is required |
| `verdict carries no signature` / `digest does not match` | a consumer received an unsigned or altered verdict | it was refused, correctly; check who wrote to `pipeline_runs` |
| `decryption failed for field` | key changed, or ciphertext moved between columns | check `CRYPTO_MASTER_KEY` is stable across restarts |
| watchdog unhealthy: `no verdict seen since` | the DAG did not run within interval + grace | check the scheduler; `docker compose logs airflow` |
| watchdog unhealthy: `said proceed=false` | the last run failed | the reasons are in the same line; fix and re-run the slot |
| `UNIQUE constraint failed: dag_run` | a DAG run already exists for that slot | `airflow dags delete`, or use another slot |
| `TopicAuthorizationException` | the principal has no ACL for that topic | that is the design: `pipeline` writes only; `consumer`/`ui` read flat + control; `watchdog` reads control |
| Kafka CLI hangs then disconnects | missing `--command-config` | the broker requires SASL; use `/tmp/admin.properties` inside `pl-kafka` |

Failing closed is the designed behaviour in every one of these. Do not work
around them by disabling a check.

## The key-rotation hazard

If the crypto service restarts **without** `CRYPTO_MASTER_KEY` set, it
generates a new key and every previously written ciphertext becomes
permanently unreadable. The same applies to `IDP_SIGNING_KEY` and previously
signed verdicts. Set both in `.env.local` before keeping any data.

## Jenkins

```bash
./setup.sh --ci
```

<http://localhost:8095>, login from `JENKINS_ADMIN_*` in `.env.local`. One job,
`avapuck`, seeded from `docker/jenkins/casc.yaml`: service tier
(`make ci-service`), then platform tier (`make ci-platform`). It polls the repo
every two minutes; the `post-commit` hook makes it poll now. The `pre-push`
hook runs `make ci` locally and refuses a failing push; `SKIP_CI=1 git push`
bypasses once, and Jenkins still goes red.

A red platform tier lists every contract that no longer holds after a change.
That list is the blast radius, shown before merge.

## Cost check

```bash
.venv/bin/python -c "
from datetime import date
from pipeline.extract.bigquery_extract import extractor_from_env
print(extractor_from_env().dry_run_bytes(date(2026,8,27)) / 1048576, 'MiB')"
```

A dry run is free. If this ever prints something large, the column list has
grown — investigate before running the real query.
