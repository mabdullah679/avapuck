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

## Re-running setup

`./setup.sh` is safe to run again at any time. Docker caches the builds and
pulls; certificates are generated once; `--merge-env` appends variables a
newer template added without touching your values; the warehouse schema is
applied idempotently on every run; both DAGs are unpaused. Data volumes are
never rebuilt except by `--clean`. A paused Docker Desktop is restarted for
you.

## The console

<http://localhost:8447>. On this machine it signs you in by itself
(`CONSOLE_DEV_AUTOLOGIN=true`): the console logs in to the IdP with
`CLIENT_SECRET_CONSOLE_ADMIN` on your behalf and verifies the token like any
typed login. Sessions last 30 days. Turn auto-login off anywhere the console
is reachable from outside; then sign in with `CONSOLE_ADMIN_USER` and that
secret, and the browser will offer to remember it.

| Tab | What it does |
|---|---|
| Upload & run | Drop or pick a CSV. Choose a pipeline: the dropdown lists every DAG in Airflow, mother and child, with presets for all, responding 200, and connected. Name the dataset if the file name is not it. Run starts one fresh run of that pipeline with the file, uncapped unless you set a cap. Shows the scheduled source and why BigQuery is not a toggle here. |
| Runs | Recent DAG runs from Airflow joined to their verdicts from the Kafka log. |
| Kafka log | `monitor.kafka_log`, every flat and control message the tap scraped, filterable by event type, dataset and slot. |
| Spark cluster | Each worker from the master's own status, colour-coded healthy / warning / unhealthy, cores and memory used, the scaler's last decisions, and a manual target. |
| Traffic | This console's request rates per category against the profile, blocked clients, recent alerts. |

Uploads land in `data/incoming/` on the host, which is `/data/incoming` for
Airflow. Each run's conf records the file, the dataset, the cap and who
started it; the extract stage event says `source: upload`.

The dataset name defaults to the file name. If that is a dataset the
schedule already ingests, the upload is held to that dataset's committed
contract, exactly as a scheduled run would be, and a file with different
columns stops at extract with the differences listed (see "Ingesting a
different CSV" below). Either name the dataset differently in the form, or
accept the new contract first. A run that fails this way shows the sentence
in the Runs tab's reasons column within seconds; Airflow does not retry it.

**Connected** means: the pipeline's last run succeeded within the watchdog's
window (interval + grace) and, for a mother pipeline, the watchdog has seen a
positive verdict in that window. It is the watchdog's own loop read from the
console, not a second heartbeat (`docs/DECISIONS.md` D22).

## Running a slot by hand

```bash
docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13T04:00:00
.venv/bin/python -m pipeline.run_pipeline --slot 2026-09-13T04:00Z
.venv/bin/python -m pipeline.run_pipeline --csv path/to/file.csv --dataset orders   # uncapped
```

All times are UTC. The slot is the start of the four-hour interval; the PDF is
named by it and the verdict on the control topic carries it. A re-run of the
same slot replaces its rows and publishes a new verdict with the same
`idempotency_key` and a new `event_id`.

## Reading the verdict

- **Console**, Runs tab: state and verdict side by side.
- **Kafka UI** <http://localhost:8090>, topic `pipeline_runs`.
- **Watchdog** <http://localhost:8446/status>: last verdict per dataset and
  whether it is on time.
- **The PDF**, "Run status" section.

`proceed=true` with `status=degraded` means the data is correct and something
worth knowing happened: BigQuery fell back to CSV, Spark fell back to pyarrow,
the Hive table did not register, the delivery copy failed. Each is a reason.

## The scale test: small file first, then 2 GB

Do not start with 2 GB. Confirm the path with a small file, then a medium
one, then the big one, and watch three places each time: the Runs tab, the
Spark tab, and `docker stats`.

```bash
python3 scripts/make_test_csv.py --rows 1000    > small.csv    # ~120 KB
python3 scripts/make_test_csv.py --rows 200000  > medium.csv   # ~24 MB
python3 scripts/make_test_csv.py --gigabytes 2  > big.csv      # ~18 M rows
```

Upload `small.csv` through the console and run it. Expect: a verdict with
`proceed=true`, a PDF, one Spark application on the master, every row on
`rpos_flat`, and a `card_info` split because the generated data carries
Luhn-valid test cards. Then `medium.csv`: watch the Spark tab, and expect the
scaler to log `hold` or `up` with a reason. Only then `big.csv`.

What the 2 GB run exercises, and what it does not:

| Stage | At 2 GB |
|---|---|
| extract | streams the file into `data/csv`, infers the schema from the first 500 rows, counts every row; minutes |
| card split | one crypto call per chunk of `PIPELINE_BATCH_ROWS`, on the driver |
| encrypt | Spark reads the CSV; **executors** call the crypto service per chunk in parallel; the single-file Parquet write at the end is one writer |
| publish | every row, two messages each, on one broker; the slowest stage by far, by decision (D16) |
| mask | per chunk on the driver: decrypt, mask, append |
| load | `execute_values` in chunks |
| report | SQL aggregates; the histogram and categories over millions of rows |

Set `PIPELINE_BATCH_ROWS` higher (20000) for the big run and lower (1000) if
the crypto service shows memory pressure. Give Kafka disk: every row is
retained. If the run is too slow to be useful, the honest levers are more
crypto workers (`--workers` in the crypto service command) and more Spark
workers (`SPARK_WORKERS_MAX`).

**None of this has been run yet.** TRUST-BOUNDARY §0 says so and lists what
the first run should confirm.

## The scaler

<http://localhost:8448/status>, or the console's Spark tab. Polls the master
every `SCALER_POLL_SECONDS`. Up when an app is waiting or utilisation is at or
above `SCALER_UP_THRESHOLD`; down when idle below `SCALER_DOWN_THRESHOLD`
and there is a worker it created to remove; never below `SPARK_WORKERS_MIN`,
never above `_MAX`, never a compose-managed worker, and a cooldown between
actions. `SCALER_ENABLED=false` keeps it observing and recommending without
acting. A manual target from the console overrides the rule until reached.

It holds the Docker socket. That is root on the host; nothing else has it.
Scaled workers are labelled `avapuck.scaled=true` and carry the compose
project label, so `./setup.sh --down` removes them.

## The traffic audit

`config/traffic_profile.json` is the written expectation, per category:
requests per client per window and in total. Over the per-client limit, that
client is blocked for `TRAFFIC_BLOCK_SECONDS`. Over the total, the whole
category is refused to non-allowlisted clients until the window drains. The
console allowlists nothing. The IdP allowlists the compose network through
`TRAFFIC_ALLOWLIST_INTERNAL`, so the pipeline's own token requests are never
blocked.

The numbers are a first guess. Tune them from the console's Traffic tab: if
a legitimate user is blocked, raise the limit for that category and restart
the console. Set `TRAFFIC_AUDIT=off` on a service to disable its audit; do
not do that to make a demo look calm.

## Ingesting a different CSV on the schedule

`CSV_SOURCE_PATH` in `.env.local` makes the *scheduled* run read a file
instead of BigQuery or the fixture, capped at `BQ_ROW_LIMIT`. Uploads through
the console are the uncapped path.

**The first run writes the dataset's contract** to
`config/datasets/<dataset>.json` under `schema`. Correct a wrong guess in the
same file's `columns` overrides, then accept:

```bash
.venv/bin/python -m pipeline.metadata.contract accept <dataset> data/csv/<dataset>_<date>.csv
```

A later export whose columns, types, sensitivity or key no longer match
**stops the extract stage** with the differences listed. Accept or fix, never
bypass. `event_time_column` is optional and drives the PDF's data window.

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| `docker compose` fails naming a variable | `.env.local` lacks it | copy the line from `.env.example`; `setup.sh` lists all missing at once |
| console: `invalid credentials` | the password is not `CLIENT_SECRET_CONSOLE_ADMIN` | it is the IdP that decides; check `.env.local` and recreate `idp` |
| console: `Airflow refused the console's credentials` | `AIRFLOW_ADMIN_PASSWORD` differs between airflow and console | both read `.env.local`; recreate both |
| console: 429 `refused: over the per-client limit` | you tripped the traffic profile | wait `TRAFFIC_BLOCK_SECONDS`, or raise the category's limit |
| `REFUSING TO WRITE: sensitive plaintext` | encryption did not apply to a chunk | check the crypto service; **do not bypass this guard** |
| `extract failed: dataset 'x' no longer matches its contract` (Runs tab, watchdog, task log) | the export changed shape, or an upload reused a scheduled dataset's name | read the differences; `contract accept` if intended, or another dataset name; not retried |
| A run shows `success` in Airflow and the console beside a verdict that says `failed` | Airflow's run state follows its last task, and `emit_verdict` succeeds whenever a verdict was published | read the verdict, not the run colour; the reasons name the stage that raised |
| Spark tab shows a worker `unhealthy` | the master lost its heartbeat | `docker compose ps`; the scaler will add one if below min |
| scaler `socket unreachable` | the socket mount is missing or the daemon refused | check `/var/run/docker.sock` on the host; `SCALER_ENABLED=false` to stop trying |
| watchdog unhealthy: `no verdict seen since` | the DAG did not run within interval + grace | check the scheduler; `docker compose logs airflow` |
| watchdog unhealthy: `said proceed=false` | the last run failed | the reasons are in the same line; fix and re-run the slot |
| `TopicAuthorizationException` | the principal has no ACL for that topic | that is the design; see `docker/kafka-init.sh` |
| tap `db_error` | Postgres unreachable from the tap | `docker compose logs tap`; rows resume from the earliest offset |

Failing closed is the designed behaviour in every one of these. Do not work
around them by disabling a check.

## The key-rotation hazard

If the crypto service restarts **without** `CRYPTO_MASTER_KEY` set, every
previously written ciphertext becomes permanently unreadable. The same applies
to `IDP_SIGNING_KEY` and previously signed verdicts, and to
`CONSOLE_SESSION_SECRET` and everyone's session. Set them in `.env.local`
before keeping any data.

## Jenkins

```bash
./setup.sh --ci
```

<http://localhost:8095>. One job, `avapuck`: `make ci-service`, then
`make ci-platform`. The `pre-push` hook runs `make ci` locally and refuses a
failing push; `SKIP_CI=1 git push` bypasses once, and Jenkins still goes red.
