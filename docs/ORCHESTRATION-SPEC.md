# Orchestration spec

Answers to the orchestration questions, as the code actually is on `main` —
not as it is aspirationally described. Where something is a stand-in or a
known limitation it is marked, and `TRUST-BOUNDARY.md` carries the longer
version. `docs/DECISIONS.md` carries the reasoning.

---

## 1. Orchestrator and version

**Apache Airflow 3.0.2** — not 2.x. This matters more than a version bump
usually does:

- Both DAGs are written against `airflow.sdk` (`from airflow.sdk import DAG,
  task, Asset, AssetWatcher`), the Airflow 3 Task SDK. Airflow 2 imports are
  not used and will not be a drop-in swap.
- `logical_date` is **not** guaranteed in the task context on Airflow 3. A run
  triggered from the UI does not supply it, while `airflow dags test` does.
  `logical_ts(ctx)` resolves it through `logical_date → data_interval_start →
  dag_run.logical_date → dag_run.run_after → now`, always as aware UTC. A
  test asserts no task reads `ctx["logical_date"]` directly.
- The consumer DAG uses Airflow 3's **event-driven scheduling**: an `Asset`
  with an `AssetWatcher` whose trigger is the common-messaging
  `MessageQueueTrigger` backed by the Kafka provider.

Pinned in `requirements.txt`:

```
apache-airflow==3.0.2
apache-airflow-task-sdk==1.0.2
apache-airflow-providers-standard==1.2.0
apache-airflow-providers-apache-kafka==1.16.0
apache-airflow-providers-common-messaging==2.0.4
confluent-kafka==2.15.0          # the provider's client (trigger side)
kafka-python==3.0.11             # the publisher's client (producer side)
```

Two Kafka clients, for two roles: the provider requires confluent-kafka; the
publisher keeps kafka-python, which since 3.x supports the idempotent
producer. `kafka-python-ng` was replaced because it did not.

**Executor:** LocalExecutor, under `airflow standalone` (api-server +
scheduler + triggerer + dag-processor in one container). A development
topology.

**Schedule:** `0 */4 * * *`, `catchup=False`, `max_active_runs=1`,
`dagrun_timeout=3h`.

---

## 2. How Spark jobs get submitted

**Neither `spark-submit` nor a managed service.** The Airflow task process *is*
the Spark driver: it builds a `SparkSession` in-process and connects to a
standalone Spark 4.0.0 cluster over `spark://spark-master:7077`.

Three constraints that fail *by degrading* (the stage falls back to pyarrow,
reports `engine: pyarrow`, and the verdict names it as a reason):

1. **A JVM must exist in the Airflow image.** Java 21.
2. **Executor Python must match the driver's minor version** (3.12), chosen
   by `spark.pyspark.python` from the driver side.
3. **`/data` must be mounted at the same path on the workers.**

**Fallback is deliberate.** With `SPARK_MASTER_URL` unset the stage runs the
identical transformation through pyarrow and the verdict is `complete`, not
`degraded`, because no Spark was configured. **Never assume which ran; read
the verdict.**

**Executors do the crypto.** The Spark path reads the CSV on the cluster and
runs `encrypt_rows` inside `mapPartitions`; each executor builds its own
crypto client from the worker container's environment (`CLIENT_SECRET_SPARK_JOB`,
`CRYPTO_URL`, the CA) and calls the crypto service one chunk at a time. The
driver only promotes the single Parquet part file at the end. This is what
makes an uncapped upload (D16) spread its heaviest work across workers, and
what the scaler (D17) grows workers for.

---

## 3. Task granularity

**One Airflow task per pipeline stage, with the engine inside the task**, plus
one terminal task for the verdict. Every task is a thin `@task` wrapper that
calls `pipeline.stages.run_stage(name, env, events)`. The task owns
orchestration (the run slot, XCom, UI logging); `pipeline/stages.py` owns
which module does the work with which inputs; the module owns the work and
has no Airflow import. `pipeline/run_pipeline.py` walks the same list.

---

## 4. The actual shape

**Two DAGs.**

`trips_pipeline`: nine tasks, strictly linear, the last one always runs.

```
extract_to_csv
     ↓
split_card_numbers
     ↓
encrypt_to_parquet          ← the only Spark stage
     ↓
publish_kafka               ← rows to two topics, in the platform envelope
     ↓
decrypt_and_mask
     ↓
register_hive_table         ← non-fatal; reported
     ↓
load_postgres               ← stamped with run_slot
     ↓
build_pdf                   ← one per run
     ↓
emit_verdict                ← trigger_rule=all_done; signed; to pipeline_runs
```

`on_run_verdict`: one task, scheduled on the `pipeline_run_verdicts` asset.
Wakes when the control topic carries a contract-valid `run.verdict` with
`proceed=true`; verifies the signature, dedupes, records, stops.

| Stage | Module | Event fields the verdict reads |
|---|---|---|
| `extract` | `pipeline/extract/fixture.py`, `metadata/contract.py` | `rows`, `mode`, `bq_attempts`, `fell_back_to_csv`, `window_*` |
| `card_split` | `pipeline/transform/card_split.py` | `rows`, `cards` |
| `encrypt` | `pipeline/transform/spark_encrypt.py` | `rows`, `engine`, `spark_configured` |
| `publish` | `pipeline/publish/kafka_publish.py` | `rows`, `topic_encrypted`, `topic_public` |
| `mask` | `pipeline/transform/hive_mask.py` | `rows`, `masked_by`, `policies` |
| `hive_register` | `pipeline/transform/hive_register.py` | `registered`, `table`, `reason` |
| `load` | `pipeline/load/postgres_load.py` | `inserted`, `updated`, `dataset` |
| `report` | `pipeline/report/pdf_report.py` | `pdf`, `collected` |

The shape of each event is `contracts/stage_event.schema.json`.

**Not a medallion.** The layering is by *sensitivity state*: plaintext
(transient, local) → ciphertext → masked → warehouse.

**No fan-out by source.** One dataset per run, named by the manifest, which
namespaces every output path, Hive table, Postgres table and Kafka envelope.

**Uploads are runs of the same DAG.** The console triggers `trips_pipeline`
through Airflow's REST API with `conf = {csv_path, dataset, row_limit,
source: "upload", uploaded_by}`. `run_env(ctx)` in the DAG turns conf into a
`RunEnv`; `pipeline/stages.py :: _source_from_conf` points the extractor at
the file for the duration of the extract stage. The extract event records
`source: upload`. `max_active_runs=1`, so an upload queues behind a scheduled
run rather than racing it.

**Everything is metadata-driven, and the metadata is a contract.** No stage
names a column literally; the first run of a dataset commits its inferred
schema and later runs are held to it.

---

## 5. Data handoff between tasks

**Files on a shared volume, addressed by date-partitioned convention. XCom
carries the stage event only — never data.**

```
data/csv/<dataset>_<YYYY-MM-DD>.csv                          extract  →  card_split, encrypt
data/cards/<dataset>_card_info_<YYYY-MM-DD>.csv              card_split (terminal)
data/parquet/dt=<YYYY-MM-DD>/<dataset>.parquet               encrypt  →  publish, mask
data/hive/<dataset>/dt=<YYYY-MM-DD>/<dataset>_masked.parquet mask     →  register, load, report
data/reports/<dataset>_report_<YYYYMMDDTHHMM>Z.pdf           report (terminal)
pipeline-reports/<same>.pdf                                  delivery copy
```

A downstream task locates its input with a glob on the run's date
(`find_input(...)`) and verifies it exists, rather than trusting the previous
task. Six slots a day share the intermediate file paths; the warehouse and
the PDF distinguish slots by `run_slot`.

**Between systems**, the handoff is the Kafka envelope: `event_id`,
`idempotency_key`, `run_slot`, `trace_id`, `dataset`, `key`, and a payload
whose schema the `event_type` selects. `contracts/`.

**Not Delta Lake, not S3/GCS.** Plain Parquet with a hidden
`.<name>.schema.json` sidecar (the dot keeps Hive from reading it), on a
local bind mount shared across containers at the same absolute path.

---

## 6. Where the code lives

```
dags/
  trips_pipeline_dag.py        the run; thin tasks over pipeline/stages.py
  verdict_listener_dag.py      the reference consumer
pipeline/
  stages.py     THE stage list; RunEnv; run_stage; emit_verdict
  verdict.py    the verdict: pure, tested hardest
  messaging/    envelope.py, contracts.py, dedupe.py, signing.py, listen.py
  common/       auth.py (JWT), trace.py (correlation id + UI logging)
  metadata/     schema.py (inference), contract.py (dataset contracts), policy_gen.py
  extract/      bigquery_extract.py, csv_source.py, fixture.py
  transform/    card_split.py, spark_encrypt.py, hive_mask.py, hive_register.py
  publish/      kafka_publish.py (rows + verdict)
  load/         postgres_load.py
  mask/         ranger.py
  report/       pdf_report.py, profile.py, collect.py
  run_pipeline.py   the same stages, no Airflow
services/       idp/ (JWT issuer + verdict signer) · crypto/ · watchdog/
contracts/      *.schema.json + examples/
config/         bq/ · datasets/ (contracts + overrides) · ranger/
docker/         Dockerfiles · kafka-entrypoint.sh · kafka-init.sh · jenkins/
scripts/        gen_certs.sh · ci/contracts_check.py · git-hooks/
tests/          make test
Jenkinsfile     make ci-service, then make ci-platform
```

**How the DAG reaches the job code:** a plain Python import. The repo is
bind-mounted into the Airflow container at `/opt/pipeline` with
`PYTHONPATH=/opt/pipeline`. Task-level imports keep DAG parse time low.

---

## 7. Local dev story

```bash
git clone https://github.com/mabdullah679/avapuck.git
cd avapuck
./setup.sh
```

Docker is the only host prerequisite. `setup.sh` creates `.env.local` from
`.env.example`, generates certificates, installs the git hooks, starts the
stack, creates topics and ACLs, runs the current slot, and reports the PDF
and the watchdog's view.

**What runs locally:** seventeen containers on the `core` profile — Airflow,
IdP, crypto, Postgres, Kafka (KRaft, SASL + ACLs), Kafka UI, tap, watchdog,
scaler, console, Spark master + `SPARK_WORKERS_MIN` workers, Hive metastore +
its DB, HiveServer2 — plus a one-shot `kafka-init`. The scaler adds workers
up to `SPARK_WORKERS_MAX` under load. `--profile ci` adds Jenkins;
`--profile ranger` adds Ranger admin. Measured working set of the original
eleven-container stack was 4.2 GB; the additions are small except Kafka,
whose heap grew to 1 GB for uncapped uploads. Give Docker 8 GB, more for
the 2 GB scale test.

```
services/
  console/    upload, run, runs+verdicts, Kafka log, Spark cluster, traffic   :8447
  idp/        JWT issuer, verdict signer, console login client               :8443
  crypto/     AES-256-GCM, 4 uvicorn workers                                  :8444
  watchdog/   absence detection                                              :8446
  tap/        flat + control topics -> monitor.kafka_log                     :8449
  scaler/     spark-worker autoscaling; holds the Docker socket               :8448
```

**Configuration:** `.env.local` and nothing else. Compose substitutes
`${VAR:?}` for every value; a missing one fails `up` by name.

**What is genuinely real locally**, verified rather than assumed
(`TRUST-BOUNDARY.md` §6): encryption on the Spark cluster; Hive tables
through Tez; Kafka SASL and ACLs for `pipeline` and `consumer`; TLS
verification enforced; the IdP signing and verifying verdicts.

**What is a stand-in or unverified locally** — full list in
`TRUST-BOUNDARY.md` §0 and §9: Ranger admin not deployed; the IdP is a local
FastAPI service; the CA is self-signed; Kafka is single-node SASL_PLAINTEXT;
BigQuery is optional; **the consumer DAG, watchdog, Kafka UI and Jenkins were
built without Docker running and have not been exercised against the stack.**

**Without Airflow at all:** `python -m pipeline.run_pipeline --slot …`.
`make test` needs no stack.

---

## Known limitations an agent should not design around

| Area | Status |
|---|---|
| Airflow topology | `airflow standalone` + LocalExecutor. No HA. |
| Scale | 100 rows per BigQuery run, an invariant. Uploads uncapped (D16) and every stage streams; **nothing above 100 rows has been run yet** (TRUST-BOUNDARY 4.4). |
| Docker socket | Mounted into `scaler` only. Root-equivalent (D17). |
| Repair semantics | Re-running `mask` alone does **not** cascade to `load`. Clear downstream tasks too. The asset cascade exists only at the team boundary (the consumer DAG). |
| Verdict rule | Hive registration failing is `degraded`, not `failed`. Change it in `pipeline/verdict.py` with a test. |
| Card splitting | ~6 digits of a 16-digit PAN encrypted. Not for live cardholder data. |
| Secrets | Dev values in `.env.example`. Fine locally, unacceptable anywhere shared. |
| Kubernetes | Removed. Compose is the deployment. |
