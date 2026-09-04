# Orchestration spec

Answers to the seven orchestration questions, as the code actually is on
`main` — not as it is aspirationally described. Where something is a stand-in
or a known limitation it is marked, and `TRUST-BOUNDARY.md` carries the longer
version.

---

## 1. Orchestrator and version

**Apache Airflow 3.0.2** — not 2.x. This matters more than a version bump
usually does:

- The DAG is written against `airflow.sdk` (`from airflow.sdk import DAG, task`),
  the Airflow 3 Task SDK. Airflow 2 imports (`airflow.decorators`,
  `airflow.models.DAG`) are not used and will not be a drop-in swap.
- `logical_date` is **not** guaranteed in the task context on Airflow 3. A run
  triggered from the UI or `airflow dags trigger` does not supply it, while
  `airflow dags test` does. `dags/trips_pipeline_dag.py :: logical_ts(ctx)`
  resolves it through `logical_date → data_interval_start →
  dag_run.logical_date → dag_run.run_after → now`. Reading
  `ctx["logical_date"]` directly is a latent bug — there is a test asserting no
  task does it.

Pinned in `requirements.txt`:

```
apache-airflow==3.0.2
apache-airflow-core==3.0.2
apache-airflow-task-sdk==1.0.2
apache-airflow-providers-standard==1.2.0
```

**Executor:** LocalExecutor, running under `airflow standalone` (api-server +
scheduler + triggerer + dag-processor in one container). That is a development
topology, not a production one — see §7.

**Schedule:** `0 */4 * * *`, `catchup=False`, `max_active_runs=1`.

---

## 2. How Spark jobs get submitted

**Neither `spark-submit` nor a managed service.** The Airflow task process *is*
the Spark driver: it builds a `SparkSession` in-process and connects to a
standalone Spark 4.0.0 cluster over `spark://spark-master:7077`.

```python
# pipeline/transform/spark_encrypt.py
SparkSession.builder
    .appName("pipeline-encrypt")
    .master(os.environ["SPARK_MASTER_URL"])      # spark://spark-master:7077
    .config("spark.driver.host", "pl-airflow")   # executors must resolve the driver
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.pyspark.python", "/usr/bin/python3.12")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.driver.memory", "900m")
```

Three constraints that are easy to get wrong and fail *silently* (the stage
falls back to pyarrow and reports `engine: pyarrow` rather than erroring):

1. **A JVM must exist in the Airflow image.** PySpark needs a local JVM even to
   reach a remote cluster. Java 21 — Spark 4 supports 17 and 21, rejects 25.
2. **Executor Python must match the driver's minor version.** The executor
   interpreter is chosen by `spark.pyspark.python` from the *driver* side; the
   worker image's own `PYSPARK_PYTHON` does not decide it. Driver is 3.12, so
   the Spark image carries a deadsnakes 3.12 alongside its stock 3.10.
3. **`/data` must be mounted at the same path on the workers.** Spark passes an
   absolute output path to executors, which write the Parquet themselves.
   Without the mount, tasks die with `Mkdirs failed to create file:/data/...`.

**Fallback is deliberate.** With `SPARK_MASTER_URL` unset — or if any of the
above breaks — the stage runs the identical transformation through pyarrow and
returns `engine: pyarrow`. At 100 rows/run Spark is genuine overhead; the
fallback keeps the data path testable without a cluster. **Never assume which
ran; read the stage result.**

Not used, and would each be a real change: Databricks Jobs API, EMR, Glue, the
`SparkKubernetesOperator`. The `k8s/` manifests contain an in-cluster
`spark-submit` Job, but those manifests predate the current stack — §7.

---

## 3. Task granularity

**One Airflow task per pipeline stage, with the engine inside the task.** Not
one task per Spark job — only one stage uses Spark at all.

Every task is a `@task`-decorated Python function that imports a module from
`pipeline/` and calls its `run()`. The task owns orchestration concerns
(logical date, tracing, logging, XCom); the module owns the work and has no
Airflow import. That split is what makes the pipeline modules unit-testable
without Airflow, and `scripts/run_chain.py` runs the same stages outside it.

---

## 4. The actual shape

**One DAG, `trips_pipeline`, eight tasks, strictly linear.** No fan-out, no
branching, no dynamic task mapping.

```
extract_to_csv
     ↓
split_card_numbers
     ↓
encrypt_to_parquet          ← the only Spark stage
     ↓
publish_kafka
     ↓
decrypt_and_mask
     ↓
register_hive_table
     ↓
load_postgres
     ↓
build_pdf
```

| Task | Module | What it does |
|---|---|---|
| `extract_to_csv` | `pipeline/extract/fixture.py` | 100 rows from BigQuery, a CSV source, or an offline fixture. 3 BigQuery attempts, then CSV fallback |
| `split_card_numbers` | `pipeline/transform/card_split.py` | PAN → first-6 / encrypted-middle / last-4. No-op when the dataset has no card column |
| `encrypt_to_parquet` | `pipeline/transform/spark_encrypt.py` | AES-256-GCM on every manifest-sensitive column, via the crypto service |
| `publish_kafka` | `pipeline/publish/kafka_publish.py` | One producer → two topics: `rpos_encrypted` (ciphertext), `rpos_flat` (never-sensitive columns) |
| `decrypt_and_mask` | `pipeline/transform/hive_mask.py` | Decrypt inside the boundary, apply Ranger policy JSON, write masked Parquet |
| `register_hive_table` | `pipeline/transform/hive_register.py` | External table + partition in HiveServer2 over JDBC |
| `load_postgres` | `pipeline/load/postgres_load.py` | Upsert on the manifest's primary key |
| `build_pdf` | `pipeline/report/pdf_report.py` | Charted PDF, filed into `pipeline-reports/` |

**Not a bronze/silver/gold medallion.** The layering is by *sensitivity state*
rather than refinement: plaintext (transient, local) → ciphertext → masked →
warehouse. Closest mapping if your agent needs one: `data/csv` ≈ bronze,
`data/parquet` (encrypted) ≈ silver, `data/hive` (masked) + Postgres ≈ gold.

**No fan-out by source.** One dataset per run. Multiple sources would be
separate DAG runs, distinguished by the manifest's `dataset` name, which
already namespaces every output path, Hive table and Postgres table.

**Everything is metadata-driven.** `pipeline/metadata/schema.py` infers each
dataset's columns, sensitivity classes, primary key and special treatments
(e.g. `card_split`) into a manifest. Every stage reads that manifest. **No
stage names a column literally** — that is an invariant, not a style
preference.

---

## 5. Data handoff between tasks

**Files on a shared volume, addressed by date-partitioned convention. XCom
carries metadata only — never data.**

```
data/csv/<dataset>_<YYYY-MM-DD>.csv                        extract  →  card_split, encrypt
data/cards/<dataset>_card_info_<YYYY-MM-DD>.csv            card_split (terminal)
data/parquet/dt=<YYYY-MM-DD>/<dataset>.parquet             encrypt  →  publish, mask
data/hive/<dataset>/dt=<YYYY-MM-DD>/<dataset>_masked.parquet   mask →  register, load, report
data/reports/<dataset>_report_<YYYY-MM-DD>.pdf             report (terminal)
pipeline-reports/<same>.pdf                                delivery copy
```

A downstream task locates its input with a glob on the logical date
(`_find(DATA / "parquet" / f"dt={d}", "*.parquet", "publish")`) rather than
pulling a path from XCom. Two consequences worth knowing:

- Re-running one stage by hand works without touching XCom.
- **Each stage verifies its input exists** rather than trusting that the
  previous task succeeded. Those are different guarantees.

**Not Delta Lake.** Plain Parquet, plus a hidden `.<name>.schema.json` sidecar
manifest beside each artifact. The leading dot is load-bearing: Hive reads
every file in a partition directory and fails parsing JSON as Parquet.

**Not S3/GCS.** A local bind mount (`./data`), shared across the Airflow and
Spark containers at the same absolute path. The k8s manifests use a
`ReadWriteMany` hostPath, which works on single-node minikube only.

XCom holds the `stage_event` dict per task — `trace_id`, `stage`, row counts,
`engine`, `registered`, `bq_attempts`, `fell_back_to_csv`. Diagnostics, not
payload.

---

## 6. Where the code lives

```
dags/
  trips_pipeline_dag.py        the only DAG; thin tasks that call pipeline/
pipeline/
  common/       auth.py (JWT), trace.py (correlation id + UI logging)
  metadata/     schema.py (inference → manifest), policy_gen.py (Ranger JSON)
  extract/      bigquery_extract.py, csv_source.py, fixture.py
  transform/    card_split.py, spark_encrypt.py, hive_mask.py, hive_register.py
  publish/      kafka_publish.py
  load/         postgres_load.py
  mask/         ranger.py
  report/       pdf_report.py, profile.py, collect.py
services/       idp/ (JWT issuer), crypto/ (AES-256-GCM) — FastAPI, run as containers
config/
  bq/           BigQuery SQL, selected by BQ_SQL_FILE
  ranger/       masking policy JSON, one file per dataset
  datasets/     per-dataset sensitivity overrides
docker/         Dockerfiles + Kafka init/JAAS
scripts/        gen_certs.sh, collect_report.sh, run_chain.py
tests/          79 tests, no Airflow import needed
setup.sh        one-command bring-up
```

**How the DAG reaches the job code:** a plain Python import, not a submitted
artifact.

```python
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.common.trace import stage_event, trace_from_context
```

The repo is bind-mounted into the Airflow container at `/opt/pipeline`, with
`PYTHONPATH=/opt/pipeline` and `AIRFLOW__CORE__DAGS_FOLDER=/opt/pipeline/dags`.
Task-level imports (`from pipeline.transform.card_split import run` *inside*
the task function) keep DAG parse time low and stop a heavy dependency from
breaking DAG collection.

The Spark image mounts the same repo at the same path, so executors import the
same modules the driver does — no packaging or `--py-files` step.

**All paths are relative to the repo root**, derived from the file's own
location. The checkout can live anywhere.

---

## 7. Local dev story

**Everything runs on a laptop. There is no cluster-only path.**

```bash
git clone https://github.com/mabdullah679/avapuck.git
cd avapuck
./setup.sh
```

Docker is the only host prerequisite — no Python, Java, Spark or Kafka client.
`setup.sh` checks prerequisites, generates the machine's own TLS certificates,
starts the stack, creates Kafka topics and ACLs, runs the pipeline, and reports
where the PDF landed. Linux, macOS and WSL2; x86_64 and arm64.

**What runs locally:** eleven long-running containers — Airflow, IdP, crypto
service, Postgres, Kafka (KRaft, SASL + ACLs), Spark master + 2 workers, Hive
metastore + metastore DB, HiveServer2 — plus a one-shot `kafka-init` that
creates topics and ACLs then exits. Measured working set across a full run is
about **4.2 GB**; caps total 13.5 GB. Give Docker 8 GB.

**What is genuinely real locally**, verified rather than assumed:
- Encryption runs on the Spark cluster (confirmed against the master's own
  completed-application list, not the stage's log line).
- Hive tables are created and queryable through HiveServer2's Tez engine.
- Kafka enforces SASL auth and per-topic ACLs.
- TLS certificate verification is enforced; a client without the CA fails closed.

**What is a stand-in locally** — full list in `TRUST-BOUNDARY.md`:
- **Apache Ranger admin is not deployed.** Policy JSON is real Ranger format;
  the engine applying it is local unless you start the `ranger` profile. Every
  masked row records `masked_by` so the two are distinguishable.
- **The IdP is a local FastAPI service** with dev client registrations.
- **The TLS CA is self-signed** and its private key sits beside the certs it signs.
- **Kafka is single-node**, `replication-factor 1`, SASL_PLAINTEXT
  (authentication, not wire encryption).
- **BigQuery is optional.** With no `GCP_PROJECT_ID` the extract stage uses an
  offline fixture, so the full chain runs with no cloud account. Live BigQuery
  needs a service-account key with `roles/bigquery.jobUser`; ~30 MB scanned per
  run, about $0.0002.

**Without Airflow at all:** `scripts/run_chain.py` runs the same stage
functions in sequence. `tests/` (79 tests) imports the pipeline modules
directly and needs no Airflow and no running stack.

**The k8s manifests under `k8s/` are stale.** They predate TLS, the card-split
stage, Kafka and the single-DAG consolidation. Docker Compose is the verified
path; treat `k8s/` as a starting point, not a deployment.

---

## Known limitations an agent should not design around

| Area | Status |
|---|---|
| Airflow topology | `airflow standalone` + LocalExecutor. No HA, no separate workers, SQLite-free but single-process |
| Scale | Hard-capped at 100 rows/run, enforced in SQL and asserted after. The row cap is an invariant, not a config default |
| Repair semantics | One DAG chained by `>>`; re-running `mask` alone does **not** cascade to `load`. An earlier asset-scheduled version did — the trade is recorded in the DAG docstring |
| Card splitting | Only ~6 digits of a 16-digit PAN end up encrypted, one of them a derivable Luhn check digit. Fine for the published test PANs used here; not for live cardholder data |
| Secrets | Dev defaults in `docker-compose.yml` and `docker/kafka-jaas.conf`. Fine locally, unacceptable anywhere shared |
