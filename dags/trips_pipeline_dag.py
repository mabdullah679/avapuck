"""Trips pipeline — asset-driven, every 4 hours.

SCHEDULING MODEL
================

Only the first DAG has a clock schedule. Every other stage is scheduled by
**data availability**: it declares the Airflow Asset it consumes, and Airflow
runs it when that asset is updated. No stage names the stage after it.

    schedule="0 */4 * * *"          ┌────────────────┐
    ─────────────────────────────▶│ 01 extract     │──▶ CSV_READY
                                  └────────────────┘
    CSV_READY ───────────────────▶│ 02 card_split  │──▶ CARDS_SPLIT
                                  └────────────────┘
    CARDS_SPLIT ─────────────────▶│ 03 encrypt     │──▶ PARQUET_READY
                                  └────────────────┘
    PARQUET_READY ───────────────▶│ 04 mask        │──▶ HIVE_READY
                                  └────────────────┘
    HIVE_READY ──────────────────▶│ 05 load        │──▶ WAREHOUSE_READY
                                  └────────────────┘
    WAREHOUSE_READY ─────────────▶│ 06 report      │──▶ REPORT_READY
                                  └────────────────┘

Stage 02 is a no-op for datasets with no card column: it emits CARDS_SPLIT
regardless, so the chain never stalls on a dataset that simply has no cards.

WHY ASSETS RATHER THAN TriggerDagRunOperator
--------------------------------------------
The previous version chained stages on *task success*, which is a weaker and
subtly different claim than *the data is there*. Three concrete consequences:

  1. **Repair triggers downstream.** Re-run the mask stage by hand, or fix a
     Parquet file, and the load stage runs on its own. With explicit triggers
     nothing happens until you also remember to trigger the next one.
  2. **No coupling.** A stage does not know what comes after it. Adding a
     second consumer of WAREHOUSE_READY -- a data-quality check, an export --
     needs no edit to any existing DAG.
  3. **The lineage is real.** Airflow's asset graph shows what produced what,
     rather than a chain of triggers that only implies it.

Each stage still *verifies* its input exists before working. Asset scheduling
says "something updated this"; the check says "and it is actually usable".
Those are different guarantees and the pipeline wants both.

IDEMPOTENCY
-----------
Every stage keys off the run's logical timestamp. The extractor maps that
timestamp to a fixed 4-hour archive window, so re-running the 08:00 slice
always reads the same rows; the Postgres load upserts on trip_id. Re-running
any run replaces its data rather than duplicating it.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Airflow runs tasks in a subprocess that does not inherit the shell's
# environment reliably, and GOOGLE_APPLICATION_CREDENTIALS in .env.local is a
# RELATIVE path -- which resolves against the worker's cwd, not the repo. Both
# are fixed here, once, at parse time. Without this the extract task hangs in
# credential discovery instead of failing cleanly.
try:
    from dotenv import dotenv_values
    for _k, _v in dotenv_values(ROOT / ".env.local").items():
        if _v and _k not in os.environ:
            os.environ[_k] = _v
    _cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if _cred and not os.path.isabs(_cred):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(ROOT / _cred)
except ImportError:
    pass

from airflow.sdk import DAG, Asset, task

from pipeline.common.trace import stage_event, trace_from_context

DATA = Path(os.environ.get("PIPELINE_DATA_ROOT") or (ROOT / "data"))

# ── The assets. These ARE the schedule. ───────────────────────────────────
CSV_READY = Asset(name="trips_csv", uri="file://data/csv")
CARDS_SPLIT = Asset(name="trips_cards_split", uri="file://data/cards")
PARQUET_READY = Asset(name="trips_parquet_encrypted", uri="file://data/parquet")
HIVE_READY = Asset(name="trips_hive_masked", uri="file://data/hive")
WAREHOUSE_READY = Asset(name="trips_warehouse", uri="postgres://warehouse/trips")
REPORT_READY = Asset(name="trips_report_pdf", uri="file://data/reports")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
    "email_on_failure": False,
}

COMMON = dict(
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["trips", "pii", "4-hourly"],
)


def _crypto(client_id: str, secret_env: str):
    """A crypto client authenticated as a specific job identity.

    Each stage uses its OWN identity, so the encrypt stage's token cannot be
    replayed to decrypt: spark-job holds crypto.encrypt, hive-job holds
    crypto.decrypt, and the IdP refuses to mint either for the wrong client.
    """
    from pipeline.common.auth import TokenClient, tls_verify_from_env
    from pipeline.transform.spark_encrypt import CryptoClient
    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "https://localhost:8443"),
        client_id=client_id,
        client_secret=os.environ.get(secret_env, f"dev-{client_id}-secret"),
        verify_tls=tls_verify_from_env(),
    )
    return CryptoClient(os.environ.get("CRYPTO_URL", "https://localhost:8444"),
                        tokens, verify_tls=tls_verify_from_env())


def _require(path: Path, stage: str) -> Path:
    """Assert the input actually exists before doing work.

    Asset scheduling says something updated upstream; this says the file is
    really there. Failing here names the missing path, which beats a
    FileNotFoundError from three frames deeper.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"{stage}: required input {path} does not exist. The upstream "
            f"asset fired but its output is missing -- check the upstream run.")
    return path


def _find(directory: Path, pattern: str, stage: str) -> Path:
    """Locate a stage's input without hardcoding the dataset name.

    The stages no longer know they process trips: the file is named for
    whatever dataset was extracted. Globbing for the run's date and taking the
    newest match keeps the handoff working for any dataset, while still
    failing loudly -- and naming the directory it searched -- when nothing
    upstream produced a file.
    """
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not matches:
        raise FileNotFoundError(
            f"{stage}: no file matching {pattern!r} in {directory}. The "
            f"upstream asset fired but its output is missing -- check the "
            f"upstream run.")
    return matches[-1]


# ── 1. Extract — the only clock-scheduled DAG ─────────────────────────────
with DAG(
    dag_id="trips_01_extract",
    description="Every 4h: pull 100 rows from BigQuery into a local CSV",
    schedule="0 */4 * * *",
    **COMMON,
) as dag_extract:

    @task(task_id="extract_to_csv", outlets=[CSV_READY],
          execution_timeout=timedelta(minutes=10))
    def extract(**ctx):
        """Bounded, cost-capped read of one 4-hour archive window.

        Emits CSV_READY on success, which is what starts stage 2. A failure
        emits nothing, so the chain stops here rather than running later
        stages on absent or stale data.
        """
        from pipeline.extract.fixture import get_extractor
        trace = trace_from_context(ctx)
        ts = ctx["logical_date"]
        ex = get_extractor()
        r = ex.extract(ts, DATA / "csv")
        return stage_event(
            "extract", trace,
            mode=type(ex).__name__,
            rows=r.row_count,
            archive_window=f"{r.window_date}..{r.window_end}",
            bytes_billed=r.bytes_billed,
            bq_job_id=r.query_id,
            csv=r.csv_path,
        )

    extract()


# ── 2. Card split — runs when CSV_READY updates ───────────────────────────
with DAG(
    dag_id="trips_02_card_split",
    description="Split PANs into prefix / encrypted middle / suffix",
    schedule=[CSV_READY],
    **COMMON,
) as dag_card_split:

    @task(task_id="split_card_numbers", outlets=[CARDS_SPLIT])
    def card_split(**ctx):
        """Break card columns into three parts; encrypt only the middle.

        Emits CARDS_SPLIT unconditionally on success, including for datasets
        with no card column at all -- the stage is a no-op there, and the
        chain must not stall just because a dataset has no cards.
        """
        from pipeline.transform.card_split import run
        trace = trace_from_context(ctx)
        ts = ctx["logical_date"]
        d = ts.date() if hasattr(ts, "date") else ts
        csv_path = _find(DATA / "csv", f"*_{d.isoformat()}.csv", "card_split")
        r = run(d, csv_path, DATA / "cards",
                _crypto("spark-job", "CLIENT_SECRET_SPARK_JOB"))
        return stage_event("card_split", trace, rows=r["rows"],
                           cards=r["cards"], columns=r["columns"],
                           dataset=r["dataset"], output=r["output"])

    card_split()


# ── 3. Encrypt — runs when CSV_READY updates ──────────────────────────────
with DAG(
    dag_id="trips_03_encrypt",
    description="AES-256-GCM encrypt sensitive fields; write Parquet",
    schedule=[CARDS_SPLIT],
    **COMMON,
) as dag_encrypt:

    @task(task_id="encrypt_to_parquet", outlets=[PARQUET_READY])
    def encrypt(**ctx):
        from pipeline.transform.spark_encrypt import run
        trace = trace_from_context(ctx)
        ts = ctx["logical_date"]
        d = ts.date() if hasattr(ts, "date") else ts
        csv_path = _find(DATA / "csv", f"*_{d.isoformat()}.csv", "encrypt")
        r = run(d, csv_path, DATA / "parquet",
                _crypto("spark-job", "CLIENT_SECRET_SPARK_JOB"))
        return stage_event("encrypt", trace, rows=r["rows"], engine=r["engine"],
                           parquet=r["parquet_path"], algorithm="AES-256-GCM",
                           identity="spark-job", dataset=r["dataset"],
                           encrypted_columns=len(r["sensitive_columns"]))

    encrypt()


# ── 4. Mask — runs when PARQUET_READY updates ─────────────────────────────
with DAG(
    dag_id="trips_04_mask",
    description="Decrypt, apply Apache Ranger masking policy, write Hive table",
    schedule=[PARQUET_READY],
    **COMMON,
) as dag_mask:

    @task(task_id="decrypt_and_mask")
    def mask(**ctx):
        """Decrypt, mask, then write. The Hive table stores MASKED values, so
        plaintext exists only inside this task's memory."""
        from pipeline.transform.hive_mask import run
        trace = trace_from_context(ctx)
        ts = ctx["logical_date"]
        d = ts.date() if hasattr(ts, "date") else ts
        pq = _find(DATA / "parquet" / f"dt={d.isoformat()}", "*.parquet", "mask")
        r = run(d, pq, DATA / "hive" / pq.stem,
                _crypto("hive-job", "CLIENT_SECRET_HIVE_JOB"))
        return stage_event("mask", trace, rows=r["rows"], masked_by=r["masked_by"],
                           policies=len(r["masks_applied"]), hive=r["hive_path"],
                           identity="hive-job", dataset=r["dataset"],
                           policy_file=r["policy_file"])

    @task(task_id="register_hive_table", outlets=[HIVE_READY])
    def register(**ctx):
        """Expose the masked Parquet as a Hive table.

        Non-fatal when Hive is down: the Parquet is already written and
        correct, and failing the chain because a metadata service is
        unavailable would be a false alarm about the data. HIVE_READY is
        emitted either way, because the DATA is ready even if the table is not.
        """
        from pipeline.transform.hive_register import run
        trace = trace_from_context(ctx)
        ts = ctx["logical_date"]
        d = ts.date() if hasattr(ts, "date") else ts
        # The DDL is built from the masked file itself, so the table matches
        # whatever dataset just ran rather than a hardcoded trips schema.
        pq = _find(DATA / "hive", f"*/dt={d.isoformat()}/*_masked.parquet",
                   "hive_register")
        r = run(d, pq)
        return stage_event("hive_register", trace, registered=r.get("registered"),
                           table=r.get("table"),
                           partition=r.get("partition"), reason=r.get("reason"))

    mask() >> register()


# ── 5. Load — runs when HIVE_READY updates ────────────────────────────────
with DAG(
    dag_id="trips_05_load",
    description="Upsert masked rows into the Postgres warehouse",
    schedule=[HIVE_READY],
    **COMMON,
) as dag_load:

    @task(task_id="load_postgres", outlets=[WAREHOUSE_READY])
    def load(**ctx):
        from pipeline.load.postgres_load import run
        trace = trace_from_context(ctx)
        ts = ctx["logical_date"]
        d = ts.date() if hasattr(ts, "date") else ts
        pq = _find(DATA / "hive", f"*/dt={d.isoformat()}/*_masked.parquet", "load")
        r = run(d, pq)
        return stage_event("load", trace, inserted=r["inserted"],
                           updated=r["updated"], run_id=r["run_id"],
                           dataset=r["dataset"])

    load()


# ── 6. Report — runs when WAREHOUSE_READY updates ─────────────────────────
with DAG(
    dag_id="trips_06_report",
    description="Build the PDF report with charts from the masked warehouse",
    schedule=[WAREHOUSE_READY],
    **COMMON,
) as dag_report:

    @task(task_id="build_pdf", outlets=[REPORT_READY])
    def report(**ctx):
        from pipeline.metadata import schema as schema_mod
        from pipeline.report.pdf_report import run
        from pipeline.report.collect import collect
        trace = trace_from_context(ctx)
        ts = ctx["logical_date"]
        d = ts.date() if hasattr(ts, "date") else ts
        # The report reads the warehouse, not a file -- but it still needs to
        # know WHICH table, so it reads the manifest the mask stage left beside
        # its output rather than assuming the dataset is trips.
        pq = _find(DATA / "hive", f"*/dt={d.isoformat()}/*_masked.parquet", "report")
        manifest = schema_mod.load_manifest(pq)
        r = run(d, DATA / "reports", dataset=manifest.dataset,
                primary_key=manifest.primary_key)
        # File the finished PDF into the delivery directory. Non-fatal by
        # contract: the run has already produced its artifact, and a failed
        # copy must not fail the DAG.
        c = collect(r["pdf_path"])
        return stage_event("report", trace, rows=r["rows"], pdf=r["pdf_path"],
                           dataset=manifest.dataset,
                           collected=c.get("dest") or c.get("reason"))

    report()
