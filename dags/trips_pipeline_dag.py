"""Trips pipeline — one DAG, six stages, every 4 hours.

STRUCTURE
=========

A single DAG owns the whole run. Each task consumes what the one before it
wrote, so the `>>` chain IS the data dependency -- there is no ordering here
that could be violated and still produce a correct result.

    schedule="0 */4 * * *"
    ──────────▶ extract ──▶ card_split ──▶ encrypt ──▶ mask
                                                         │
                          report ◀── load ◀── register ◀─┘

  extract      100 rows from BigQuery (or a CSV source) -> data/csv
  card_split   PANs -> prefix / encrypted middle / suffix -> data/cards
               A NO-OP when the dataset has no card column, which is the
               normal case; the run continues either way.
  encrypt      AES-256-GCM on every sensitive column -> data/parquet
  mask         decrypt, apply Ranger policy, write masked -> data/hive
  register     expose the masked Parquet as a Hive external table
  load         upsert into Postgres
  report       PDF with charts, filed into pipeline-reports/

WHY ONE DAG RATHER THAN SIX CHAINED BY ASSETS
---------------------------------------------
An earlier version ran six separate DAGs scheduled on Airflow Assets, so a
manual repair of one stage cascaded downstream on its own. That is a genuinely
nice property, and it was traded for a plainer one: a single graph that shows
the whole run, triggers once, and backfills once.

The cost is real and worth stating -- re-running `mask` by hand no longer
makes `load` follow automatically; clear the downstream tasks too, or re-run
the DAG from that task onward. `docs/EXECUTION-FLOW.md` records what the asset
model gave up.

Each stage still *verifies* its input exists before working, rather than
trusting that the previous task succeeded. Those are different guarantees and
the pipeline wants both.

IDEMPOTENCY
-----------
Every stage keys off the run's logical timestamp. The extractor maps that
timestamp to a fixed 4-hour archive window, so re-running the 08:00 slice
always reads the same rows; the Postgres load upserts on the dataset's key.
Re-running any run replaces its data rather than duplicating it.
"""
from __future__ import annotations

import logging
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

from airflow.sdk import DAG, task

from pipeline.common.trace import (stage_end, stage_event, stage_start,
                                   trace_from_context)

# Task logs go to the Airflow UI; this is the logger every stage writes to.
log = logging.getLogger("airflow.task")

DATA = Path(os.environ.get("PIPELINE_DATA_ROOT") or (ROOT / "data"))

# ── The assets. These ARE the schedule. ───────────────────────────────────

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


def logical_ts(ctx: dict):
    """The run's logical timestamp, however Airflow chose to supply it.

    `ctx["logical_date"]` is NOT always present. A run triggered from the UI
    Run button (or `airflow dags trigger`) on Airflow 3 arrives without it,
    while `airflow dags test` supplies it -- so a KeyError here fails only on
    the UI path, then sits in `up_for_retry` for the retry delay looking like
    the scheduler is stuck. Fall back the same way trace_from_context does.
    """
    ts = (ctx.get("logical_date")
          or ctx.get("data_interval_start")
          or (ctx.get("dag_run") and getattr(ctx["dag_run"], "logical_date", None))
          or (ctx.get("dag_run") and getattr(ctx["dag_run"], "run_after", None)))
    if ts is None:
        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc)
    return ts


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

    A predecessor task succeeding says work happened upstream; this says the file is
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


# ── The pipeline: one DAG, six stages ─────────────────────────────────────
with DAG(
    dag_id="trips_pipeline",
    description=("BigQuery/CSV -> card split -> encrypt -> mask -> load -> PDF, "
                 "every 4 hours"),
    schedule="0 */4 * * *",
    **COMMON,
) as dag:

    @task(task_id="extract_to_csv", execution_timeout=timedelta(minutes=10))
    def extract(**ctx):
        """Bounded, cost-capped read of one window, with a CSV fallback.

        BigQuery is attempted up to BQ_ATTEMPTS times (default 3). If all of
        them fail, the stage falls back to the CSV source rather than failing
        the run -- a transient BigQuery outage should not stop the pipeline
        when a local source can serve the same window.

        Every attempt is logged with its exception, and the stage result
        records which source actually produced the data (`mode`) and how many
        BigQuery attempts were made, so a fallback is visible in the UI rather
        than looking like an ordinary run.
        """
        from pipeline.extract.fixture import get_extractor, extract_with_fallback
        trace = trace_from_context(ctx)
        _t0 = stage_start("extract", trace)
        ts = logical_ts(ctx)
        r, ex, attempts, fell_back = extract_with_fallback(ts, DATA / "csv", log)
        _ev = stage_event(
            "extract", trace,
            mode=type(ex).__name__,
            bq_attempts=attempts,
            fell_back_to_csv=fell_back,
            rows=r.row_count,
            archive_window=f"{r.window_date}..{r.window_end}",
            bytes_billed=r.bytes_billed,
            bq_job_id=r.query_id,
            csv=r.csv_path,
        )
        stage_end("extract", trace, _t0, **{k: v for k, v in _ev.items()
                                            if k not in ("trace_id", "stage")})
        return _ev

    @task(task_id="split_card_numbers")
    def card_split(**ctx):
        """Break card columns into three parts; encrypt only the middle.

        Emits CARDS_SPLIT unconditionally on success, including for datasets
        with no card column at all -- the stage is a no-op there, and the
        chain must not stall just because a dataset has no cards.
        """
        from pipeline.transform.card_split import run
        trace = trace_from_context(ctx)
        _t0 = stage_start("card_split", trace)
        ts = logical_ts(ctx)
        d = ts.date() if hasattr(ts, "date") else ts
        csv_path = _find(DATA / "csv", f"*_{d.isoformat()}.csv", "card_split")
        r = run(d, csv_path, DATA / "cards",
                _crypto("spark-job", "CLIENT_SECRET_SPARK_JOB"))
        _ev = stage_event("card_split", trace, rows=r["rows"],
                           cards=r["cards"], columns=r["columns"],
                           dataset=r["dataset"], output=r["output"])
        stage_end("card_split", trace, _t0, **{k: v for k, v in _ev.items()
                                            if k not in ("trace_id", "stage")})
        return _ev

    @task(task_id="encrypt_to_parquet")
    def encrypt(**ctx):
        from pipeline.transform.spark_encrypt import run
        trace = trace_from_context(ctx)
        _t0 = stage_start("encrypt", trace)
        ts = logical_ts(ctx)
        d = ts.date() if hasattr(ts, "date") else ts
        csv_path = _find(DATA / "csv", f"*_{d.isoformat()}.csv", "encrypt")
        r = run(d, csv_path, DATA / "parquet",
                _crypto("spark-job", "CLIENT_SECRET_SPARK_JOB"))
        _ev = stage_event("encrypt", trace, rows=r["rows"], engine=r["engine"],
                           parquet=r["parquet_path"], algorithm="AES-256-GCM",
                           identity="spark-job", dataset=r["dataset"],
                           encrypted_columns=len(r["sensitive_columns"]))
        stage_end("encrypt", trace, _t0, **{k: v for k, v in _ev.items()
                                            if k not in ("trace_id", "stage")})
        return _ev

    @task(task_id="publish_kafka")
    def publish(**ctx):
        """Publish each encrypted row to two Kafka topics from one publisher.

        Encrypted sensitive fields to one topic, non-sensitive business
        columns to the other. No sensitive value is ever published in the
        clear -- see TRUST-BOUNDARY.md 2.9 for why the "unencrypted" topic
        carries the never-sensitive columns rather than decrypted PII.
        """
        from pipeline.publish.kafka_publish import run
        trace = trace_from_context(ctx)
        _t0 = stage_start("publish", trace)
        ts = logical_ts(ctx)
        d = ts.date() if hasattr(ts, "date") else ts
        pq = _find(DATA / "parquet" / f"dt={d.isoformat()}", "*.parquet", "publish")
        r = run(d, pq)
        _ev = stage_event("publish", trace, rows=r["rows"],
                           topic_encrypted=r["topic_encrypted"],
                           topic_public=r["topic_public"],
                           encrypted_fields=r["encrypted_fields"],
                           public_fields=r["public_fields"],
                           dataset=r["dataset"])
        stage_end("publish", trace, _t0, **{k: v for k, v in _ev.items()
                                            if k not in ("trace_id", "stage")})
        return _ev

    @task(task_id="decrypt_and_mask")
    def mask(**ctx):
        """Decrypt, mask, then write. The Hive table stores MASKED values, so
        plaintext exists only inside this task's memory."""
        from pipeline.transform.hive_mask import run
        trace = trace_from_context(ctx)
        _t0 = stage_start("mask", trace)
        ts = logical_ts(ctx)
        d = ts.date() if hasattr(ts, "date") else ts
        pq = _find(DATA / "parquet" / f"dt={d.isoformat()}", "*.parquet", "mask")
        r = run(d, pq, DATA / "hive" / pq.stem,
                _crypto("hive-job", "CLIENT_SECRET_HIVE_JOB"))
        _ev = stage_event("mask", trace, rows=r["rows"], masked_by=r["masked_by"],
                           policies=len(r["masks_applied"]), hive=r["hive_path"],
                           identity="hive-job", dataset=r["dataset"],
                           policy_file=r["policy_file"])
        stage_end("mask", trace, _t0, **{k: v for k, v in _ev.items()
                                            if k not in ("trace_id", "stage")})
        return _ev

    @task(task_id="register_hive_table")
    def register(**ctx):
        """Expose the masked Parquet as a Hive table.

        Non-fatal when Hive is down: the Parquet is already written and
        correct, and failing the chain because a metadata service is
        unavailable would be a false alarm about the data. HIVE_READY is
        emitted either way, because the DATA is ready even if the table is not.
        """
        from pipeline.transform.hive_register import run
        trace = trace_from_context(ctx)
        _t0 = stage_start("hive_register", trace)
        ts = logical_ts(ctx)
        d = ts.date() if hasattr(ts, "date") else ts
        # The DDL is built from the masked file itself, so the table matches
        # whatever dataset just ran rather than a hardcoded trips schema.
        pq = _find(DATA / "hive", f"*/dt={d.isoformat()}/*_masked.parquet",
                   "hive_register")
        r = run(d, pq)
        _ev = stage_event("hive_register", trace, registered=r.get("registered"),
                           table=r.get("table"),
                           partition=r.get("partition"), reason=r.get("reason"))
        stage_end("hive_register", trace, _t0, **{k: v for k, v in _ev.items()
                                            if k not in ("trace_id", "stage")})
        return _ev

    @task(task_id="load_postgres")
    def load(**ctx):
        from pipeline.load.postgres_load import run
        trace = trace_from_context(ctx)
        _t0 = stage_start("load", trace)
        ts = logical_ts(ctx)
        d = ts.date() if hasattr(ts, "date") else ts
        pq = _find(DATA / "hive", f"*/dt={d.isoformat()}/*_masked.parquet", "load")
        r = run(d, pq)
        _ev = stage_event("load", trace, inserted=r["inserted"],
                           updated=r["updated"], run_id=r["run_id"],
                           dataset=r["dataset"])
        stage_end("load", trace, _t0, **{k: v for k, v in _ev.items()
                                            if k not in ("trace_id", "stage")})
        return _ev

    @task(task_id="build_pdf")
    def report(**ctx):
        from pipeline.metadata import schema as schema_mod
        from pipeline.report.pdf_report import run
        from pipeline.report.collect import collect
        trace = trace_from_context(ctx)
        _t0 = stage_start("report", trace)
        ts = logical_ts(ctx)
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
        _ev = stage_event("report", trace, rows=r["rows"], pdf=r["pdf_path"],
                           dataset=manifest.dataset,
                           collected=c.get("dest") or c.get("reason"))
        stage_end("report", trace, _t0, **{k: v for k, v in _ev.items()
                                            if k not in ("trace_id", "stage")})
        return _ev

    # The whole run, in order. Each task consumes what the one before it
    # wrote, so the chain IS the data dependency -- there is no step here
    # that could run out of order and still be correct.
    (
        extract()
        >> card_split()
        >> encrypt()
        >> publish()
        >> mask()
        >> register()
        >> load()
        >> report()
    )
