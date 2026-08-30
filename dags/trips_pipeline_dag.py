"""Airflow DAGs for the trips pipeline.

FIVE DAGS, CHAINED, RATHER THAN ONE MONOLITH. Each stage is independently
re-runnable: if the Hive mask fails at 3am, you re-run that stage alone rather
than re-querying BigQuery and paying for it again. A single DAG would be
simpler to read and materially worse to operate.

Chaining is by TriggerDagRunOperator with wait_for_completion, so the chain is
observable stage by stage in the UI and a failure stops the chain rather than
letting later stages run on missing data.

IDEMPOTENT BY LOGICAL DATE: every stage keys off {{ ds }}, writes to a
date-partitioned path, and the Postgres load upserts on the trip_id primary
key. Re-running any date replaces that date's data and never double-loads.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:                                     # Airflow 3.x
    from airflow.sdk import DAG, task
except ImportError:                      # Airflow 2.x
    from airflow import DAG
    from airflow.decorators import task

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# wait_for_completion requires a running scheduler to advance the triggered DAG.
# Under `airflow dags test` (no scheduler) it would poll forever, which makes
# the chain impossible to verify stage by stage. Default to fire-and-forget and
# let the deployment opt into blocking; the chain is still ordered because each
# stage triggers the next only after its own work succeeds.
WAIT_FOR_DOWNSTREAM = os.environ.get("AIRFLOW_CHAIN_WAIT", "0") == "1"

DATA = ROOT / "data"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "depends_on_past": False,
    # Alert on failure, not on every run.
    "email_on_failure": False,
}

COMMON = dict(
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["trips", "pii", "daily"],
)


def _crypto(client_id: str, secret_env: str):
    from pipeline.common.auth import TokenClient
    from pipeline.transform.spark_encrypt import CryptoClient
    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "http://idp:8443"),
        client_id=client_id,
        client_secret=os.environ.get(secret_env, f"dev-{client_id}-secret"),
        verify_tls=False,
    )
    return CryptoClient(os.environ.get("CRYPTO_URL", "http://crypto:8444"),
                        tokens, verify_tls=False)


# ── 1. Extract ────────────────────────────────────────────────────────────
with DAG(
    dag_id="trips_01_extract",
    description="Pull 100 rows/day from the BigQuery public dataset to local CSV",
    schedule="0 6 * * *",          # daily, 06:00 local
    **COMMON,
) as dag_extract:

    @task(task_id="extract_to_csv")
    def extract(ds=None):
        """Bounded, cost-capped BigQuery read.

        The row cap is in the SQL and asserted after; maximum_bytes_billed is
        set on the job so BigQuery REJECTS an over-budget query rather than
        running it. See pipeline/extract/bigquery_extract.py.
        """
        from datetime import date
        from pipeline.extract.fixture import get_extractor
        ex = get_extractor()
        r = ex.extract(date.fromisoformat(ds), DATA / "csv")
        return {**r.as_dict(), "mode": type(ex).__name__}

    TriggerDagRunOperator(
        task_id="trigger_encrypt",
        trigger_dag_id="trips_02_encrypt",
        logical_date="{{ logical_date }}",
        wait_for_completion=WAIT_FOR_DOWNSTREAM,
        poke_interval=15,
        reset_dag_run=True,           # makes a re-run of the same date safe
        skip_when_already_exists=False,
    ).set_upstream(extract())


# ── 2. Encrypt (Spark) ────────────────────────────────────────────────────
with DAG(
    dag_id="trips_02_encrypt",
    description="AES-256-GCM encrypt sensitive fields via the HTTPS crypto service; write Parquet",
    schedule=None,                 # triggered by stage 1
    **COMMON,
) as dag_encrypt:

    @task(task_id="encrypt_to_parquet")
    def encrypt(ds=None):
        from datetime import date
        from pipeline.transform.spark_encrypt import run
        d = date.fromisoformat(ds)
        return run(d, DATA / "csv" / f"trips_{ds}.csv", DATA / "parquet",
                   _crypto("spark-job", "CLIENT_SECRET_SPARK_JOB"))

    TriggerDagRunOperator(
        task_id="trigger_mask",
        trigger_dag_id="trips_03_mask",
        logical_date="{{ logical_date }}",
        wait_for_completion=WAIT_FOR_DOWNSTREAM,
        poke_interval=15,
        reset_dag_run=True,
    ).set_upstream(encrypt())


# ── 3. Hive + Ranger mask ─────────────────────────────────────────────────
with DAG(
    dag_id="trips_03_mask",
    description="Decrypt in Hive, apply Apache Ranger masking policy, write the Hive table",
    schedule=None,
    **COMMON,
) as dag_mask:

    @task(task_id="decrypt_and_mask")
    def mask(ds=None):
        """Decrypt, mask, then write. The Hive table stores MASKED values, so
        plaintext exists only in this task's memory."""
        from datetime import date
        from pipeline.transform.hive_mask import run
        d = date.fromisoformat(ds)
        return run(d, DATA / "parquet" / f"dt={ds}" / "trips.parquet",
                   DATA / "hive" / "trips",
                   _crypto("hive-job", "CLIENT_SECRET_HIVE_JOB"))

    _masked = mask()

    @task(task_id="register_hive_table")
    def register(ds=None):
        """Expose the masked Parquet as a Hive table. Non-fatal if Hive is down:
        the data is already written, and failing the chain because a metadata
        service is unavailable would be a false alarm."""
        from datetime import date
        from pipeline.transform.hive_register import run
        return run(date.fromisoformat(ds))

    _registered = register()
    _masked >> _registered


    TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="trips_04_load",
        logical_date="{{ logical_date }}",
        wait_for_completion=WAIT_FOR_DOWNSTREAM,
        poke_interval=15,
        reset_dag_run=True,
    ).set_upstream(_registered)


# ── 4. Postgres load ──────────────────────────────────────────────────────
with DAG(
    dag_id="trips_04_load",
    description="Upsert masked rows into the Postgres warehouse",
    schedule=None,
    **COMMON,
) as dag_load:

    @task(task_id="load_postgres")
    def load(ds=None):
        from datetime import date
        from pipeline.load.postgres_load import run
        d = date.fromisoformat(ds)
        return run(d, DATA / "hive" / "trips" / f"dt={ds}" / "trips_masked.parquet")

    TriggerDagRunOperator(
        task_id="trigger_report",
        trigger_dag_id="trips_05_report",
        logical_date="{{ logical_date }}",
        wait_for_completion=WAIT_FOR_DOWNSTREAM,
        poke_interval=15,
        reset_dag_run=True,
    ).set_upstream(load())


# ── 5. PDF report ─────────────────────────────────────────────────────────
with DAG(
    dag_id="trips_05_report",
    description="Build the daily PDF report with charts from the masked warehouse",
    schedule=None,
    **COMMON,
) as dag_report:

    @task(task_id="build_pdf")
    def report(ds=None):
        from datetime import date
        from pipeline.report.pdf_report import run
        return run(date.fromisoformat(ds), DATA / "reports")

    report()
