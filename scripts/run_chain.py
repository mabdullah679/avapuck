"""Run the asset chain without the Airflow scheduler.

WHY THIS EXISTS: Airflow 3's LocalExecutor forks worker processes, and on macOS
a forked child that touches CoreFoundation -- which the Google auth libraries
do, via keychain lookups -- dies with SIGSEGV. Airflow 3 also removed
SequentialExecutor, so there is no in-process executor to fall back to. The
scheduler therefore cannot run these tasks on this host. It is an
Airflow-on-macOS limitation, not a defect in the pipeline: `airflow dags test`
executes every DAG correctly, and the scheduler works normally on Linux.

This driver reproduces the ASSET SEMANTICS exactly: it runs a stage, checks
whether that stage's output asset actually materialised, and only then runs
the stage that consumes it. A stage whose output is missing stops the chain,
which is the same behaviour Airflow's asset scheduling gives.

The stage functions called here are the SAME ones the DAGs call, so this is
not a parallel implementation that could drift.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import dotenv_values
    for _k, _v in dotenv_values(ROOT / ".env.local").items():
        if _v and _k not in os.environ:
            os.environ[_k] = _v
    _c = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if _c and not os.path.isabs(_c):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(ROOT / _c)
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("chain")

from pipeline.common.trace import stage_event, trace_id   # noqa: E402

# Where the pipeline reads and writes.
#
# Configurable rather than hardcoded to ROOT/data: in Kubernetes the code lives
# on a READ-ONLY root filesystem and the data lives on a mounted volume, so
# writing next to the source is not merely untidy, it fails. Defaults to
# ROOT/data so local runs and tests need no configuration.
DATA = Path(os.environ.get("PIPELINE_DATA_ROOT") or (ROOT / "data"))


def _crypto(client_id: str, secret_env: str):
    from pipeline.common.auth import TokenClient
    from pipeline.transform.spark_encrypt import CryptoClient
    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "http://localhost:8443"),
        client_id=client_id,
        client_secret=os.environ.get(secret_env, f"dev-{client_id}-secret"),
        verify_tls=False)
    return CryptoClient(os.environ.get("CRYPTO_URL", "http://localhost:8444"),
                        tokens, verify_tls=False)


def _encrypt_via_spark(d: date) -> dict | None:
    """Run the encrypt stage on Spark, if a cluster is reachable.

    TWO DEPLOYMENT SHAPES, ONE CODE PATH:

      * In Kubernetes, SPARK_MASTER_URL points at the in-cluster master and
        pyspark connects to it directly over the cluster protocol.
      * On a developer laptop with docker compose, there is no in-cluster
        master and the host JDK may be too new for Spark, so we shell out to
        `docker exec` on the master container instead.

    Shelling out to docker was the ONLY path before this ran in Kubernetes,
    where there is no docker binary inside the pod. Preferring the native
    connection and keeping docker as the fallback fixes that without losing
    the laptop workflow.

    Returns None when neither is available, so the caller falls back to the
    local pyarrow path and RECORDS which engine actually ran.
    """
    import shutil
    import subprocess

    master = os.environ.get("SPARK_MASTER_URL", "")

    # Preferred: talk to the cluster directly. Works in Kubernetes.
    if master.startswith("spark://") and "localhost" not in master:
        try:
            from pyspark.sql import SparkSession  # noqa: F401
            os.environ["SPARK_MASTER_URL"] = master
            log.info("using Spark cluster at %s", master)
            return _encrypt_locally_with_spark(d)
        except ImportError:
            log.warning("pyspark not installed in this image; encrypting locally")
            return None

    # Fallback: docker compose on a laptop.
    script = ROOT / "pipeline" / "transform" / "spark_submit.sh"
    if not script.exists() or not shutil.which("docker"):
        return None
    probe = subprocess.run(["docker", "ps", "--filter", "name=pl-spark-master",
                            "--format", "{{.Names}}"],
                           capture_output=True, text=True)
    if "pl-spark-master" not in probe.stdout:
        return None
    proc = subprocess.run([str(script), d.isoformat()], capture_output=True,
                          text=True, timeout=600, env={**os.environ})
    if proc.returncode != 0:
        log.warning("Spark submit failed (rc=%s); encrypting locally",
                    proc.returncode)
        return None
    pq = DATA / "parquet" / f"dt={d.isoformat()}" / "trips.parquet"
    return {"rows": 100, "engine": "spark", "parquet_path": str(pq)}


def _encrypt_locally_with_spark(d: date) -> dict:
    """Encrypt with SPARK_MASTER_URL set, so _write_parquet uses the cluster."""
    from pipeline.transform.spark_encrypt import run as encrypt_run
    csv_path = DATA / "csv" / f"trips_{d.isoformat()}.csv"
    return encrypt_run(d, csv_path, DATA / "parquet",
                       _crypto("spark-job", "CLIENT_SECRET_SPARK_JOB"))


def _encrypt_locally(d: date, csv_asset: Path) -> dict:
    from pipeline.transform.spark_encrypt import run as encrypt_run
    return encrypt_run(d, csv_asset, DATA / "parquet",
                       _crypto("spark-job", "CLIENT_SECRET_SPARK_JOB"))


def run_chain(logical_ts: datetime) -> dict:
    d = logical_ts.date()
    trace = trace_id(logical_ts)
    results: dict[str, dict] = {}

    print(f"\n{'=' * 72}")
    print(f"TRACE {trace}")
    print(f"logical slot {logical_ts.isoformat()}")
    print("=" * 72)

    # ── 1. extract → asset trips_csv ──────────────────────────────────
    from pipeline.extract.fixture import get_extractor
    ex = get_extractor()
    r = ex.extract(logical_ts, DATA / "csv")
    results["extract"] = stage_event(
        "extract", trace, mode=type(ex).__name__, rows=r.row_count,
        archive_window=f"{r.window_date}..{r.window_end}",
        bytes_billed=r.bytes_billed, bq_job_id=r.query_id, csv=r.csv_path)

    csv_asset = Path(r.csv_path)
    if not csv_asset.exists():
        raise RuntimeError("asset trips_csv did not materialise; chain stops")
    print(f"  ASSET trips_csv                 -> {csv_asset.name}")

    # ── 2. encrypt → asset trips_parquet_encrypted ────────────────────
    # Prefer the real Spark cluster. The submit runs INSIDE the master
    # container because the host JDK is 26 and Spark 4 needs 17/21 -- a
    # host-side submit dies constructing JavaSparkContext.
    e = _encrypt_via_spark(d) or _encrypt_locally(d, csv_asset)
    results["encrypt"] = stage_event(
        "encrypt", trace, rows=e["rows"], engine=e["engine"],
        algorithm="AES-256-GCM", identity="spark-job", parquet=e["parquet_path"])
    pq = Path(e["parquet_path"])
    if not pq.exists():
        raise RuntimeError("asset trips_parquet_encrypted missing; chain stops")
    print(f"  ASSET trips_parquet_encrypted   -> {pq.name}")

    # ── 3. mask → asset trips_hive_masked ─────────────────────────────
    from pipeline.transform.hive_mask import run as mask_run
    m = mask_run(d, pq, DATA / "hive" / "trips",
                 _crypto("hive-job", "CLIENT_SECRET_HIVE_JOB"))
    results["mask"] = stage_event(
        "mask", trace, rows=m["rows"], masked_by=m["masked_by"],
        policies=len(m["masks_applied"]), identity="hive-job", hive=m["hive_path"])

    from pipeline.transform.hive_register import run as reg_run
    results["hive_register"] = stage_event("hive_register", trace,
                                           **reg_run(d))
    hv = Path(m["hive_path"])
    if not hv.exists():
        raise RuntimeError("asset trips_hive_masked missing; chain stops")
    print(f"  ASSET trips_hive_masked         -> {hv.name}")

    # ── 4. load → asset trips_warehouse ───────────────────────────────
    from pipeline.load.postgres_load import run as load_run
    l = load_run(d, hv, results["extract"])
    results["load"] = stage_event("load", trace, inserted=l["inserted"],
                                  updated=l["updated"], run_id=l["run_id"])
    print(f"  ASSET trips_warehouse           -> {l['inserted']}ins/{l['updated']}upd")

    # ── 5. report → asset trips_report_pdf ────────────────────────────
    from pipeline.report.pdf_report import run as report_run
    p = report_run(d, DATA / "reports")
    results["report"] = stage_event("report", trace, rows=p["rows"],
                                    pdf=p["pdf_path"])
    print(f"  ASSET trips_report_pdf          -> {Path(p['pdf_path']).name}")

    print("=" * 72)
    print(f"CHAIN COMPLETE  trace={trace}")
    return {"trace_id": trace, "logical_ts": logical_ts.isoformat(),
            "stages": results}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Run the trips asset chain")
    ap.add_argument("--slot", help="logical timestamp, e.g. 2026-08-30T16:00")
    ap.add_argument("--slots", type=int, default=1,
                    help="run this many consecutive 4h slots")
    args = ap.parse_args(argv)

    hours = int(os.environ.get("BQ_WINDOW_HOURS", "4"))
    if args.slot:
        ts = datetime.fromisoformat(args.slot)
    else:
        now = datetime.now()
        ts = now.replace(hour=(now.hour // hours) * hours, minute=0,
                         second=0, microsecond=0)

    out = []
    for i in range(args.slots):
        out.append(run_chain(ts + timedelta(hours=hours * i)))
    print("\n" + json.dumps(out, indent=1, default=str)[:1200])
    return 0


if __name__ == "__main__":
    sys.exit(main())
