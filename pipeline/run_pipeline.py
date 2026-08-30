"""Run the whole chain for one logical date, without Airflow.

Airflow orchestrates this in production; this entry point exists so the data
path can be exercised and tested without a scheduler running. Same functions,
same order, same assertions.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("pipeline")

DATA = ROOT / "data"


def build_crypto_client():
    from pipeline.common.auth import TokenClient
    from pipeline.transform.spark_encrypt import CryptoClient
    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "http://localhost:8443"),
        client_id=os.environ.get("PIPELINE_CLIENT_ID", "spark-job"),
        client_secret=os.environ.get("CLIENT_SECRET_SPARK_JOB", "dev-spark-secret"),
        verify_tls=False,
    )
    return CryptoClient(os.environ.get("CRYPTO_URL", "http://localhost:8444"),
                        tokens, verify_tls=False)


def build_decrypt_client():
    from pipeline.common.auth import TokenClient
    from pipeline.transform.spark_encrypt import CryptoClient
    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "http://localhost:8443"),
        client_id="hive-job",
        client_secret=os.environ.get("CLIENT_SECRET_HIVE_JOB", "dev-hive-secret"),
        verify_tls=False,
    )
    return CryptoClient(os.environ.get("CRYPTO_URL", "http://localhost:8444"),
                        tokens, verify_tls=False)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--stage", default="all",
                    choices=["all", "extract", "encrypt", "mask", "load", "report"])
    args = ap.parse_args(argv)
    d = date.fromisoformat(args.date)
    results = {}

    if args.stage in ("all", "extract"):
        from pipeline.extract.fixture import get_extractor
        ex = get_extractor()
        r = ex.extract(d, DATA / "csv")
        mode = type(ex).__name__
        results["extract"] = {**r.as_dict(), "mode": mode}
        log.info("EXTRACT  %d rows via %s -> %s", r.row_count, mode, r.csv_path)

    if args.stage in ("all", "encrypt"):
        from pipeline.transform.spark_encrypt import run as encrypt_run
        r = encrypt_run(d, DATA / "csv" / f"trips_{d.isoformat()}.csv",
                        DATA / "parquet", build_crypto_client())
        results["encrypt"] = r
        log.info("ENCRYPT  %d rows -> %s (%s)", r["rows"], r["parquet_path"], r["engine"])

    if args.stage in ("all", "mask"):
        from pipeline.transform.hive_mask import run as mask_run
        r = mask_run(d, DATA / "parquet" / f"dt={d.isoformat()}" / "trips.parquet",
                     DATA / "hive" / "trips", build_decrypt_client())
        results["mask"] = r
        log.info("MASK     %d rows via %s -> %s", r["rows"], r["masked_by"], r["hive_path"])

    if args.stage in ("all", "load"):
        from pipeline.load.postgres_load import run as load_run
        r = load_run(d, DATA / "hive" / "trips" / f"dt={d.isoformat()}" / "trips_masked.parquet",
                     results.get("extract"))
        results["load"] = r
        log.info("LOAD     %d inserted / %d updated", r["inserted"], r["updated"])

    if args.stage in ("all", "report"):
        from pipeline.report.pdf_report import run as report_run
        r = report_run(d, DATA / "reports")
        results["report"] = r
        log.info("REPORT   %s", r["pdf_path"])

    print("\n" + json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
