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

DATA = Path(os.environ.get("PIPELINE_DATA_ROOT") or (ROOT / "data"))


def build_crypto_client():
    from pipeline.common.auth import TokenClient, tls_verify_from_env
    from pipeline.transform.spark_encrypt import CryptoClient
    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "https://localhost:8443"),
        client_id=os.environ.get("PIPELINE_CLIENT_ID", "spark-job"),
        client_secret=os.environ.get("CLIENT_SECRET_SPARK_JOB", "dev-spark-secret"),
        verify_tls=tls_verify_from_env(),
    )
    return CryptoClient(os.environ.get("CRYPTO_URL", "https://localhost:8444"),
                        tokens, verify_tls=tls_verify_from_env())


def build_decrypt_client():
    from pipeline.common.auth import TokenClient, tls_verify_from_env
    from pipeline.transform.spark_encrypt import CryptoClient
    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "https://localhost:8443"),
        client_id="hive-job",
        client_secret=os.environ.get("CLIENT_SECRET_HIVE_JOB", "dev-hive-secret"),
        verify_tls=tls_verify_from_env(),
    )
    return CryptoClient(os.environ.get("CRYPTO_URL", "https://localhost:8444"),
                        tokens, verify_tls=tls_verify_from_env())


def _find(directory: Path, pattern: str, stage: str) -> Path:
    """Locate a stage's input by pattern; see the same helper in the DAG."""
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not matches:
        raise FileNotFoundError(
            f"{stage}: no file matching {pattern!r} in {directory}. Run the "
            f"previous stage first.")
    return matches[-1]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--stage", default="all",
                    choices=["all", "extract", "encrypt", "mask", "load", "report"])
    ap.add_argument("--csv", help="ingest this CSV instead of the configured "
                                  "extractor (sets CSV_SOURCE_PATH)")
    args = ap.parse_args(argv)
    d = date.fromisoformat(args.date)
    if args.csv:
        os.environ["CSV_SOURCE_PATH"] = args.csv
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
        csv_path = _find(DATA / "csv", f"*_{d.isoformat()}.csv", "encrypt")
        r = encrypt_run(d, csv_path, DATA / "parquet", build_crypto_client())
        results["encrypt"] = r
        log.info("ENCRYPT  %d rows -> %s (%s)", r["rows"], r["parquet_path"], r["engine"])

    if args.stage in ("all", "mask"):
        from pipeline.transform.hive_mask import run as mask_run
        pq = _find(DATA / "parquet" / f"dt={d.isoformat()}", "*.parquet", "mask")
        r = mask_run(d, pq, DATA / "hive" / pq.stem, build_decrypt_client())
        results["mask"] = r
        log.info("MASK     %d rows via %s -> %s", r["rows"], r["masked_by"], r["hive_path"])

    if args.stage in ("all", "load"):
        from pipeline.load.postgres_load import run as load_run
        pq = _find(DATA / "hive", f"*/dt={d.isoformat()}/*_masked.parquet", "load")
        r = load_run(d, pq, results.get("extract"))
        results["load"] = r
        log.info("LOAD     %d inserted / %d updated into warehouse.%s",
                 r["inserted"], r["updated"], r["dataset"])

    if args.stage in ("all", "report"):
        from pipeline.metadata import schema as schema_mod
        from pipeline.report.pdf_report import run as report_run
        pq = _find(DATA / "hive", f"*/dt={d.isoformat()}/*_masked.parquet", "report")
        manifest = schema_mod.load_manifest(pq)
        r = report_run(d, DATA / "reports", dataset=manifest.dataset,
                       primary_key=manifest.primary_key)
        results["report"] = r
        log.info("REPORT   %s", r["pdf_path"])

    print("\n" + json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
