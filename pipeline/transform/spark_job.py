"""Spark entry point for the encrypt stage. Submitted by spark_submit.sh.

Runs inside the Spark master container, where the JDK is 17. Calls the same
`run()` used by the local path, so there is exactly one implementation of the
encryption logic and no chance of the two drifting apart.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, "/opt/pipeline")

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")

from pipeline.common.auth import TokenClient, tls_verify_from_env  # noqa: E402
from pipeline.transform.spark_encrypt import CryptoClient, run   # noqa: E402

DATA = Path(os.environ.get("PIPELINE_DATA_ROOT") or "/opt/pipeline/data")


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: spark_job.py YYYY-MM-DD", file=sys.stderr)
        return 2
    d = date.fromisoformat(sys.argv[1])

    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "https://idp:8443"),
        client_id="spark-job",
        client_secret=os.environ["CLIENT_SECRET_SPARK_JOB"],
        verify_tls=tls_verify_from_env(),
    )
    crypto = CryptoClient(os.environ.get("CRYPTO_URL", "https://crypto:8444"),
                          tokens, verify_tls=tls_verify_from_env())

    result = run(d, DATA / "csv" / f"trips_{d.isoformat()}.csv",
                 DATA / "parquet", crypto)
    print(f"\nENCRYPT {result['rows']} rows via {result['engine']} "
          f"-> {result['parquet_path']}")
    if result["engine"] != "spark":
        print("WARNING: fell back to pyarrow; this was meant to run on Spark",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
