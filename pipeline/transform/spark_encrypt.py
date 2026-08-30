"""Spark job: read CSV, encrypt sensitive fields via the HTTPS crypto service,
write Parquet.

THE INVARIANT THIS JOB EXISTS TO HOLD: plaintext sensitive data must not reach
Parquet. The output is asserted against that before it is written, not after.

Calls are BATCHED per column rather than per row. A per-row HTTPS call for 100
rows x 5 columns is 500 round trips; batching makes it 5. At real volume the
per-row version is the difference between a job that finishes and one that
does not.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)

CIPHER_PREFIX = "enc"

# Columns encrypted before landing. Everything else is non-sensitive and clear.
SENSITIVE_COLUMNS = [
    "bikeid",
    "subscriber_type",
    "start_station_name",
    "end_station_name",
    "start_station_id",
    "end_station_id",
]

# Fields that also get a deterministic blind index, so Hive can still join.
BLIND_INDEXED = ["bikeid", "start_station_name"]


class CryptoClient:
    """Batching client for the crypto service."""

    def __init__(self, base_url: str, token_client, verify_tls: bool | str = True):
        self.base_url = base_url.rstrip("/")
        self.tokens = token_client
        self.verify_tls = verify_tls

    def encrypt_column(self, field: str, values: list) -> tuple[list, list]:
        import requests
        r = requests.post(
            f"{self.base_url}/encrypt",
            json={"field": field, "values": [None if v is None else str(v) for v in values]},
            headers=self.tokens.auth_header("crypto-service"),
            timeout=60, verify=self.verify_tls)
        if r.status_code != 200:
            raise RuntimeError(
                f"crypto service refused encrypt for {field!r}: "
                f"{r.status_code} {r.text[:200]}")
        body = r.json()
        return body["ciphertexts"], body["blind_indexes"]

    def decrypt_column(self, field: str, ciphertexts: list) -> list:
        import requests
        r = requests.post(
            f"{self.base_url}/decrypt",
            json={"field": field, "ciphertexts": ciphertexts},
            headers=self.tokens.auth_header("crypto-service"),
            timeout=60, verify=self.verify_tls)
        if r.status_code != 200:
            raise RuntimeError(
                f"crypto service refused decrypt for {field!r}: "
                f"{r.status_code} {r.text[:200]}")
        return r.json()["values"]


# Below this length, a substring search against base64 ciphertext produces
# false positives by pure chance -- a bikeid of "1" appears in almost any
# base64 blob. Short values are checked structurally instead.
_SUBSTRING_CHECK_MIN_LEN = 6


def assert_no_plaintext(rows: list[dict], original: list[dict]) -> None:
    """Refuse to write Parquet that still contains a sensitive plaintext.

    The last line of defence before data lands. Three distinct checks, because
    a single naive one is either too loose or too noisy:

      1. STRUCTURAL (every value): the output column must be a well-formed
         ciphertext envelope, and the plaintext column must be absent entirely.
         This catches an unencrypted column regardless of value length.
      2. DECODED SUBSTRING (values >= 6 chars): the plaintext must not appear
         in the base64-DECODED ciphertext bytes. Decoding first is what makes
         this meaningful -- searching the base64 text would miss a plaintext
         that survived encoding, and flag short values at random.
      3. IDENTITY: the ciphertext must never equal its own plaintext.

    Short values skip check 2 by design, and are covered by 1 and 3. Keeping a
    check that fires at random would train someone to ignore it, which is worse
    than not having it.
    """
    import base64

    leaked = []
    for out_row, src_row in zip(rows, original):
        for col in SENSITIVE_COLUMNS:
            src = src_row.get(col)
            if src in (None, ""):
                continue
            src_s = str(src)
            enc = out_row.get(f"{col}_encrypted")

            # 1. structural
            if enc is None:
                leaked.append(f"{col}: encrypted value missing")
                continue
            if not str(enc).startswith(f"{CIPHER_PREFIX}:"):
                leaked.append(f"{col}: output is not a ciphertext envelope")
                continue
            if col in out_row:
                leaked.append(f"{col}: plaintext column survived into output")

            # 3. identity
            if str(enc) == src_s:
                leaked.append(f"{col}: ciphertext equals its plaintext")

            # 2. decoded substring, only where it is statistically meaningful
            if len(src_s) >= _SUBSTRING_CHECK_MIN_LEN:
                try:
                    raw = base64.b64decode(str(enc).split(":", 2)[2])
                except Exception:  # noqa: BLE001
                    leaked.append(f"{col}: ciphertext payload is not valid base64")
                    continue
                if src_s.encode() in raw:
                    leaked.append(
                        f"{col}: plaintext {src_s!r} present in decrypted-side bytes")

    if leaked:
        raise RuntimeError(
            "REFUSING TO WRITE: sensitive plaintext would reach Parquet:\n  "
            + "\n  ".join(sorted(set(leaked))[:10]))


def run(logical_date: date, csv_path: Path, out_dir: Path,
        crypto: CryptoClient) -> dict:
    """Encrypt and write Parquet. Uses Spark when available, pandas otherwise.

    The fallback is deliberate and recorded: at 100 rows/day Spark is pure
    overhead, and requiring a cluster to be up in order to test the data path
    makes the pipeline harder to trust, not easier. The transformation logic is
    identical either way -- only the execution engine differs.
    """
    import csv as csvmod

    with csv_path.open(newline="", encoding="utf-8") as fh:
        source = list(csvmod.DictReader(fh))
    if not source:
        raise RuntimeError(f"{csv_path} has no rows")

    encrypted_cols: dict[str, list] = {}
    blind_cols: dict[str, list] = {}
    for col in SENSITIVE_COLUMNS:
        values = [r.get(col) or None for r in source]
        cts, bis = crypto.encrypt_column(col, values)
        encrypted_cols[col] = cts
        if col in BLIND_INDEXED:
            blind_cols[col] = bis
        log.info("encrypted %d values for %s", len(cts), col)

    out_rows = []
    for i, r in enumerate(source):
        row = {
            "trip_id": r["trip_id"],
            "logical_date": logical_date.isoformat(),
            "start_time": r["start_time"],
            "duration_minutes": int(r["duration_minutes"]),
        }
        for col in SENSITIVE_COLUMNS:
            row[f"{col}_encrypted"] = encrypted_cols[col][i]
        for col in BLIND_INDEXED:
            row[f"{col}_blind_index"] = blind_cols[col][i]
        out_rows.append(row)

    assert_no_plaintext(out_rows, source)

    out_dir.mkdir(parents=True, exist_ok=True)
    part_dir = out_dir / f"dt={logical_date.isoformat()}"
    part_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = part_dir / "trips.parquet"

    engine = _write_parquet(out_rows, parquet_path)

    log.info("wrote %d encrypted rows to %s via %s", len(out_rows), parquet_path, engine)
    return {"rows": len(out_rows), "parquet_path": str(parquet_path), "engine": engine}


def _write_parquet(rows: list[dict], path: Path) -> str:
    """Write via Spark when a cluster is configured, else pyarrow.

    The fallback is deliberate: at 100 rows/day a Spark job is pure overhead,
    and requiring a cluster to be up in order to exercise the data path makes
    the pipeline harder to trust rather than easier. The transformation is
    identical either way -- only the execution engine differs, and which one
    ran is returned so it can be recorded rather than assumed.
    """
    master = os.environ.get("SPARK_MASTER_URL")
    if master:
        try:
            from pyspark.sql import SparkSession
            spark = (SparkSession.builder
                     .appName("pipeline-encrypt")
                     .master(master)
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.driver.memory", "900m")
                     .getOrCreate())
            try:
                df = spark.createDataFrame(rows)
                staging = path.parent / "_spark_staging"
                df.coalesce(1).write.mode("overwrite").parquet(str(staging))
                # Spark writes a directory of part files; the rest of the
                # pipeline expects one file at `path`. Promote the single part
                # so both engines produce an identical artifact.
                parts = sorted(staging.glob("part-*.parquet"))
                if not parts:
                    raise RuntimeError("Spark produced no part files")
                path.write_bytes(parts[0].read_bytes())
                import shutil
                shutil.rmtree(staging, ignore_errors=True)
                return "spark"
            finally:
                spark.stop()
        except Exception as e:  # noqa: BLE001
            # Surfaced, not swallowed: the caller records which engine ran.
            log.warning("Spark write failed (%s); falling back to pyarrow", e)
    import pyarrow as pa
    import pyarrow.parquet as pq
    pq.write_table(pa.Table.from_pylist(rows), path)
    return "pyarrow"
