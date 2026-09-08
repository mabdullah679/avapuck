"""Encrypt stage: read CSV, encrypt sensitive fields via the HTTPS crypto
service, write Parquet. Streams; runs on Spark when a cluster is configured.

THE INVARIANT THIS JOB EXISTS TO HOLD: plaintext sensitive data must not reach
Parquet. Every chunk is asserted against that before it is written.

SHAPE (docs/DECISIONS.md D16)
The transformation is `encrypt_rows`, one chunk at a time: rename to safe
names, one crypto call per sensitive column per chunk, key the row, assert.
Two engines drive it:

  * Spark: the CSV is read by the cluster, and each EXECUTOR runs
    `encrypt_rows` over its partition in chunks, calling the crypto service
    itself with the spark-job identity (the worker containers carry that
    secret and the CA). That is what lets a multi-gigabyte upload spread its
    crypto calls across workers instead of funnelling through the driver.
  * pyarrow: the driver streams the CSV through the same function and appends
    to one Parquet file. Used when no cluster is configured, or as the
    reported fallback when the cluster fails.

Both write one Parquet file with an all-string schema and the manifest beside
it, so nothing downstream knows which engine ran; the stage event says.
"""
from __future__ import annotations

import logging
import os
import shutil
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)

CIPHER_PREFIX = "enc"

# Which columns are sensitive is a property of the DATASET, from the manifest.
# These names remain only as the documented expectation for the trips
# dataset, so a regression that silently stops classifying them is visible in
# tests rather than in production.
TRIPS_SENSITIVE_COLUMNS = [
    "bike_id",
    "subscriber_type",
    "start_station_name",
    "end_station_name",
    "start_station_id",
    "end_station_id",
]


class CryptoClient:
    """Batching client for the crypto service. One call per column per chunk."""

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
            timeout=120, verify=self.verify_tls)
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
            timeout=120, verify=self.verify_tls)
        if r.status_code != 200:
            raise RuntimeError(
                f"crypto service refused decrypt for {field!r}: "
                f"{r.status_code} {r.text[:200]}")
        return r.json()["values"]


def executor_crypto_client() -> CryptoClient:
    """The spark-job crypto client, built from THIS process's environment.

    Called on executors, where the driver's client object cannot travel. The
    worker containers carry CLIENT_SECRET_SPARK_JOB, IDP_URL, CRYPTO_URL and
    the CA bundle for exactly this purpose.
    """
    from pipeline.common.auth import TokenClient, tls_verify_from_env
    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "https://localhost:8443"),
        client_id="spark-job",
        client_secret=os.environ["CLIENT_SECRET_SPARK_JOB"],
        verify_tls=tls_verify_from_env(),
    )
    return CryptoClient(os.environ.get("CRYPTO_URL", "https://localhost:8444"),
                        tokens, verify_tls=tls_verify_from_env())


# Below this length, a substring search against base64 ciphertext produces
# false positives by pure chance -- a bikeid of "1" appears in almost any
# base64 blob. Short values are checked structurally instead.
_SUBSTRING_CHECK_MIN_LEN = 6


def assert_no_plaintext(rows: list[dict], original: list[dict],
                        sensitive_columns: list[str]) -> None:
    """Refuse to write Parquet that still contains a sensitive plaintext.

    The last line of defence before data lands. Three distinct checks, because
    a single naive one is either too loose or too noisy:

      1. STRUCTURAL (every value): the output column must be a well-formed
         ciphertext envelope, and the plaintext column must be absent entirely.
      2. DECODED SUBSTRING (values >= 6 chars): the plaintext must not appear
         in the base64-DECODED ciphertext bytes.
      3. IDENTITY: the ciphertext must never equal its own plaintext.

    Short values skip check 2 by design, and are covered by 1 and 3.
    """
    import base64

    leaked = []
    for out_row, src_row in zip(rows, original):
        for col in sensitive_columns:
            src = src_row.get(col)
            if src in (None, ""):
                continue
            src_s = str(src)
            enc = out_row.get(f"{col}_encrypted")

            if enc is None:
                leaked.append(f"{col}: encrypted value missing")
                continue
            if not str(enc).startswith(f"{CIPHER_PREFIX}:"):
                leaked.append(f"{col}: output is not a ciphertext envelope")
                continue
            # A sensitive column must not appear under its own name -- with one
            # exception: a sensitive PRIMARY KEY is carried as its blind index
            # under the key's name. Allowed ONLY when the value is demonstrably
            # not the plaintext, checked here rather than assumed.
            if col in out_row and str(out_row[col]) == src_s:
                leaked.append(f"{col}: plaintext column survived into output")

            if str(enc) == src_s:
                leaked.append(f"{col}: ciphertext equals its plaintext")

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


# ── The transformation, one chunk at a time ───────────────────────────────

def output_columns(manifest) -> list[str]:
    """The encrypted artifact's columns, in a stable order."""
    key = manifest.primary_key
    cols = [key, "logical_date"]
    cols += [c for c in manifest.public_columns if c != key]
    cols += [f"{c}_encrypted" for c in manifest.sensitive_columns]
    cols += [f"{c}_blind_index" for c in manifest.blind_indexed]
    return cols


def rename_to_safe(rows: list[dict], manifest) -> list[dict]:
    """CSV header names -> the manifest's SQL-safe names."""
    rename = {c.source_name: c.name for c in manifest.columns}
    return [{rename.get(k, k): v for k, v in r.items()} for r in rows]


def encrypt_rows(source: list[dict], manifest, logical_date_iso: str, crypto) -> list[dict]:
    """Encrypt one chunk of safe-named rows. Asserts before returning."""
    from pipeline.metadata import schema as schema_mod

    sensitive = manifest.sensitive_columns
    blind_indexed = manifest.blind_indexed
    key = manifest.primary_key
    public = manifest.public_columns
    key_is_sensitive = key in sensitive

    encrypted_cols: dict[str, list] = {}
    blind_cols: dict[str, list] = {}
    for col in sensitive:
        values = [r.get(col) or None for r in source]
        cts, bis = crypto.encrypt_column(col, values)
        encrypted_cols[col] = cts
        if col in blind_indexed:
            blind_cols[col] = bis

    all_names = [c.name for c in manifest.columns]
    out_rows = []
    for i, r in enumerate(source):
        if manifest.key_is_synthetic:
            key_value = schema_mod.row_hash(r, all_names, logical_date_iso)
        elif key_is_sensitive:
            # Keyed on the blind index: deterministic, so a re-run still
            # replaces the same row, but never the plaintext.
            key_value = blind_cols[key][i]
        else:
            key_value = r.get(key)
        row = {key: key_value, "logical_date": logical_date_iso}
        for col in public:
            if col != key:
                row[col] = r.get(col)
        for col in sensitive:
            row[f"{col}_encrypted"] = encrypted_cols[col][i]
        for col in blind_indexed:
            row[f"{col}_blind_index"] = blind_cols[col][i]
        out_rows.append(row)

    assert_no_plaintext(out_rows, source, sensitive)
    return out_rows


# ── Engines ───────────────────────────────────────────────────────────────

def run(logical_date: date, csv_path: Path, out_dir: Path,
        crypto: CryptoClient) -> dict:
    """Encrypt and write Parquet. Spark when configured, pyarrow otherwise.

    The fallback is deliberate and recorded: the stage event carries `engine`
    and the verdict names a pyarrow run as degraded only when Spark was
    configured. The transformation is identical either way.
    """
    from pipeline.common.batching import parquet_row_count
    from pipeline.metadata import schema as schema_mod

    csv_path = Path(csv_path)
    manifest = schema_mod.load_manifest(csv_path)
    d_iso = logical_date.isoformat()
    part_dir = Path(out_dir) / f"dt={d_iso}"
    part_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = part_dir / f"{manifest.dataset}.parquet"

    engine = None
    if os.environ.get("SPARK_MASTER_URL"):
        try:
            _write_via_spark(csv_path, parquet_path, manifest, d_iso)
            engine = "spark"
        except Exception as e:  # noqa: BLE001 -- surfaced in the event, not swallowed
            log.warning("Spark encrypt failed (%s); falling back to pyarrow", e)
    if engine is None:
        _write_via_pyarrow(csv_path, parquet_path, manifest, d_iso, crypto)
        engine = "pyarrow"

    rows = parquet_row_count(parquet_path)
    if rows == 0:
        raise RuntimeError(f"{csv_path} has no rows")

    # The manifest travels WITH the data to the next stage. Re-inferring it
    # from the Parquet would be guessing twice.
    schema_mod.write_manifest(manifest, parquet_path)

    log.info("wrote %d encrypted rows to %s via %s", rows, parquet_path, engine)
    return {"rows": rows, "parquet_path": str(parquet_path), "engine": engine,
            "dataset": manifest.dataset, "sensitive_columns": manifest.sensitive_columns}


def _write_via_pyarrow(csv_path: Path, parquet_path: Path, manifest, d_iso: str,
                       crypto) -> None:
    from pipeline.common.batching import ParquetSink, iter_csv_chunks
    with ParquetSink(parquet_path, output_columns(manifest)) as sink:
        for chunk in iter_csv_chunks(csv_path):
            sink.write(encrypt_rows(rename_to_safe(chunk, manifest), manifest, d_iso, crypto))


def _encrypt_partition(manifest_json: str, d_iso: str, columns: list[str], batch: int):
    """Build the executor-side function. Everything it needs is picklable."""
    def fn(rows):
        from pipeline.common.batching import chunked
        from pipeline.metadata.schema import SchemaManifest
        manifest = SchemaManifest.from_json(manifest_json)
        crypto = executor_crypto_client()
        for chunk in chunked((r.asDict() for r in rows), batch):
            for out in encrypt_rows(chunk, manifest, d_iso, crypto):
                yield tuple(None if out.get(c) is None else str(out.get(c)) for c in columns)
    return fn


def _write_via_spark(csv_path: Path, parquet_path: Path, manifest, d_iso: str) -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, StructField, StructType

    from pipeline.common.batching import batch_rows

    builder = (SparkSession.builder
               .appName("pipeline-encrypt")
               .master(os.environ["SPARK_MASTER_URL"])
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.driver.memory", "900m"))
    # In containers the driver must advertise a name the executors can
    # resolve; Spark otherwise offers the container's own hostname.
    driver_host = os.environ.get("SPARK_DRIVER_HOST")
    if driver_host:
        builder = (builder.config("spark.driver.host", driver_host)
                   .config("spark.driver.bindAddress", "0.0.0.0"))
    # The executor interpreter is chosen by the DRIVER's config; must match
    # this container's Python minor version.
    executor_python = os.environ.get("PYSPARK_PYTHON")
    if executor_python:
        builder = builder.config("spark.pyspark.python", executor_python)
    spark = builder.getOrCreate()
    try:
        df = spark.read.csv(str(csv_path), header=True, inferSchema=False)
        safe = {c.source_name: c.name for c in manifest.columns}
        df = df.toDF(*[safe.get(c, c) for c in df.columns])
        columns = output_columns(manifest)
        rdd = df.rdd.mapPartitions(
            _encrypt_partition(manifest.to_json(), d_iso, columns, batch_rows()))
        schema = StructType([StructField(c, StringType(), True) for c in columns])
        out = spark.createDataFrame(rdd, schema=schema)
        staging = parquet_path.parent / "_spark_staging"
        # One artifact file downstream, as with pyarrow. The single writer is a
        # bottleneck at scale (TRUST-BOUNDARY); the crypto work above is not.
        out.coalesce(1).write.mode("overwrite").parquet(str(staging))
        parts = sorted(staging.glob("part-*.parquet"))
        if not parts:
            raise RuntimeError("Spark produced no part files")
        shutil.move(str(parts[0]), str(parquet_path))
        shutil.rmtree(staging, ignore_errors=True)
    finally:
        spark.stop()
