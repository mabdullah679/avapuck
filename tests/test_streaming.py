"""The data path streams: every stage walks rows in chunks, and the 100-row
run takes exactly the same path as a ten-million-row upload."""
from __future__ import annotations

import csv
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.common import batching                       # noqa: E402
from pipeline.metadata import schema as schema_mod          # noqa: E402
from pipeline.transform import card_split, hive_mask, spark_encrypt  # noqa: E402

HEADER = ["id", "customer_email", "sale_price", "card_info"]
VISA = "4111111111111111"


def _rows(n):
    return [[str(i), f"user{i}@example.com", f"{i}.50", VISA] for i in range(n)]


def _csv(tmp_path, n=7, name="sales_2026-01-01.csv"):
    p = tmp_path / name
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(_rows(n))
    schema_mod.write_manifest(schema_mod.infer(p), p)
    return p


class FakeCrypto:
    """Counts calls per field; ciphertext is marked, never the plaintext."""

    def __init__(self):
        self.calls: list[tuple[str, int]] = []

    def encrypt_column(self, field, values):
        self.calls.append((field, len(values)))
        import base64
        cts = [None if v is None else "enc:v1:" + base64.b64encode(b"\x00" * 24 + str(i).encode()).decode()
               for i, v in enumerate(values)]
        bis = [None if v is None else f"bi{field}{i}" for i, v in enumerate(values)]
        return cts, bis

    def decrypt_column(self, field, ciphertexts):
        self.calls.append((field, len(ciphertexts)))
        return [None if c is None else f"plain-{field}-{i}" for i, c in enumerate(ciphertexts)]


@pytest.fixture(autouse=True)
def _small_chunks(monkeypatch, tmp_path):
    monkeypatch.setenv("PIPELINE_BATCH_ROWS", "3")
    monkeypatch.delenv("SPARK_MASTER_URL", raising=False)
    monkeypatch.setattr(schema_mod, "DATASET_CONFIG_DIR", tmp_path / "datasets")


# ── batching helpers ──────────────────────────────────────────────────────

def test_chunked_splits_evenly_and_keeps_the_tail():
    assert list(batching.chunked(range(7), 3)) == [[0, 1, 2], [3, 4, 5], [6]]


def test_csv_chunks_respect_the_batch_size(tmp_path):
    p = _csv(tmp_path, 7)
    sizes = [len(c) for c in batching.iter_csv_chunks(p)]
    assert sizes == [3, 3, 1]


def test_parquet_sink_appends_chunks_into_one_file(tmp_path):
    out = tmp_path / "x.parquet"
    with batching.ParquetSink(out, ["a", "b"]) as sink:
        sink.write([{"a": "1", "b": 2}, {"a": None, "b": "y"}])
        sink.write([{"a": "3", "b": None}])
    assert batching.parquet_row_count(out) == 3
    rows = [r for c in batching.iter_parquet_batches(out, 2) for r in c]
    assert rows == [{"a": "1", "b": "2"}, {"a": None, "b": "y"}, {"a": "3", "b": None}]
    assert batching.parquet_columns(out) == ["a", "b"]


def test_empty_sink_still_writes_a_valid_file(tmp_path):
    out = tmp_path / "empty.parquet"
    with batching.ParquetSink(out, ["a"]):
        pass
    assert batching.parquet_row_count(out) == 0


# ── encrypt ───────────────────────────────────────────────────────────────

def test_encrypt_streams_one_crypto_call_per_column_per_chunk(tmp_path):
    p = _csv(tmp_path, 7)
    crypto = FakeCrypto()
    r = spark_encrypt.run(date(2026, 1, 1), p, tmp_path / "parquet", crypto)
    assert r["engine"] == "pyarrow" and r["rows"] == 7
    sensitive = set(r["sensitive_columns"])
    assert sensitive == {"customer_email", "card_info"}
    # 7 rows in chunks of 3 = 3 chunks; one call per sensitive column per chunk.
    for col in sensitive:
        assert [n for f, n in crypto.calls if f == col] == [3, 3, 1]

    out = Path(r["parquet_path"])
    rows = [x for c in batching.iter_parquet_batches(out, 100) for x in c]
    assert len(rows) == 7
    assert all(x["customer_email_encrypted"].startswith("enc:v1:") for x in rows)
    blob = out.read_bytes()
    assert b"user3@example.com" not in blob and VISA.encode() not in blob


def test_encrypt_output_columns_are_stable_and_all_string(tmp_path):
    p = _csv(tmp_path, 4)
    r = spark_encrypt.run(date(2026, 1, 1), p, tmp_path / "parquet", FakeCrypto())
    import pyarrow.parquet as pq
    schema = pq.read_schema(r["parquet_path"])
    assert all(str(t) == "string" for t in schema.types)
    m = schema_mod.load_manifest(p)
    assert schema.names == spark_encrypt.output_columns(m)


def test_encrypt_refuses_a_chunk_that_leaks(tmp_path):
    p = _csv(tmp_path, 4)

    class Leaky(FakeCrypto):
        def encrypt_column(self, field, values):
            return list(values), [None] * len(values)       # "ciphertext" == plaintext

    with pytest.raises(RuntimeError, match="REFUSING TO WRITE"):
        spark_encrypt.run(date(2026, 1, 1), p, tmp_path / "parquet", Leaky())


def test_executor_partition_function_is_self_contained(tmp_path, monkeypatch):
    """What runs on a Spark executor: builds its own client, yields tuples."""
    p = _csv(tmp_path, 5)
    m = schema_mod.load_manifest(p)
    fake = FakeCrypto()
    monkeypatch.setattr(spark_encrypt, "executor_crypto_client", lambda: fake)
    cols = spark_encrypt.output_columns(m)
    fn = spark_encrypt._encrypt_partition(m.to_json(), "2026-01-01", cols, 2)

    class Row(dict):
        def asDict(self):
            return dict(self)

    rows = [Row({c.name: v for c, v in zip(m.columns, r)}) for r in _rows(5)]
    out = list(fn(iter(rows)))
    assert len(out) == 5 and all(len(t) == len(cols) for t in out)
    assert [n for f, n in fake.calls if f == "customer_email"] == [2, 2, 1]


# ── mask ──────────────────────────────────────────────────────────────────

def test_mask_streams_and_never_writes_plaintext(tmp_path, monkeypatch):
    p = _csv(tmp_path, 7)
    enc = spark_encrypt.run(date(2026, 1, 1), p, tmp_path / "parquet", FakeCrypto())
    from pipeline.metadata import policy_gen
    monkeypatch.setattr(policy_gen, "POLICY_DIR", tmp_path / "ranger")
    crypto = FakeCrypto()
    r = hive_mask.run(date(2026, 1, 1), Path(enc["parquet_path"]), tmp_path / "hive" / "sales", crypto)
    assert r["rows"] == 7
    assert [n for f, n in crypto.calls if f == "customer_email"] == [3, 3, 1]
    rows = [x for c in batching.iter_parquet_batches(Path(r["hive_path"]), 100) for x in c]
    assert len(rows) == 7
    assert "customer_email_masked" in rows[0] and "customer_email_encrypted" not in rows[0]
    assert all(x["customer_email_masked"] != f"plain-customer_email-{i % 3}" for i, x in enumerate(rows))
    assert Path(r["hive_path"]).read_bytes().count(b"plain-") == 0


# ── load ──────────────────────────────────────────────────────────────────

def test_upsert_is_in_execute_values_form(tmp_path):
    p = _csv(tmp_path, 3)
    m = schema_mod.load_manifest(p)
    from pipeline.load import postgres_load as L
    cols = L._warehouse_columns(m, {"id", "logical_date", "run_slot", "trace_id",
                                    "sale_price", "customer_email_masked", "masked_by"})
    sql = L._build_upsert(m, cols)
    assert "VALUES %s" in sql and "ON CONFLICT (id)" in sql and "RETURNING (xmax = 0)" in sql
    assert [c for c, _ in cols][:4] == ["id", "logical_date", "run_slot", "trace_id"]


# ── card split ────────────────────────────────────────────────────────────

def test_card_split_streams_in_chunks(tmp_path):
    p = _csv(tmp_path, 7)
    crypto = FakeCrypto()
    r = card_split.run(date(2026, 1, 1), p, tmp_path / "cards", crypto)
    assert r["rows"] == 7 and r["cards"] == 1
    assert [n for f, n in crypto.calls if f == "card_info_middle"] == [3, 3, 1]


# ── uploads are uncapped ──────────────────────────────────────────────────

def test_csv_source_is_uncapped_when_told(tmp_path, monkeypatch):
    from pipeline.extract.csv_source import CsvSourceExtractor, row_limit_from_env
    src = tmp_path / "big.csv"
    with src.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "v"])
        w.writerows([[str(i), "x"] for i in range(250)])

    monkeypatch.setenv("CSV_ROW_LIMIT", "0")
    assert row_limit_from_env() is None
    r = CsvSourceExtractor(src, row_limit=None, dataset="big").extract(
        datetime(2026, 1, 1, tzinfo=timezone.utc), tmp_path / "out")
    assert r.row_count == 250

    monkeypatch.delenv("CSV_ROW_LIMIT")
    monkeypatch.setenv("BQ_ROW_LIMIT", "100")
    assert row_limit_from_env() == 100
    r = CsvSourceExtractor(src, row_limit=100, dataset="big").extract(
        datetime(2026, 1, 1, tzinfo=timezone.utc), tmp_path / "out2")
    assert r.row_count == 100


def test_extract_stage_honours_an_upload_in_conf(tmp_path, monkeypatch):
    """A console upload arrives as DAG-run conf and is uncapped."""
    from pipeline import stages
    src = tmp_path / "incoming" / "orders.csv"
    src.parent.mkdir()
    with src.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_email", "total"])
        w.writerows([[str(i), f"c{i}@x.com", "1"] for i in range(150)])

    monkeypatch.delenv("CSV_SOURCE_PATH", raising=False)
    monkeypatch.setenv("EXTRACT_MODE", "fixture")   # would be the fixture without conf
    env = stages.RunEnv.build(tmp_path / "data", datetime(2026, 1, 1, 4, tzinfo=timezone.utc),
                              conf={"csv_path": str(src), "dataset": "orders", "row_limit": 0,
                                    "source": "upload"})
    ev = stages.run_stage("extract", env, {})
    assert ev["source"] == "upload" and ev["dataset"] == "orders"
    assert ev["rows"] == 150 and ev["row_limit"] == 0
    assert ev["mode"] == "CsvSourceExtractor"
    assert "CSV_SOURCE_PATH" not in __import__("os").environ    # restored afterwards
