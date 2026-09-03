"""Tests for the two-topic Kafka publisher.

The claim that matters: no sensitive value is ever published in the clear.
"""
import csv

import pytest

from pipeline.metadata import schema as schema_mod
from pipeline.publish import kafka_publish


class FakeProducer:
    """Records what was sent, per topic."""

    def __init__(self):
        self.sent = {}

    def send(self, topic, key=None, value=None):
        self.sent.setdefault(topic, []).append((key, value))

    def flush(self, timeout=None):
        pass

    def close(self, timeout=None):
        pass


def _publisher(fake):
    return kafka_publish.TwoTopicPublisher(
        "unused:9092", "t_enc", "t_pub", producer=fake)


def _dataset(tmp_path, rows, header):
    csv_path = tmp_path / "ds_2026-01-01.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    schema_mod.write_manifest(schema_mod.infer(csv_path), csv_path)
    return csv_path


class TestSplit:
    def test_sensitive_goes_only_to_the_encrypted_topic(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        csv_path = _dataset(
            tmp_path,
            [["1", "a@x.com", "9.99"], ["2", "b@x.com", "1.50"],
             ["3", "c@x.com", "4.25"]],
            ["id", "customer_email", "sale_price"])

        # Stand in for the encrypt stage's output.
        parquet = tmp_path / "enc.parquet"
        pq.write_table(pa.table({
            "id": ["1", "2", "3"],
            "customer_email_encrypted": ["enc:v1:aaa", "enc:v1:bbb", "enc:v1:ccc"],
            "sale_price": ["9.99", "1.50", "4.25"],
        }), parquet)
        manifest = schema_mod.load_manifest(csv_path)
        schema_mod.write_manifest(manifest, parquet)

        fake = FakeProducer()
        r = kafka_publish.run("2026-01-01", parquet, publisher=_publisher(fake))

        assert r["rows"] == 3
        enc = fake.sent["t_enc"]
        pub = fake.sent["t_pub"]
        assert len(enc) == len(pub) == 3

        # The ciphertext is on the encrypted topic...
        assert any("customer_email_encrypted" in v for _, v in enc)
        # ...and no plaintext email is on either.
        blob = str(fake.sent)
        assert "a@x.com" not in blob
        assert "b@x.com" not in blob

    def test_public_payload_carries_business_columns(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        csv_path = _dataset(
            tmp_path,
            [["1", "a@x.com", "9.99"], ["2", "b@x.com", "1.50"],
             ["3", "c@x.com", "4.25"]],
            ["id", "customer_email", "sale_price"])
        parquet = tmp_path / "enc.parquet"
        pq.write_table(pa.table({
            "id": ["1", "2", "3"],
            "customer_email_encrypted": ["enc:v1:a", "enc:v1:b", "enc:v1:c"],
            "sale_price": ["9.99", "1.50", "4.25"],
        }), parquet)
        schema_mod.write_manifest(schema_mod.load_manifest(csv_path), parquet)

        fake = FakeProducer()
        kafka_publish.run("2026-01-01", parquet, publisher=_publisher(fake))

        _, first = fake.sent["t_pub"][0]
        assert "sale_price" in first
        assert not any(k.startswith("customer_email") for k in first)

    def test_both_topics_get_the_same_row_count(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        csv_path = _dataset(tmp_path, [[str(i), f"{i}@x.com", "1"] for i in range(5)],
                            ["id", "customer_email", "sale_price"])
        parquet = tmp_path / "enc.parquet"
        pq.write_table(pa.table({
            "id": [str(i) for i in range(5)],
            "customer_email_encrypted": [f"enc:v1:{i}" for i in range(5)],
            "sale_price": ["1"] * 5,
        }), parquet)
        schema_mod.write_manifest(schema_mod.load_manifest(csv_path), parquet)

        fake = FakeProducer()
        kafka_publish.run("2026-01-01", parquet, publisher=_publisher(fake))
        assert len(fake.sent["t_enc"]) == len(fake.sent["t_pub"]) == 5


class TestGuard:
    def test_refuses_if_a_sensitive_column_reaches_the_public_topic(self):
        """The split is manifest-derived, so this should be impossible --
        which is exactly why it is asserted."""
        class M:
            sensitive_columns = ["customer_email"]

        with pytest.raises(AssertionError, match="sensitive"):
            kafka_publish._assert_no_plaintext_published(
                M(), [], ["sale_price", "customer_email"])

    def test_passes_when_the_split_is_clean(self):
        class M:
            sensitive_columns = ["customer_email"]

        kafka_publish._assert_no_plaintext_published(M(), [], ["sale_price"])
