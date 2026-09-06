"""Tests for the Kafka publisher: rows to two topics, the verdict to a third.

The claims that matter: no sensitive value is ever published in the clear,
and every message that leaves satisfies its contract.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone

import pytest

from pipeline.messaging import envelope as E
from pipeline.messaging.contracts import ContractError, validate_message
from pipeline.metadata import schema as schema_mod
from pipeline.publish import kafka_publish as KP

SLOT = datetime(2026, 1, 1, 4, tzinfo=timezone.utc)
CTX = KP.RunContext(run_slot=SLOT, trace_id="ds-20260101T0400-abcdef",
                    run_id="scheduled__2026-01-01T04:00:00+00:00", try_number=1)
TOPICS = {"encrypted": "t_enc", "public": "t_pub", "control": "t_ctl"}


class FakeProducer:
    """Records what was sent, per topic, as the bytes that would hit the wire."""

    def __init__(self):
        self.sent: dict[str, list] = {}

    def send(self, topic, key=None, value=None):
        self.sent.setdefault(topic, []).append((key, E.serialize(value)))

    def flush(self, timeout=None):
        pass

    def close(self, timeout=None):
        pass

    def messages(self, topic):
        return [json.loads(v) for _, v in self.sent.get(topic, [])]


def _publisher(fake):
    return KP.Publisher("unused:9092", producer=fake)


def _dataset(tmp_path, rows, header):
    csv_path = tmp_path / "ds_2026-01-01.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    schema_mod.write_manifest(schema_mod.infer(csv_path), csv_path)
    return csv_path


def _encrypted_parquet(tmp_path, n=3):
    import pyarrow as pa
    import pyarrow.parquet as pq

    csv_path = _dataset(tmp_path, [[str(i), f"{i}@x.com", "9.99"] for i in range(n)],
                        ["id", "customer_email", "sale_price"])
    parquet = tmp_path / "enc.parquet"
    pq.write_table(pa.table({
        "id": [str(i) for i in range(n)],
        "logical_date": ["2026-01-01"] * n,
        "customer_email_encrypted": [f"enc:v1:{'a' * 8}{i}" for i in range(n)],
        "sale_price": ["9.99"] * n,
    }), parquet)
    schema_mod.write_manifest(schema_mod.load_manifest(csv_path), parquet)
    return parquet


class TestRows:
    def test_sensitive_goes_only_to_the_encrypted_topic(self, tmp_path):
        fake = FakeProducer()
        r = KP.publish_rows(CTX, _encrypted_parquet(tmp_path), publisher=_publisher(fake), topics=TOPICS)

        assert r["rows"] == 3
        enc, pub = fake.messages("t_enc"), fake.messages("t_pub")
        assert len(enc) == len(pub) == 3
        assert all("customer_email_encrypted" in m["payload"] for m in enc)
        blob = str(fake.sent)
        assert "0@x.com" not in blob and "1@x.com" not in blob

    def test_every_message_satisfies_its_contract(self, tmp_path):
        fake = FakeProducer()
        KP.publish_rows(CTX, _encrypted_parquet(tmp_path), publisher=_publisher(fake), topics=TOPICS)
        for topic in ("t_enc", "t_pub"):
            for m in fake.messages(topic):
                validate_message(m)

    def test_envelope_carries_the_run_and_the_row_key(self, tmp_path):
        fake = FakeProducer()
        KP.publish_rows(CTX, _encrypted_parquet(tmp_path), publisher=_publisher(fake), topics=TOPICS)
        env = fake.messages("t_pub")[0]["envelope"]
        assert env["event_type"] == "row.flat"
        assert env["dataset"] == "ds"
        assert env["run_slot"] == "2026-01-01T04:00:00Z"
        assert env["logical_date"] == "2026-01-01"
        assert env["trace_id"] == CTX.trace_id
        assert env["run_id"] == CTX.run_id
        assert env["key"] == "0"
        assert env["producer"] == "pipeline"
        # The Kafka record key is the row key, so a row's history stays ordered.
        assert fake.sent["t_pub"][0][0] == "0"

    def test_flat_payload_carries_business_columns_only(self, tmp_path):
        fake = FakeProducer()
        KP.publish_rows(CTX, _encrypted_parquet(tmp_path), publisher=_publisher(fake), topics=TOPICS)
        payload = fake.messages("t_pub")[0]["payload"]
        assert "sale_price" in payload
        assert not any(k.startswith("customer_email") for k in payload)

    def test_encrypted_and_flat_share_the_idempotency_scheme_but_not_the_key(self, tmp_path):
        fake = FakeProducer()
        KP.publish_rows(CTX, _encrypted_parquet(tmp_path), publisher=_publisher(fake), topics=TOPICS)
        enc, pub = fake.messages("t_enc")[0]["envelope"], fake.messages("t_pub")[0]["envelope"]
        assert enc["key"] == pub["key"]
        assert enc["idempotency_key"] != pub["idempotency_key"]   # different event types

    def test_rerun_produces_same_idempotency_keys_and_new_event_ids(self, tmp_path):
        pq = _encrypted_parquet(tmp_path)
        a, b = FakeProducer(), FakeProducer()
        KP.publish_rows(CTX, pq, publisher=_publisher(a), topics=TOPICS)
        rerun = KP.RunContext(run_slot=SLOT, trace_id=CTX.trace_id, run_id="manual__repair", try_number=1)
        KP.publish_rows(rerun, pq, publisher=_publisher(b), topics=TOPICS)
        ea, eb = a.messages("t_pub")[0]["envelope"], b.messages("t_pub")[0]["envelope"]
        assert ea["idempotency_key"] == eb["idempotency_key"]
        assert ea["event_id"] != eb["event_id"]

    def test_both_topics_get_the_same_row_count(self, tmp_path):
        fake = FakeProducer()
        KP.publish_rows(CTX, _encrypted_parquet(tmp_path, n=5), publisher=_publisher(fake), topics=TOPICS)
        assert len(fake.sent["t_enc"]) == len(fake.sent["t_pub"]) == 5


class TestGuard:
    def test_refuses_if_a_sensitive_column_reaches_the_public_topic(self):
        class M:
            sensitive_columns = ["customer_email"]

        with pytest.raises(AssertionError, match="sensitive"):
            KP._assert_no_plaintext_published(M(), ["sale_price", "customer_email"])

    def test_passes_when_the_split_is_clean(self):
        class M:
            sensitive_columns = ["customer_email"]

        KP._assert_no_plaintext_published(M(), ["sale_price"])

    def test_publisher_refuses_a_message_that_violates_its_contract(self):
        fake = FakeProducer()
        with _publisher(fake) as p:
            with pytest.raises(ContractError):
                p.send("t_pub", "k", {"envelope": {"event_type": "row.flat"}, "payload": {}})
        assert fake.sent == {}


class FakeSigner:
    def __init__(self):
        self.calls = []

    def sign(self, envelope, payload):
        self.calls.append((envelope["event_id"], payload["proceed"]))
        return "fake.jws.signature"


class TestVerdict:
    PAYLOAD = {"proceed": True, "status": "complete", "reasons": [], "stages": {},
               "outputs": {"warehouse_table": "warehouse.ds", "hive_table": None,
                           "pdf": "/x.pdf", "rows_published": 3, "rows_loaded": 3, "topics": {}},
               "data_window": {"start": "2026-01-01", "end": "2026-01-01", "source": "logical_date"},
               "computed_at": "2026-01-01T04:09:00Z"}

    def test_verdict_is_signed_then_sent_to_the_control_topic(self):
        fake, signer = FakeProducer(), FakeSigner()
        r = KP.publish_verdict(CTX, "ds", self.PAYLOAD, signer, publisher=_publisher(fake), topics=TOPICS)
        [msg] = fake.messages("t_ctl")
        validate_message(msg)
        assert msg["signature"] == "fake.jws.signature"
        assert msg["envelope"]["event_type"] == "run.verdict"
        assert msg["envelope"]["key"] is None
        assert fake.sent["t_ctl"][0][0] == "ds"          # keyed by dataset
        assert signer.calls == [(msg["envelope"]["event_id"], True)]
        assert r["proceed"] is True and r["topic"] == "t_ctl"

    def test_verdict_idempotency_key_is_per_slot(self):
        fake, signer = FakeProducer(), FakeSigner()
        KP.publish_verdict(CTX, "ds", self.PAYLOAD, signer, publisher=_publisher(fake), topics=TOPICS)
        other = KP.RunContext(run_slot=SLOT.replace(hour=8), trace_id="t", run_id="r")
        KP.publish_verdict(other, "ds", self.PAYLOAD, signer, publisher=_publisher(fake), topics=TOPICS)
        a, b = fake.messages("t_ctl")
        assert a["envelope"]["idempotency_key"] != b["envelope"]["idempotency_key"]

    def test_unsigned_verdict_cannot_be_sent(self):
        class NoSig:
            def sign(self, envelope, payload):
                return None

        fake = FakeProducer()
        with pytest.raises(ContractError):
            KP.publish_verdict(CTX, "ds", self.PAYLOAD, NoSig(), publisher=_publisher(fake), topics=TOPICS)
        assert fake.sent == {}


def test_coerce_slot_accepts_the_shapes_callers_use():
    from datetime import date
    assert KP.coerce_slot(date(2026, 1, 1)) == datetime(2026, 1, 1)
    assert KP.coerce_slot("2026-01-01T04:00:00Z") == SLOT
    assert KP.coerce_slot(SLOT) is SLOT
