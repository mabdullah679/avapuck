"""The envelope's two identifiers behave as docs/DECISIONS.md D2 says."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.messaging import envelope as E          # noqa: E402
from pipeline.messaging.contracts import ContractError  # noqa: E402

SLOT = datetime(2026, 9, 6, 4, 0, 0, tzinfo=timezone.utc)


def _build(**kw):
    base = dict(event_type=E.EVENT_ROW_FLAT, dataset="trips", run_slot=SLOT,
                trace_id="trips-20260906T0400-1a2b3c", run_id="scheduled__2026-09-06T04:00:00+00:00",
                try_number=1, key="abc123", producer="pipeline",
                payload={"sale_price": "9.99"})
    base.update(kw)
    return E.build(**base)


class TestIdentifiers:
    def test_idempotency_key_ignores_run_id_and_try(self):
        a = _build(run_id="run-1", try_number=1)
        b = _build(run_id="run-2", try_number=3)
        assert a["envelope"]["idempotency_key"] == b["envelope"]["idempotency_key"]

    def test_event_id_changes_with_run_id(self):
        a = _build(run_id="run-1")
        b = _build(run_id="run-2")
        assert a["envelope"]["event_id"] != b["envelope"]["event_id"]

    def test_event_id_changes_with_try_number(self):
        a = _build(try_number=1)
        b = _build(try_number=2)
        assert a["envelope"]["event_id"] != b["envelope"]["event_id"]

    def test_same_emission_gives_same_event_id(self):
        """A Kafka redelivery is byte-identical, so the id it carries is too."""
        assert _build()["envelope"]["event_id"] == _build()["envelope"]["event_id"]

    def test_idempotency_key_differs_per_row(self):
        assert (_build(key="a")["envelope"]["idempotency_key"]
                != _build(key="b")["envelope"]["idempotency_key"])

    def test_idempotency_key_differs_per_slot(self):
        other = SLOT.replace(hour=8)
        assert (_build()["envelope"]["idempotency_key"]
                != _build(run_slot=other)["envelope"]["idempotency_key"])

    def test_idempotency_key_differs_per_event_type(self):
        assert (_build(event_type=E.EVENT_ROW_FLAT)["envelope"]["idempotency_key"]
                != _build(event_type=E.EVENT_ROW_ENCRYPTED,
                          payload={"x_encrypted": "enc:v1:aa"})["envelope"]["idempotency_key"])

    def test_identifiers_are_32_hex(self):
        env = _build()["envelope"]
        for f in ("event_id", "idempotency_key"):
            assert len(env[f]) == 32 and int(env[f], 16) >= 0

    def test_key_is_recomputable_by_anyone(self):
        env = _build()["envelope"]
        assert env["idempotency_key"] == E.idempotency_key(
            E.EVENT_ROW_FLAT, "trips", SLOT, "abc123")
        assert env["event_id"] == E.event_id(
            env["idempotency_key"], env["run_id"], env["try_number"])


class TestTimestamps:
    def test_run_slot_and_logical_date_are_utc(self):
        env = _build()["envelope"]
        assert env["run_slot"] == "2026-09-06T04:00:00Z"
        assert env["logical_date"] == "2026-09-06"

    def test_naive_datetime_is_treated_as_utc(self):
        assert _build(run_slot=datetime(2026, 9, 6, 4))["envelope"]["run_slot"] == "2026-09-06T04:00:00Z"

    def test_aware_non_utc_is_converted(self):
        from datetime import timedelta
        est = timezone(timedelta(hours=-4))
        assert (_build(run_slot=datetime(2026, 9, 6, 0, tzinfo=est))["envelope"]["run_slot"]
                == "2026-09-06T04:00:00Z")

    def test_slot_label_is_filename_safe(self):
        assert E.slot_label(SLOT) == "20260906T0400Z"

    def test_produced_at_is_iso_z(self):
        assert _build()["envelope"]["produced_at"].endswith("Z")


class TestShape:
    def test_key_is_stringified_or_null(self):
        assert _build(key=42)["envelope"]["key"] == "42"
        assert _build(key=None)["envelope"]["key"] is None

    def test_unknown_event_type_is_refused(self):
        with pytest.raises(ValueError, match="unknown event_type"):
            _build(event_type="row.plaintext")

    def test_payload_must_be_a_dict(self):
        with pytest.raises(TypeError):
            _build(payload=["not", "a", "dict"])

    def test_meta_and_payload_no_longer_share_a_namespace(self):
        """The old flat dict let a column named `dataset` overwrite the metadata."""
        msg = _build(payload={"dataset": "NOT-trips", "logical_date": "1999-01-01"})
        assert msg["envelope"]["dataset"] == "trips"
        assert msg["payload"]["dataset"] == "NOT-trips"

    def test_signature_only_present_when_given(self):
        assert "signature" not in _build()
        assert _build(signature="a.b.c")["signature"] == "a.b.c"


class TestParse:
    def test_roundtrip_bytes(self):
        msg = _build()
        assert E.parse(E.serialize(msg)) == json.loads(E.serialize(msg))

    def test_parse_validates(self):
        msg = _build()
        del msg["envelope"]["trace_id"]
        with pytest.raises(ContractError, match="trace_id"):
            E.parse(msg)

    def test_parse_rejects_non_json(self):
        with pytest.raises(ContractError, match="not JSON"):
            E.parse(b"{nope")

    def test_parse_rejects_missing_envelope(self):
        with pytest.raises(ContractError, match="no envelope"):
            E.parse({"payload": {}})

    def test_parse_rejects_unknown_event_type(self):
        msg = _build()
        msg["envelope"]["event_type"] = "row.mystery"
        with pytest.raises(ContractError):
            E.parse(msg)

    def test_encrypted_payload_must_be_ciphertext(self):
        msg = _build(event_type=E.EVENT_ROW_ENCRYPTED,
                     payload={"email_encrypted": "alice@example.com"})
        with pytest.raises(ContractError, match="email_encrypted"):
            E.parse(msg)

    def test_encrypted_payload_rejects_plain_column_names(self):
        msg = _build(event_type=E.EVENT_ROW_ENCRYPTED, payload={"email": "enc:v1:aa"})
        with pytest.raises(ContractError):
            E.parse(msg)

    def test_flat_payload_rejects_ciphertext_columns(self):
        msg = _build(payload={"email_encrypted": "enc:v1:aa"})
        with pytest.raises(ContractError):
            E.parse(msg)

    def test_verdict_requires_signature(self):
        msg = _build(event_type=E.EVENT_RUN_VERDICT, key=None, payload={
            "proceed": True, "status": "complete", "reasons": [], "stages": {},
            "outputs": {"warehouse_table": None, "hive_table": None, "pdf": None,
                        "rows_published": None, "rows_loaded": None, "topics": {}},
            "data_window": {"start": None, "end": None, "source": "unknown"},
            "computed_at": "2026-09-06T04:09:00Z"})
        with pytest.raises(ContractError, match="signature"):
            E.parse(msg)


def test_canonical_json_is_deterministic_and_compact():
    a = E.canonical_json({"b": 1, "a": [1, 2], "c": {"z": None, "y": "x"}})
    b = E.canonical_json({"c": {"y": "x", "z": None}, "a": [1, 2], "b": 1})
    assert a == b == '{"a":[1,2],"b":1,"c":{"y":"x","z":null}}'
