"""The Kafka tap turns records into kafka_log rows, keeping the malformed ones."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.messaging import build, EVENT_ROW_FLAT   # noqa: E402
from pipeline.messaging.envelope import serialize       # noqa: E402
from services.tap import app as tap                     # noqa: E402

SLOT = datetime(2026, 9, 8, 4, tzinfo=timezone.utc)


def test_valid_message_becomes_a_typed_row():
    msg = build(event_type=EVENT_ROW_FLAT, dataset="trips", run_slot=SLOT, trace_id="t",
                run_id="r", try_number=1, key="k1", producer="pipeline", payload={"a": 1})
    row = tap.row_for("rpos_flat", 0, 42, serialize(msg))
    assert row[:5] == ("rpos_flat", 0, 42, "row.flat", "trips")
    assert row[5] == SLOT and row[6] == "t" and row[9] == "k1" and row[10] == "pipeline"
    assert json.loads(row[12]) == {"a": 1}
    assert json.loads(row[13])["envelope"]["event_id"] == msg["envelope"]["event_id"]


def test_contract_violation_is_kept_and_marked_invalid():
    row = tap.row_for("rpos_flat", 1, 7, b'{"envelope": {"event_type": "row.flat"}, "payload": {}}')
    assert row[3] == "invalid" and row[4] is None
    assert row[13].startswith('{"envelope"')


def test_tombstone_is_kept_and_marked_invalid():
    row = tap.row_for("pipeline_runs", 0, 0, None)
    assert row[3] == "invalid" and row[13] is None


def test_insert_dedupes_on_topic_partition_offset():
    assert 'ON CONFLICT (topic, partition, "offset") DO NOTHING' in tap.INSERT
    assert "monitor.kafka_log" in tap.DDL


def test_tap_never_reads_the_ciphertext_topic(monkeypatch):
    assert "rpos_encrypted" not in tap.TOPICS
