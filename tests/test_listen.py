"""The trigger-side filter: wakes only on a valid, positive verdict."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.messaging import build, EVENT_ROW_FLAT, EVENT_RUN_VERDICT   # noqa: E402
from pipeline.messaging.envelope import serialize                        # noqa: E402
from pipeline.messaging.listen import accept_verdict                      # noqa: E402

SLOT = datetime(2026, 9, 8, 4, tzinfo=timezone.utc)
PAYLOAD = {"proceed": True, "status": "complete", "reasons": [], "stages": {},
           "outputs": {"warehouse_table": "warehouse.t", "hive_table": None, "pdf": None,
                       "rows_published": 1, "rows_loaded": 1, "topics": {}},
           "data_window": {"start": None, "end": None, "source": "logical_date"},
           "computed_at": "2026-09-08T04:09:00Z"}


class Msg:
    def __init__(self, value):
        self._v = value

    def value(self):
        return self._v


def _verdict(proceed=True):
    msg = build(event_type=EVENT_RUN_VERDICT, dataset="t", run_slot=SLOT, trace_id="tr", run_id="r",
                try_number=1, key=None, producer="pipeline", payload={**PAYLOAD, "proceed": proceed},
                signature="a.b.c")
    return Msg(serialize(msg))


def test_positive_verdict_wakes_with_the_message():
    out = accept_verdict(_verdict(True))
    assert out["envelope"]["event_type"] == "run.verdict" and out["payload"]["proceed"] is True


def test_negative_verdict_does_not_wake():
    assert accept_verdict(_verdict(False)) is None


def test_row_messages_do_not_wake():
    row = build(event_type=EVENT_ROW_FLAT, dataset="t", run_slot=SLOT, trace_id="tr", run_id="r",
                try_number=1, key="k", producer="pipeline", payload={"a": 1})
    assert accept_verdict(Msg(serialize(row))) is None


def test_contract_violations_and_tombstones_do_not_wake():
    assert accept_verdict(Msg(b'{"envelope": {"event_type": "run.verdict"}}')) is None
    assert accept_verdict(Msg(None)) is None


def test_finds_the_message_among_airflows_serialized_arguments():
    """Seen live: the asset-watcher path calls f("__var", "list", message,
    __var={}, __type="dict"). The message is found by shape, not position."""
    out = accept_verdict("__var", "list", _verdict(True), __var={}, __type="dict")
    assert out is not None and out["payload"]["proceed"] is True
    assert accept_verdict(_verdict(True)) is not None            # the plain shape still works


def test_no_message_at_all_keeps_waiting():
    assert accept_verdict("__var", "list", __var={}) is None
