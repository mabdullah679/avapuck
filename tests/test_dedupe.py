"""Drop a repeat, upsert a replay, process a first sight. In that order."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.messaging import build, EVENT_ROW_FLAT              # noqa: E402
from pipeline.messaging.dedupe import InMemorySeenStore, accept   # noqa: E402

SLOT = datetime(2026, 9, 6, 4, tzinfo=timezone.utc)


def _msg(run_id="run-1", try_number=1, key="k1"):
    return build(event_type=EVENT_ROW_FLAT, dataset="trips", run_slot=SLOT, trace_id="t",
                 run_id=run_id, try_number=try_number, key=key, producer="pipeline",
                 payload={"sale_price": "1"})


def test_first_sight_is_processed():
    d = accept(InMemorySeenStore(), "team-a", _msg())
    assert d.process and not d.replay and d.reason == "first sight"


def test_exact_redelivery_is_dropped():
    store = InMemorySeenStore()
    accept(store, "team-a", _msg())
    d = accept(store, "team-a", _msg())
    assert not d.process
    assert "already consumed" in d.reason


def test_retry_is_processed_as_a_replay():
    """Same unit of work, new emission: process it, and it supersedes."""
    store = InMemorySeenStore()
    accept(store, "team-a", _msg(try_number=1))
    d = accept(store, "team-a", _msg(try_number=2))
    assert d.process and d.replay
    assert "supersedes" in d.reason


def test_rerun_of_the_slot_is_a_replay_too():
    store = InMemorySeenStore()
    accept(store, "team-a", _msg(run_id="scheduled__x"))
    d = accept(store, "team-a", _msg(run_id="manual__y"))
    assert d.process and d.replay


def test_different_rows_are_independent():
    store = InMemorySeenStore()
    accept(store, "team-a", _msg(key="k1"))
    d = accept(store, "team-a", _msg(key="k2"))
    assert d.process and not d.replay


def test_dedupe_is_per_consumer():
    store = InMemorySeenStore()
    accept(store, "team-a", _msg())
    d = accept(store, "team-b", _msg())
    assert d.process and not d.replay


def test_postgres_store_sql_is_well_formed():
    """No database in unit tests; check the statements the store would run."""
    from pipeline.messaging.dedupe import PostgresSeenStore

    class Cur:
        def __init__(self): self.sql = []; self._rows = []
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, sql, params=None): self.sql.append(" ".join(sql.split()))
        def fetchone(self): return (0,) if "count(*)" in self.sql[-1] else (1,)

    class Conn:
        def __init__(self): self.cur = Cur()
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return self.cur

    conn = Conn()
    store = PostgresSeenStore(conn)
    store.ensure_table()
    assert any("CREATE TABLE IF NOT EXISTS consumer.consumed_events" in s for s in conn.cur.sql)
    assert any("PRIMARY KEY (consumer, event_id)" in s for s in conn.cur.sql)
    is_new, unit_seen = store.first_sight("team-a", "e1", "u1")
    assert is_new is True and unit_seen is False
    assert any("ON CONFLICT (consumer, event_id) DO NOTHING RETURNING 1" in s for s in conn.cur.sql)
