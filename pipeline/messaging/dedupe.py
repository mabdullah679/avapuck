"""Consumer-side dedupe, enforced in code every consumer imports.

The rule (docs/DECISIONS.md D2), stated once:

  * seen this `event_id` before     -> DROP. Kafka delivered it twice.
  * new event_id, seen its
    `idempotency_key` before        -> PROCESS, and it SUPERSEDES the earlier
                                        result. A retry or a deliberate re-run
                                        of the same slot. Upsert, do not add.
  * neither seen                    -> PROCESS. First sight of this unit.

`accept` applies the rule against a `SeenStore` and records the sighting in
the same step, so a consumer cannot process first and forget to record. Two
stores ship: in-memory for tests and single-process consumers, Postgres for a
DAG that must survive restarts.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Decision:
    process: bool
    replay: bool          # True when the idempotency_key was seen under another event_id
    reason: str


class SeenStore(Protocol):
    def first_sight(self, consumer: str, event_id: str,
                    idempotency_key: str) -> tuple[bool, bool]:
        """Record the sighting. Returns (event_is_new, unit_seen_before)."""


class InMemorySeenStore:
    """Process-local. Fine for tests, the watchdog, and single-shot consumers."""

    def __init__(self):
        self._events: dict[str, dict[str, str]] = {}     # consumer -> event_id -> idem
        self._units: dict[str, set[str]] = {}            # consumer -> idem keys

    def first_sight(self, consumer: str, event_id: str,
                    idempotency_key: str) -> tuple[bool, bool]:
        events = self._events.setdefault(consumer, {})
        units = self._units.setdefault(consumer, set())
        if event_id in events:
            return False, True
        unit_seen = idempotency_key in units
        events[event_id] = idempotency_key
        units.add(idempotency_key)
        return True, unit_seen


class PostgresSeenStore:
    """Durable, per-consumer. One row per event ever accepted.

    INSERT ... ON CONFLICT DO NOTHING is the whole atomicity story: two
    workers racing on the same event get exactly one insert between them, and
    the loser sees `event_is_new=False`.
    """

    def __init__(self, conn, table: str = "consumer.consumed_events"):
        self.conn = conn
        self.table = table

    def ensure_table(self) -> None:
        schema = self.table.split(".")[0] if "." in self.table else None
        with self.conn, self.conn.cursor() as cur:
            if schema:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    consumer         TEXT NOT NULL,
                    event_id         TEXT NOT NULL,
                    idempotency_key  TEXT NOT NULL,
                    consumed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (consumer, event_id)
                )""")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_consumed_unit "
                        f"ON {self.table} (consumer, idempotency_key)")

    def first_sight(self, consumer: str, event_id: str,
                    idempotency_key: str) -> tuple[bool, bool]:
        with self.conn, self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT count(*) FROM {self.table}
                WHERE consumer = %s AND idempotency_key = %s AND event_id <> %s
            """, (consumer, idempotency_key, event_id))
            unit_seen = cur.fetchone()[0] > 0
            cur.execute(f"""
                INSERT INTO {self.table} (consumer, event_id, idempotency_key)
                VALUES (%s, %s, %s)
                ON CONFLICT (consumer, event_id) DO NOTHING
                RETURNING 1
            """, (consumer, event_id, idempotency_key))
            is_new = cur.fetchone() is not None
        return is_new, unit_seen


def accept(store: SeenStore, consumer: str, message: dict) -> Decision:
    """Apply the dedupe rule to a parsed message and record the sighting."""
    env = message["envelope"]
    is_new, unit_seen = store.first_sight(consumer, env["event_id"], env["idempotency_key"])
    if not is_new:
        return Decision(process=False, replay=False,
                        reason=f"event {env['event_id']} already consumed by {consumer!r}")
    if unit_seen:
        return Decision(process=True, replay=True,
                        reason=f"unit {env['idempotency_key']} seen before under another "
                               f"event; this emission supersedes it (upsert)")
    return Decision(process=True, replay=False, reason="first sight")
