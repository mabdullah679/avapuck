"""Kafka tap: scrape-only. Every message on the flat and control topics
becomes one row in monitor.kafka_log, which the console shows as a table.

Read-only twice over: the `tap` principal has Read on those two topics and
nothing else, and this service opens no producer. It never touches the
ciphertext topic. docs/DECISIONS.md D19.

Rows are inserted in batches, deduplicated on (topic, partition, offset), so
a restart that replays the topic from the earliest offset fills gaps without
duplicating what is already there. Messages that fail their contract are
kept too, marked `invalid`, because a log that hides malformed traffic is
not a log.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from fastapi import FastAPI, Response

from pipeline.messaging.contracts import ContractError
from pipeline.messaging.envelope import parse

log = logging.getLogger("tap")
logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")

TOPICS = [t for t in (os.environ.get("KAFKA_TOPIC_PUBLIC", "rpos_flat"),
                      os.environ.get("KAFKA_TOPIC_CONTROL", "pipeline_runs")) if t]
FLUSH_ROWS = int(os.environ.get("TAP_FLUSH_ROWS", "500"))
FLUSH_SECONDS = float(os.environ.get("TAP_FLUSH_SECONDS", "2"))

DDL = """
CREATE SCHEMA IF NOT EXISTS monitor;
CREATE TABLE IF NOT EXISTS monitor.kafka_log (
    id               BIGSERIAL PRIMARY KEY,
    topic            TEXT NOT NULL,
    partition        INTEGER NOT NULL,
    "offset"         BIGINT NOT NULL,
    event_type       TEXT NOT NULL,
    dataset          TEXT,
    run_slot         TIMESTAMPTZ,
    trace_id         TEXT,
    event_id         TEXT,
    idempotency_key  TEXT,
    key              TEXT,
    producer         TEXT,
    produced_at      TIMESTAMPTZ,
    payload          JSONB,
    raw              TEXT,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic, partition, "offset")
);
CREATE INDEX IF NOT EXISTS idx_kafka_log_type ON monitor.kafka_log (event_type, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_kafka_log_slot ON monitor.kafka_log (dataset, run_slot);
"""

INSERT = """
INSERT INTO monitor.kafka_log
    (topic, partition, "offset", event_type, dataset, run_slot, trace_id, event_id,
     idempotency_key, key, producer, produced_at, payload, raw)
VALUES %s
ON CONFLICT (topic, partition, "offset") DO NOTHING
"""

app = FastAPI(title="Kafka tap", version="1.0.0")


def _ts(s: str | None):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def row_for(topic: str, partition: int, offset: int, value: bytes | None) -> tuple:
    """One kafka_log row from one record. Contract violations are kept, marked."""
    raw = None if value is None else value.decode("utf-8", errors="replace")
    try:
        msg = parse(value) if value is not None else None
    except ContractError:
        msg = None
    if msg is None:
        return (topic, partition, offset, "invalid", None, None, None, None, None, None,
                None, None, None, raw)
    env = msg["envelope"]
    return (topic, partition, offset, env["event_type"], env["dataset"], _ts(env["run_slot"]),
            env["trace_id"], env["event_id"], env["idempotency_key"], env["key"],
            env["producer"], _ts(env["produced_at"]), json.dumps(msg["payload"]), raw)


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.written = 0
        self.invalid = 0
        self.last_flush = None
        self.consumer_error = None
        self.db_error = None


STATS = Stats()


def _sasl() -> dict:
    user = os.environ.get("KAFKA_SASL_USERNAME")
    if not user:
        return {}
    return {"security_protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            "sasl_mechanism": os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN"),
            "sasl_plain_username": user,
            "sasl_plain_password": os.environ.get("KAFKA_SASL_PASSWORD", "")}


def _flush(conn, rows: list[tuple]) -> None:
    from psycopg2.extras import execute_values
    if not rows:
        return
    with conn, conn.cursor() as cur:
        execute_values(cur, INSERT, rows, page_size=len(rows))
    with STATS.lock:
        STATS.written += len(rows)
        STATS.invalid += sum(1 for r in rows if r[3] == "invalid")
        STATS.last_flush = datetime.now(timezone.utc).isoformat()


def consume_forever() -> None:
    from kafka import KafkaConsumer

    from pipeline.load.postgres_load import connection_from_env
    while True:
        conn = None
        try:
            conn = connection_from_env()
            with conn, conn.cursor() as cur:
                cur.execute(DDL)
            STATS.db_error = None
            consumer = KafkaConsumer(
                *TOPICS, bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092").split(","),
                group_id=None, auto_offset_reset="earliest", enable_auto_commit=False,
                consumer_timeout_ms=int(FLUSH_SECONDS * 1000), **_sasl())
            STATS.consumer_error = None
            log.info("tapping %s from the earliest offset", TOPICS)
            buf: list[tuple] = []
            last = time.monotonic()
            while True:
                for rec in consumer:
                    buf.append(row_for(rec.topic, rec.partition, rec.offset, rec.value))
                    if len(buf) >= FLUSH_ROWS:
                        _flush(conn, buf); buf = []; last = time.monotonic()
                if buf and time.monotonic() - last >= FLUSH_SECONDS:
                    _flush(conn, buf); buf = []; last = time.monotonic()
        except Exception as e:  # noqa: BLE001
            msg = str(e)[:200]
            if "kafka" in type(e).__module__:
                STATS.consumer_error = msg
            else:
                STATS.db_error = msg
            log.error("tap loop failed (%s); retrying in 15s", e)
            time.sleep(15)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass


@app.on_event("startup")
def _start() -> None:
    threading.Thread(target=consume_forever, name="tap", daemon=True).start()


@app.get("/healthz")
def healthz() -> Response:
    ok = STATS.consumer_error is None and STATS.db_error is None
    body = json.dumps({"healthy": ok, "consumer_error": STATS.consumer_error, "db_error": STATS.db_error})
    return Response(content=body, media_type="application/json", status_code=200 if ok else 503)


@app.get("/status")
def status() -> dict:
    with STATS.lock:
        return {"topics": TOPICS, "rows_written": STATS.written, "invalid_rows": STATS.invalid,
                "last_flush": STATS.last_flush, "consumer_error": STATS.consumer_error,
                "db_error": STATS.db_error}
