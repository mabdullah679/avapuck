"""Reference consumer: wakes on a `run.verdict` that says proceed, records it.

This DAG stands in for what another team would build (docs/DECISIONS.md D7).
It shows the whole boundary working with no coupling to the producer beyond
the platform contract:

  * It is scheduled on an ASSET, not a cron. The asset has a watcher backed by
    the Kafka MessageQueueTrigger, which runs in the triggerer and calls
    `pipeline.messaging.listen.accept_verdict` on every message. Only a
    contract-valid `run.verdict` with `proceed=true` wakes this DAG.
  * Its first task VERIFIES the verdict's signature against the IdP's JWKS
    and DEDUPES through the shared library before doing anything.
  * It records the verdict and stops. A real consumer would then read the
    flat topic for that run slot, or query the warehouse for that `run_slot`.

The `kafka_control` connection is supplied by the environment
(AIRFLOW_CONN_KAFKA_CONTROL); it authenticates as the `consumer` principal,
which may read the flat topic and the control topic and nothing else.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import DAG, Asset, AssetWatcher, task

log = logging.getLogger("airflow.task")

CONTROL_TOPIC = os.environ.get("KAFKA_TOPIC_CONTROL", "pipeline_runs")
CONSUMER_NAME = "demo-consumer"

verdicts = Asset(
    "pipeline_run_verdicts",
    watchers=[AssetWatcher(
        name="control_topic",
        trigger=MessageQueueTrigger(
            scheme="kafka",
            topics=[CONTROL_TOPIC],
            kafka_config_id="kafka_control",
            apply_function="pipeline.messaging.listen.accept_verdict",
            poll_timeout=1,
            poll_interval=5,
        ),
    )],
)


def _payloads(ctx: dict) -> list[dict]:
    """The message(s) that woke this run, from the asset events' extra."""
    out = []
    events = ctx.get("triggering_asset_events") or {}
    groups = events.values() if hasattr(events, "values") else events
    for group in groups:
        for ev in group:
            extra = getattr(ev, "extra", None) or (ev.get("extra") if isinstance(ev, dict) else None) or {}
            payload = extra.get("payload")
            if isinstance(payload, dict):
                out.append(payload)
    return out


with DAG(
    dag_id="on_run_verdict",
    description="Reference consumer: verifies, dedupes and records each proceed verdict",
    schedule=[verdicts],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["consumer", "reference"],
) as dag:

    @task(task_id="record_verdict")
    def record(**ctx):
        from pipeline.load.postgres_load import connection_from_env
        from pipeline.messaging.dedupe import PostgresSeenStore, accept
        from pipeline.messaging.envelope import parse
        from pipeline.messaging.signing import verifier_from_env

        messages = _payloads(ctx)
        if not messages:
            log.warning("woke with no verdict payload in the asset event; nothing to do")
            return {"recorded": 0}

        verifier = verifier_from_env()
        conn = connection_from_env()
        recorded = 0
        try:
            store = PostgresSeenStore(conn)
            store.ensure_table()
            for raw in messages:
                msg = parse(raw)                     # contract, again -- cheap, and structural
                claims = verifier.verify(msg)        # signature, issuer, audience, subject, digest
                decision = accept(store, CONSUMER_NAME, msg)
                env, verdict = msg["envelope"], msg["payload"]
                log.info("verdict %s for %s %s: %s", env["event_id"], env["dataset"],
                         env["run_slot"], decision.reason)
                if not decision.process:
                    continue
                with conn, conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS consumer.received_verdicts (
                            idempotency_key TEXT PRIMARY KEY,
                            event_id        TEXT NOT NULL,
                            dataset         TEXT NOT NULL,
                            run_slot        TIMESTAMPTZ NOT NULL,
                            trace_id        TEXT NOT NULL,
                            proceed         BOOLEAN NOT NULL,
                            status          TEXT NOT NULL,
                            reasons         TEXT[] NOT NULL,
                            signed_by       TEXT NOT NULL,
                            replay          BOOLEAN NOT NULL,
                            received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
                        )""")
                    # UPSERT on the idempotency key: a replay of the same slot
                    # replaces the earlier record. That is the "upsert" verb of
                    # the dedupe rule, made concrete.
                    cur.execute("""
                        INSERT INTO consumer.received_verdicts
                            (idempotency_key, event_id, dataset, run_slot, trace_id, proceed,
                             status, reasons, signed_by, replay)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (idempotency_key) DO UPDATE SET
                            event_id = EXCLUDED.event_id, proceed = EXCLUDED.proceed,
                            status = EXCLUDED.status, reasons = EXCLUDED.reasons,
                            replay = EXCLUDED.replay, received_at = now()
                    """, (env["idempotency_key"], env["event_id"], env["dataset"],
                          datetime.fromisoformat(env["run_slot"].replace("Z", "+00:00")),
                          env["trace_id"], verdict["proceed"], verdict["status"],
                          verdict["reasons"], claims["sub"], decision.replay))
                recorded += 1
        finally:
            conn.close()
        log.info("recorded %d verdict(s) as %s", recorded, CONSUMER_NAME)
        return {"recorded": recorded, "consumer": CONSUMER_NAME}

    record()
