"""Watchdog: notices when the pipeline says nothing.

A dead scheduler cannot report itself, and a DAG that never started emits no
verdict. Loud failures already have a home (the ALERT log line, the red task,
the proceed=false verdict); this service exists for the one failure mode that
is silent (docs/DECISIONS.md D8).

It consumes the control topic as the `watchdog` principal, verifies every
verdict's signature, keeps the latest verdict per dataset, and evaluates two
conditions on a timer:

  ABSENT    no verdict for a dataset within INTERVAL + GRACE of its last run
            slot (or, for a dataset never seen, since this process started)
  NEGATIVE  the latest verdict said proceed=false

Either makes /healthz answer 503, which the compose healthcheck surfaces, and
posts to WATCHDOG_WEBHOOK_URL if one is set (Slack-compatible `text`, plus a
structured body). Recovery posts too. Everything is also logged as an ALERT
line so `docker compose logs watchdog` is enough with no webhook.

The consumer starts from the EARLIEST offset on every boot, so a restart
replays the topic's retained history and the watchdog knows the last verdict
without waiting for the next run.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Response

from pipeline.common.auth import AuthError
from pipeline.messaging.envelope import EVENT_RUN_VERDICT, parse
from pipeline.messaging.contracts import ContractError
from pipeline.messaging.signing import verifier_from_env

log = logging.getLogger("watchdog")
logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")

INTERVAL = timedelta(hours=float(os.environ.get("WATCHDOG_INTERVAL_HOURS", "4")))
GRACE = timedelta(minutes=float(os.environ.get("WATCHDOG_GRACE_MINUTES", "30")))
EXPECTED = [d for d in os.environ.get("WATCHDOG_DATASETS", "").split(",") if d.strip()]
WEBHOOK = os.environ.get("WATCHDOG_WEBHOOK_URL") or None
TOPIC = os.environ.get("KAFKA_TOPIC_CONTROL", "pipeline_runs")
CHECK_EVERY_SECONDS = 30

app = FastAPI(title="Pipeline watchdog", version="1.0.0")


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.started_at = datetime.now(timezone.utc)
        self.last: dict[str, dict] = {}         # dataset -> latest verdict summary
        self.rejected = 0                        # messages that failed contract/signature
        self.healthy: bool | None = None         # None until first evaluation
        self.reasons: list[str] = []
        self.consumer_error: str | None = None

    def observe(self, msg: dict, signer: str) -> None:
        env, payload = msg["envelope"], msg["payload"]
        slot = datetime.fromisoformat(env["run_slot"].replace("Z", "+00:00"))
        entry = {
            "run_slot": env["run_slot"], "produced_at": env["produced_at"],
            "event_id": env["event_id"], "trace_id": env["trace_id"],
            "proceed": bool(payload.get("proceed")), "status": payload.get("status"),
            "reasons": list(payload.get("reasons") or []), "signed_by": signer,
            "seen_at": _iso(datetime.now(timezone.utc)),
        }
        with self.lock:
            prev = self.last.get(env["dataset"])
            # Keep the LATEST slot, not the latest arrival: a replayed old
            # verdict must not make a newer one disappear.
            if prev is None or slot >= datetime.fromisoformat(prev["run_slot"].replace("Z", "+00:00")):
                self.last[env["dataset"]] = entry

    def evaluate(self, now: datetime | None = None) -> tuple[bool, list[str]]:
        now = now or datetime.now(timezone.utc)
        deadline = INTERVAL + GRACE
        reasons: list[str] = []
        with self.lock:
            datasets = set(self.last) | set(EXPECTED)
            if not datasets and now - self.started_at > deadline:
                reasons.append(f"no verdict from any dataset since the watchdog started "
                               f"{_iso(self.started_at)}; deadline {deadline}")
            for ds in sorted(datasets):
                entry = self.last.get(ds)
                if entry is None:
                    if now - self.started_at > deadline:
                        reasons.append(f"{ds}: no verdict seen since {_iso(self.started_at)}")
                    continue
                slot = datetime.fromisoformat(entry["run_slot"].replace("Z", "+00:00"))
                due = slot + deadline
                if now > due:
                    reasons.append(f"{ds}: last verdict was for slot {entry['run_slot']}; "
                                   f"the next was due by {_iso(due)} and has not arrived")
                if not entry["proceed"]:
                    reasons.append(f"{ds}: last verdict for {entry['run_slot']} said "
                                   f"proceed=false ({entry['status']}: "
                                   f"{'; '.join(entry['reasons'][:3]) or 'no reason given'})")
            if self.consumer_error:
                reasons.append(f"watchdog cannot read the control topic: {self.consumer_error}")
        return (not reasons), reasons

    def snapshot(self) -> dict:
        healthy, reasons = self.evaluate()
        with self.lock:
            return {
                "healthy": healthy, "reasons": reasons,
                "interval_hours": INTERVAL.total_seconds() / 3600,
                "grace_minutes": GRACE.total_seconds() / 60,
                "expected_datasets": EXPECTED, "topic": TOPIC,
                "started_at": _iso(self.started_at), "rejected_messages": self.rejected,
                "datasets": dict(sorted(self.last.items())),
                "webhook_configured": bool(WEBHOOK),
            }


STATE = State()


def _notify(healthy: bool, reasons: list[str]) -> None:
    """Log the transition; post it if a webhook is configured. Never raises."""
    kind = "verdict_ok" if healthy else "verdict_absent_or_negative"
    line = "; ".join(reasons) if reasons else "all datasets reporting on time and proceeding"
    (log.info if healthy else log.error)("ALERT kind=%s healthy=%s %s", kind, healthy, line)
    if not WEBHOOK:
        return
    try:
        import requests
        text = ("Pipeline watchdog: RECOVERED. " if healthy else "Pipeline watchdog: ALERT. ") + line
        requests.post(WEBHOOK, json={"text": text, "healthy": healthy, "reasons": reasons,
                                     "at": _iso(datetime.now(timezone.utc))}, timeout=10)
    except Exception as e:  # noqa: BLE001
        log.warning("webhook post failed: %s", e)


def _sasl() -> dict:
    user = os.environ.get("KAFKA_SASL_USERNAME")
    if not user:
        return {}
    return {"security_protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            "sasl_mechanism": os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN"),
            "sasl_plain_username": user,
            "sasl_plain_password": os.environ.get("KAFKA_SASL_PASSWORD", "")}


def consume_forever() -> None:
    from kafka import KafkaConsumer
    verifier = verifier_from_env()
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC, bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092").split(","),
                group_id=None, auto_offset_reset="earliest", enable_auto_commit=False,
                consumer_timeout_ms=1000, **_sasl())
            STATE.consumer_error = None
            log.info("consuming %s from the earliest offset", TOPIC)
            while True:
                for record in consumer:
                    try:
                        msg = parse(record.value)
                    except ContractError as e:
                        STATE.rejected += 1
                        log.warning("dropping message that violates its contract: %s", e)
                        continue
                    if msg["envelope"]["event_type"] != EVENT_RUN_VERDICT:
                        continue
                    try:
                        claims = verifier.verify(msg)
                    except AuthError as e:
                        STATE.rejected += 1
                        log.error("ALERT kind=verdict_unverifiable event=%s %s",
                                  msg["envelope"]["event_id"], e)
                        continue
                    STATE.observe(msg, claims["sub"])
                # consumer_timeout_ms elapsed with nothing new; loop and poll again.
        except Exception as e:  # noqa: BLE001
            STATE.consumer_error = str(e)[:200]
            log.error("control topic consumer failed (%s); retrying in 15s", e)
            time.sleep(15)


def evaluate_forever() -> None:
    while True:
        healthy, reasons = STATE.evaluate()
        with STATE.lock:
            changed = STATE.healthy is not healthy or (not healthy and reasons != STATE.reasons)
            STATE.healthy, STATE.reasons = healthy, reasons
        if changed:
            _notify(healthy, reasons)
        time.sleep(CHECK_EVERY_SECONDS)


@app.on_event("startup")
def _start_threads() -> None:
    threading.Thread(target=consume_forever, name="consumer", daemon=True).start()
    threading.Thread(target=evaluate_forever, name="evaluator", daemon=True).start()


@app.get("/healthz")
def healthz() -> Response:
    healthy, reasons = STATE.evaluate()
    body = json.dumps({"healthy": healthy, "reasons": reasons})
    return Response(content=body, media_type="application/json",
                    status_code=200 if healthy else 503)


@app.get("/status")
def status() -> dict:
    return STATE.snapshot()
