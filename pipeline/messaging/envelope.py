"""The message envelope: two identifiers, two verbs.

Every message the platform publishes is `{envelope, payload, signature?}`.
The envelope carries two ids that mean different things, and a consumer
applies a different verb to each (docs/DECISIONS.md D2):

  idempotency_key   deterministic per LOGICAL UNIT OF WORK --
                    sha256(schema_version | event_type | dataset | run_slot | key).
                    A re-run of the same slot produces the same key, so a
                    consumer UPSERTS on it and the re-run replaces rather than
                    duplicates. This is non-negotiable 5 extended to Kafka.

  event_id          deterministic per EMISSION --
                    sha256(idempotency_key | run_id | try_number).
                    Kafka delivering the same message twice yields the same
                    event_id, so a consumer DROPS a repeat. A retry or a
                    re-run has a different run_id/try_number and is therefore
                    a new event carrying the same idempotency_key: processed,
                    and it supersedes the earlier result.

Both are hashes rather than UUIDs for the same reason the trace id is
(pipeline/common/trace.py): any party can recompute them from the facts, and a
repair is visibly the same unit of work rather than a fresh one.

Nothing here knows Kafka. `build` returns a dict; `parse` takes bytes or a
dict and validates it against `contracts/`. The transport is somebody else's
problem, which is what keeps this importable by a triggerer, a consumer DAG
and a watchdog alike.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any

SCHEMA_VERSION = "1"

EVENT_ROW_ENCRYPTED = "row.encrypted"
EVENT_ROW_FLAT = "row.flat"
EVENT_RUN_VERDICT = "run.verdict"
EVENT_TYPES = (EVENT_ROW_ENCRYPTED, EVENT_ROW_FLAT, EVENT_RUN_VERDICT)

_HASH_LEN = 32


def _utc(ts: datetime) -> datetime:
    """Treat a naive datetime as UTC; convert an aware one. Airflow is UTC."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def iso_z(ts: datetime) -> str:
    """UTC, second precision, `Z` suffix. The only timestamp format on the wire."""
    return _utc(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


def slot_label(ts: datetime) -> str:
    """Filename-safe run slot: 20260906T0400Z. Used for per-run artifacts."""
    return _utc(ts).strftime("%Y%m%dT%H%MZ")


def _json_default(v: Any):
    if isinstance(v, datetime):
        return iso_z(v)
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def canonical_json(obj: Any) -> str:
    """Deterministic serialisation: sorted keys, no whitespace. Signing hashes this."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=_json_default)


def _digest(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:_HASH_LEN]


def idempotency_key(event_type: str, dataset: str, run_slot: datetime | str,
                    key: Any) -> str:
    """The logical unit of work. Same inputs, same key, on every machine."""
    slot = run_slot if isinstance(run_slot, str) else iso_z(run_slot)
    return _digest(SCHEMA_VERSION, event_type, dataset, slot,
                   "" if key is None else str(key))


def event_id(idem_key: str, run_id: str, try_number: int) -> str:
    """The emission. A Kafka redelivery repeats it; a retry or re-run does not."""
    return _digest(idem_key, run_id, str(int(try_number)))


def build(*, event_type: str, dataset: str, run_slot: datetime, trace_id: str,
          run_id: str, try_number: int, key: Any, producer: str,
          payload: dict, signature: str | None = None,
          produced_at: datetime | None = None) -> dict:
    """Assemble one message. Pure; validate with `contracts.validate_message`."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown event_type {event_type!r}; one of {EVENT_TYPES}")
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dict")
    slot = _utc(run_slot)
    idem = idempotency_key(event_type, dataset, slot, key)
    msg = {
        "envelope": {
            "schema_version": SCHEMA_VERSION,
            "event_type": event_type,
            "event_id": event_id(idem, run_id, try_number),
            "idempotency_key": idem,
            "dataset": dataset,
            "logical_date": slot.date().isoformat(),
            "run_slot": iso_z(slot),
            "trace_id": trace_id,
            "run_id": run_id,
            "try_number": int(try_number),
            "key": None if key is None else str(key),
            "producer": producer,
            "produced_at": iso_z(produced_at or datetime.now(timezone.utc)),
        },
        "payload": payload,
    }
    if signature is not None:
        msg["signature"] = signature
    return msg


def parse(raw: bytes | bytearray | str | dict) -> dict:
    """Decode and VALIDATE a message. Raises ContractError on any deviation.

    Validation is not optional and not a separate call, so a consumer cannot
    forget it. A message that does not match its contract is refused here,
    before any dedupe or business logic sees it.
    """
    from pipeline.messaging.contracts import ContractError, validate_message

    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ContractError(f"message is not JSON: {e}") from e
    else:
        msg = raw
    validate_message(msg)
    return msg


def serialize(msg: dict) -> bytes:
    """Wire bytes. Not canonical -- readable in the Kafka UI matters more here."""
    return json.dumps(msg, default=_json_default, ensure_ascii=False).encode()
