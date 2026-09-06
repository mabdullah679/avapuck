"""Shared messaging library: the one place the platform's Kafka contract lives.

Every producer builds messages with `envelope.build`; every consumer parses
them with `envelope.parse` (which validates against `contracts/`), decides
whether to act with `dedupe.accept`, and checks a verdict's signature with
`signing.VerdictVerifier`. Nothing about the message shape is known anywhere
else, which is what makes the dedupe rule structural rather than documentary
(docs/DECISIONS.md D2, D3).

Imported by the pipeline's publish stage, the reference consumer DAG and the
watchdog. When external teams arrive this module becomes a package; the
boundary is already drawn so that is a packaging step, not a refactor.
"""
from pipeline.messaging.envelope import (  # noqa: F401
    EVENT_ROW_ENCRYPTED, EVENT_ROW_FLAT, EVENT_RUN_VERDICT, EVENT_TYPES,
    SCHEMA_VERSION, build, canonical_json, event_id, idempotency_key, iso_z,
    parse, slot_label,
)
from pipeline.messaging.contracts import ContractError  # noqa: F401
from pipeline.messaging.dedupe import Decision, accept  # noqa: F401
