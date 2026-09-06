"""Publish to Kafka: two data topics for rows, one control topic for the verdict.

Every message goes out wrapped in the platform envelope (pipeline/messaging)
and is validated against its contract BEFORE it is sent. A message that would
violate the contract is refused here, not discovered by a consumer.

ROWS (publish_rows) -- runs after the encrypt stage, reading the encrypted
Parquet. Each row becomes two messages from ONE producer:

    <topic_encrypted>   row.encrypted   the sensitive fields, AS CIPHERTEXT
    <topic_public>      row.flat        the never-sensitive business columns

Both carry the primary key in the envelope, so a consumer with decrypt rights
can rejoin them and one without still gets usable analytics.

VERDICT (publish_verdict) -- runs last, always. One signed run.verdict message
on the control topic saying whether downstream should proceed. See
pipeline/verdict.py for the rule and docs/DECISIONS.md D5, D6.

WHAT IS NOT PUBLISHED, AND WHY
No sensitive value is ever published in the clear. The requirement was
originally phrased as "the encrypted value to one topic and the unencrypted
value to the other"; taken literally that would put real names, emails and
card numbers into a durable, replicated, retained log. The "unencrypted" topic
therefore carries the columns that were never sensitive to begin with, and
`_assert_no_plaintext_published` refuses the send otherwise. TRUST-BOUNDARY 2.9.

DELIVERY
acks=all and an idempotent producer: the broker de-duplicates a retried
batch, so a network blip cannot double-write a row. That covers Kafka's own
retries. A re-run of the whole slot is a different emission on purpose and is
handled by consumers through the envelope's idempotency_key.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from pipeline.messaging import envelope as env_mod
from pipeline.messaging.contracts import validate_message
from pipeline.metadata import schema as schema_mod

log = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP = "kafka:9092"
DEFAULT_TOPIC_ENCRYPTED = "rpos_encrypted"
DEFAULT_TOPIC_PUBLIC = "rpos_flat"
DEFAULT_TOPIC_CONTROL = "pipeline_runs"
DEFAULT_PRINCIPAL = "pipeline"


@dataclass(frozen=True)
class RunContext:
    """Who is publishing, for which run. Everything the envelope needs."""
    run_slot: datetime
    trace_id: str
    run_id: str
    try_number: int = 1
    producer: str = DEFAULT_PRINCIPAL


def topics_from_env() -> dict[str, str]:
    return {
        "encrypted": os.environ.get("KAFKA_TOPIC_ENCRYPTED", DEFAULT_TOPIC_ENCRYPTED),
        "public": os.environ.get("KAFKA_TOPIC_PUBLIC", DEFAULT_TOPIC_PUBLIC),
        "control": os.environ.get("KAFKA_TOPIC_CONTROL", DEFAULT_TOPIC_CONTROL),
    }


class Publisher:
    """One producer for every topic this stage writes.

    A single producer rather than one per topic: the two messages for a row
    are the same event seen two ways, and sharing a producer keeps them on the
    same connection, the same retry policy, and the same ordering guarantees.
    """

    def __init__(self, bootstrap: str, producer=None):
        self.bootstrap = bootstrap
        self._producer = producer   # injectable for tests
        self._owned = producer is None
        self.sent = 0

    def __enter__(self):
        if self._producer is None:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap.split(","),
                **_sasl_config(),
                value_serializer=env_mod.serialize,
                key_serializer=lambda k: None if k is None else str(k).encode(),
                # A row that Kafka has not durably accepted must not be
                # reported as published, and a retried batch must not land
                # twice. At 100 rows/run the throughput cost of either is nil.
                acks="all",
                enable_idempotence=True,
                retries=5,
                max_in_flight_requests_per_connection=5,
                linger_ms=50,
                max_block_ms=30000,
            )
        return self

    def __exit__(self, *exc):
        if self._owned and self._producer is not None:
            try:
                self._producer.flush(timeout=30)
            finally:
                self._producer.close(timeout=30)

    def send(self, topic: str, key, message: dict) -> None:
        """Validate against the contract, then send. Never the other way round."""
        validate_message(message)
        self._producer.send(topic, key=key, value=message)
        self.sent += 1

    def flush(self) -> None:
        self._producer.flush(timeout=30)


def _jsonable(row: dict) -> dict:
    """Parquet gives back datetimes and Decimals; the wire and the schema want JSON."""
    out = {}
    for k, v in row.items():
        if v is None or isinstance(v, (str, int, float, bool)):
            out[k] = v
        else:
            out[k] = env_mod._json_default(v)
    return out


def publish_rows(ctx: RunContext, parquet_path: Path | str, publisher: Publisher | None = None,
                 topics: dict[str, str] | None = None) -> dict:
    """Publish every row of the encrypted Parquet as row.encrypted + row.flat."""
    import pyarrow.parquet as pq

    parquet_path = Path(parquet_path)
    manifest = schema_mod.load_manifest(parquet_path)
    table = pq.read_table(parquet_path)
    rows = table.to_pylist()
    topics = topics or topics_from_env()
    key_col = manifest.primary_key

    # The encrypt stage renames a sensitive column `x` to `x_encrypted` (and
    # adds `x_blind_index` where it applies), so resolve against the FILE
    # rather than assuming the manifest's names survived unchanged.
    present = set(table.column_names)
    enc_cols = [c for c in present if c.endswith(("_encrypted", "_blind_index"))]
    pub_cols = [c for c in manifest.public_columns if c in present]
    _assert_no_plaintext_published(manifest, pub_cols)

    if not enc_cols:
        log.info("%s has no encrypted columns; encrypted topic gets empty payloads",
                 manifest.dataset)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)
    pub = publisher or Publisher(bootstrap)
    sent = 0
    with pub as p:
        for r in rows:
            key = r.get(key_col)
            common = dict(dataset=manifest.dataset, run_slot=ctx.run_slot,
                          trace_id=ctx.trace_id, run_id=ctx.run_id,
                          try_number=ctx.try_number, key=key, producer=ctx.producer)
            p.send(topics["encrypted"], key,
                   env_mod.build(event_type=env_mod.EVENT_ROW_ENCRYPTED,
                                 payload=_jsonable({c: r.get(c) for c in enc_cols}),
                                 **common))
            p.send(topics["public"], key,
                   env_mod.build(event_type=env_mod.EVENT_ROW_FLAT,
                                 payload=_jsonable({c: r.get(c) for c in pub_cols}),
                                 **common))
            sent += 1

    log.info("published %d rows to %s and %s", sent, topics["encrypted"], topics["public"])
    return {"dataset": manifest.dataset, "rows": sent,
            "topic_encrypted": topics["encrypted"], "topic_public": topics["public"],
            "encrypted_fields": len(enc_cols), "public_fields": len(pub_cols),
            "bootstrap": bootstrap}


def publish_verdict(ctx: RunContext, dataset: str, payload: dict, signer,
                    publisher: Publisher | None = None,
                    topics: dict[str, str] | None = None) -> dict:
    """Sign and publish one run.verdict to the control topic, keyed by dataset."""
    topics = topics or topics_from_env()
    msg = env_mod.build(event_type=env_mod.EVENT_RUN_VERDICT, dataset=dataset,
                        run_slot=ctx.run_slot, trace_id=ctx.trace_id, run_id=ctx.run_id,
                        try_number=ctx.try_number, key=None, producer=ctx.producer,
                        payload=payload)
    msg["signature"] = signer.sign(msg["envelope"], payload)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)
    pub = publisher or Publisher(bootstrap)
    with pub as p:
        p.send(topics["control"], dataset, msg)
        p.flush()

    env = msg["envelope"]
    log.info("published verdict proceed=%s status=%s for %s %s to %s (event %s)",
             payload.get("proceed"), payload.get("status"), dataset, env["run_slot"],
             topics["control"], env["event_id"])
    return {"topic": topics["control"], "event_id": env["event_id"],
            "idempotency_key": env["idempotency_key"], "proceed": payload.get("proceed"),
            "status": payload.get("status")}


def _assert_no_plaintext_published(manifest, pub_cols: list[str]) -> None:
    """Refuse to publish if a sensitive column is routed to the public topic.

    The column split is derived from the manifest, so this should be
    impossible -- which is exactly why it is worth asserting. A rename or a
    classifier change that quietly moved a column would otherwise publish PII
    to a topic that is not supposed to hold any.
    """
    sensitive = set(manifest.sensitive_columns)
    leaked = [c for c in pub_cols if c in sensitive]
    if leaked:
        raise AssertionError(
            f"refusing to publish: {leaked} are sensitive columns but were "
            f"routed to the public topic")


def _sasl_config() -> dict:
    """SASL settings for the broker, or {} when auth is not configured.

    The broker denies any principal with no matching ACL, so an unauthenticated
    client does not fail open -- it fails at connect. Returning {} when
    KAFKA_SASL_USERNAME is unset keeps a plain local broker usable for tests
    without a second code path.

    NOTE: SASL_PLAINTEXT authenticates the client; it does not encrypt the
    connection. Set KAFKA_SECURITY_PROTOCOL=SASL_SSL once the broker has certs.
    """
    user = os.environ.get("KAFKA_SASL_USERNAME")
    if not user:
        return {}
    return {
        "security_protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        "sasl_mechanism": os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN"),
        "sasl_plain_username": user,
        "sasl_plain_password": os.environ.get("KAFKA_SASL_PASSWORD", ""),
    }


def coerce_slot(logical_ts) -> datetime:
    """A datetime, a date, or an ISO string -> the run slot as a datetime."""
    if isinstance(logical_ts, datetime):
        return logical_ts
    if isinstance(logical_ts, date):
        return datetime.combine(logical_ts, datetime.min.time())
    return datetime.fromisoformat(str(logical_ts).replace("Z", "+00:00"))
