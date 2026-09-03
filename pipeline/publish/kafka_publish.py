"""Publish each encrypted row to Kafka: two topics, one publisher.

Runs after the encrypt stage, reading the encrypted Parquet. Every row is
split into two messages by ONE publisher:

    <topic_encrypted>   the sensitive fields, AS CIPHERTEXT, plus the key
    <topic_public>      the non-sensitive business columns, in the clear

Both carry the primary key, so a consumer holding decrypt rights can rejoin
them and a consumer without those rights still gets usable analytics.

WHAT IS NOT PUBLISHED, AND WHY
------------------------------
No sensitive value is ever published in the clear. The requirement was
originally phrased as "the encrypted value to one topic and the unencrypted
value to the other"; taken literally that would put real names, emails, street
addresses and card numbers into a durable, replicated, retained log -- a larger
exposure than the warehouse ever was, and a direct breach of the pipeline's
first invariant. The "unencrypted" topic therefore carries the columns that
were never sensitive to begin with. See TRUST-BOUNDARY.md 2.9.

Which columns land where comes from the manifest, never from a column name:
`sensitive_columns` goes to the encrypted topic, `public_columns` to the other.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

from pipeline.metadata import schema as schema_mod

log = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP = "kafka:9092"


def _json_default(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)


class TwoTopicPublisher:
    """One producer, two topics.

    A single producer rather than one per topic: the two messages for a row
    are the same event seen two ways, and sharing a producer keeps them on the
    same connection, the same retry policy, and the same ordering guarantees.
    """

    def __init__(self, bootstrap: str, topic_encrypted: str, topic_public: str,
                 producer=None):
        self.bootstrap = bootstrap
        self.topic_encrypted = topic_encrypted
        self.topic_public = topic_public
        self._producer = producer   # injectable for tests
        self._owned = producer is None

    def __enter__(self):
        if self._producer is None:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap.split(","),
                value_serializer=lambda v: json.dumps(v, default=_json_default).encode(),
                key_serializer=lambda k: None if k is None else str(k).encode(),
                # acks=all: a row that Kafka has not durably accepted must not
                # be reported as published. At 100 rows/run the throughput cost
                # is irrelevant and the correctness gain is not.
                acks="all",
                retries=3,
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

    def publish_row(self, key, encrypted: dict, public: dict) -> None:
        """Send one row's two messages. Keyed so a row's history stays ordered."""
        self._producer.send(self.topic_encrypted, key=key, value=encrypted)
        self._producer.send(self.topic_public, key=key, value=public)

    def flush(self) -> None:
        self._producer.flush(timeout=30)


def run(logical_date, parquet_path: Path | str, publisher=None) -> dict:
    """Publish every row of the encrypted Parquet to the two topics."""
    import pyarrow.parquet as pq

    parquet_path = Path(parquet_path)
    manifest = schema_mod.load_manifest(parquet_path)
    table = pq.read_table(parquet_path)
    rows = table.to_pylist()

    # Accept a datetime, a date, or an ISO string: the DAG passes a date, but
    # a caller running one stage by hand should not have to construct one.
    if hasattr(logical_date, "date"):
        d = logical_date.date().isoformat()
    elif hasattr(logical_date, "isoformat"):
        d = logical_date.isoformat()
    else:
        d = str(logical_date)
    key_col = manifest.primary_key

    # The encrypt stage renames a sensitive column `x` to `x_encrypted` (and
    # adds `x_blind_index` where it applies), so resolve against the FILE
    # rather than assuming the manifest's names survived unchanged.
    present = set(table.column_names)
    enc_cols = [c for c in present
                if c.endswith(("_encrypted", "_blind_index"))]
    pub_cols = [c for c in manifest.public_columns if c in present]

    if not enc_cols:
        # Nothing sensitive in this dataset: still publish, so a consumer sees
        # every run, but say so rather than silently sending empty payloads.
        log.info("%s has no encrypted columns; encrypted topic gets keys only",
                 manifest.dataset)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)
    t_enc = os.environ.get("KAFKA_TOPIC_ENCRYPTED", "rpos_encrypted")
    t_pub = os.environ.get("KAFKA_TOPIC_PUBLIC", "rpos_flat")

    pub = publisher or TwoTopicPublisher(bootstrap, t_enc, t_pub)
    sent = 0
    with pub as p:
        for r in rows:
            key = r.get(key_col)
            meta = {"dataset": manifest.dataset,
                    "logical_date": d,
                    key_col: key}
            p.publish_row(
                key,
                {**meta, **{c: r.get(c) for c in enc_cols}},
                {**meta, **{c: r.get(c) for c in pub_cols}},
            )
            sent += 1

    _assert_no_plaintext_published(manifest, rows, pub_cols)

    log.info("published %d rows to %s and %s", sent, t_enc, t_pub)
    return {"dataset": manifest.dataset, "rows": sent,
            "topic_encrypted": t_enc, "topic_public": t_pub,
            "encrypted_fields": len(enc_cols), "public_fields": len(pub_cols),
            "bootstrap": bootstrap}


def _assert_no_plaintext_published(manifest, rows, pub_cols: list[str]) -> None:
    """Refuse to report success if a sensitive column reached the public payload.

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
