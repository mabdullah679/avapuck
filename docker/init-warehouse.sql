-- Warehouse schema. Created at container init.
--
-- Design notes that matter:
--   * Dataset tables are created at runtime by pipeline/load/postgres_load.py
--     from each dataset's manifest, held to its contract. Only the shared
--     tables live here.
--   * Every load is an UPSERT keyed on the manifest's primary key, so
--     re-running a slot replaces rather than duplicates. Idempotency is a
--     property of the schema, not of the loader remembering to be careful.
--   * Sensitive columns hold MASKED values. Plaintext PII never lands here.
--   * Every row records the run_slot that loaded it, so the per-run PDF and
--     the verdict refer to exactly one run (docs/DECISIONS.md D13).

CREATE SCHEMA IF NOT EXISTS warehouse;

-- Per-run audit. Every load records what it did, so a re-run is traceable.
CREATE TABLE IF NOT EXISTS warehouse.load_audit (
    run_id           TEXT PRIMARY KEY,
    logical_date     DATE NOT NULL,
    rows_extracted   INTEGER NOT NULL,
    rows_loaded      INTEGER NOT NULL,
    rows_updated     INTEGER NOT NULL,
    bytes_billed     BIGINT,
    extract_mode     TEXT NOT NULL,
    masked_by        TEXT NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    finished_at      TIMESTAMPTZ NOT NULL,
    run_slot         TIMESTAMPTZ,
    trace_id         TEXT,
    dataset          TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_logical_date ON warehouse.load_audit (logical_date);
CREATE INDEX IF NOT EXISTS idx_audit_run_slot     ON warehouse.load_audit (run_slot);

-- ── Consumer side ─────────────────────────────────────────────────────────
-- The reference consumer DAG (dags/verdict_listener_dag.py) records the
-- verdicts it accepted. Its dedupe ledger is created by the shared library
-- (pipeline/messaging/dedupe.py) so every consumer's ledger has one shape.
-- Same database user as the pipeline for the demo; a real consumer gets its
-- own role. TRUST-BOUNDARY §9.
CREATE SCHEMA IF NOT EXISTS consumer;

CREATE TABLE IF NOT EXISTS consumer.consumed_events (
    consumer         TEXT NOT NULL,
    event_id         TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    consumed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
CREATE INDEX IF NOT EXISTS idx_consumed_unit ON consumer.consumed_events (consumer, idempotency_key);

-- ── Monitoring ────────────────────────────────────────────────────────────
-- The Kafka tap (services/tap) writes every message it scrapes from the flat
-- and control topics here; the console shows it as a table. Scrape-only:
-- the tap has no Kafka write permission. docs/DECISIONS.md D19.
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
);
