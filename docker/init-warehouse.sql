-- Warehouse schema. Created at container init.
--
-- Design notes that matter:
--   * The natural key is (trip_id), and the load is an UPSERT keyed on it, so
--     re-running a logical date replaces rather than duplicates. Idempotency
--     is a property of the schema, not of the loader remembering to be careful.
--   * Sensitive columns hold MASKED values. Plaintext PII never lands here.
--   * blind_index columns allow equality joins on values we cannot read.

CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.trips (
    trip_id              TEXT PRIMARY KEY,
    logical_date         DATE NOT NULL,

    -- masked, per Ranger policy
    bike_id_masked        TEXT,
    subscriber_type_masked TEXT,
    start_station_masked TEXT,
    end_station_masked   TEXT,

    -- deterministic handles for joins on unreadable values
    bike_id_blind_index   TEXT,
    start_station_blind_index TEXT,

    -- non-sensitive, clear
    start_time           TIMESTAMPTZ NOT NULL,
    duration_minutes     INTEGER NOT NULL CHECK (duration_minutes > 0),

    -- provenance: which policy engine masked this, and when
    masked_by            TEXT NOT NULL,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trips_logical_date ON warehouse.trips (logical_date);
CREATE INDEX IF NOT EXISTS idx_trips_start_time   ON warehouse.trips (start_time);
CREATE INDEX IF NOT EXISTS idx_trips_bike_id_bi    ON warehouse.trips (bike_id_blind_index);

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
    finished_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_logical_date ON warehouse.load_audit (logical_date);
