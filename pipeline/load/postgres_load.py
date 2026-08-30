"""Load masked rows into Postgres.

IDEMPOTENT BY CONSTRUCTION: the load is an UPSERT on the trip_id primary key,
so re-running a logical date replaces its rows rather than duplicating them.
That property lives in the schema and the statement, not in the loader
remembering to delete first -- which is the version that eventually forgets.
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

UPSERT = """
INSERT INTO warehouse.trips (
    trip_id, logical_date, bikeid_masked, subscriber_type_masked,
    start_station_masked, end_station_masked,
    bikeid_blind_index, start_station_blind_index,
    start_time, duration_minutes, masked_by
) VALUES (%(trip_id)s, %(logical_date)s, %(bikeid_masked)s, %(subscriber_type_masked)s,
          %(start_station_masked)s, %(end_station_masked)s,
          %(bikeid_blind_index)s, %(start_station_blind_index)s,
          %(start_time)s, %(duration_minutes)s, %(masked_by)s)
ON CONFLICT (trip_id) DO UPDATE SET
    logical_date              = EXCLUDED.logical_date,
    bikeid_masked             = EXCLUDED.bikeid_masked,
    subscriber_type_masked    = EXCLUDED.subscriber_type_masked,
    start_station_masked      = EXCLUDED.start_station_masked,
    end_station_masked        = EXCLUDED.end_station_masked,
    bikeid_blind_index        = EXCLUDED.bikeid_blind_index,
    start_station_blind_index = EXCLUDED.start_station_blind_index,
    start_time                = EXCLUDED.start_time,
    duration_minutes          = EXCLUDED.duration_minutes,
    masked_by                 = EXCLUDED.masked_by,
    loaded_at                 = now()
RETURNING (xmax = 0) AS inserted
"""


def connection_from_env():
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5433")),
        dbname=os.environ.get("POSTGRES_DB", "analytics"),
        user=os.environ.get("POSTGRES_USER", "pipeline"),
        password=os.environ.get("POSTGRES_PASSWORD", "pipeline-dev-password"),
        connect_timeout=10,
    )


def run(logical_date: date, hive_parquet: Path, extract_meta: dict | None = None) -> dict:
    import pyarrow.parquet as pq

    rows = pq.read_table(hive_parquet).to_pylist()
    if not rows:
        raise RuntimeError(f"{hive_parquet} has no rows")

    started = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    inserted = updated = 0

    conn = connection_from_env()
    try:
        with conn, conn.cursor() as cur:
            for r in rows:
                cur.execute(UPSERT, {
                    "trip_id": r["trip_id"],
                    "logical_date": logical_date,
                    "bikeid_masked": r.get("bikeid_masked"),
                    "subscriber_type_masked": r.get("subscriber_type_masked"),
                    "start_station_masked": r.get("start_station_masked"),
                    "end_station_masked": r.get("end_station_masked"),
                    "bikeid_blind_index": r.get("bikeid_blind_index"),
                    "start_station_blind_index": r.get("start_station_blind_index"),
                    "start_time": r["start_time"],
                    "duration_minutes": int(r["duration_minutes"]),
                    "masked_by": r.get("masked_by", "unknown"),
                })
                if cur.fetchone()[0]:
                    inserted += 1
                else:
                    updated += 1

            cur.execute("""
                INSERT INTO warehouse.load_audit (
                    run_id, logical_date, rows_extracted, rows_loaded, rows_updated,
                    bytes_billed, extract_mode, masked_by, started_at, finished_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (run_id, logical_date, (extract_meta or {}).get("row_count", len(rows)),
                  inserted, updated, (extract_meta or {}).get("bytes_billed", 0),
                  (extract_meta or {}).get("mode", "unknown"),
                  rows[0].get("masked_by", "unknown"), started,
                  datetime.now(timezone.utc)))
    finally:
        conn.close()

    log.info("loaded %d inserted / %d updated for %s", inserted, updated, logical_date)
    return {"run_id": run_id, "inserted": inserted, "updated": updated,
            "total": len(rows)}
