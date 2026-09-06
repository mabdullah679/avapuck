"""Load masked rows into Postgres.

IDEMPOTENT BY CONSTRUCTION: the load is an UPSERT on the manifest's primary
key, so re-running a logical date replaces its rows rather than duplicating
them. That property lives in the schema and the statement, not in the loader
remembering to delete first -- which is the version that eventually forgets.

Every row also records the RUN SLOT that loaded it and the run's trace id.
Six runs a day share a logical date; the slot is what lets the per-run PDF
and the verdict refer to exactly one of them (docs/DECISIONS.md D13).
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

# Columns every warehouse table carries regardless of dataset.
_RUN_COLUMNS = [("logical_date", "DATE"), ("run_slot", "TIMESTAMPTZ"), ("trace_id", "TEXT")]


def _warehouse_columns(manifest, rows: list[dict]) -> list[tuple[str, str]]:
    """The warehouse table's columns, in a stable order, as (name, sql_type).

    Derived from the manifest rather than declared, so a dataset's table
    matches the dataset. Masked and blind-index columns are TEXT regardless of
    the source type: masking turns a number into a hash or a run of 'n's.
    """
    key = manifest.primary_key
    key_col = manifest.column(key)

    cols: list[tuple[str, str]] = [
        # A synthetic key is a hex digest, and a SENSITIVE natural key arrives
        # as its blind index (the encrypt stage substitutes it rather than
        # writing the plaintext) -- both are text. Only a non-sensitive natural
        # key keeps the type the data had.
        (key, key_col.sql_type
         if key_col and not manifest.key_is_synthetic and not manifest.key_is_sensitive
         else "TEXT"),
        *_RUN_COLUMNS,
    ]
    for col in manifest.public_columns:
        if col == key:
            continue
        spec = manifest.column(col)
        cols.append((col, spec.sql_type if spec else "TEXT"))
    for col in manifest.blind_indexed:
        cols.append((f"{col}_blind_index", "TEXT"))
    for col in manifest.sensitive_columns:
        cols.append((f"{col}_masked", "TEXT"))
    cols.append(("masked_by", "TEXT"))

    # Only keep what the masked rows actually carry, so a manifest that has
    # drifted from the data produces a table matching the data, not the claim.
    always = {key} | {c for c, _ in _RUN_COLUMNS}
    present = set(rows[0]) if rows else set()
    return [(c, t) for c, t in cols if c in present or c in always]


def _ensure_table(cur, manifest, columns: list[tuple[str, str]]) -> None:
    """Create the dataset's table, and add any column it has grown.

    Runtime DDL is deliberate here: the pipeline accepts datasets it has never
    seen, so the table cannot be declared ahead of time in init-warehouse.sql.
    It is additive only -- never a DROP, never a type change -- so an existing
    table with data is widened rather than replaced. Which columns exist is
    gated upstream by the dataset contract (pipeline/metadata/contract.py):
    the extract stage fails on an unexpected column before this ever runs.
    """
    key = manifest.primary_key
    body = ",\n    ".join(f"{name} {sql_type}" for name, sql_type in columns)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS warehouse.{manifest.dataset} (
            {body},
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY ({key})
        )
    """)
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'warehouse' AND table_name = %s
    """, (manifest.dataset,))
    existing = {r[0] for r in cur.fetchall()}
    for name, sql_type in columns:
        if name not in existing:
            log.info("adding column %s %s to warehouse.%s", name, sql_type,
                     manifest.dataset)
            cur.execute(
                f"ALTER TABLE warehouse.{manifest.dataset} "
                f"ADD COLUMN {name} {sql_type}")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{manifest.dataset}_run_slot "
                f"ON warehouse.{manifest.dataset} (run_slot)")


def _ensure_audit_columns(cur) -> None:
    """load_audit predates run slots; widen it in place on older volumes."""
    for name, sql_type in (("run_slot", "TIMESTAMPTZ"), ("trace_id", "TEXT"),
                           ("dataset", "TEXT")):
        cur.execute(f"ALTER TABLE warehouse.load_audit ADD COLUMN IF NOT EXISTS "
                    f"{name} {sql_type}")


def _build_upsert(manifest, columns: list[tuple[str, str]]) -> str:
    """The UPSERT for this dataset.

    Still an upsert on the primary key, so non-negotiable #5 holds exactly as
    before: re-running a logical date replaces its rows instead of duplicating
    them. Only the column list is now derived rather than typed out.
    """
    key = manifest.primary_key
    names = [c for c, _ in columns]
    placeholders = ", ".join(f"%({c})s" for c in names)
    updates = ",\n    ".join(
        f"{c} = EXCLUDED.{c}" for c in names if c != key)
    return f"""
INSERT INTO warehouse.{manifest.dataset} ({", ".join(names)})
VALUES ({placeholders})
ON CONFLICT ({key}) DO UPDATE SET
    {updates},
    loaded_at = now()
RETURNING (xmax = 0) AS inserted
"""


def connection_from_env():
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5433")),
        dbname=os.environ.get("POSTGRES_DB", "analytics"),
        user=os.environ.get("POSTGRES_USER", "pipeline"),
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


def run(logical_date: date, hive_parquet: Path, extract_meta: dict | None = None, *,
        run_slot: datetime | None = None, trace_id: str | None = None) -> dict:
    import pyarrow.parquet as pq

    from pipeline.metadata import schema as schema_mod

    manifest = schema_mod.load_manifest(hive_parquet)

    rows = pq.read_table(hive_parquet).to_pylist()
    if not rows:
        raise RuntimeError(f"{hive_parquet} has no rows")

    if run_slot is None:
        run_slot = datetime.combine(logical_date, datetime.min.time(), tzinfo=timezone.utc)
    elif run_slot.tzinfo is None:
        run_slot = run_slot.replace(tzinfo=timezone.utc)

    columns = _warehouse_columns(manifest, rows)
    upsert = _build_upsert(manifest, columns)
    names = [c for c, _ in columns]

    started = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    inserted = updated = 0

    conn = connection_from_env()
    try:
        with conn, conn.cursor() as cur:
            _ensure_table(cur, manifest, columns)
            _ensure_audit_columns(cur)

            for r in rows:
                params = {c: r.get(c) for c in names}
                # logical_date is the run's date, not whatever the row carried
                # -- that is what makes a re-run overwrite its own partition.
                params["logical_date"] = logical_date
                params["run_slot"] = run_slot
                params["trace_id"] = trace_id
                cur.execute(upsert, params)
                if cur.fetchone()[0]:
                    inserted += 1
                else:
                    updated += 1

            cur.execute("""
                INSERT INTO warehouse.load_audit (
                    run_id, logical_date, rows_extracted, rows_loaded, rows_updated,
                    bytes_billed, extract_mode, masked_by, started_at, finished_at,
                    run_slot, trace_id, dataset)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (run_id, logical_date, (extract_meta or {}).get("rows", len(rows)),
                  inserted, updated, (extract_meta or {}).get("bytes_billed", 0),
                  (extract_meta or {}).get("mode", "unknown"),
                  rows[0].get("masked_by", "unknown"), started,
                  datetime.now(timezone.utc), run_slot, trace_id, manifest.dataset))
    finally:
        conn.close()

    log.info("loaded %d inserted / %d updated into warehouse.%s for %s (slot %s)",
             inserted, updated, manifest.dataset, logical_date, run_slot.isoformat())
    return {"run_id": run_id, "inserted": inserted, "updated": updated,
            "total": len(rows), "dataset": manifest.dataset}
