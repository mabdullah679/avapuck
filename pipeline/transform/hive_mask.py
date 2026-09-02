"""Hive job: read encrypted Parquet, decrypt via the crypto service, apply
Ranger masking, write the Hive table.

THE ORDER MATTERS AND IS DELIBERATE: decrypt, then mask, then write. The Hive
table stores the MASKED value, never the plaintext -- so even a direct file
read of the warehouse yields nothing sensitive. The plaintext exists only in
this process's memory, between the decrypt response and the mask call.

Masking rules come from Ranger policy. This module chooses nothing.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

from pipeline.mask.ranger import apply_mask, get_provider

log = logging.getLogger(__name__)

DATABASE = "trips_warehouse"

# The audience the warehouse serves. Analysts see masked values; the
# data_steward view is a separate, access-controlled path and is NOT what the
# pipeline writes.
MASK_GROUP = os.environ.get("MASK_AUDIENCE_GROUP", "analyst")


def masked_name(column: str) -> str:
    """The output column a sensitive column becomes.

    One rule, applied uniformly, replacing the old hand-written map: the
    warehouse column is always `<column>_masked`. A map was only ever needed
    because two trips columns were abbreviated by hand
    (start_station_name -> start_station_masked); uniformity is worth more
    than those two shorter names now that the set is not fixed.
    """
    return f"{column}_masked"


def assert_no_plaintext_survives(masked_rows: list[dict],
                                 plaintext: dict[str, list],
                                 column_map: dict[str, str]) -> None:
    """The masked output must not equal the plaintext for any masked column.

    A mask that returns its input unchanged -- a misconfigured MASK_NONE, an
    unhandled type -- is the failure this catches. Without it, a policy typo
    silently publishes PII.
    """
    leaked = []
    for col, out_col in column_map.items():
        src = plaintext.get(col, [])
        for i, row in enumerate(masked_rows):
            if i >= len(src):
                break
            original, out = src[i], row.get(out_col)
            if original in (None, "") or out is None:
                continue
            if str(out) == str(original):
                leaked.append(f"{out_col} equals its plaintext ({original!r})")
    if leaked:
        raise RuntimeError(
            "REFUSING TO WRITE: masking did not change these values:\n  "
            + "\n  ".join(sorted(set(leaked))[:10]))


def run(logical_date: date, parquet_path: Path, out_dir: Path, crypto) -> dict:
    import pyarrow.parquet as pq

    from pipeline.metadata import policy_gen
    from pipeline.metadata import schema as schema_mod

    manifest = schema_mod.load_manifest(parquet_path)
    table_name = manifest.dataset

    rows = pq.read_table(parquet_path).to_pylist()
    if not rows:
        raise RuntimeError(f"{parquet_path} has no rows")

    column_map = {col: masked_name(col) for col in manifest.sensitive_columns}

    # A dataset nobody has authored policy for gets a generated one, written
    # to config/ranger/ for review. Generation happens ONCE (ensure_policy
    # never overwrites), so a reviewer's edits are what enforce from then on.
    # This keeps non-negotiable #3 intact: the rules still live in policy JSON
    # and are still read back through the Ranger provider below -- this module
    # decides nothing about how a column is masked.
    overrides = schema_mod._load_overrides(manifest.dataset)
    policy_file, created = policy_gen.ensure_policy(
        manifest, DATABASE, table_name, overrides)
    if created:
        log.info("no policy existed for %s; generated %s -- review it",
                 manifest.dataset, policy_file)

    provider = get_provider(extra_policy_paths=[policy_file])
    masks = provider.masks_for(DATABASE, table_name, MASK_GROUP)
    log.info("masking via %s: %s", provider.name, masks)

    missing = set(column_map) - set(masks)
    if missing:
        raise RuntimeError(
            f"no Ranger masking policy for {sorted(missing)}. Refusing to write "
            f"an unmasked sensitive column -- add a policy to {policy_file}.")

    plaintext: dict[str, list] = {}
    for col in column_map:
        cts = [r.get(f"{col}_encrypted") for r in rows]
        plaintext[col] = crypto.decrypt_column(col, cts)
        log.info("decrypted %d values for %s", len(plaintext[col]), col)

    key = manifest.primary_key
    passthrough = [c for c in manifest.public_columns if c != key]

    out_rows = []
    for i, r in enumerate(rows):
        row = {
            key: r.get(key),
            "logical_date": r["logical_date"],
            "masked_by": provider.name,
        }
        for col in passthrough:
            row[col] = r.get(col)
        for col in manifest.blind_indexed:
            row[f"{col}_blind_index"] = r.get(f"{col}_blind_index")
        for col, out_col in column_map.items():
            row[out_col] = apply_mask(masks[col], plaintext[col][i])
        out_rows.append(row)

    assert_no_plaintext_survives(out_rows, plaintext, column_map)

    out_dir.mkdir(parents=True, exist_ok=True)
    part = out_dir / f"dt={logical_date.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    dest = part / f"{manifest.dataset}_masked.parquet"

    import pyarrow as pa
    pq.write_table(pa.Table.from_pylist(out_rows), dest)

    schema_mod.write_manifest(manifest, dest)
    _write_hive_ddl(out_dir.parent / f"{manifest.dataset}_table.hql",
                    out_dir, manifest, column_map)

    log.info("wrote %d masked rows to %s", len(out_rows), dest)
    return {"rows": len(out_rows), "hive_path": str(dest),
            "masked_by": provider.name, "masks_applied": masks,
            "dataset": manifest.dataset, "policy_file": str(policy_file)}


# Postgres/inferred type -> the Hive type that holds it. Masked and
# blind-index columns are always STRING: a mask turns a number into
# "nnnn" or a hash, neither of which is a number any more.
_HIVE_TYPE = {
    "BIGINT": "BIGINT",
    "DOUBLE PRECISION": "DOUBLE",
    "TIMESTAMP": "STRING",
    "TEXT": "STRING",
}


def _write_hive_ddl(path: Path, warehouse_dir: Path, manifest,
                    column_map: dict[str, str]) -> None:
    """External table over the masked Parquet, partitioned by date.

    Columns are derived from the manifest rather than written out by hand, so
    the DDL matches whatever the dataset actually contains.
    """
    key = manifest.primary_key
    key_col = manifest.column(key)

    # A synthetic or sensitive key is a digest/blind index, not the source
    # value, so it is STRING regardless of what the column originally held.
    key_type = (_HIVE_TYPE.get(key_col.sql_type, "STRING")
                if key_col and not manifest.key_is_synthetic
                and not manifest.key_is_sensitive else "STRING")

    cols: list[tuple[str, str]] = [
        (key, key_type),
        ("logical_date", "STRING"),
    ]
    for col in manifest.public_columns:
        if col == key:
            continue
        spec = manifest.column(col)
        cols.append((col, _HIVE_TYPE.get(spec.sql_type, "STRING") if spec else "STRING"))
    for col in manifest.blind_indexed:
        cols.append((f"{col}_blind_index", "STRING"))
    for out_col in column_map.values():
        cols.append((out_col, "STRING"))
    cols.append(("masked_by", "STRING"))

    width = max(len(c) for c, _ in cols)
    body = ",\n".join(f"    {c.ljust(width)}  {t}" for c, t in cols)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"""-- Generated by pipeline/transform/hive_mask.py
CREATE DATABASE IF NOT EXISTS {DATABASE};

CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{manifest.dataset} (
{body}
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '{warehouse_dir.resolve()}';

MSCK REPAIR TABLE {DATABASE}.{manifest.dataset};
""")
