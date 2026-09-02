"""Choose what to chart for a dataset nobody has seen before.

The report used to ask four questions it already knew the answers to: trips by
hour, duration histogram, top stations. For an arbitrary dataset there is no
such list, so this module derives one from the warehouse table's own shape.

THE RULE IT APPLIES
===================
Charts follow from column TYPE and cardinality, not from column names:

  * a numeric column          -> distribution (histogram)
  * a low-cardinality column  -> category counts (top values)
  * a timestamp column        -> counts over time (by hour)

Which is the same reasoning that produced the original three trips charts --
duration_minutes is numeric, start_station_masked is categorical, start_time is
a timestamp -- so the trips report keeps the shape it had while any other
dataset gets the equivalent treatment for its own columns.

Columns are capped and ranked so a 40-column export produces a readable page
rather than 40 charts.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)

# A categorical chart is only informative in a band: one value per row says
# nothing (it is an id), and 200 distinct values do not fit on a page.
MIN_CATEGORY_VALUES = 2
MAX_CATEGORY_VALUES = 12

MAX_CHARTS = 3

NUMERIC_TYPES = {"BIGINT", "DOUBLE PRECISION"}


def _column_types(cur, dataset: str) -> dict[str, str]:
    cur.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'warehouse' AND table_name = %s
    """, (dataset,))
    return {r[0]: r[1] for r in cur.fetchall()}


def _is_numeric(pg_type: str) -> bool:
    return pg_type in ("bigint", "integer", "smallint", "double precision",
                       "numeric", "real")


def _is_timestamp(pg_type: str) -> bool:
    return pg_type.startswith("timestamp")


# Columns that describe the pipeline rather than the data. Charting them tells
# the reader nothing about the dataset.
_STRUCTURAL = {"logical_date", "loaded_at", "masked_by", "row_hash"}


def choose_charts(cur, dataset: str, logical_date, primary_key: str) -> list[dict]:
    """Pick up to MAX_CHARTS specs, each a dict the renderer knows how to draw.

    Runs one cheap probe per candidate column rather than reasoning from the
    manifest alone, because what matters is the distribution actually present
    for THIS date -- a column that is entirely null today cannot be charted
    however promising its type looks.
    """
    types = _column_types(cur, dataset)
    specs: list[dict] = []

    numeric, categorical, temporal = [], [], []
    for col, pg_type in sorted(types.items()):
        if col in _STRUCTURAL or col == primary_key:
            continue
        # A blind index is a hash by construction: high cardinality, no
        # meaning to a reader. Masked columns stay eligible -- a masked
        # category still counts, which is the point of the top-stations chart.
        if col.endswith("_blind_index"):
            continue
        if _is_timestamp(pg_type):
            temporal.append(col)
        elif _is_numeric(pg_type):
            numeric.append(col)
        else:
            categorical.append(col)

    for col in temporal:
        cur.execute(
            f"SELECT count(*) FROM warehouse.{dataset} "
            f"WHERE logical_date = %s AND {col} IS NOT NULL", (logical_date,))
        if cur.fetchone()[0] > 0:
            specs.append({"kind": "by_hour", "column": col,
                          "title": f"Records by hour of {_label(col)}"})
            break

    for col in numeric:
        cur.execute(
            f"SELECT count({col}), min({col}), max({col}) "
            f"FROM warehouse.{dataset} WHERE logical_date = %s", (logical_date,))
        n, lo, hi = cur.fetchone()
        # A column with a single value has no distribution to show.
        if n and lo is not None and hi is not None and hi > lo:
            specs.append({"kind": "histogram", "column": col, "min": float(lo),
                          "max": float(hi),
                          "title": f"Distribution of {_label(col)}"})
            break

    # Rank rather than take the first alphabetically. A mask that collapses a
    # column to two or three values (MASK_SHOW_LAST_4 over short ids yields
    # "***", "**") is technically categorical but tells the reader nothing,
    # so prefer the column with the most distinct values still inside the
    # readable band -- which is the one carrying the most information.
    ranked = []
    for col in categorical:
        cur.execute(
            f"SELECT count(DISTINCT {col}) FROM warehouse.{dataset} "
            f"WHERE logical_date = %s AND {col} IS NOT NULL", (logical_date,))
        distinct = cur.fetchone()[0]
        if MIN_CATEGORY_VALUES <= distinct <= MAX_CATEGORY_VALUES:
            ranked.append((distinct, col))
    if ranked:
        _, col = max(ranked)
        specs.append({"kind": "categories", "column": col,
                      "title": f"Records by {_label(col)}"})

    if not specs:
        log.info("no chartable column found for %s; the report will be "
                 "tables and totals only", dataset)
    return specs[:MAX_CHARTS]


def _label(column: str) -> str:
    """A human label for a column name, noting when it is masked."""
    if column.endswith("_masked"):
        return column[:-len("_masked")].replace("_", " ") + " (masked)"
    return column.replace("_", " ")


def headline_metrics(cur, dataset: str, logical_date, primary_key: str,
                     types: dict[str, str] | None = None) -> list[tuple[str, str]]:
    """The tiles across the top: row count, plus stats for one numeric column.

    Falls back to the row count alone when nothing numeric is present, which is
    a legitimate outcome for a dataset of codes and labels.
    """
    types = types if types is not None else _column_types(cur, dataset)

    cur.execute(f"SELECT count(*) FROM warehouse.{dataset} "
                f"WHERE logical_date = %s", (logical_date,))
    tiles = [(f"{cur.fetchone()[0]:,}", "RECORDS")]

    for col, pg_type in sorted(types.items()):
        if col in _STRUCTURAL or col == primary_key or not _is_numeric(pg_type):
            continue
        # max() is rounded like the others: a raw double renders as
        # 2564.37434038074 and blows the tile's width apart.
        cur.execute(
            f"SELECT round(avg({col})::numeric, 1), round(max({col})::numeric, 1), "
            f"       round(sum({col})::numeric, 1) "
            f"FROM warehouse.{dataset} WHERE logical_date = %s", (logical_date,))
        avg, mx, total_v = cur.fetchone()
        if avg is None:
            continue
        # The tile is ~43mm wide; the prefix eats 4-6 of the available
        # characters, so the column label has to be short or it collides with
        # the next tile. Truncated with an ellipsis rather than silently cut.
        label = _label(col).upper()
        if len(label) > 12:
            label = label[:11] + "…"
        tiles += [(_fmt_number(avg), f"AVG {label}"),
                  (_fmt_number(mx), f"MAX {label}"),
                  (_fmt_number(total_v), f"TOTAL {label}")]
        break

    return tiles[:4]


def _fmt_number(value) -> str:
    """Thousands-separated, and without a pointless trailing .0."""
    if value is None:
        return "—"
    as_float = float(value)
    if as_float == int(as_float):
        return f"{int(as_float):,}"
    return f"{as_float:,.1f}"
