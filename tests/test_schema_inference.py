"""Invariants for the metadata layer that lets any CSV through the pipeline.

The schema manifest decides what gets encrypted, what gets masked, and what the
warehouse is keyed on. A wrong answer here is not a cosmetic bug -- it is
either a leak or a duplicate load, so each test below corresponds to a claim
the generic path makes.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.metadata import schema as schema_mod
from pipeline.metadata.schema import (
    DIRECT_IDENTIFIER,
    PUBLIC,
    QUASI_IDENTIFIER,
    infer,
    row_hash,
    safe_column,
)


def _write_csv(path: Path, header: list[str], rows: list[list]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    return path


# ── Classification ────────────────────────────────────────────────────────

def test_trips_schema_reproduces_the_original_hardcoded_sensitive_set(tmp_path):
    """The generic path must not weaken what the trips pipeline protected.

    Before the metadata layer, these six columns were a literal list in the
    encrypt job. If inference stops finding one of them, a column that used to
    be encrypted silently stops being -- which is why this test names them.
    """
    from pipeline.transform.spark_encrypt import TRIPS_SENSITIVE_COLUMNS

    csv_path = _write_csv(
        tmp_path / "trips_2026-09-01.csv",
        ["trip_id", "bike_id", "subscriber_type", "start_station_id",
         "start_station_name", "end_station_id", "end_station_name",
         "start_time", "duration_minutes"],
        [[f"t{i}", str(i), "Local365", "2537", "Zilker Park West", "2536",
          "Barton Springs", "2026-09-01T05:00:00", "12"] for i in range(5)])

    manifest = infer(csv_path)
    assert set(TRIPS_SENSITIVE_COLUMNS) <= set(manifest.sensitive_columns)


def test_numeric_measures_are_not_classified_as_identifiers(tmp_path):
    """A decimal must not read as a phone number.

    `1527.95901823672` is digits and punctuation, which an unguarded phone
    regex matches happily -- and encrypting an area column both wastes crypto
    calls and makes the value useless for the report.
    """
    csv_path = _write_csv(
        tmp_path / "areas_2026-09-01.csv",
        ["acres", "shape_area", "shape_length"],
        [["1527.95901823672", "6190151.26965332", "28441.1107511796"],
         ["793.180505379294", "3212833.44396973", "12041.403486466"],
         ["468.374126408812", "1897345.77697754", "12234.8063854637"]])

    manifest = infer(csv_path)
    assert manifest.sensitive_columns == []
    assert all(c.sensitivity == PUBLIC for c in manifest.columns)


@pytest.mark.parametrize("header,expected", [
    ("email", DIRECT_IDENTIFIER),
    ("customer_name", DIRECT_IDENTIFIER),
    ("home_address", DIRECT_IDENTIFIER),
    ("county_des", QUASI_IDENTIFIER),
    ("start_station_name", QUASI_IDENTIFIER),
    ("globalid", QUASI_IDENTIFIER),
    ("sale_year", PUBLIC),
    ("oil_gas_sale_status", PUBLIC),
])
def test_column_name_classification(tmp_path, header, expected):
    csv_path = _write_csv(tmp_path / "d_2026-09-01.csv", [header],
                          [["a"], ["b"], ["c"]])
    assert infer(csv_path).columns[0].sensitivity == expected


def test_values_classify_a_column_whose_name_says_nothing(tmp_path):
    """Real exports carry headers like `f_3`; the values still give it away."""
    csv_path = _write_csv(
        tmp_path / "d_2026-09-01.csv", ["f_3"],
        [["ada@example.com"], ["bob@example.com"], ["cy@example.org"],
         ["dee@example.net"]])
    assert infer(csv_path).columns[0].sensitivity == DIRECT_IDENTIFIER


# ── Key selection ─────────────────────────────────────────────────────────

def test_unique_column_becomes_the_natural_key(tmp_path):
    csv_path = _write_csv(
        tmp_path / "d_2026-09-01.csv", ["parcel_id", "status"],
        [["CO-1", "Sale"], ["CO-2", "Sale"], ["CO-3", "Closed"]])
    manifest = infer(csv_path)
    assert manifest.primary_key == "parcel_id"
    assert manifest.key_is_synthetic is False


def test_no_unique_column_falls_back_to_a_synthetic_key(tmp_path):
    csv_path = _write_csv(
        tmp_path / "d_2026-09-01.csv", ["status", "year"],
        [["Sale", "26"], ["Sale", "26"], ["Closed", "26"]])
    manifest = infer(csv_path)
    assert manifest.primary_key == schema_mod.ROW_HASH_KEY
    assert manifest.key_is_synthetic is True


def test_row_hash_is_stable_for_the_same_row_and_date():
    """Idempotency depends on this: same row, same date -> same key."""
    row = {"a": "1", "b": "x"}
    cols = ["a", "b"]
    assert row_hash(row, cols, "2026-09-01") == row_hash(row, cols, "2026-09-01")


def test_row_hash_differs_across_dates_and_rows():
    cols = ["a", "b"]
    base = row_hash({"a": "1", "b": "x"}, cols, "2026-09-01")
    assert base != row_hash({"a": "1", "b": "x"}, cols, "2026-09-02")
    assert base != row_hash({"a": "2", "b": "x"}, cols, "2026-09-01")


def test_a_sensitive_primary_key_is_always_blind_indexed(tmp_path):
    """The encrypt stage keys such a row on the blind index.

    Without the index there is nothing to key on but the plaintext, so this is
    a correctness requirement rather than a performance one.
    """
    csv_path = _write_csv(
        tmp_path / "d_2026-09-01.csv", ["parcel_id", "status"],
        [["CO-1", "Sale"], ["CO-2", "Sale"], ["CO-3", "Closed"]])
    manifest = infer(csv_path)
    assert manifest.key_is_sensitive
    assert manifest.primary_key in manifest.blind_indexed


# ── Identifier safety ─────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("Oil & Gas Sale Status", "oil_gas_sale_status"),
    ("Shape__Area", "shape_area"),
    ("﻿OBJECTID", "objectid"),
    ("Sale Year", "sale_year"),
    ("123abc", "c_123abc"),
])
def test_headers_become_safe_sql_identifiers(raw, expected):
    """These reach SQL DDL directly; an unquoted `&` or BOM is a syntax error."""
    assert safe_column(raw) == expected


def test_colliding_headers_stay_distinct(tmp_path):
    """`Sale Year` and `Sale_Year` both normalise to sale_year."""
    csv_path = _write_csv(tmp_path / "d_2026-09-01.csv",
                          ["Sale Year", "Sale_Year"], [["26", "27"]])
    names = [c.name for c in infer(csv_path).columns]
    assert len(set(names)) == len(names)


# ── Dataset naming ────────────────────────────────────────────────────────

def test_per_date_files_resolve_to_one_dataset():
    """Otherwise every day would create its own warehouse table."""
    a = schema_mod.dataset_name_for(Path("data/csv/trips_2026-09-01.csv"))
    b = schema_mod.dataset_name_for(Path("data/csv/trips_2026-09-02.csv"))
    assert a == b == "trips"


def test_export_id_suffix_is_stripped():
    name = schema_mod.dataset_name_for(
        Path("BLM_CO_Q2_2026_Oil_and_Gas_Lease_Sale_-2400573660170243848.csv"))
    assert name == "blm_co_q2_2026_oil_and_gas_lease_sale"


# ── Overrides ─────────────────────────────────────────────────────────────

def test_override_can_raise_a_column_the_classifier_called_public(tmp_path, monkeypatch):
    """A wrong guess must be correctable without editing classifier code."""
    monkeypatch.setattr(schema_mod, "DATASET_CONFIG_DIR", tmp_path / "cfg")
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "d.json").write_text(
        '{"columns": {"internal_code": {"sensitivity": "direct_identifier"}}}')

    csv_path = _write_csv(tmp_path / "d.csv", ["internal_code"],
                          [["x"], ["y"], ["z"]])
    manifest = infer(csv_path, dataset="d")
    assert manifest.columns[0].sensitivity == DIRECT_IDENTIFIER
    assert "internal_code" in manifest.overrides_applied


def test_declared_primary_key_that_does_not_exist_fails_loudly(tmp_path, monkeypatch):
    monkeypatch.setattr(schema_mod, "DATASET_CONFIG_DIR", tmp_path / "cfg")
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "d.json").write_text('{"primary_key": "nope"}')

    csv_path = _write_csv(tmp_path / "d.csv", ["a"], [["1"]])
    with pytest.raises(RuntimeError, match="not a column"):
        infer(csv_path, dataset="d")


# ── Empty and malformed input ─────────────────────────────────────────────

def test_header_only_csv_is_rejected(tmp_path):
    csv_path = _write_csv(tmp_path / "d_2026-09-01.csv", ["a", "b"], [])
    with pytest.raises(RuntimeError, match="no rows"):
        infer(csv_path)
