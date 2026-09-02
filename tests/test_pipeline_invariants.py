"""The invariants that must hold for this pipeline to be trustworthy.

These are not unit tests of convenience. Each one corresponds to a claim the
system makes about protecting data, and a failure here means the claim is false.
"""
from __future__ import annotations

import csv
import os
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.mask.ranger import LocalPolicyProvider, MaskingError, apply_mask
from pipeline.transform.spark_encrypt import (
    TRIPS_SENSITIVE_COLUMNS as SENSITIVE_COLUMNS,
    assert_no_plaintext,
)


# ── Masking ───────────────────────────────────────────────────────────────

def test_every_sensitive_column_has_a_masking_policy():
    """A sensitive column with no policy would be published in the clear."""
    masks = LocalPolicyProvider().masks_for("trips_warehouse", "trips", "analyst")
    missing = set(SENSITIVE_COLUMNS) - set(masks)
    assert not missing, f"no Ranger masking policy for: {sorted(missing)}"


def test_analyst_masks_are_not_mask_none():
    """MASK_NONE for the analyst audience would defeat the entire point."""
    masks = LocalPolicyProvider().masks_for("trips_warehouse", "trips", "analyst")
    unmasked = [c for c, m in masks.items() if m in ("MASK_NONE", "NONE")]
    assert not unmasked, f"analyst sees these columns unmasked: {unmasked}"


def test_unknown_mask_type_raises_rather_than_passing_through():
    """Silently returning the input on an unrecognised policy publishes PII."""
    with pytest.raises(MaskingError):
        apply_mask("MASK_SOMETHING_INVENTED", "sensitive-value")


@pytest.mark.parametrize("mask_type,value,expected", [
    ("MASK_SHOW_LAST_4", "847293", "**7293"),
    ("MASK_NULL", "2536", None),
    ("MASK_SHOW_FIRST_8", "Barton Springs & Riverside", "Barton S******************"),
    ("MASK", "Local365", "xxxxxnnn"),
])
def test_ranger_mask_semantics(mask_type, value, expected):
    assert apply_mask(mask_type, value) == expected


def test_mask_preserves_null():
    assert apply_mask("MASK_SHOW_LAST_4", None) is None


# ── Plaintext guard ───────────────────────────────────────────────────────

def test_plaintext_guard_catches_unencrypted_column():
    src = [{c: "Barton Springs & Riverside" for c in SENSITIVE_COLUMNS}]
    out = [{f"{c}_encrypted": "enc:v1:AAAA" for c in SENSITIVE_COLUMNS}]
    out[0]["bike_id"] = "Barton Springs & Riverside"      # survived in the clear
    with pytest.raises(RuntimeError, match="REFUSING TO WRITE"):
        assert_no_plaintext(out, src, SENSITIVE_COLUMNS)


def test_plaintext_guard_catches_missing_ciphertext():
    src = [{c: "value-here" for c in SENSITIVE_COLUMNS}]
    out = [{f"{c}_encrypted": "enc:v1:AAAA" for c in SENSITIVE_COLUMNS}]
    out[0]["bike_id_encrypted"] = None
    with pytest.raises(RuntimeError, match="REFUSING TO WRITE"):
        assert_no_plaintext(out, src, SENSITIVE_COLUMNS)


def test_plaintext_guard_catches_non_envelope_output():
    """A value that was never encrypted at all must be caught."""
    src = [{c: "value-here" for c in SENSITIVE_COLUMNS}]
    out = [{f"{c}_encrypted": "enc:v1:AAAA" for c in SENSITIVE_COLUMNS}]
    out[0]["subscriber_type_encrypted"] = "Local365"
    with pytest.raises(RuntimeError, match="REFUSING TO WRITE"):
        assert_no_plaintext(out, src, SENSITIVE_COLUMNS)


def test_plaintext_guard_allows_short_values_without_false_positives():
    """A 1-char bikeid must not trip the substring check by chance."""
    import base64
    src = [{c: "1" for c in SENSITIVE_COLUMNS}]
    payload = base64.b64encode(b"\x00" * 28).decode()
    out = [{f"{c}_encrypted": f"enc:v1:{payload}" for c in SENSITIVE_COLUMNS}]
    assert_no_plaintext(out, src, SENSITIVE_COLUMNS)            # must not raise


# ── Extract cost controls ─────────────────────────────────────────────────

def test_extractor_refuses_an_unbounded_row_limit():
    from pipeline.extract.bigquery_extract import BigQueryExtractor
    with pytest.raises(ValueError, match="sanity ceiling"):
        BigQueryExtractor("p", "d", "t", row_limit=100_000)


def test_query_never_uses_select_star():
    """SELECT * scans every column and is the main way this gets expensive."""
    from pipeline.extract.bigquery_extract import BigQueryExtractor
    pytest.importorskip("google.cloud.bigquery")
    sql, _ = BigQueryExtractor("p", "bigquery-public-data.x", "y").build_query(
        date(2026, 8, 27))
    assert "SELECT *" not in sql.upper()
    assert "LIMIT @row_limit" in sql


def test_query_is_parameterised_not_interpolated():
    from pipeline.extract.bigquery_extract import BigQueryExtractor
    pytest.importorskip("google.cloud.bigquery")
    sql, params = BigQueryExtractor("p", "bigquery-public-data.x", "y").build_query(
        date(2026, 8, 27))
    assert "2026-08-27" not in sql, "date was interpolated into the SQL string"
    assert {p.name for p in params} == {"window_start", "window_end", "row_limit"}


# ── Extract output ────────────────────────────────────────────────────────

def test_fixture_extract_is_idempotent(tmp_path):
    from pipeline.extract.fixture import FixtureExtractor
    a = FixtureExtractor(row_limit=100).extract(date(2026, 8, 27), tmp_path / "a")
    b = FixtureExtractor(row_limit=100).extract(date(2026, 8, 27), tmp_path / "b")
    assert Path(a.csv_path).read_text() == Path(b.csv_path).read_text()


def test_fixture_respects_the_row_cap(tmp_path):
    from pipeline.extract.fixture import FixtureExtractor
    r = FixtureExtractor(row_limit=100).extract(date(2026, 8, 27), tmp_path)
    with open(r.csv_path) as fh:
        assert len(list(csv.DictReader(fh))) == 100
