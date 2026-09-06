"""The dataset contract: written once, never regenerated, fails on drift."""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.metadata import contract as C        # noqa: E402
from pipeline.metadata import schema as schema_mod  # noqa: E402


@pytest.fixture(autouse=True)
def _isolated_contract_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(schema_mod, "DATASET_CONFIG_DIR", tmp_path / "datasets")


def _csv(tmp_path, name, header, rows):
    p = tmp_path / f"{name}_2026-09-06.csv"
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return p


def _rows(n=5, email=True):
    return [[str(i), f"u{i}@x.com" if email else "n/a", "9.99", f"2026-09-0{(i % 5) + 1}T10:00:00"]
            for i in range(n)]


HEADER = ["id", "customer_email", "sale_price", "sold_at"]


def test_first_sight_writes_a_generated_contract(tmp_path):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    m = schema_mod.infer(p)
    check = C.ensure_contract(m, p)
    assert check.created is True
    doc = json.loads(check.path.read_text())
    assert doc["schema"]["_generated"] is True
    assert doc["schema"]["primary_key"] == "id"
    assert doc["schema"]["columns"]["customer_email"]["sensitivity"] == "direct_identifier"
    assert doc["schema"]["columns"]["sale_price"]["sql_type"] == "DOUBLE PRECISION"


def test_same_data_again_matches_and_does_not_rewrite(tmp_path):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    m = schema_mod.infer(p)
    first = C.ensure_contract(m, p)
    before = first.path.read_text()
    second = C.ensure_contract(schema_mod.infer(p), p)
    assert second.created is False and second.differences == []
    assert first.path.read_text() == before


def test_a_new_column_fails_naming_it(tmp_path):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    C.ensure_contract(schema_mod.infer(p), p)
    p2 = _csv(tmp_path, "sales", HEADER + ["agent_phone"],
              [r + ["+1 555 010 0000"] for r in _rows()])
    with pytest.raises(C.ContractMismatch) as ei:
        C.ensure_contract(schema_mod.infer(p2), p2)
    msg = str(ei.value)
    assert "'agent_phone': in the data, not in the contract" in msg
    assert "contract accept" in msg     # tells the human how to accept it


def test_a_missing_column_fails(tmp_path):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    C.ensure_contract(schema_mod.infer(p), p)
    p2 = _csv(tmp_path, "sales", HEADER[:3], [r[:3] for r in _rows()])
    with pytest.raises(C.ContractMismatch, match="'sold_at': in the contract, missing"):
        C.ensure_contract(schema_mod.infer(p2), p2)


def test_a_changed_type_fails(tmp_path):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    C.ensure_contract(schema_mod.infer(p), p)
    rows = _rows()
    rows[0][2] = "nine ninety-nine"       # sale_price stops being numeric
    p2 = _csv(tmp_path, "sales", HEADER, rows)
    with pytest.raises(C.ContractMismatch, match="'sale_price' sql_type: contract 'DOUBLE PRECISION', observed 'TEXT'"):
        C.ensure_contract(schema_mod.infer(p2), p2)


def test_a_reclassified_column_fails(tmp_path):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    C.ensure_contract(schema_mod.infer(p), p)
    # Same column, but its values no longer look like emails and its name
    # says nothing -- still classified by name, so simulate by renaming.
    p2 = _csv(tmp_path, "sales", ["id", "customer_ref", "sale_price", "sold_at"],
              [[r[0], "ref-" + r[0], r[2], r[3]] for r in _rows()])
    with pytest.raises(C.ContractMismatch) as ei:
        C.ensure_contract(schema_mod.infer(p2), p2)
    assert "customer_email" in str(ei.value) and "customer_ref" in str(ei.value)


def test_a_changed_primary_key_fails(tmp_path):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    C.ensure_contract(schema_mod.infer(p), p)
    rows = _rows()
    rows[1][0] = rows[0][0]               # id no longer unique -> synthetic key
    p2 = _csv(tmp_path, "sales", HEADER, rows)
    with pytest.raises(C.ContractMismatch, match="primary_key"):
        C.ensure_contract(schema_mod.infer(p2), p2)


def test_column_order_is_not_a_schema_change(tmp_path):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    C.ensure_contract(schema_mod.infer(p), p)
    reordered = ["sold_at", "id", "sale_price", "customer_email"]
    p2 = _csv(tmp_path, "sales", reordered,
              [[r[3], r[0], r[2], r[1]] for r in _rows()])
    check = C.ensure_contract(schema_mod.infer(p2), p2)
    assert check.differences == []


def test_existing_override_file_gains_a_schema_block_and_keeps_its_overrides(tmp_path):
    d = schema_mod.DATASET_CONFIG_DIR
    d.mkdir(parents=True)
    (d / "sales.json").write_text(json.dumps({"columns": {"sold_at": {"sensitivity": "public"}}}))
    p = _csv(tmp_path, "sales", HEADER, _rows())
    check = C.ensure_contract(schema_mod.infer(p), p)
    doc = json.loads(check.path.read_text())
    assert doc["columns"] == {"sold_at": {"sensitivity": "public"}}
    assert "schema" in doc and check.created


def test_accept_rewrites_the_block_and_reports_the_change(tmp_path):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    C.ensure_contract(schema_mod.infer(p), p)
    p2 = _csv(tmp_path, "sales", HEADER + ["note"], [r + ["x"] for r in _rows()])
    changes = C.accept("sales", p2)
    assert any("'note'" in c for c in changes)
    doc = json.loads(C.contract_path("sales").read_text())
    assert doc["schema"]["_generated"] is False
    assert "note" in doc["schema"]["columns"]
    # And now the new shape passes.
    assert C.ensure_contract(schema_mod.infer(p2), p2).differences == []


def test_accept_cli_entry_point(tmp_path, capsys):
    p = _csv(tmp_path, "sales", HEADER, _rows())
    assert C.main(["accept", "sales", str(p)]) == 0
    assert "contract for 'sales' updated" in capsys.readouterr().out
    assert C.main(["bogus"]) == 2


class TestEventTime:
    def test_declared_event_time_column_yields_the_data_window(self, tmp_path):
        d = schema_mod.DATASET_CONFIG_DIR
        d.mkdir(parents=True)
        (d / "sales.json").write_text(json.dumps({"event_time_column": "sold_at"}))
        p = _csv(tmp_path, "sales", HEADER, _rows())
        check = C.ensure_contract(schema_mod.infer(p), p)
        assert check.event_time_column == "sold_at"
        assert check.window == ("2026-09-01T10:00:00", "2026-09-05T10:00:00")

    def test_undeclared_means_no_window(self, tmp_path):
        p = _csv(tmp_path, "sales", HEADER, _rows())
        check = C.ensure_contract(schema_mod.infer(p), p)
        assert check.event_time_column is None and check.window is None

    def test_declaring_a_column_that_does_not_exist_fails(self, tmp_path):
        d = schema_mod.DATASET_CONFIG_DIR
        d.mkdir(parents=True)
        (d / "sales.json").write_text(json.dumps({"event_time_column": "shipped_at"}))
        p = _csv(tmp_path, "sales", HEADER, _rows())
        with pytest.raises(RuntimeError, match="shipped_at"):
            C.ensure_contract(schema_mod.infer(p), p)

    def test_window_uses_the_source_header_name(self, tmp_path):
        d = schema_mod.DATASET_CONFIG_DIR
        d.mkdir(parents=True)
        (d / "sales.json").write_text(json.dumps({"event_time_column": "sold_at"}))
        p = _csv(tmp_path, "sales", ["id", "customer_email", "sale_price", "Sold At"], _rows())
        check = C.ensure_contract(schema_mod.infer(p), p)
        assert check.window is not None


def test_invalid_json_contract_refuses_to_guess(tmp_path):
    d = schema_mod.DATASET_CONFIG_DIR
    d.mkdir(parents=True)
    (d / "sales.json").write_text("{not json")
    p = _csv(tmp_path, "sales", HEADER, _rows())
    with pytest.raises(RuntimeError):
        schema_mod.infer(p)   # the override loader already refuses; the contract would too
