"""The contracts directory is well-formed, self-consistent, and gate-able.

These are the checks the two CI tiers run (make ci-service, make ci-platform),
expressed as tests so `pytest` alone catches a broken contract.
"""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.messaging import contracts as C   # noqa: E402


# ── Service tier ──────────────────────────────────────────────────────────

def test_there_is_a_contract_for_every_event_type():
    from pipeline.messaging.envelope import EVENT_TYPES
    assert set(C.SCHEMA_FOR_EVENT) == set(EVENT_TYPES)


@pytest.mark.parametrize("path", C.schema_files(), ids=lambda p: p.name)
def test_every_schema_is_valid_draft_2020_12(path):
    schema = json.loads(path.read_text())
    Draft202012Validator.check_schema(schema)
    assert schema["$id"].endswith(path.name), "the $id must name the file"


def test_examples_exist_for_every_message_schema():
    have = {C.schema_name_for_example(p) for p, _ in C.iter_examples()}
    want = set(C.SCHEMA_FOR_EVENT.values()) | {C.STAGE_EVENT_SCHEMA}
    assert want <= have, f"missing examples for {sorted(want - have)}"


@pytest.mark.parametrize("path,doc", C.iter_examples(), ids=lambda x: getattr(x, "name", ""))
def test_every_example_validates_against_its_schema(path, doc):
    name = C.schema_name_for_example(path)
    errors = list(C.validator_for(name).iter_errors(doc))
    assert not errors, f"{path.name}: {[e.message for e in errors][:3]}"


# ── Platform tier ─────────────────────────────────────────────────────────

def test_payload_schemas_embed_the_envelope_without_overriding_it():
    assert C.payload_schemas_embed_envelope() == []


def test_a_payload_schema_that_redefines_an_envelope_field_is_caught(monkeypatch):
    bad = copy.deepcopy(C.load_schema("row_flat.schema.json"))
    bad["allOf"][1]["properties"]["envelope"]["properties"]["trace_id"] = {"type": "integer"}
    monkeypatch.setattr(C, "load_schema",
                        lambda n, _b=bad, _o=C.load_schema.__wrapped__: _b if n == "row_flat.schema.json" else _o(n))
    problems = C.payload_schemas_embed_envelope()
    assert any("row_flat" in p and "trace_id" in p for p in problems)


class TestBackwardCompatibility:
    def _env(self):
        return copy.deepcopy(C.load_schema(C.ENVELOPE_SCHEMA))

    def test_identical_is_compatible(self):
        assert C.check_backward_compatible(self._env(), self._env()) == []

    def test_adding_an_optional_field_is_compatible(self):
        new = self._env()
        new["properties"]["envelope"]["properties"]["region"] = {"type": "string"}
        assert C.check_backward_compatible(self._env(), new) == []

    def test_removing_a_required_field_is_breaking(self):
        new = self._env()
        new["properties"]["envelope"]["required"].remove("trace_id")
        del new["properties"]["envelope"]["properties"]["trace_id"]
        problems = C.check_backward_compatible(self._env(), new)
        assert any("trace_id" in p and "no longer required" in p for p in problems)
        assert any("trace_id" in p and "removed" in p for p in problems)

    def test_changing_a_type_is_breaking(self):
        new = self._env()
        new["properties"]["envelope"]["properties"]["try_number"]["type"] = "string"
        problems = C.check_backward_compatible(self._env(), new)
        assert any("try_number" in p and "type changed" in p for p in problems)

    def test_widening_a_type_is_compatible(self):
        old = self._env()
        old["properties"]["envelope"]["properties"]["key"]["type"] = "string"
        assert C.check_backward_compatible(old, self._env()) == []   # new allows string|null

    def test_removing_an_enum_value_is_breaking(self):
        new = self._env()
        new["properties"]["envelope"]["properties"]["event_type"]["enum"].remove("run.verdict")
        problems = C.check_backward_compatible(self._env(), new)
        assert any("enum values removed" in p and "run.verdict" in p for p in problems)

    def test_adding_an_enum_value_is_compatible(self):
        new = self._env()
        new["properties"]["envelope"]["properties"]["event_type"]["enum"].append("row.masked")
        assert C.check_backward_compatible(self._env(), new) == []

    def test_changing_a_const_is_breaking(self):
        new = self._env()
        new["properties"]["envelope"]["properties"]["schema_version"]["const"] = "2"
        problems = C.check_backward_compatible(self._env(), new)
        assert any("schema_version" in p and "const changed" in p for p in problems)

    def test_nested_allof_is_walked(self):
        old = C.load_schema("run_verdict.schema.json")
        new = copy.deepcopy(old)
        new["allOf"][1]["properties"]["payload"]["required"].remove("proceed")
        problems = C.check_backward_compatible(old, new)
        assert any("proceed" in p for p in problems)


# ── Error reporting ───────────────────────────────────────────────────────

def test_contract_error_names_the_path_and_the_problem():
    with pytest.raises(C.ContractError) as ei:
        C.validate_stage_event({"trace_id": "t", "stage": "load", "inserted": -1,
                                "updated": 0, "run_id": "r", "dataset": "d"})
    assert "inserted" in str(ei.value)


def test_unknown_stage_is_refused():
    with pytest.raises(C.ContractError):
        C.validate_stage_event({"trace_id": "t", "stage": "teleport"})
