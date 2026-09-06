"""Load and enforce the JSON Schema contracts under contracts/.

Two jobs. At runtime, `validate_message` and `validate_stage_event` are what
`envelope.parse` and the verdict task call, so a malformed message or stage
event is refused rather than interpreted. In CI, `check_backward_compatible`
and `iter_examples` are what the two make targets run (docs/DECISIONS.md D10,
D11).

Schemas reference each other by relative `$ref` (`envelope.schema.json`)
resolved against their `$id`, so the files are self-describing and portable;
the registry below simply preloads them so nothing is fetched.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import best_match
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = ROOT / "contracts"
EXAMPLES_DIR = CONTRACTS_DIR / "examples"

ENVELOPE_SCHEMA = "envelope.schema.json"
STAGE_EVENT_SCHEMA = "stage_event.schema.json"

# event_type -> payload schema. The envelope decides which contract applies.
SCHEMA_FOR_EVENT = {
    "row.encrypted": "row_encrypted.schema.json",
    "row.flat": "row_flat.schema.json",
    "run.verdict": "run_verdict.schema.json",
}


class ContractError(ValueError):
    """A message or stage event does not match its contract. Never swallowed."""


def schema_files() -> list[Path]:
    return sorted(CONTRACTS_DIR.glob("*.schema.json"))


@lru_cache(maxsize=None)
def load_schema(name: str) -> dict:
    path = CONTRACTS_DIR / name
    if not path.exists():
        raise ContractError(f"no contract named {name!r} in {CONTRACTS_DIR}")
    return json.loads(path.read_text())


@lru_cache(maxsize=None)
def _registry() -> Registry:
    resources = []
    for path in schema_files():
        schema = json.loads(path.read_text())
        resources.append((schema["$id"], Resource.from_contents(schema)))
    return Registry().with_resources(resources)


@lru_cache(maxsize=None)
def validator_for(name: str) -> Draft202012Validator:
    schema = load_schema(name)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, registry=_registry())


def _raise(errors, what: str) -> None:
    errors = list(errors)
    if not errors:
        return
    primary = best_match(errors)
    where = "/".join(str(p) for p in primary.absolute_path) or "<root>"
    others = len(errors) - 1
    more = f" (+{others} more)" if others else ""
    raise ContractError(f"{what} violates its contract at {where}: {primary.message}{more}")


def validate_message(msg: dict) -> None:
    """Validate a whole message against the schema its event_type selects."""
    if not isinstance(msg, dict) or "envelope" not in msg:
        raise ContractError("message has no envelope")
    event_type = (msg.get("envelope") or {}).get("event_type")
    name = SCHEMA_FOR_EVENT.get(event_type)
    if name is None:
        raise ContractError(f"unknown event_type {event_type!r}; no contract selects it")
    _raise(validator_for(name).iter_errors(msg), f"{event_type} message")


def validate_stage_event(event: dict) -> None:
    """Validate one stage's returned dict. Called before the verdict is computed."""
    stage = (event or {}).get("stage", "<none>")
    _raise(validator_for(STAGE_EVENT_SCHEMA).iter_errors(event), f"stage event {stage!r}")


def iter_examples() -> list[tuple[Path, dict]]:
    """Every example instance, paired with the parsed document."""
    out = []
    for path in sorted(EXAMPLES_DIR.glob("*.json")):
        out.append((path, json.loads(path.read_text())))
    return out


def schema_name_for_example(path: Path) -> str:
    """`run_verdict.example.json` -> `run_verdict.schema.json`."""
    stem = path.name.split(".")[0]
    return f"{stem}.schema.json"


# ── Platform tier: envelope embedding and backward compatibility ──────────

def payload_schemas_embed_envelope() -> list[str]:
    """Problems with how payload schemas use the envelope. Empty is good.

    A payload schema must `$ref` the envelope and may only *narrow* its own
    `event_type` and `key`; redefining any other envelope field is how the
    platform contract would drift one payload at a time.
    """
    allowed_overrides = {"event_type", "key"}
    problems = []
    for name in SCHEMA_FOR_EVENT.values():
        schema = load_schema(name)
        parts = schema.get("allOf", [])
        refs = [p.get("$ref") for p in parts if "$ref" in p]
        if ENVELOPE_SCHEMA not in refs:
            problems.append(f"{name}: does not $ref {ENVELOPE_SCHEMA}")
        for part in parts:
            env = part.get("properties", {}).get("envelope", {})
            extra = set(env.get("properties", {})) - allowed_overrides
            if extra:
                problems.append(f"{name}: redefines envelope fields {sorted(extra)}")
            if "required" in env or "additionalProperties" in env:
                problems.append(f"{name}: alters envelope required/additionalProperties")
    return problems


def check_backward_compatible(old: dict, new: dict, path: str = "") -> list[str]:
    """Changes in `new` that would break a consumer written against `old`.

    Deliberately narrow and explainable: a required field removed, a field's
    type changed, an enum value removed, a nested object made incompatible.
    Additions are fine -- that is the whole point of having a version field
    and additive evolution.
    """
    problems: list[str] = []
    here = path or "<root>"

    for req in old.get("required", []):
        if req not in new.get("required", []):
            problems.append(f"{here}: required field {req!r} is no longer required")

    if "type" in old and "type" in new and _types(old) != _types(new):
        if not _types(old) <= _types(new):
            problems.append(f"{here}: type changed {sorted(_types(old))} -> {sorted(_types(new))}")

    if "enum" in old and "enum" in new:
        removed = set(map(str, old["enum"])) - set(map(str, new["enum"]))
        if removed:
            problems.append(f"{here}: enum values removed {sorted(removed)}")
    if "const" in old and "const" in new and old["const"] != new["const"]:
        problems.append(f"{here}: const changed {old['const']!r} -> {new['const']!r}")

    old_props = old.get("properties", {})
    new_props = new.get("properties", {})
    for prop, old_sub in old_props.items():
        if prop not in new_props:
            if prop in old.get("required", []):
                problems.append(f"{here}: property {prop!r} removed")
            continue
        problems += check_backward_compatible(old_sub, new_props[prop], f"{here}.{prop}")

    for i, (o, n) in enumerate(zip(old.get("allOf", []), new.get("allOf", []))):
        problems += check_backward_compatible(o, n, f"{here}.allOf[{i}]")
    return problems


def _types(schema: dict) -> frozenset[str]:
    t = schema.get("type")
    if t is None:
        return frozenset()
    return frozenset([t] if isinstance(t, str) else t)
