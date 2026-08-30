"""Contract bundle loader.

The pipeline never reads a binding YAML directly at runtime. It loads a
COMPILED BUNDLE with a content hash, so that every Gold row can name exactly
which configuration produced it. That is the difference between configuration
that is inspectable and configuration that is merely external.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"


def _load(rel: str) -> dict:
    return yaml.safe_load((CONFIG / rel).read_text(encoding="utf-8"))


def _hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]


@dataclass
class Bundle:
    dictionary: dict
    jurisdictions: dict
    operators: dict
    rules: dict
    scoring: dict
    classification: dict
    contract: dict
    bindings: dict[str, dict]
    binding_hashes: dict[str, str]
    lookups: dict[str, dict]
    bundle_hash: str

    # -- convenience accessors ------------------------------------------
    @property
    def contract_version(self) -> str:
        return self.contract["version"]

    @property
    def dictionary_version(self) -> str:
        return self.dictionary["dictionary_version"]

    def minor_units(self, currency: str) -> int:
        try:
            return self.jurisdictions["currencies"][currency]["minor_units"]
        except KeyError as e:
            raise KeyError(f"currency {currency!r} is not declared in jurisdictions.yaml") from e

    def fiscal_start_month(self, jurisdiction: str) -> int:
        try:
            return self.jurisdictions["jurisdictions"][jurisdiction]["fiscal_year_start_month"]
        except KeyError as e:
            raise KeyError(f"jurisdiction {jurisdiction!r} is not declared") from e

    def classification_of(self, canonical_path: str) -> str:
        """Look a field's classification up in the DICTIONARY -- the single
        place meaning is declared. Never inferred from a name."""
        entity, _, field = canonical_path.partition(".")
        ent = self.dictionary.get("entities", {}).get(entity)
        if ent and field in ent.get("fields", {}):
            return ent["fields"][field].get("classification", "non_pci")
        fact = self.dictionary.get("facts", {}).get(entity)
        if fact:
            m = fact.get("metrics", {}).get(field)
            if m:
                return m.get("classification", "non_pci")
        return "non_pci"

    def lookup_table(self, ref: str) -> dict:
        """Resolve a 'file.yaml#/a/b' reference into the table it names."""
        rel, _, pointer = ref.partition("#")
        rel = rel.removeprefix("lookups/").removeprefix("config/")
        table = self.lookups.get(rel)
        if table is None:
            table = _load(f"lookups/{rel}")
            self.lookups[rel] = table
        node = table
        for part in [p for p in pointer.split("/") if p]:
            node = node[part]
        return node


def load_bundle() -> Bundle:
    dictionary = _load("canonical/dictionary.yaml")
    jurisdictions = _load("canonical/jurisdictions.yaml")
    operators = _load("canonical/operators.yaml")
    rules = _load("canonical/rules.yaml")
    scoring = _load("canonical/scoring.yaml")
    classification = _load("classification/policy.yaml")
    contract = _load("contracts/quarterly_performance/v1.0.yaml")

    bindings, hashes = {}, {}
    for p in sorted((CONFIG / "bindings").glob("*.yaml")):
        b = yaml.safe_load(p.read_text(encoding="utf-8"))
        bindings[b["service_id"]] = b
        hashes[b["service_id"]] = _hash(b)

    lookups = {p.name: yaml.safe_load(p.read_text(encoding="utf-8"))
               for p in sorted((CONFIG / "lookups").glob("*.yaml"))}

    bundle = Bundle(
        dictionary=dictionary, jurisdictions=jurisdictions, operators=operators,
        rules=rules, scoring=scoring, classification=classification, contract=contract,
        bindings=bindings, binding_hashes=hashes, lookups=lookups, bundle_hash="",
    )
    bundle.bundle_hash = _hash({
        "dictionary": dictionary, "jurisdictions": jurisdictions, "operators": operators,
        "rules": rules, "scoring": scoring, "classification": classification,
        "contract": contract, "bindings": bindings,
    })
    _validate_closed_vocabulary(bundle)
    return bundle


def _validate_closed_vocabulary(b: Bundle) -> None:
    """Enforce the closed operator vocabulary at load time.

    A binding that reaches for an operator nobody declared is refused before a
    single record is read. This is the check that keeps 'config over code' from
    decaying quietly: you cannot smuggle a new transform in through a binding.
    """
    declared = set(b.operators["operators"])
    used: set[str] = set()

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "ops" and isinstance(v, list):
                    for op in v:
                        used.update(op.keys())
                else:
                    walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    for svc, binding in b.bindings.items():
        walk(binding)
    unknown = used - declared
    if unknown:
        raise ValueError(
            f"Bindings use operators not declared in canonical/operators.yaml: "
            f"{sorted(unknown)}. Add a named, documented, tested operator -- do not "
            f"special-case this in pipeline code."
        )
