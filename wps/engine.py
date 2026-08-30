"""The translation engine.

This module contains the entire mapping capability of the platform and NOT ONE
service-specific fact. Grep it for 'service_a' or 'COBOL' or 'xpath' and you
find nothing. Everything a service does differently lives in its binding.

That property is the thesis. If a mapping ever needs to be expressed here, the
correct response is to add a named operator to the closed vocabulary -- not a
branch in this file.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from wps.config import Bundle
from wps.io.decryption import DecryptionProvider, default_provider
from wps.operators import REGISTRY, MappingError
from wps.parse import parse


@dataclass
class RowContext:
    """Resolution scope for one record in flight."""
    bundle: Bundle
    binding: dict
    values: dict[str, Any] = field(default_factory=dict)
    rules_applied: dict[str, str] = field(default_factory=dict)
    decryption: DecryptionProvider = None

    def resolve(self, ref: str):
        """Resolve a reference used by an operator argument.

        '$.name'            -> a binding-level declaration
        'contract.currency' -> a canonical value resolved earlier this record
        """
        if ref.startswith("$."):
            return self.binding.get(ref[2:])
        return self.values.get(ref)

    def note_rule(self, rule_ref: str):
        self._pending_rule = rule_ref


@dataclass
class MappedRecord:
    values: dict[str, Any]
    rules: dict[str, str]
    errors: list[dict]
    source_service: str


# Canonical paths that other mappings depend on. Resolved first so that
# to_minor_units and fiscal_to_calendar_quarter always have what they need.
_PRIORITY = ("contract.settlement_currency", "contract.jurisdiction",
             "contract.contract_id", "merchant.merchant_id", "period.period_id")


def _apply_ops(value, ops: list[dict], ctx: RowContext, path: str, errors: list):
    for op_spec in ops:
        for name, args in op_spec.items():
            fn = REGISTRY.get(name)
            if fn is None:
                raise RuntimeError(f"operator {name!r} is declared but unimplemented")
            try:
                value = fn(value, args or {}, ctx)
            except MappingError as e:
                errors.append({"field": path, "operator": name, "reason": e.reason,
                               "value": repr(e.value)})
                return None
            except Exception as e:  # noqa: BLE001 - surfaced, never swallowed
                errors.append({"field": path, "operator": name, "reason": str(e),
                               "value": repr(value)})
                return None
    return value


def _source_value(rec: dict, spec: dict, binding: dict):
    src = spec.get("from")
    if src is None:
        return None
    return rec.get(src)


def _recompute_canonical(path: str, spec: dict, rec: dict, ctx: RowContext, errors: list):
    """Honour a binding's declared canonical_recompute.

    A service reporting a variant rule may still be able to REACH the canonical
    figure if it separately reports the components its rule netted out. The
    binding declares which fields those are and how to combine them; nothing
    about that arithmetic is service-specific here.

    Where a binding declares canonical_recomputable: false, no canonical value
    is produced and the gap is recorded. That refusal is deliberate -- an
    imputed reconciliation looks like evidence.
    """
    if not spec.get("canonical_recomputable"):
        return
    cr = spec.get("canonical_recompute")
    base = ctx.values.get(path)
    if cr is None:
        # Already canonical as mapped.
        ctx.values[path + "__canonical"] = base
        return
    if base is None:
        ctx.values[path + "__canonical"] = None
        return
    total = base
    for src_field in cr.get("from", []):
        part = _apply_ops(rec.get(src_field), cr.get("ops", []), ctx,
                          f"{path}__canonical", errors)
        if part is None:
            ctx.values[path + "__canonical"] = None
            return
        total = total + part if cr.get("combine") == "add" else total - part
    ctx.values[path + "__canonical"] = total


def map_record(rec: dict, binding: dict, bundle: Bundle,
               decryption: DecryptionProvider) -> MappedRecord:
    ctx = RowContext(bundle=bundle, binding=binding, decryption=decryption)
    errors: list[dict] = []
    mappings = binding.get("mappings", {})

    def do(path: str, spec: dict):
        if not isinstance(spec, dict) or "ops" not in spec:
            return
        raw = _source_value(rec, spec, binding)
        out = _apply_ops(raw, spec["ops"], ctx, path, errors)
        ctx.values[path] = out
        if "applies_rule" in spec:
            ctx.rules_applied[path] = spec["applies_rule"]
        _recompute_canonical(path, spec, rec, ctx, errors)

    for p in _PRIORITY:
        if p in mappings:
            do(p, mappings[p])
    for path, spec in mappings.items():
        if path in _PRIORITY or path == "quarterly_performance":
            continue
        do(path, spec)

    # Metric-narrow services declare a pivot: metrics arrive as rows keyed by
    # a code rather than as columns. Declared in config, applied generically.
    qp = mappings.get("quarterly_performance")
    if isinstance(qp, dict) and "pivot" in qp:
        pv = qp["pivot"]
        key_raw = rec.get(f"{pv['from_record']}.{pv['key_field']}")
        val_raw = rec.get(f"{pv['from_record']}.{pv['value_field']}")
        table = bundle.lookup_table(pv["key_lookup"])
        metric = table.get(str(key_raw).strip()) if key_raw else None
        if metric and metric in qp.get("metrics", {}):
            spec = qp["metrics"][metric]
            path = f"quarterly_performance.{metric}"
            out = _apply_ops(val_raw, spec.get("ops", []), ctx, path, errors)
            ctx.values[path] = out
            if "applies_rule" in spec:
                ctx.rules_applied[path] = spec["applies_rule"]

    return MappedRecord(values=ctx.values, rules=ctx.rules_applied, errors=errors,
                        source_service=binding["service_id"])


def map_service(service_id: str, source_path: Path, bundle: Bundle,
                decryption: DecryptionProvider | None = None) -> list[MappedRecord]:
    binding = bundle.bindings[service_id]
    decryption = decryption or default_provider()
    return [map_record(rec, binding, bundle, decryption)
            for rec in parse(binding, source_path)]
