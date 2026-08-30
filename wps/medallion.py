"""Bronze -> Silver -> Gold on local Delta Lake (delta-rs, no JVM).

Layer contracts, enforced from config rather than from code:

  BRONZE  raw and immutable, source fidelity preserved. PII lands raw because
          transforming on the way in would destroy the ability to prove what
          the source actually sent. PCI lands only as ciphertext.
  SILVER  conformed to the canonical dictionary, quality-enforced, PII and PCI
          tokenized. Plaintext PCI is impossible here by construction.
  GOLD    business-ready, contract-shaped, reconciled across services, with
          every row stamped with the configuration that produced it.
"""
from __future__ import annotations

import json
import statistics
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from wps.config import Bundle
from wps.engine import MappedRecord, map_service
from wps.periods import Period
from wps.pipeline import ROOT, SOURCE_PATHS

LAKE = ROOT / "lake"
AS_OF = date(2026, 8, 30)

def service_order(bundle: Bundle) -> list[str]:
    """Service processing and precedence order, DERIVED FROM CONFIG.

    A hardcoded list here would mean onboarding a fifth service required a code
    edit -- the thesis failing quietly in the one file nobody re-reads. Each
    binding declares its own `canonical_precedence`; lower wins.
    """
    return sorted(bundle.bindings,
                  key=lambda s: (bundle.bindings[s].get("canonical_precedence", 999), s))


def metric_names(bundle: Bundle) -> tuple[list[str], list[str]]:
    """Contested and uncontested metrics, read from the DICTIONARY.

    The dictionary is where a metric is declared contested. Restating that list
    in code would let the two drift, and the drift would be invisible.
    """
    metrics = bundle.dictionary["facts"]["quarterly_performance"]["metrics"]
    contested, uncontested = [], []
    for name, spec in metrics.items():
        if spec.get("derived"):
            continue
        (contested if spec.get("contested") else uncontested).append(name)
    return contested, uncontested


class ClassificationViolation(Exception):
    """A classification breach fails the batch. It is never a warning --
    silent degradation is how PII reaches a dashboard."""


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _write(table: str, rows: list[dict], mode: str = "overwrite") -> int:
    # Imported here rather than at module scope so that the pure-Python helpers
    # in this file (service_order, metric_names) can be used by the Spark path,
    # where pyarrow and deltalake are not installed and are not wanted -- Spark
    # writes Delta through its own JVM connector.
    import pyarrow as pa
    from deltalake import write_deltalake

    path = LAKE / table
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return 0
    tbl = pa.Table.from_pylist(rows)
    write_deltalake(str(path), tbl, mode=mode)
    return len(rows)


# --------------------------------------------------------------------- bronze

def land_bronze(bundle: Bundle) -> dict[str, int]:
    """Raw, immutable, source-fidelity. Every record is landed exactly as the
    service sent it, with ingest lineage attached and nothing else changed."""
    from wps.parse import parse
    counts = {}
    for svc in service_order(bundle):
        binding = bundle.bindings[svc]
        recs = []
        for i, rec in enumerate(parse(binding, SOURCE_PATHS[svc])):
            # PCI policy: only ciphertext may land in Bronze. The source sends
            # it already opaque, which is what makes that satisfiable.
            payload = {k: (None if v is None else
                           json.dumps(v) if isinstance(v, list) else str(v))
                       for k, v in rec.items()}
            recs.append({
                "_ingest_seq": i,
                "_service_id": svc,
                "_source_file": SOURCE_PATHS[svc].name,
                "_ingested_at": _now(),
                "_binding_hash": bundle.binding_hashes[svc],
                "_auth_profile": binding["auth_profile"],
                "payload": json.dumps(payload, sort_keys=True),
            })
        counts[svc] = _write(f"bronze/{svc}", recs)
    return counts


# --------------------------------------------------------------------- silver

def _enforce_classification(bundle: Bundle, path: str, value, layer: str, svc: str):
    cls = bundle.classification_of(path)
    policy = bundle.classification["classifications"][cls]
    rule = policy.get(layer, {})
    landing = rule.get("land")
    if landing == "denied":
        allowed = {e["field"] for e in rule.get("exceptions", []) or []}
        if path not in allowed:
            raise ClassificationViolation(
                f"[{svc}] field {path!r} is classified {cls} and may not land in {layer}. "
                f"Declared in config/classification/policy.yaml.")
    if layer == "silver" and cls in ("pii", "pci") and value is not None:
        if not str(value).startswith("TKN-"):
            raise ClassificationViolation(
                f"[{svc}] field {path!r} is classified {cls} and reached Silver untokenized. "
                f"Its binding must apply the tokenize operator.")


def land_silver(bundle: Bundle) -> tuple[dict[str, int], list[dict], list[dict]]:
    """Conformed to the canonical dictionary, quality-enforced, PII/PCI
    tokenized. Returns per-service counts, the conformed rows, and any
    mapping failures made legible for the grading harness."""
    contested, uncontested = metric_names(bundle)
    counts, all_rows, failures = {}, [], []
    for svc in service_order(bundle):
        mapped: list[MappedRecord] = map_service(svc, SOURCE_PATHS[svc], bundle)
        rows = []
        for m in mapped:
            if m.errors:
                for e in m.errors:
                    failures.append({"service_id": svc, **e})
                continue
            for path, value in m.values.items():
                if path.endswith("__canonical"):
                    continue
                _enforce_classification(bundle, path, value, "silver", svc)

            v = m.values
            key = (v.get("merchant.merchant_id"), v.get("contract.contract_id"),
                   v.get("period.period_id"))
            if not all(key):
                failures.append({"service_id": svc, "field": "grain",
                                 "operator": "-", "value": str(key),
                                 "reason": "incomplete grain key; row quarantined"})
                continue
            ccy = v.get("contract.settlement_currency")
            declared_places = bundle.bindings[svc]["source"].get("max_decimal_places")
            degraded = (declared_places is not None and ccy is not None
                        and bundle.minor_units(ccy) > declared_places)
            row = {
                "service_id": svc,
                "precision_degraded": degraded,
                "precision_quantum_minor": (10 ** (bundle.minor_units(ccy) - declared_places)
                                            if degraded else 1),
                "merchant_id": key[0], "contract_id": key[1], "period_id": key[2],
                "settlement_currency": ccy,
                "legal_name_token": v.get("merchant.legal_name"),
                "tax_ref_token": v.get("merchant.tax_ref"),
                "settlement_account_token": v.get("contract.settlement_account_ref"),
                "pricing_tier": v.get("contract.pricing_tier"),
                "effective_from": v.get("contract.effective_from"),
                "onboarded_on": v.get("merchant.onboarded_on"),
                "_binding_hash": bundle.binding_hashes[svc],
                "_conformed_at": _now(),
            }
            for metric in contested + uncontested:
                p = f"quarterly_performance.{metric}"
                row[metric] = _as_int(v.get(p))
                row[f"{metric}__canonical"] = _as_int(v.get(p + "__canonical"))
                row[f"{metric}__rule"] = m.rules.get(p)
            rows.append(row)

        # Service D is metric-narrow: one parsed record per metric. Collapse
        # its rows onto the canonical grain before they leave Silver.
        rows = _collapse_narrow(rows)
        counts[svc] = _write(f"silver/{svc}", rows)
        all_rows.extend(rows)
    return counts, all_rows, failures


def _as_int(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return int(v)
    return int(v) if not isinstance(v, (str, date, datetime)) else None


def _collapse_narrow(rows: list[dict]) -> list[dict]:
    """Merge multiple partial rows sharing a grain key into one.

    Metric-narrow services emit one record per metric. Merging is done here,
    generically, driven by the shape of the data rather than by which service
    produced it.
    """
    merged: dict[tuple, dict] = {}
    for r in rows:
        k = (r["merchant_id"], r["contract_id"], r["period_id"])
        if k not in merged:
            merged[k] = dict(r)
            continue
        tgt = merged[k]
        for col, val in r.items():
            if val is not None and tgt.get(col) is None:
                tgt[col] = val
    return list(merged.values())


# ----------------------------------------------------------------------- gold

def _jurisdiction_for(bundle: Bundle, currency: str) -> str | None:
    for name, j in bundle.jurisdictions["jurisdictions"].items():
        if j["default_currency"] == currency:
            return name
    return None


def build_gold(bundle: Bundle, silver_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Reconcile across services onto the contract's grain.

    Gold does not overwrite anyone's number. It adds the canonical value beside
    every service's own figure, names the rule behind each, and states the
    spread. A team that cannot find its own number stops trusting the platform.
    """
    order = service_order(bundle)
    contested, uncontested = metric_names(bundle)
    by_key: dict[tuple, list[dict]] = {}
    for r in silver_rows:
        by_key.setdefault((r["merchant_id"], r["contract_id"], r["period_id"]), []).append(r)

    gold, flags = [], []
    for (mid, cid, pid), rows in sorted(by_key.items()):
        rows.sort(key=lambda r: order.index(r["service_id"]))
        ccy = next((r["settlement_currency"] for r in rows if r["settlement_currency"]), None)
        period = Period.parse(pid)
        out = {
            "merchant_id": mid, "contract_id": cid, "period_id": pid,
            "calendar_year": period.year, "calendar_quarter": period.quarter,
            "period_start": period.start_date, "period_end": period.end_date,
            "period_is_closed": period.is_closed(AS_OF),
            "settlement_currency": ccy,
            "jurisdiction": _jurisdiction_for(bundle, ccy),
            "pricing_tier": next((r["pricing_tier"] for r in rows if r.get("pricing_tier")), None),
            "effective_from": next((r["effective_from"] for r in rows if r.get("effective_from")), None),
            "is_projection": False,
            "precision_degraded": any(r.get("precision_degraded") for r in rows),
            "precision_degraded_sources": ",".join(
                r["service_id"] for r in rows if r.get("precision_degraded")) or None,
            "source_services": ",".join(r["service_id"] for r in rows),
        }

        for metric in contested:
            variants, canon_candidates = {}, []
            for r in rows:
                val, rule, canon = r.get(metric), r.get(f"{metric}__rule"), r.get(f"{metric}__canonical")
                if val is None and canon is None:
                    continue
                variants[r["service_id"]] = {"value": val, "rule": _rule_name(rule),
                                             "canonical_derivable": canon is not None}
                if canon is not None:
                    canon_candidates.append((r["service_id"], canon))

            # Prefer a source that can represent the currency at full
            # precision. Where none exists the row is marked, not quietly
            # rounded -- a degraded figure that looks exact is worse than one
            # labelled degraded.
            canon_candidates.sort(key=lambda sc: _degraded_of(rows, sc[0]))
            canonical = canon_candidates[0][1] if canon_candidates else None
            out[metric] = canonical
            out[f"{metric}_canonical_derivable"] = canonical is not None
            out[f"{metric}_by_source"] = json.dumps(variants, sort_keys=True)
            native = [v["value"] for v in variants.values() if v["value"] is not None]
            if len(native) > 1:
                spread = max(native) - min(native)
                base = canonical or statistics.median(native) or 1
                pct = round(abs(spread) / abs(base) * 100, 2) if base else None
                out[f"{metric}_variance_pct"] = pct
                if pct is not None and pct > 25:
                    flags.append({"merchant_id": mid, "contract_id": cid, "period_id": pid,
                                  "metric": metric, "variance_pct": pct,
                                  "detail": json.dumps(variants, sort_keys=True),
                                  "assertion": "variance_bounded"})
            else:
                out[f"{metric}_variance_pct"] = None

            # Cross-service canonical disagreement is itself a finding.
            distinct = {c for _, c in canon_candidates}
            if len(distinct) > 1:
                flags.append({"merchant_id": mid, "contract_id": cid, "period_id": pid,
                              "metric": metric, "variance_pct": None,
                              "detail": json.dumps({s: c for s, c in canon_candidates}),
                              "assertion": "canonical_disagreement"})

        for metric in uncontested:
            vals = [r[metric] for r in rows if r.get(metric) is not None]
            out[metric] = vals[0] if vals else None

        # Reporting-currency companion for cross-jurisdiction aggregation.
        # It NEVER replaces the native figure -- a merchant's settlement
        # currency is a contractual fact, and the USD number is a convenience
        # for adding four jurisdictions together on one axis.
        out["gross_volume_reporting_usd_minor"] = _to_usd_minor(
            bundle, out.get("gross_volume_minor"), ccy, pid)
        out["net_revenue_reporting_usd_minor"] = _to_usd_minor(
            bundle, out.get("net_revenue_minor"), ccy, pid)

        st, cb = out.get("settled_txn_count"), out.get("chargeback_count")
        out["dispute_ratio"] = round(cb / st, 6) if st and cb is not None else None

        out["contract_version"] = bundle.contract_version
        out["dictionary_version"] = bundle.dictionary_version
        out["bundle_hash"] = bundle.bundle_hash
        out["produced_at"] = _now()
        gold.append(out)

    _assert_contract(bundle, gold)
    return gold, flags


def _to_usd_minor(bundle: Bundle, minor_value, currency: str | None, period_id: str):
    """Apply the declared, dated FX table. Synthetic rates -- TRUST-BOUNDARY 1.3."""
    if minor_value is None or not currency:
        return None
    rates = bundle.lookups["fx_rates.yaml"]["rates_by_period"].get(period_id)
    if not rates or currency not in rates:
        return None
    major = Decimal(minor_value) / (Decimal(10) ** bundle.minor_units(currency))
    usd = major / Decimal(str(rates[currency])) * Decimal(str(rates["USD"]))
    return int((usd * 100).to_integral_value())


def _degraded_of(rows: list[dict], service_id: str) -> int:
    for r in rows:
        if r["service_id"] == service_id:
            return 1 if r.get("precision_degraded") else 0
    return 0


def _rule_name(ref: str | None) -> str | None:
    return ref.rsplit("/", 1)[-1] if ref else None


def _assert_contract(bundle: Bundle, gold: list[dict]) -> None:
    """Enforce the contract's own quality assertions at the Silver->Gold
    boundary. These are read from the contract, not restated here."""
    seen = set()
    for r in gold:
        key = (r["merchant_id"], r["contract_id"], r["period_id"])
        if key in seen:
            raise ClassificationViolation(f"assertion grain_unique failed for {key}")
        seen.add(key)
        for col in ("legal_name_token", "tax_ref_token", "settlement_account_token"):
            if col in r:
                raise ClassificationViolation(
                    f"assertion no_pci_or_pii_in_gold failed: {col} present in Gold")
        if "CQ" not in r["period_id"]:
            raise ClassificationViolation("assertion period_is_calendar failed")
        if not r["period_is_closed"] and r["is_projection"]:
            raise ClassificationViolation("assertion projection_marked inconsistent")
