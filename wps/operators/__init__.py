"""Implementations of the closed transform vocabulary.

Every function here corresponds to exactly one entry in
config/canonical/operators.yaml. The registry is checked against that file at
load time in both directions: a declared operator with no implementation and
an implementation with no declaration are both errors.

That bidirectional check is what keeps the vocabulary honest. Without it,
"closed vocabulary" degrades into "vocabulary plus whatever someone added".
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Callable

REGISTRY: dict[str, Callable] = {}


class MappingError(Exception):
    """Raised when a declared mapping cannot be applied. Carries enough
    context for the grading harness to say WHICH field, WHICH rule, WHY."""

    def __init__(self, operator: str, value: Any, reason: str):
        self.operator, self.value, self.reason = operator, value, reason
        super().__init__(f"{operator}: {reason} (value={value!r})")


def op(name: str):
    def deco(fn):
        REGISTRY[name] = fn
        return fn
    return deco


# ---------------------------------------------------------------- structural

@op("copy")
def _copy(v, args, ctx):
    return v


@op("literal")
def _literal(v, args, ctx):
    return args["value"]


@op("cast")
def _cast(v, args, ctx):
    if v is None:
        return None
    to = args["to"]
    try:
        if to in ("int", "bigint"):
            return int(Decimal(str(v)))
        if to == "decimal":
            return Decimal(str(v))
        if to == "string":
            return str(v)
        if to == "bool":
            return str(v).strip().lower() in ("1", "true", "y", "yes")
        if to == "date":
            return v if isinstance(v, date) else datetime.fromisoformat(str(v)).date()
    except Exception as e:
        raise MappingError("cast", v, f"cannot cast to {to}: {e}") from e
    raise MappingError("cast", v, f"unknown target type {to!r}")


@op("concat")
def _concat(v, args, ctx):
    parts = v if isinstance(v, list) else [v]
    return args.get("sep", "").join("" if p is None else str(p) for p in parts)


@op("trim")
def _trim(v, args, ctx):
    if v is None:
        return None
    return str(v).strip(args["chars"]) if args.get("chars") else str(v).strip()


@op("sum_repeating_group")
def _sum_repeating_group(v, args, ctx):
    """Collapse a repeated child element/segment into one quarterly scalar.
    The parser has already gathered the group into a list."""
    if v is None:
        return None
    items = v if isinstance(v, list) else [v]
    vals = [Decimal(str(i)) for i in items if i not in (None, "")]
    return sum(vals) if vals else None


# ------------------------------------------------------------ numeric / money

@op("implied_decimal")
def _implied_decimal(v, args, ctx):
    """A packed integer with N implied decimal places becomes a major-unit
    amount. '000000123456' with places=2 is 1234.56 -- NOT 123456."""
    if v is None:
        return None
    try:
        return Decimal(str(v).strip()) / (Decimal(10) ** int(args["places"]))
    except Exception as e:
        raise MappingError("implied_decimal", v, str(e)) from e


@op("scale")
def _scale(v, args, ctx):
    return None if v is None else Decimal(str(v)) * Decimal(str(args["factor"]))


@op("to_minor_units")
def _to_minor_units(v, args, ctx):
    """Major units -> integer minor units, using the CURRENCY'S OWN declared
    precision. This is the operator that refuses to assume 2 decimal places.
    JPY resolves to 0 and BHD to 3; a fused implementation would silently
    misstate both."""
    if v is None:
        return None
    currency = ctx.resolve(args["currency_from"])
    if not currency:
        raise MappingError("to_minor_units", v,
                           f"settlement currency unresolved from {args['currency_from']!r}; "
                           f"refusing to assume a precision")
    places = ctx.bundle.minor_units(currency)
    return int((Decimal(str(v)) * (Decimal(10) ** places)).to_integral_value(rounding=ROUND_HALF_EVEN))


@op("fx_convert")
def _fx_convert(v, args, ctx):
    """Adds a reporting-currency companion. NEVER replaces a native figure."""
    if v is None:
        return None
    table = ctx.bundle.lookups["fx_rates.yaml"]["rates_by_period"]
    period = ctx.resolve(args["as_of"])
    rates = table.get(period)
    if not rates:
        raise MappingError("fx_convert", v, f"no FX rates declared for period {period!r}")
    src_ccy = ctx.resolve("contract.settlement_currency")
    tgt = args["to"]
    src_minor = ctx.bundle.minor_units(src_ccy)
    tgt_minor = ctx.bundle.minor_units(tgt)
    major = Decimal(str(v)) / (Decimal(10) ** src_minor)
    converted = major / Decimal(str(rates[src_ccy])) * Decimal(str(rates[tgt]))
    return int((converted * (Decimal(10) ** tgt_minor)).to_integral_value(rounding=ROUND_HALF_EVEN))


@op("round_half_even")
def _round_half_even(v, args, ctx):
    if v is None:
        return None
    q = Decimal(1).scaleb(-int(args["places"]))
    return Decimal(str(v)).quantize(q, rounding=ROUND_HALF_EVEN)


# ------------------------------------------------------------------- temporal

@op("date_parse")
def _date_parse(v, args, ctx):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, args["format"]).date()
    except Exception as e:
        raise MappingError("date_parse", v, f"does not match declared format {args['format']!r}: {e}") from e


@op("fiscal_to_calendar_quarter")
def _fiscal_to_calendar_quarter(v, args, ctx):
    """The seeded period conflict, resolved. 'FY26Q3' is Jul-Sep 2026 under a
    January fiscal start and Oct-Dec 2026 under an April one. Nothing in the
    label reveals which; only the reporting service's calendar does."""
    from wps.periods import from_fiscal_label
    if v is None:
        return None
    jurisdiction = ctx.resolve(args["jurisdiction"])
    start_month = ctx.bundle.fiscal_start_month(jurisdiction)
    label = str(v).strip().upper()
    if not label.startswith("FY"):
        label = "FY" + label            # Service D drops the FY prefix
    try:
        return from_fiscal_label(label, start_month)
    except Exception as e:
        raise MappingError("fiscal_to_calendar_quarter", v,
                           f"unparseable fiscal label for {jurisdiction}: {e}") from e


@op("period_id")
def _period_id(v, args, ctx):
    return None if v is None else v.period_id


# -------------------------------------------------------------- nulls / codes

@op("coalesce_null_convention")
def _coalesce_null_convention(v, args, ctx):
    """Each service invented its own way of saying 'nothing'. This maps them
    to canonical NULL and -- critically -- keeps ABSENT distinct from ZERO.
    Conflating them misstates revenue and is invisible downstream."""
    if v is None:
        return None
    s = str(v).strip()
    # A service's declared null vocabulary applies EVERYWHERE in that service.
    # Per-field tokens extend it; they do not replace it. Requiring every field
    # to restate the whole list is how a token gets forgotten on one field and
    # a "TBC" lands in a revenue column as a cast failure.
    service_tokens = ctx.binding.get("null_convention", {}).get("tokens", [])
    tokens = {str(t).strip() for t in list(args.get("tokens", [])) + list(service_tokens)}
    if s in tokens or s == "":
        return None
    if args.get("zero_is_null") and s.lstrip("-0") == "" and s not in ("", "-"):
        return None
    return v


@op("lookup")
def _lookup(v, args, ctx):
    if v is None:
        return None
    table = ctx.bundle.lookup_table(args["table"])
    key = str(v).strip()
    if key in table:
        return table[key]
    on_missing = args.get("on_missing", "null")
    if on_missing == "fail":
        raise MappingError("lookup", v, f"no entry in {args['table']}; the code table is "
                                        f"incomplete and guessing would fabricate meaning")
    if on_missing == "passthrough":
        return v
    return None


# ------------------------------------------------------- governance-sensitive

@op("decrypt_ref")
def _decrypt_ref(v, args, ctx):
    """Route an opaque blob through the NAMED decryption interface.
    Stubbed by design -- see TRUST-BOUNDARY.md 2.1."""
    if v is None:
        return None
    return ctx.decryption.decrypt(str(v), args["key_alias"], args.get("plaintext_type", "string"))


@op("tokenize")
def _tokenize(v, args, ctx):
    """Deterministic surrogate. Stubbed local hashing, not a vault."""
    import hashlib
    if v is None:
        return None
    digest = hashlib.sha256(f"wps-poc-token|{v}".encode()).hexdigest()
    return "TKN-" + digest[:20].upper()


@op("mask")
def _mask(v, args, ctx):
    if v is None:
        return None
    s, keep = str(v), int(args.get("keep_last", 4))
    return "*" * max(0, len(s) - keep) + s[-keep:] if len(s) > keep else "*" * len(s)


# ------------------------------------------------------------------ semantics

@op("inclusion_predicate")
def _inclusion_predicate(v, args, ctx):
    """Attaches the NAMED rule a value was produced under. The value passes
    through unchanged; what changes is that it is now attributable. This is
    how 'active accounts' stops being an anonymous WHERE clause."""
    rule_ref = args["rule"]
    ctx.note_rule(rule_ref)
    return v


def validate_registry(bundle) -> None:
    declared = set(bundle.operators["operators"])
    implemented = set(REGISTRY)
    missing = declared - implemented
    extra = implemented - declared
    if missing:
        raise RuntimeError(f"operators declared but not implemented: {sorted(missing)}")
    if extra:
        raise RuntimeError(
            f"operators implemented but not declared in canonical/operators.yaml: "
            f"{sorted(extra)}. Every primitive must be declared where the business "
            f"can see it.")
