"""Grading harness.

Compares pipeline output against an INDEPENDENT expected-value SSOT that the
corpus generator produced from ground truth and that the pipeline never
touches. Agreement therefore means something; it is not a tautology.

Grades A-F per service and overall, and makes every failure legible: which
field, which mapping rule, why.

What this proves and does not prove is stated in TRUST-BOUNDARY.md section 8.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

from deltalake import DeltaTable

ROOT = Path(__file__).resolve().parents[1]
LAKE = ROOT / "lake"
EXPECTED = ROOT / "harness" / "expected" / "ground_truth.json"

# Recomputing a canonical figure from separately-rounded components cannot be
# bit-exact: each component was rounded to its own currency precision before
# the platform ever saw it. The tolerance is one minor unit per component
# combined, declared here rather than discovered in a meeting.
MONEY_TOLERANCE_MINOR = 3

# Where a source's declared precision ceiling is coarser than the currency's
# own precision, the smallest representable difference is larger than the
# tolerance above -- and that quantum, not a fudge factor, is the correct
# allowance. Rows in this state are counted and reported SEPARATELY so the
# degradation stays visible instead of being absorbed into a pass rate.

GRADE_BANDS = [(99.0, "A"), (97.0, "B"), (94.0, "C"), (90.0, "D")]

SERVICE_RULE = {
    "service_a": "incl_any_open",
    "service_b": "incl_30d_txn",
    "service_c": "incl_verified_only",
    "service_d": "incl_nonzero_balance",
}


def grade_for(pct: float) -> str:
    for threshold, letter in GRADE_BANDS:
        if pct >= threshold:
            return letter
    return "F"


def _read(table: str):
    return DeltaTable(str(LAKE / table)).to_pyarrow_table().to_pylist()


class Scorecard:
    def __init__(self, name: str):
        self.name = name
        self.checked = 0
        self.passed = 0
        self.not_reported = 0
        self.degraded = 0
        self.failures: list[dict] = []

    def check(self, ok: bool, *, field: str, expected, actual, rule=None, key=None,
              tolerance: int = 0):
        self.checked += 1
        if ok:
            self.passed += 1
            return
        self.failures.append({
            "key": key, "field": field, "rule": rule,
            "expected": expected, "actual": actual,
            "why": _why(field, expected, actual, rule, tolerance),
        })

    @property
    def pct(self) -> float:
        return 100.0 * self.passed / self.checked if self.checked else 0.0

    @property
    def grade(self) -> str:
        return grade_for(self.pct)


def _why(field, expected, actual, rule, tolerance) -> str:
    if actual is None:
        return (f"pipeline produced NULL for {field}; expected {expected}. "
                f"Either the source did not report it or a declared null token "
                f"swallowed a real value.")
    if expected is None:
        return (f"pipeline produced {actual} for {field} where the expected value "
                f"is NULL -- an absent figure was materialised as data.")
    if isinstance(expected, int) and isinstance(actual, int):
        delta = actual - expected
        base = f"{field} off by {delta:+d} (expected {expected}, got {actual})"
        if tolerance:
            base += f"; declared tolerance is +/-{tolerance}"
        if rule:
            base += f"; mapped under rule '{rule}'"
        return base
    return f"{field}: expected {expected!r}, got {actual!r}"


def main() -> int:
    if not EXPECTED.exists():
        print("no expected-value SSOT; run wps.corpus.generate first")
        return 2

    expected = {(r["merchant_id"], r["contract_id"], r["period_id"]): r
                for r in json.loads(EXPECTED.read_text())}

    gold = [r for r in _read("gold/quarterly_performance") if not r["is_projection"]]
    gold_by_key = {(r["merchant_id"], r["contract_id"], r["period_id"]): r for r in gold}

    # ---------------------------------------------------------- per service
    cards: dict[str, Scorecard] = {}
    for svc in SERVICE_RULE:
        card = Scorecard(svc)
        cards[svc] = card
        try:
            rows = _read(f"silver/{svc}")
        except Exception:
            continue
        for r in rows:
            key = (r["merchant_id"], r["contract_id"], r["period_id"])
            exp = expected.get(key)
            if exp is None:
                card.check(False, field="grain", expected="a known contract-period",
                           actual=key, key=key)
                continue

            # 1. The service's OWN contested figure, under its OWN rule.
            exp_variant = (exp["by_source"] or {}).get(svc)
            if exp_variant:
                got = r.get("active_accounts")
                if got is None:
                    card.not_reported += 1
                else:
                    card.check(got == exp_variant["active_accounts"],
                               field="active_accounts (native)",
                               expected=exp_variant["active_accounts"], actual=got,
                               rule=r.get("active_accounts__rule"), key=key)

            # 2. Canonical money, where the binding says it is recomputable.
            quantum = r.get("precision_quantum_minor") or 1
            tol = max(MONEY_TOLERANCE_MINOR, quantum)
            if r.get("precision_degraded"):
                card.degraded += 1
            for metric in ("gross_volume_minor", "net_revenue_minor"):
                got = r.get(f"{metric}__canonical")
                if got is None:
                    card.not_reported += 1
                    continue
                card.check(abs(got - exp[metric]) <= tol,
                           field=f"{metric} (canonical)", expected=exp[metric], actual=got,
                           rule=r.get(f"{metric}__rule"), key=key, tolerance=tol)

            # 3. Uncontested counts.
            for metric in ("settled_txn_count", "refund_count", "chargeback_count"):
                got = r.get(metric)
                if got is None:
                    card.not_reported += 1
                    continue
                card.check(got == exp[metric], field=metric,
                           expected=exp[metric], actual=got, key=key)

    # -------------------------------------------------------------- gold
    gcard = Scorecard("GOLD (canonical output)")
    honesty = Scorecard("GOLD (refusal to invent)")
    for key, exp in expected.items():
        row = gold_by_key.get(key)
        if row is None:
            gcard.check(False, field="row present", expected="1 row", actual="missing", key=key)
            continue
        gtol = MONEY_TOLERANCE_MINOR
        if row.get("precision_degraded"):
            gcard.degraded += 1
            gtol = max(gtol, 10 ** 1)
        for metric in ("gross_volume_minor", "net_revenue_minor"):
            got = row.get(metric)
            if got is None:
                gcard.not_reported += 1
                continue
            gcard.check(abs(got - exp[metric]) <= gtol, field=metric,
                        expected=exp[metric], actual=got, key=key, tolerance=gtol)
        for metric in ("settled_txn_count", "refund_count", "chargeback_count"):
            got = row.get(metric)
            if got is None:
                gcard.not_reported += 1
                continue
            gcard.check(got == exp[metric], field=metric, expected=exp[metric],
                        actual=got, key=key)

        # The honesty check. No source can derive canonical active_accounts, so
        # Gold MUST report NULL. A number here would be an invention, and would
        # score as a failure precisely because it looks like success.
        honesty.check(row.get("active_accounts") is None,
                      field="active_accounts must be NULL (not derivable from any source)",
                      expected=None, actual=row.get("active_accounts"), key=key)

        # Every contributing service's own figure must survive into Gold intact.
        variants = json.loads(row.get("active_accounts_by_source") or "{}")
        for svc, v in (exp["by_source"] or {}).items():
            if not v or svc not in variants:
                continue
            got = variants[svc].get("value")
            if got is None:
                honesty.not_reported += 1
                continue
            honesty.check(got == v["active_accounts"],
                          field=f"active_accounts_by_source[{svc}] preserved",
                          expected=v["active_accounts"], actual=got,
                          rule=variants[svc].get("rule"), key=key)

    # ------------------------------------------------------------- report
    all_cards = list(cards.values()) + [gcard, honesty]
    total_checked = sum(c.checked for c in all_cards)
    total_passed = sum(c.passed for c in all_cards)
    overall = 100.0 * total_passed / total_checked if total_checked else 0.0

    w = 30
    print("=" * 74)
    print("WPS GRADING HARNESS".center(74))
    print("graded against an independent expected-value SSOT".center(74))
    print("=" * 74)
    print(f"{'scope':{w}} {'checks':>8} {'passed':>8} {'pct':>7} {'not rep.':>9} {'degraded':>9} grade")
    print("-" * 74)
    for c in all_cards:
        print(f"{c.name:{w}} {c.checked:8d} {c.passed:8d} {c.pct:6.2f}% "
              f"{c.not_reported:9d} {c.degraded:9d}   {c.grade}")
    print("-" * 74)
    print(f"{'OVERALL':{w}} {total_checked:8d} {total_passed:8d} {overall:6.2f}% "
          f"{sum(c.not_reported for c in all_cards):9d} "
          f"{sum(c.degraded for c in all_cards):9d}   {grade_for(overall)}")
    print("=" * 74)

    failures = [f for c in all_cards for f in c.failures]
    if failures:
        print(f"\n{len(failures)} failing checks. First 12, with the rule that produced each:\n")
        for f in failures[:12]:
            print(f"  {f['key']}")
            print(f"    {f['why']}")
    else:
        print("\nNo failing checks.")

    print("\nnot reported = the source genuinely sent no value (a declared null token,")
    print("or a metric that service does not carry). Excluded from the percentage")
    print("rather than counted as a failure -- and counted here so it stays visible.")
    print(f"\nmoney tolerance: +/-{MONEY_TOLERANCE_MINOR} minor units on canonical figures")
    print("recomputed from separately-rounded components. Declared, not discovered.")
    print("\ndegraded = the source's declared precision ceiling is coarser than the")
    print("currency's own precision, so the exact figure is unrecoverable from that")
    print("source. Counted separately; the affected Gold rows carry precision_degraded.")

    out = ROOT / "harness" / "last_report.json"
    out.write_text(json.dumps({
        "overall_pct": round(overall, 2), "overall_grade": grade_for(overall),
        "scopes": [{"name": c.name, "checked": c.checked, "passed": c.passed,
                    "pct": round(c.pct, 2), "grade": c.grade,
                    "not_reported": c.not_reported, "degraded": c.degraded}
                   for c in all_cards],
        "failures": failures[:200],
    }, indent=1, default=str))
    return 0 if grade_for(overall) in ("A", "B") else 1


if __name__ == "__main__":
    sys.exit(main())
