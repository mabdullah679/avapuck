"""Forward projections for the in-flight quarter and the next one.

The dashboard is computed in batch before the quarterly meeting, with enough
lead time to show where the current quarter is heading and what the next one
looks like. Those figures are ESTIMATES and are never allowed to read as facts:
every projected row is marked, carries a variability range, and is rendered
distinctly downstream.

The method is deliberately simple and declared rather than clever. A more
sophisticated model would be harder to defend in a room where the output moves
money, and would still be an estimate.
"""
from __future__ import annotations

import statistics
from datetime import date

from wps.periods import Period

METHOD = "trailing-4-quarter weighted mean of quarter-over-quarter growth"
MIN_HISTORY = 2
BAND_MIN_PCT = 8.0     # floor on the variability band, even for steady series

PROJECTED_METRICS = ["gross_volume_minor", "net_revenue_minor",
                     "gross_volume_reporting_usd_minor", "net_revenue_reporting_usd_minor",
                     "settled_txn_count", "refund_count", "chargeback_count"]


def _elapsed_fraction(p: Period, as_of: date) -> float:
    total = (p.end_date - p.start_date).days + 1
    done = (as_of - p.start_date).days
    return max(0.0, min(1.0, done / total))


def project(gold_rows: list[dict], as_of: date) -> list[dict]:
    """Return projected rows for the in-flight quarter (completed to
    quarter-end) and the following quarter."""
    by_contract: dict[tuple, list[dict]] = {}
    for r in gold_rows:
        by_contract.setdefault((r["merchant_id"], r["contract_id"]), []).append(r)

    out = []
    for (mid, cid), rows in by_contract.items():
        rows.sort(key=lambda r: (r["calendar_year"], r["calendar_quarter"]))
        closed = [r for r in rows if r["period_is_closed"]]
        in_flight = next((r for r in rows if not r["period_is_closed"]), None)
        if len(closed) < MIN_HISTORY:
            continue

        template = closed[-1]
        current = in_flight and Period.parse(in_flight["period_id"])
        current = current or Period.parse(closed[-1]["period_id"]).next()
        frac = _elapsed_fraction(current, as_of)

        for horizon, period in ((0, current), (1, current.next())):
            row = {k: template[k] for k in (
                "merchant_id", "contract_id", "settlement_currency", "jurisdiction",
                "pricing_tier", "contract_version", "dictionary_version", "bundle_hash")}
            row.update({
                "period_id": period.period_id,
                "calendar_year": period.year, "calendar_quarter": period.quarter,
                "period_start": period.start_date, "period_end": period.end_date,
                "period_is_closed": False,
                "is_projection": True,
                "projection_method": METHOD,
                "projection_horizon_quarters": horizon,
                "projection_basis_quarters": len(closed[-4:]),
                "source_services": template["source_services"],
            })

            for metric in PROJECTED_METRICS:
                hist = [r[metric] for r in closed[-4:] if r.get(metric) is not None]
                if len(hist) < MIN_HISTORY:
                    row[metric] = None
                    row[f"{metric}_low"] = None
                    row[f"{metric}_high"] = None
                    continue
                growth = _mean_growth(hist)
                base = hist[-1]
                point = base * (growth ** (horizon + 1))

                # For the in-flight quarter, blend what has actually settled so
                # far with the projected remainder rather than ignoring it.
                if horizon == 0 and in_flight and in_flight.get(metric) is not None and frac > 0:
                    actual = in_flight[metric]
                    point = actual + (point * (1 - frac))

                band = max(BAND_MIN_PCT, _volatility_pct(hist)) / 100.0
                band *= (1.0 + 0.6 * horizon)      # further out, wider
                row[metric] = int(point)
                row[f"{metric}_low"] = max(0, int(point * (1 - band)))
                row[f"{metric}_high"] = int(point * (1 + band))

            row["dispute_ratio"] = (
                round(row["chargeback_count"] / row["settled_txn_count"], 6)
                if row.get("settled_txn_count") and row.get("chargeback_count") is not None else None)
            # Contested account counts are NOT projected. Canonical
            # active_accounts is not derivable from any source even for closed
            # quarters, so projecting it would be an estimate built on an
            # unknown -- an invented number wearing two layers of confidence.
            row["active_accounts"] = None
            row["active_accounts_canonical_derivable"] = False
            row["active_accounts_by_source"] = "{}"
            row["active_accounts_variance_pct"] = None
            for m in ("gross_volume_minor", "net_revenue_minor"):
                row[f"{m}_canonical_derivable"] = True
                row[f"{m}_by_source"] = "{}"
                row[f"{m}_variance_pct"] = None
            out.append(row)
    return out


def _mean_growth(hist: list[int]) -> float:
    ratios = [b / a for a, b in zip(hist, hist[1:]) if a]
    if not ratios:
        return 1.0
    weights = list(range(1, len(ratios) + 1))          # recent quarters weigh more
    g = sum(r * w for r, w in zip(ratios, weights)) / sum(weights)
    return max(0.5, min(1.8, g))                        # refuse absurd extrapolation


def _volatility_pct(hist: list[int]) -> float:
    ratios = [b / a for a, b in zip(hist, hist[1:]) if a]
    if len(ratios) < 2:
        return BAND_MIN_PCT
    return min(60.0, statistics.pstdev(ratios) * 100 * 2)
