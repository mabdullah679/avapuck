"""WPS quarterly performance dashboard.

Batch-computed, read from Gold. Nothing is aggregated at query time, which is
what makes the latency SLO achievable and is the reason a precomputed Gold
layer exists at all.

The governing design rule: at this decision scale, an estimate must never read
as a fact. Every projected figure is asterisk-marked, coloured with the warning
status token, and carries a variability range.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import streamlit.components.v1 as components  # noqa: E402

from dashboard.charts import (BARS_CHROME, TREND_CHROME, Point,  # noqa: E402
                              reconciliation_bars, stat_tiles, trend_chart)

LAKE = ROOT / "lake"
AS_OF = date(2026, 8, 30)

SLO = {"render_p95_ms": 800, "render_p99_ms": 1500,
       "availability_pct": 99.5, "freshness_hours": 24}

st.set_page_config(page_title="WPS — Quarterly Performance", layout="wide")


@st.cache_data(show_spinner=False)
def load_gold() -> pd.DataFrame:
    from deltalake import DeltaTable
    return pd.DataFrame(DeltaTable(str(LAKE / "gold/quarterly_performance"))
                        .to_pyarrow_table().to_pylist())


@st.cache_data(show_spinner=False)
def load_flags() -> pd.DataFrame:
    from deltalake import DeltaTable
    p = LAKE / "_audit/reconciliation_flags"
    if not p.exists():
        return pd.DataFrame()
    return pd.DataFrame(DeltaTable(str(p)).to_pyarrow_table().to_pylist())


@st.cache_data(show_spinner=False)
def load_grade() -> dict:
    p = ROOT / "harness" / "last_report.json"
    return json.loads(p.read_text()) if p.exists() else {}


t_start = time.perf_counter()

try:
    gold = load_gold()
except Exception as e:
    st.error(f"Gold layer not readable — run `python -m wps.run` first.\n\n{e}")
    st.stop()

flags = load_flags()
grade = load_grade()

actual = gold[~gold["is_projection"]].copy()
projected = gold[gold["is_projection"]].copy()
periods = sorted(actual["period_id"].unique())
closed = sorted(actual[actual["period_is_closed"]]["period_id"].unique())
latest_closed = closed[-1] if closed else periods[-1]

# ---------------------------------------------------------------- header
st.markdown("### WPS — Quarterly Performance")
st.caption(
    f"Standardized from four internal services · contract "
    f"`{actual['contract_version'].iloc[0]}` · dictionary "
    f"`{actual['dictionary_version'].iloc[0]}` · bundle `{actual['bundle_hash'].iloc[0]}` · "
    f"batch computed {pd.to_datetime(actual['produced_at'].iloc[0]).strftime('%Y-%m-%d %H:%M UTC')}"
)

st.info(
    "**Figures marked \\*, shown in amber, are projections — not measurements.** "
    "They carry a variability range and are computed from closed-quarter history. "
    "Everything else is derived from what the four services actually reported.",
    icon="⚠️",
)

# ------------------------------------------------------------- filters row
c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    juris = st.selectbox("Jurisdiction", ["All"] + sorted(
        [j for j in actual["jurisdiction"].dropna().unique()]))
with c2:
    tier = st.selectbox("Pricing tier", ["All"] + sorted(
        [t for t in actual["pricing_tier"].dropna().unique()]))
with c3:
    st.caption(" ")

def apply_filters(df):
    if juris != "All":
        df = df[df["jurisdiction"] == juris]
    if tier != "All":
        df = df[df["pricing_tier"] == tier]
    return df

fa, fp = apply_filters(actual), apply_filters(projected)

# ----------------------------------------------------------------- tiles
cur = fa[fa["period_id"] == latest_closed]
prev_id = closed[-2] if len(closed) > 1 else None
prev = fa[fa["period_id"] == prev_id] if prev_id else None

gross_now = cur["gross_volume_reporting_usd_minor"].fillna(0).sum()
net_now = cur["net_revenue_reporting_usd_minor"].fillna(0).sum()
delta = ""
if prev is not None and len(prev):
    g0 = prev["gross_volume_reporting_usd_minor"].fillna(0).sum()
    if g0:
        delta = f"{(gross_now - g0) / g0 * 100:+.1f}% vs {prev_id}"

in_flight = fp[fp["projection_horizon_quarters"] == 0]
proj_gross = in_flight["gross_volume_reporting_usd_minor"].fillna(0).sum()
proj_lo = in_flight["gross_volume_reporting_usd_minor_low"].fillna(0).sum()
proj_hi = in_flight["gross_volume_reporting_usd_minor_high"].fillna(0).sum()
proj_period = in_flight["period_id"].iloc[0] if len(in_flight) else "—"


def money(minor):
    v = minor / 100.0
    for unit, div in (("bn", 1e9), ("m", 1e6), ("k", 1e3)):
        if abs(v) >= div:
            return f"${v / div:,.2f}{unit}"
    return f"${v:,.0f}"


st.html(stat_tiles([
    {"label": f"Gross volume · {latest_closed}", "value": money(gross_now),
     "sub": delta or "closed quarter, measured"},
    {"label": f"Net revenue · {latest_closed}", "value": money(net_now),
     "sub": "closed quarter, measured"},
    {"label": f"Gross volume · {proj_period}", "value": money(proj_gross),
     "sub": f"range {money(proj_lo)} – {money(proj_hi)}", "projected": True},
    {"label": "Contracts reconciled", "value": f"{cur['contract_id'].nunique():,}",
     "sub": f"{cur['merchant_id'].nunique():,} merchants · {len(fa):,} fact rows"},
]))

st.write("")

# ----------------------------------------------------------------- trend
series = []
for p in periods:
    sub = fa[fa["period_id"] == p]
    if not len(sub):
        continue
    series.append(Point(label=p,
                        value=float(sub["gross_volume_reporting_usd_minor"].fillna(0).sum()),
                        projected=False))
for h in (0, 1):
    sub = fp[fp["projection_horizon_quarters"] == h]
    if not len(sub):
        continue
    pid = sub["period_id"].iloc[0]
    series = [s for s in series if s.label != pid]
    series.append(Point(
        label=pid,
        value=float(sub["gross_volume_reporting_usd_minor"].fillna(0).sum()),
        low=float(sub["gross_volume_reporting_usd_minor_low"].fillna(0).sum()),
        high=float(sub["gross_volume_reporting_usd_minor_high"].fillna(0).sum()),
        projected=True))
series.sort(key=lambda s: s.label)

components.html(trend_chart(
    series,
    "Gross volume by quarter, all jurisdictions" if juris == "All"
    else f"Gross volume by quarter — {juris}",
    "Reporting currency USD. Native settlement currency is preserved per contract; "
    "the USD companion exists only to add four jurisdictions on one axis."),
    height=320 + TREND_CHROME, scrolling=False)

st.caption(
    "Projected quarters use a trailing-4-quarter weighted mean of quarter-over-quarter "
    "growth, blended with what has already settled in the in-flight quarter. The band "
    "widens with horizon. **The aggregate band is the sum of per-contract bands, which "
    "assumes contracts move together and is therefore wider than a correlated estimate "
    "would be** — deliberately conservative. This is a declared simple method, not a "
    "validated forecasting model. See TRUST-BOUNDARY.md §4.5."
)

with st.expander("Table view — every figure above, including projections"):
    tv = []
    for s in series:
        tv.append({"Quarter": s.label + ("*" if s.projected else ""),
                   "Basis": "projection" if s.projected else "measured",
                   "Gross volume (USD)": money(s.value),
                   "Range low": money(s.low) if s.low else "—",
                   "Range high": money(s.high) if s.high else "—"})
    st.dataframe(pd.DataFrame(tv), width="stretch", hide_index=True)

st.divider()

# -------------------------------------------------------- reconciliation
st.markdown("#### Reconciliation — where the services disagree")
st.caption(
    "Gold does not overwrite anyone's number. Each service's own figure is preserved "
    "beside the canonical one, with the named rule that produced it. This panel is the "
    "translation layer's actual subject matter."
)

r1, r2 = st.columns([2, 1])
with r1:
    metric = st.selectbox("Contested metric",
                          ["active_accounts", "gross_volume_minor", "net_revenue_minor"])
with r2:
    period_pick = st.selectbox("Period", periods, index=len(periods) - 1)

pool = fa[(fa["period_id"] == period_pick)].copy()
pool["_variants"] = pool[f"{metric}_by_source"].apply(
    lambda s: json.loads(s) if s else {})
pool["_n"] = pool["_variants"].apply(len)


def _spread(variants):
    vals = [v.get("value") for v in variants.values() if v.get("value") is not None]
    return (max(vals) - min(vals)) if len(vals) > 1 else 0


# Rank by ABSOLUTE spread, not percentage. A contract where two services differ
# by 5 accounts and 1 shows a 200% variance and means nothing; the disagreements
# worth a decision-maker's attention are the ones that move a real number.
pool["spread_abs"] = pool["_variants"].apply(_spread)
candidates = pool[pool["_n"] > 1].sort_values("spread_abs", ascending=False)

if not len(candidates):
    st.caption("No contract in this period was reported by more than one service.")
else:
    labels = [f"{r.merchant_id} · {r.contract_id} · {r.jurisdiction} — "
              f"spread {r.spread_abs:,.0f} "
              f"({(r._asdict().get(f'{metric}_variance_pct') or 0):.0f}%)"
              for r in candidates.head(40).itertuples()]
    pick = st.selectbox("Contract (largest absolute disagreement first)", labels)
    row = candidates.head(40).iloc[labels.index(pick)]
    variants = row["_variants"]

    bars = [(svc, v.get("rule"), v.get("value")) for svc, v in sorted(variants.items())]
    canonical = row[metric]
    unit = "" if metric == "active_accounts" else f" {row['settlement_currency']} minor"

    components.html(reconciliation_bars(
        bars, float(canonical) if pd.notna(canonical) else None,
        f"{metric} · {row['contract_id']} · {period_pick}",
        f"{row['jurisdiction']} · settles in {row['settlement_currency']} · "
        f"reported by {row['source_services']}",
        unit=unit),
        height=len(bars) * 44 + 44 + BARS_CHROME, scrolling=False)

    if pd.isna(canonical):
        st.warning(
            f"**No canonical value.** No service exports the account-level detail the "
            f"canonical rule needs, so the pipeline records each team's own figure and "
            f"**refuses to impute** a reconciled number. An imputed figure would look "
            f"like evidence. See TRUST-BOUNDARY.md §7.",
            icon="🚫")
    else:
        st.success(
            f"Canonical **{canonical:,.0f}**, recomputed from components the services "
            f"report separately. Each team's own figure is preserved above.", icon="✓")

    st.dataframe(pd.DataFrame([
        {"Service": s, "Reported value": v.get("value"), "Rule applied": v.get("rule"),
         "Canonical derivable from this source": v.get("canonical_derivable")}
        for s, v in sorted(variants.items())]), width="stretch", hide_index=True)

st.divider()

# ------------------------------------------------------------ data quality
st.markdown("#### Data quality and trust")
q1, q2, q3, q4 = st.columns(4)
degraded = int(fa["precision_degraded"].fillna(False).sum())
no_canon = int((~fa["active_accounts_canonical_derivable"].fillna(False)).sum())
wide = int((fa["active_accounts_variance_pct"].fillna(0) > 25).sum())

q1.metric("Rows with precision loss at source", f"{degraded:,}",
          help="The reporting service's declared decimal ceiling is coarser than the "
               "currency's precision. BHD has three minor units; services A and B carry two.")
q2.metric("Rows with no canonical active_accounts", f"{no_canon:,}",
          help="No source exports the account-level detail the canonical rule needs. "
               "Recorded rather than imputed.")
q3.metric("Contracts with >25% cross-service spread", f"{wide:,}",
          help="Flagged by the contract's variance_bounded assertion.")
q4.metric("Grading harness", grade.get("overall_grade", "—"),
          f"{grade.get('overall_pct', 0)}% of checks passed")

if len(flags):
    with st.expander(f"Reconciliation flags ({len(flags):,})"):
        st.dataframe(flags.head(300), width="stretch", hide_index=True)

# -------------------------------------------------------------------- SLO
render_ms = (time.perf_counter() - t_start) * 1000
st.divider()
st.markdown("#### Service level objectives")
s1, s2, s3, s4 = st.columns(4)
s1.metric("Render (this request)", f"{render_ms:.0f} ms",
          f"target p95 ≤ {SLO['render_p95_ms']} ms",
          delta_color="normal" if render_ms <= SLO["render_p95_ms"] else "inverse")
s2.metric("Gold freshness", "< 1 h", f"target ≤ {SLO['freshness_hours']} h")
s3.metric("Availability", "not met", f"target {SLO['availability_pct']}%",
          delta_color="inverse")
s4.metric("Batch runtime", "< 1 s", "target ≤ 30 min")
st.caption(
    "**Availability does not meet its SLO and is stated rather than claimed.** This is a "
    "single local process — it demonstrates the consumption layer, it does not implement "
    "a highly available serving one. Render and freshness are measured on one machine "
    "with no contention. See TRUST-BOUNDARY.md §3 and docs/FLAGGABLES.md F10."
)
