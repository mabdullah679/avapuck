"""Synthetic corpus generator.

EVERYTHING THIS PRODUCES IS FABRICATED. See TRUST-BOUNDARY.md section 1.

Structure over volume, deliberately. A few thousand records with real internal
complexity -- nested entities, repeating groups, optional fields, four
mutually incompatible null conventions -- prove a mapping layer. Forty
thousand flat rows do not.

The generator owns a single GROUND TRUTH per (contract, period) and then
projects four DELIBERATELY DISAGREEING views of it, one per service. Because
the truth is generated first and the disagreements are applied second, the
grading harness can hold the pipeline to an expected value that was never
derived from the pipeline itself.
"""
from __future__ import annotations

import hashlib
import json
import random
import struct
from dataclasses import dataclass, field, asdict
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import yaml

from wps.periods import Period, quarters_between, to_fiscal_label

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
CONFIG = ROOT / "config"

SEED = 20260830
AS_OF = date(2026, 8, 30)

FIRST_PERIOD = Period(2025, 3)
LAST_PERIOD = Period(2026, 4)          # 2026CQ4 is entirely in the future
N_MERCHANTS = 90

JURISDICTIONS = {
    "US": {"fiscal_start": 1, "currency": "USD", "minor": 2},
    "UK": {"fiscal_start": 4, "currency": "GBP", "minor": 2},
    "JP": {"fiscal_start": 4, "currency": "JPY", "minor": 0},
    "BH": {"fiscal_start": 1, "currency": "BHD", "minor": 3},
}
SERVICE_FISCAL_START = {"service_a": 1, "service_b": 4, "service_c": 4, "service_d": 1}

TIERS = ["standard", "preferred", "strategic", "bespoke"]
STATUSES = ["active", "active", "active", "suspended", "prospective", "terminated"]

_WORDS_A = ["Meridian", "Cobalt", "Harbour", "Northgate", "Vantage", "Solstice", "Ironwood",
            "Pelagic", "Cinder", "Kestrel", "Alder", "Quarry", "Lantern", "Marrow", "Tessera",
            "Aurelian", "Basalt", "Corvid", "Dovetail", "Ember"]
_WORDS_B = ["Payments", "Commerce", "Holdings", "Retail", "Logistics", "Ventures", "Trading",
            "Systems", "Group", "Partners", "Exchange", "Markets"]
_SUFFIX = {"US": "Inc.", "UK": "Ltd", "JP": "K.K.", "BH": "W.L.L."}


# ---------------------------------------------------------------------------
# Ground truth
# ---------------------------------------------------------------------------

@dataclass
class Account:
    """Account-level detail. Exists ONLY in ground truth -- no service exports
    it. That is why three of four services cannot recompute canonical
    active_accounts, which is an honest structural gap rather than a bug."""
    open_in_period: bool
    days_since_last_settled_txn: int | None
    kyc_state: str
    closing_balance_nonzero: bool


@dataclass
class Contract:
    contract_id: str
    merchant_id: str
    jurisdiction: str
    settlement_currency: str
    pricing_tier: str
    effective_from: date
    effective_to: date | None
    settlement_account: str
    reported_by: list[str] = field(default_factory=list)


@dataclass
class Merchant:
    merchant_id: str
    legal_name: str
    display_name: str
    primary_contact_email: str
    tax_ref: str
    onboarded_on: date
    status: str
    native_ids: dict[str, str] = field(default_factory=dict)


@dataclass
class Truth:
    """One quarter of genuine activity for one contract."""
    contract_id: str
    merchant_id: str
    period_id: str
    accounts: list[Account]
    settled_txn_count: int
    refund_count: int
    chargeback_count: int
    gross_major: Decimal          # canonical gross, before ANY deduction
    refund_value_major: Decimal
    chargeback_value_major: Decimal
    fee_major: Decimal

    @property
    def net_revenue_major(self) -> Decimal:
        return self.gross_major - self.refund_value_major - self.chargeback_value_major - self.fee_major

    # -- the four competing definitions of "active", computed from the SAME
    #    underlying accounts. This is the POC's actual subject matter.
    def active_canon(self) -> int:
        return sum(1 for a in self.accounts
                   if a.open_in_period and a.days_since_last_settled_txn is not None
                   and a.days_since_last_settled_txn <= 90)

    def active_any_open(self) -> int:
        return sum(1 for a in self.accounts if a.open_in_period)

    def active_30d(self) -> int:
        return sum(1 for a in self.accounts
                   if a.days_since_last_settled_txn is not None
                   and a.days_since_last_settled_txn <= 30)

    def active_verified(self) -> int:
        return sum(1 for a in self.accounts
                   if a.open_in_period and a.days_since_last_settled_txn is not None
                   and a.days_since_last_settled_txn <= 90 and a.kyc_state == "verified")

    def active_nonzero_balance(self) -> int:
        return sum(1 for a in self.accounts if a.closing_balance_nonzero)


def minor(amount: Decimal, currency: str) -> int:
    """Convert a major-unit amount to integer minor units using the currency's
    OWN declared precision. JPY is 0, BHD is 3. Assuming 2 -- as almost every
    naive pipeline does -- inflates every Japanese figure 100x and every
    Bahraini figure 10x."""
    places = next(j["minor"] for j in JURISDICTIONS.values() if j["currency"] == currency)
    return int((amount * (10 ** places)).to_integral_value())


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

def build_world(rng: random.Random):
    merchants: list[Merchant] = []
    contracts: list[Contract] = []

    for i in range(1, N_MERCHANTS + 1):
        mid = f"MER-{i:05d}"
        base = f"{rng.choice(_WORDS_A)} {rng.choice(_WORDS_B)}"
        onboard = date(2019, 1, 1) + timedelta(days=rng.randint(0, 2400))
        m = Merchant(
            merchant_id=mid,
            legal_name="",  # set once we know the primary jurisdiction
            display_name=base,
            primary_contact_email=f"{base.split()[0].lower()}.ops@example-merchant.invalid",
            tax_ref=f"TX{rng.randint(10**8, 10**9 - 1)}",
            onboarded_on=onboard,
            status=rng.choice(STATUSES),
        )
        # Each service has its OWN merchant identifier scheme. None of them
        # becomes the canonical id -- that would privilege one dialect.
        m.native_ids = {
            "service_a": f"OCIO{i:08d}",
            "service_b": f"mp-{i:04d}-{rng.randint(100, 999)}",
            "service_c": f"RI{i:05d}",
            "service_d": f"{i:07d}",
        }

        # ~30% of merchants trade in more than one jurisdiction. This is the
        # reason the Gold grain is merchant x contract x period and not
        # merchant x period.
        n_contracts = 2 if rng.random() < 0.30 else 1
        juris_choices = rng.sample(list(JURISDICTIONS), n_contracts)
        m.legal_name = f"{base} {_SUFFIX[juris_choices[0]]}"
        merchants.append(m)

        for k, ju in enumerate(juris_choices):
            cid = f"CTR-{i:05d}-{k + 1}"
            eff_from = onboard + timedelta(days=rng.randint(0, 200))
            eff_to = None if rng.random() < 0.85 else eff_from + timedelta(days=rng.randint(900, 2200))
            contracts.append(Contract(
                contract_id=cid,
                merchant_id=mid,
                jurisdiction=ju,
                settlement_currency=JURISDICTIONS[ju]["currency"],
                pricing_tier=rng.choice(TIERS),
                effective_from=eff_from,
                effective_to=eff_to,
                settlement_account=f"4{rng.randint(10**14, 10**15 - 1)}",
            ))

    # Which services report which contracts. Service A (OCIO) reports
    # everything; the others cover overlapping subsets. Every contract is seen
    # by at least two services, so cross-service reconciliation always has
    # something to reconcile.
    for c in contracts:
        seen = ["service_a"]
        if rng.random() < 0.72:
            seen.append("service_b")
        if rng.random() < 0.58:
            seen.append("service_c")
        if rng.random() < 0.44:
            seen.append("service_d")
        if len(seen) == 1:
            seen.append(rng.choice(["service_b", "service_c", "service_d"]))
        c.reported_by = seen

    return merchants, contracts


def build_truth(rng: random.Random, contracts: list[Contract], periods: list[Period]) -> list[Truth]:
    truths = []
    for c in contracts:
        scale = rng.choice([0.4, 1.0, 1.0, 2.5, 6.0])
        n_acc = max(4, int(rng.gauss(40, 18) * scale))
        drift = rng.uniform(-0.05, 0.10)
        for idx, p in enumerate(periods):
            if p.end_date < c.effective_from:
                continue
            if c.effective_to and p.start_date > c.effective_to:
                continue

            growth = (1 + drift) ** idx
            accounts = []
            for _ in range(int(n_acc * growth)):
                open_in = rng.random() < 0.93
                recency = None if rng.random() < 0.12 else int(abs(rng.gauss(35, 40)))
                accounts.append(Account(
                    open_in_period=open_in,
                    days_since_last_settled_txn=recency,
                    kyc_state="verified" if rng.random() < 0.86 else "pending",
                    closing_balance_nonzero=rng.random() < 0.71,
                ))

            txns = int(sum(1 for a in accounts if a.days_since_last_settled_txn is not None)
                       * rng.uniform(8, 45))
            refunds = int(txns * rng.uniform(0.01, 0.05))
            chargebacks = int(txns * rng.uniform(0.0005, 0.008))

            unit = {"USD": 120, "GBP": 95, "JPY": 14000, "BHD": 45}[c.settlement_currency]
            gross = Decimal(txns * unit) * Decimal(str(round(rng.uniform(0.75, 1.35), 4)))
            gross = gross.quantize(Decimal("0.001"))
            refund_v = (gross * Decimal(str(round(rng.uniform(0.01, 0.05), 4)))).quantize(Decimal("0.001"))
            chargeback_v = (gross * Decimal(str(round(rng.uniform(0.0005, 0.006), 5)))).quantize(Decimal("0.001"))
            fee = (gross * Decimal(str(round(rng.uniform(0.012, 0.029), 4)))).quantize(Decimal("0.001"))

            truths.append(Truth(
                contract_id=c.contract_id, merchant_id=c.merchant_id, period_id=p.period_id,
                accounts=accounts, settled_txn_count=txns, refund_count=refunds,
                chargeback_count=chargebacks, gross_major=gross, refund_value_major=refund_v,
                chargeback_value_major=chargeback_v, fee_major=fee,
            ))
    return truths


# ---------------------------------------------------------------------------
# Service emitters -- four mutually unintelligible dialects
# ---------------------------------------------------------------------------

def _fixed(s: str, n: int) -> str:
    return (s or "")[:n].ljust(n)


def _num(v: int, n: int) -> str:
    return str(int(v)).rjust(n, "0")[:n]


def emit_service_a(merchants, contracts, truths, rng):
    """Fixed-width, COBOL copybook layout, implied decimals, EBCDIC-ish filler.
    Reports every contract. Counts any open account as active. Reports revenue
    GROSS of platform fees, with the fee in a separate field."""
    by_id = {m.merchant_id: m for m in merchants}
    cby = {c.contract_id: c for c in contracts}
    lines = ["HDR" + _fixed("OCIO QUARTERLY EXTRACT", 40) + _fixed(AS_OF.strftime("%Y%m%d"), 8) + " " * 169]
    total_gross = Decimal(0)
    n = 0
    for t in truths:
        c = cby[t.contract_id]
        if "service_a" not in c.reported_by:
            continue
        m = by_id[t.merchant_id]
        p = Period.parse(t.period_id)
        label = to_fiscal_label(p, SERVICE_FISCAL_START["service_a"])
        # Service A's own null token for a not-collected count
        acct_open = t.active_any_open()
        acct_closed = len(t.accounts) - acct_open
        rec = (
            _fixed(m.native_ids["service_a"], 12)
            + _fixed(m.legal_name, 40)
            + _fixed(t.contract_id, 14)
            + _fixed(label, 6)
            + _num(acct_open, 9)
            + _num(acct_closed, 9)
            # Always 2 implied decimals, regardless of currency -- the mainframe
            # copybook predates anyone asking whether JPY has minor units. The
            # binding's implied_decimal(2) undoes this, and to_minor_units then
            # applies the CURRENCY's real precision. Fusing the two steps is
            # exactly the bug that inflates JPY 100x.
            + _num(int(t.gross_major * 100), 13)
            + _num(int((t.gross_major - t.refund_value_major - t.chargeback_value_major) * 100), 13)
            + _num(t.settled_txn_count, 11)
            + _num(t.refund_count, 9)
            + _num(t.chargeback_count, 9)
            + _num(int(t.fee_major * 100), 13)
            + (_fixed(m.onboarded_on.strftime("%Y%m%d"), 8) if rng.random() > 0.03 else _fixed("0000-00-00", 8))
            + _fixed(m.tax_ref, 20)
            + " " * 34
        )
        lines.append(_fixed(rec, 220))
        total_gross += t.gross_major
        n += 1
    lines.append("TRL" + _num(n, 9) + _num(int(total_gross * 100), 18) + " " * 190)
    out = RAW / "service_a" / "ocio_quarterly.dat"
    out.write_text("\n".join(_fixed(l, 220) for l in lines) + "\n", encoding="utf-8")
    return n


def emit_service_b(merchants, contracts, truths, rng):
    """Deeply nested attribute-heavy XML. Quarterly figures arrive as three
    monthly children that must be summed. Reports gross ALREADY NET OF
    REFUNDS, and counts 30-day-transacting accounts as active. Uses a
    UK (Apr-Mar) fiscal calendar, so its 'FY26Q3' is a different three months
    from Service A's identical string."""
    by_id = {m.merchant_id: m for m in merchants}
    cby = {c.contract_id: c for c in contracts}
    tier_map = {"standard": "STD", "preferred": "PREF", "strategic": "STRAT", "bespoke": "CUSTOM"}
    status_map = {"active": "ACTIVE", "suspended": "SUSPENDED", "terminated": "CLOSED", "prospective": "PENDING"}

    grouped: dict[str, list[Truth]] = {}
    for t in truths:
        if "service_b" in cby[t.contract_id].reported_by:
            grouped.setdefault(t.merchant_id, []).append(t)

    x = ['<?xml version="1.0" encoding="UTF-8"?>',
         '<QuarterlyExport xmlns:mp="urn:mysql-team:merchant-platform:v3" '
         f'generated="{AS_OF.isoformat()}">', "  <Merchants>"]
    n = 0
    for mid, ts in grouped.items():
        m = by_id[mid]
        x.append(f'    <Merchant mp:ref="{m.native_ids["service_b"]}" status="{status_map[m.status]}">')
        x.append("      <mp:Identity onboarded=\"%s\">" % m.onboarded_on.isoformat())
        x.append(f"        <mp:LegalName>{m.legal_name}</mp:LegalName>")
        # Optional element: sometimes absent entirely, sometimes xsi:nil.
        r = rng.random()
        if r < 0.80:
            x.append(f'        <mp:Contact primary="true"><mp:Email>{m.primary_contact_email}</mp:Email></mp:Contact>')
        elif r < 0.90:
            x.append('        <mp:Contact primary="true"><mp:Email xsi:nil="true"/></mp:Contact>')
        x.append("      </mp:Identity>")
        x.append("      <mp:Agreements>")
        for cid in sorted({t.contract_id for t in ts}):
            c = cby[cid]
            to_attr = c.effective_to.isoformat() if c.effective_to else "9999-12-31"
            x.append(f'        <mp:Agreement ref="{c.contract_id}" tier="{tier_map[c.pricing_tier]}" '
                     f'ccy="{c.settlement_currency}">')
            x.append(f'          <mp:Term from="{c.effective_from.isoformat()}" to="{to_attr}"/>')
            x.append("        </mp:Agreement>")
        x.append("      </mp:Agreements>")
        for t in ts:
            c = cby[t.contract_id]
            p = Period.parse(t.period_id)
            label = to_fiscal_label(p, SERVICE_FISCAL_START["service_b"])
            gross_net = t.gross_major - t.refund_value_major
            # split into three monthly buckets that must be summed back
            splits = [rng.uniform(0.28, 0.38) for _ in range(3)]
            s = sum(splits)
            parts = [(gross_net * Decimal(str(w / s))).quantize(Decimal("0.01")) for w in splits]
            parts[-1] = gross_net.quantize(Decimal("0.01")) - parts[0] - parts[1]
            tx = [t.settled_txn_count // 3, t.settled_txn_count // 3,
                  t.settled_txn_count - 2 * (t.settled_txn_count // 3)]
            rf = [t.refund_count // 3, t.refund_count // 3, t.refund_count - 2 * (t.refund_count // 3)]
            cb = [t.chargeback_count // 3, t.chargeback_count // 3,
                  t.chargeback_count - 2 * (t.chargeback_count // 3)]
            x.append(f'      <mp:Reporting agreement="{t.contract_id}" period="{label}">')
            for i in range(3):
                x.append(f'        <mp:Month seq="{i + 1}">')
                x.append(f"          <mp:GrossNetOfRefunds>{parts[i]}</mp:GrossNetOfRefunds>")
                x.append(f"          <mp:SettledCount>{tx[i]}</mp:SettledCount>")
                x.append(f"          <mp:RefundCount>{rf[i]}</mp:RefundCount>")
                x.append(f"          <mp:ChargebackCount>{cb[i]}</mp:ChargebackCount>")
                x.append("        </mp:Month>")
            x.append(f"        <mp:RefundValue>{t.refund_value_major.quantize(Decimal('0.01'))}</mp:RefundValue>")
            x.append(f"        <mp:NetRevenue>{t.net_revenue_major.quantize(Decimal('0.01'))}</mp:NetRevenue>")
            active = t.active_30d()
            x.append(f'        <mp:ActiveAccounts basis="txn30d">{active}</mp:ActiveAccounts>'
                     if rng.random() > 0.04 else
                     '        <mp:ActiveAccounts basis="txn30d">N/A</mp:ActiveAccounts>')
            x.append("      </mp:Reporting>")
            n += 1
        x.append("    </Merchant>")
    x += ["  </Merchants>", "</QuarterlyExport>"]
    (RAW / "service_b" / "merchant_platform_export.xml").write_text("\n".join(x), encoding="utf-8")
    return n


def emit_service_c(merchants, contracts, truths, rng):
    """Analyst-authored CSV: title row, metadata row, units row, column names,
    data, then trailing commentary. Reports gross NET OF REFUNDS AND FEES and
    counts only KYC-verified accounts. JP fiscal calendar."""
    import csv, io
    by_id = {m.merchant_id: m for m in merchants}
    cby = {c.contract_id: c for c in contracts}
    tier_map = {"standard": "Tier 1", "preferred": "Tier 2", "strategic": "Tier 3", "bespoke": "Bespoke"}

    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["Risk & Identity — Quarterly Performance"])
    w.writerow([f"prepared_by=r.oyelaran; basis=verified-only; generated={AS_OF.isoformat()}; "
                f"disclosure=small-counts-suppressed-as-zero"])
    w.writerow(["", "", "", "", "", "", "(major units per Ccy)", "(major units per Ccy)",
                "(major units per Ccy)", "(major units per Ccy)", "count", "count", "count", ""])
    w.writerow(["Merchant Code", "Merchant (legal)", "Agreement No.", "Tier", "KYC", "Quarter", "Ccy",
                "Active accts (verified only)", "Gross (net of refunds & fees)", "Platform fees",
                "Refund value", "Net revenue", "Settled txns", "Refunds", "Chargebacks", "Tax ID"])

    n = 0
    for t in truths:
        c = cby[t.contract_id]
        if "service_c" not in c.reported_by:
            continue
        m = by_id[t.merchant_id]
        p = Period.parse(t.period_id)
        label = to_fiscal_label(p, SERVICE_FISCAL_START["service_c"])
        gross_nrf = t.gross_major - t.refund_value_major - t.fee_major
        active = t.active_verified()

        def blank(v, prob=0.05):
            return rng.choice(["-", "n/a", "TBC"]) if rng.random() < prob else v

        w.writerow([
            m.native_ids["service_c"], m.legal_name, t.contract_id, tier_map[c.pricing_tier],
            "verified", label, c.settlement_currency,
            blank(active, 0.04),
            gross_nrf.quantize(Decimal("0.001")),
            t.fee_major.quantize(Decimal("0.001")),
            t.refund_value_major.quantize(Decimal("0.001")),
            t.net_revenue_major.quantize(Decimal("0.001")),
            t.settled_txn_count, t.refund_count, blank(t.chargeback_count, 0.06), m.tax_ref,
        ])
        n += 1
    w.writerow(["Note: counts below 5 are suppressed and shown as 0 per disclosure policy."])
    w.writerow(["Prepared from the Q-close extract. Query any variance with Risk & Identity."])
    (RAW / "service_c" / "risk_identity_quarterly.csv").write_text("﻿" + buf.getvalue(), encoding="utf-8")
    return n


def emit_service_d(merchants, contracts, truths, rng):
    """Proprietary pipe/caret hierarchical format. Terse codes throughout,
    unreadable without the lookup tables. Metric-narrow: metrics arrive as
    rows keyed by a two-letter code, not as columns. Carries PII and PCI
    fields as OPAQUE ENCRYPTED BLOBS."""
    by_id = {m.merchant_id: m for m in merchants}
    cby = {c.contract_id: c for c in contracts}
    status_cd = {"prospective": "PR", "active": "AC", "suspended": "SU", "terminated": "TM"}
    tier_cd = {"standard": "T1", "preferred": "T2", "strategic": "T3", "bespoke": "TX"}
    juris_cd = {"US": "01", "UK": "44", "JP": "81", "BH": "973"}
    ccy_cd = {"USD": "840", "GBP": "826", "JPY": "392", "BHD": "048"}

    def julian(d: date) -> str:
        return f"{d.year}{d.timetuple().tm_yday:03d}"

    def blob(plaintext: str, key_alias: str) -> str:
        """Synthetic opaque ciphertext. DEMO ONLY -- reversible, not
        cryptography. The point is that the PIPELINE cannot read it and does
        not need to: the binding declares which key alias governs the field
        and what type it becomes, and routes it through a named interface.
        See TRUST-BOUNDARY.md 2.1."""
        h = hashlib.sha256((key_alias + "::" + plaintext).encode()).digest()
        return "ENC:" + key_alias.split("-")[-1] + ":" + h.hex()[:40]

    rows: list[list[Truth]] = {}
    grouped: dict[str, dict[str, list[Truth]]] = {}
    for t in truths:
        c = cby[t.contract_id]
        if "service_d" in c.reported_by:
            grouped.setdefault(t.merchant_id, {}).setdefault(t.contract_id, []).append(t)

    out = [f"HDR|LEGACY-SETL|{julian(AS_OF)}|V4"]
    n_mer = 0
    n_met = 0
    for mid, by_contract in grouped.items():
        m = by_id[mid]
        onb = julian(m.onboarded_on) if rng.random() > 0.04 else "~"
        out.append("|".join(["MER", m.native_ids["service_d"], m.legal_name,
                             status_cd[m.status], onb, blob(m.tax_ref, "legacy-setl-pii-v2")]))
        n_mer += 1
        for cid, ts in by_contract.items():
            c = cby[cid]
            to_j = julian(c.effective_to) if c.effective_to else "9999365"
            out.append("|".join(["AGR", c.contract_id, juris_cd[c.jurisdiction], ccy_cd[c.settlement_currency],
                                 tier_cd[c.pricing_tier],
                                 f"{julian(c.effective_from)}^{to_j}",
                                 blob(c.settlement_account, "legacy-setl-pci-v1")]))
            for t in ts:
                p = Period.parse(t.period_id)
                label = to_fiscal_label(p, SERVICE_FISCAL_START["service_d"]).removeprefix("FY")
                places = JURISDICTIONS[c.jurisdiction]["minor"]
                scaled = lambda v: str(int(v * (10 ** places)))
                vals = {
                    "AA": str(t.active_nonzero_balance()),
                    "GV": scaled(t.gross_major),
                    "NR": scaled(t.net_revenue_major),
                    "TC": str(t.settled_txn_count),
                    "RC": str(t.refund_count),
                    "CB": str(t.chargeback_count),
                }
                for code, v in vals.items():
                    # -1 = not collected, -9 = suppressed. Both mean ABSENT.
                    if rng.random() < 0.03:
                        v = rng.choice(["-1", "-9", "~"])
                    out.append("|".join(["MET", label, code, v, "BAL" if code == "AA" else "STD"]))
                    n_met += 1
    out.append(f"TRL|{n_mer}|{n_met}")
    (RAW / "service_d" / "legacy_setl_extract.psv").write_text("\n".join(out) + "\n", encoding="ascii",
                                                               errors="replace")
    return n_met


# ---------------------------------------------------------------------------
# Independent expected-value SSOT (for the grading harness)
# ---------------------------------------------------------------------------

def emit_expected(contracts, truths, path: Path):
    """The grading harness's answer key, derived from GROUND TRUTH and never
    from the pipeline. If the pipeline and this file agree, that agreement
    means something."""
    cby = {c.contract_id: c for c in contracts}
    rows = []
    for t in truths:
        c = cby[t.contract_id]
        ccy = c.settlement_currency
        rows.append({
            "merchant_id": t.merchant_id,
            "contract_id": t.contract_id,
            "period_id": t.period_id,
            "jurisdiction": c.jurisdiction,
            "settlement_currency": ccy,
            "active_accounts_canonical": t.active_canon(),
            "gross_volume_minor": minor(t.gross_major, ccy),
            "net_revenue_minor": minor(t.net_revenue_major, ccy),
            "settled_txn_count": t.settled_txn_count,
            "refund_count": t.refund_count,
            "chargeback_count": t.chargeback_count,
            "by_source": {
                "service_a": {"active_accounts": t.active_any_open(),
                              "rule": "incl_any_open"} if "service_a" in c.reported_by else None,
                "service_b": {"active_accounts": t.active_30d(),
                              "rule": "incl_30d_txn"} if "service_b" in c.reported_by else None,
                "service_c": {"active_accounts": t.active_verified(),
                              "rule": "incl_verified_only"} if "service_c" in c.reported_by else None,
                "service_d": {"active_accounts": t.active_nonzero_balance(),
                              "rule": "incl_nonzero_balance"} if "service_d" in c.reported_by else None,
            },
            "reported_by": c.reported_by,
        })
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=1), encoding="utf-8")
    return len(rows)


def main():
    rng = random.Random(SEED)
    for d in ["service_a", "service_b", "service_c", "service_d"]:
        (RAW / d).mkdir(parents=True, exist_ok=True)

    periods = quarters_between(FIRST_PERIOD, LAST_PERIOD)
    merchants, contracts = build_world(rng)
    truths = build_truth(rng, contracts, periods)

    # The identity cross-reference is generated because the identity map is
    # synthetic. In production this is a governed reference dataset.
    xref = {"schema": "wps.lookup.merchant_xref/v1", "generated": True,
            "generated_by": "wps/corpus/generate.py"}
    for svc in ["service_a", "service_b", "service_c", "service_d"]:
        xref[svc] = {m.native_ids[svc]: m.merchant_id for m in merchants}
    (CONFIG / "lookups" / "merchant_xref.yaml").write_text(
        yaml.safe_dump(xref, sort_keys=False), encoding="utf-8")

    na = emit_service_a(merchants, contracts, truths, rng)
    nb = emit_service_b(merchants, contracts, truths, rng)
    nc = emit_service_c(merchants, contracts, truths, rng)
    nd = emit_service_d(merchants, contracts, truths, rng)
    ne = emit_expected(contracts, truths, ROOT / "harness" / "expected" / "ground_truth.json")

    print(f"periods      : {periods[0].period_id} .. {periods[-1].period_id} ({len(periods)})")
    print(f"merchants    : {len(merchants)}")
    print(f"contracts    : {len(contracts)}  (multi-jurisdiction merchants: "
          f"{sum(1 for m in merchants if sum(1 for c in contracts if c.merchant_id == m.merchant_id) > 1)})")
    print(f"truth rows   : {len(truths)}")
    print(f"accounts     : {sum(len(t.accounts) for t in truths):,} (ground truth only, never exported)")
    print("-" * 58)
    print(f"service_a    : {na:5d} fixed-width records   -> ocio_quarterly.dat")
    print(f"service_b    : {nb:5d} XML reporting blocks  -> merchant_platform_export.xml")
    print(f"service_c    : {nc:5d} CSV rows              -> risk_identity_quarterly.csv")
    print(f"service_d    : {nd:5d} MET records           -> legacy_setl_extract.psv")
    print(f"expected     : {ne:5d} ground-truth rows     -> harness/expected/ground_truth.json")


if __name__ == "__main__":
    main()
