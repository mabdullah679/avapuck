# AVP Consultation — Data Platform Engagement

**Date captured:** 2026-08-29
**Role:** Assistant Vice President — consultancy service for technical,
infrastructure, and financial decision-making
**Subject:** Internal-use data platform

---

## 1. Engagement Framing

Advisory across three decision axes:

| Axis | Scope |
|---|---|
| Technical | Architecture, pipeline design, tooling selection |
| Infrastructure | Data lake, compute, availability, response-time targets |
| Financial | Cost of platform decisions, BI-driven spend decisions |

**Consumer:** internal only (not a customer-facing product).

---

## 2. Business Objective

Big data pipeline supporting **BI decision-making for the next 2 quarters**
within the upcoming 365-day horizon.

> **Open question:** "next 2 quarters of the upcoming 365 days" — is the
> *platform delivery* scoped to 2 quarters, or is the platform meant to
> *serve decisions* covering a 2-quarter forward window? This changes whether
> the 2 quarters is a delivery deadline or a reporting horizon. Confirm.

---

## 3. Architecture

### Data Lake — Medallion Architecture

```
    ┌─────────┐     ┌─────────┐     ┌─────────┐
    │ BRONZE  │ ──▶ │ SILVER  │ ──▶ │  GOLD   │
    │  raw    │     │ cleaned │     │ business│
    │ ingest  │     │conformed│     │  ready  │
    └─────────┘     └─────────┘     └─────────┘
                                         │
                                         ▼
                                   ┌───────────┐
                                   │ Dashboards│
                                   └───────────┘
```

- **Bronze** — raw landing, immutable, source-fidelity
- **Silver** — cleaned, deduplicated, conformed, quality-enforced
- **Gold** — business-level aggregates, serving layer

**In-scope for design + development:** the **Gold layer pipeline**.
Bronze and Silver are named as architecture but Gold is where the explicit
build ask sits.

### Dashboards

Consumption layer on top of Gold. Two hard non-functional requirements:

- **Low response times** — interactive latency for urgent decision-making
- **High availability** — dashboards must be up when decisions are urgent

> Both need numeric SLOs before design. "Low" and "high" are not yet
> specifications. See §7.

---

## 4. Single Source of Truth — International Contracts

Central SSOT covering **international contracts for all merchants,
inclusive** — no merchant excluded, no per-merchant special-casing.

**Core design principle — configuration over code:**

- Contract terms are **configurable per pipeline**
- **NOT** hardcoded
- **NOT** hidden inside backend logic

This is the architectural stance to preserve: contract rules live in a
declared, inspectable configuration surface that a pipeline reads at
runtime — not embedded in application code where they become invisible to
the business and unauditable.

**Why it matters:** international contract terms vary by jurisdiction and
change on business timelines, not release timelines. Hardcoding them means
every contract change becomes an engineering deployment.

---

## 5. Research Items

Items explicitly flagged for research (not yet decided):

### 5.1 Airflow — ETL orchestration
Research Apache Airflow as the ETL pipeline orchestrator.
- Fit against the medallion layer transitions
- Managed vs. self-hosted (MWAA / Cloud Composer / Astronomer / self-run)
- Cost model — feeds the financial advisory axis

### 5.2 Data classification — PCI, PII, non-PCI
Research the three-way classification:
- **PCI** — payment card data, PCI-DSS scope
- **PII** — personally identifiable information
- **non-PCI** — everything outside cardholder-data scope

Drives layer-by-layer handling: what is allowed to land in Bronze, what
must be tokenized/masked before Silver, what may surface in Gold and on
dashboards. Merchant + contract data makes this load-bearing, not optional.

### 5.3 Authentication protocols
Research authentication solutions — **also configurable** (same
config-over-code stance as the contracts SSOT).

### 5.4 Merchant credibility score
A scoring construct over merchant data.

> **Open question:** is this a *deliverable of the Gold layer* (a computed
> metric the pipeline produces) or a *pre-existing input* the platform
> ingests? Currently ambiguous in the notes. Confirm.

---

## 6. Scope Boundaries

Two explicit boundaries were stated. These are the contract edges of the
engagement — worth holding firmly.

### 6.1 Water Processing System (WPS)

```
  ┌──────────────────────────┐
  │  Water Processing System │
  │                          │
  │  • consultation      ✓   │
  │  • decision-making   ✓   │  ◀── OUR SCOPE ENDS HERE
  │  • implementation    ✓   │
  └──────────────────────────┘
```

Requirements for consultation, decision-making, **and implementation**
end at WPS. Note this boundary includes implementation — broader
involvement than advisory alone.

### 6.2 WPS → Databricks

```
  WPS ──────▶ Databricks
              ▲
              └── SCOPE ENDS HERE
```

The output of WPS flows to Databricks. **The scope ends at that handoff.**
What happens inside/after Databricks is not ours.

> **Assumption to confirm:** WPS is read here as the client's operational
> domain system — the physical water-processing plant/operation whose
> output data feeds the platform. If "WPS" is instead an internal codename
> for a software component, §6 needs rewriting. **This is the single
> biggest ambiguity in the notes — resolve first.**

---

## 7. Gaps to Close Before Design

Ordered by how much they block the work.

| # | Gap | Blocks |
|---|---|---|
| 1 | WPS definition — domain system or software component? | All of §6, ingestion design |
| 2 | Dashboard SLOs — p95/p99 latency target, availability % | Serving layer + infra sizing + cost |
| 3 | Data volume + velocity — rows/day, batch or streaming | Every architecture decision |
| 4 | Merchant credibility score — output or input? | Gold layer scope |
| 5 | "2 quarters" — delivery deadline or reporting horizon? | Roadmap |
| 6 | Merchant count + jurisdictions in the contracts SSOT | Config schema design |
| 7 | Existing stack — what's already running vs. greenfield? | Build vs. integrate |
| 8 | Budget envelope | The financial advisory axis has no anchor |

---

## 8. Immediate Next Actions

1. **Resolve the WPS ambiguity** — one clarifying question, unblocks the most
2. **Convert "low latency" / "high availability" into numbers** — no serving
   layer can be designed against adjectives
3. **Run the three research items** — Airflow, PCI/PII classification,
   auth protocols
4. **Draft the contracts SSOT config schema** — this is the piece with the
   clearest stated design principle, so it can start before the gaps close
5. **Confirm Databricks handoff format** — the scope boundary needs a defined
   interface contract, even though what's past it isn't ours

---

## 9. Verbatim Source Notes

Preserved unedited for reference.

```
assistant vice president consultancy service for technical, infrastructure,
and financial decision-making from a data platform for internal use.

used big data pipeline for BI decision making for next 2 quarters of the
upcoming 365 days.
data lake
medallion architecture
bronze. silver. gold. layers
gold data pipeline design and development
data platform dashboards with low response times and high availability for
urgent decision making.
central ssot for international contracts for all merchants inclusive,
configurable per pipeline instead of hardcoded/hidden in backend.
- research airflow to make ETL pipelines
- water processing system (our requirements for consultation, decision
  making, and implementation ends here)
- the output of WPS -> data bricks (the scope ends here)
- research pci, pii, non-pci
- merchant credibility score
- research protocols for authentication solutions also configurable
```
