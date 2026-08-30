# WPS POC — Build Prompt

> Paste this into a fresh Claude Code session at the repo root.
> Self-contained: assumes no prior conversation.

---

## Context

You are building a **proof of concept** for a fintech data platform. It is a
POC, not production. Its job is to prove a vision works, so that an AVP can
present it onward with confidence.

### The problem

Every internal service reports quarterly performance in its own dialect. The
OCIO team has a format. The MySQL team has another. The PII team encrypts
with a scheme nobody else understands. Each is internally coherent and
mutually unintelligible. Reconciliation today is manual or nonexistent.

### The system — WPS (Water Processing System)

"Water processing" is a metaphor: dirty water in, potable water out. Raw
per-service data in, standardized data out. WPS *is* the medallion pipeline —
not a separate component feeding it.

```
 4 services          per-service DAGs        medallion             consumers
 (4 dialects,        (Airflow)               layers
  4 formats,
  4 auth protocols)

 ┌──────────┐                              ┌────────┐
 │ Service A│──pull──▶ ┌──────────┐  ──▶   │ BRONZE │ raw, immutable
 ├──────────┤          │  parse   │        ├────────┤
 │ Service B│──pull──▶ │  + map   │  ──▶   │ SILVER │ conformed
 ├──────────┤          │  dialect │        ├────────┤
 │ Service C│──pull──▶ │    ↓     │  ──▶   │  GOLD  │ ──▶ Databricks
 ├──────────┤          │ canonical│        └────────┘ ──▶ dashboard
 │ Service D│──pull──▶ └──────────┘             │
 └──────────┘                                   ▼
                                          meetings → investments
                                              → next quarter
```

### The thesis being proved

A **config-driven translation layer** can absorb four mutually incompatible
service dialects and emit one standardized quarterly performance model,
without the mapping logic being hardcoded or hidden in backend code.

This is the whole point. If mappings end up hardcoded, the POC has failed
even if it runs — the fragmentation would simply have moved one layer up,
into code, where the business cannot see or audit it.

### What the dashboards drive

Billion-dollar financial decisions, architecture decisions, and org
restructuring at a fintech giant. This is why correctness and honest
uncertainty-marking matter more than feature count.

---

## Success criteria

The POC passes when a walkthrough delivers **three things**:

1. **The happy path, end to end** — one person can load pipelines, get
   standardized Gold output, and follow the chain through to dashboard.
2. **Features that actually work** — usable, not mocked.
3. **An explicit trust boundary** — a plainly stated list of what is stubbed,
   assumed, synthetic, or unverified.

Criterion 3 is not a caveats appendix. It is a **first-class deliverable**.
The AVP presents this onward; anything overclaimed becomes their exposure in
a room you are not in. An honest untrustable-list is what makes the work safe
to carry.

Judged qualitatively by one person (the AVP) who is already convinced and
trusts the builder. There is no deadline. **Scope is bounded by the
walkthrough**: once the happy path runs and the trust boundary is honest, it
is done. Further iteration is scope creep.

---

## Hard constraints

| Constraint | Value |
|---|---|
| Cost | **Zero.** Always-free tiers or local only. |
| Databricks | No always-free tier exists (14-day trial; Community Edition deprecated). Use **local Delta Lake** for medallion storage. Databricks is a downstream *consumer* of Gold, out of scope. |
| Airflow | Cloud-oriented in design, runnable locally for the POC. |
| Data | 100% synthetic. No real data. Learnings transfer to the real lab later. |
| Environment | Dedicated to this requirement; its own field. |

---

## Build these

### 1. Synthetic corpus — richer per-record structure

Four services, four **deliberately incompatible** formats:

| Service | Format | Dialect flavor |
|---|---|---|
| A | Fixed-width (COBOL-style) | Legacy mainframe; packed fields, implied decimals |
| B | XML | Deeply nested, attribute-heavy, schema-ish |
| C | CSV, bespoke multi-row header | Analyst-authored, human conventions |
| D | Proprietary delimited (pipe/caret) | Terse codes, lookup-table dependent |

**Structure over volume.** Prefer records with real internal complexity —
nested entities, repeated groups, optional fields, per-service null
conventions — over a large flat corpus. A few thousand rich records prove a
mapping layer; 40,000 flat rows do not.

**Seed genuine semantic conflicts.** At minimum:
- The same term meaning different things across services (e.g. "active
  account" with different inclusion rules per team)
- Different terms meaning the same thing
- Divergent date/period conventions (fiscal vs. calendar quarter)
- Divergent currency/precision handling
- Per-service null/missing/zero conventions that do not agree

These conflicts are the POC's actual subject matter. Without them the
translation layer has nothing to prove.

### 2. Canonical dictionary + contract SSOT

Governance model, non-negotiable:

- SSOT file is **write-only to its creator**
- Everyone else is **read-only, after decryption**
- The SSOT defines **contracts**; contracts are the public surface
- All access authenticated
- **Every service must fit the contract bindings or be reworked until it
  does. No flexibility.**

Mappings live in **configuration**, never in code. Adding a service or
changing a mapping must not require touching pipeline logic.

> The SSOT schema needs a **deep, explicit argument** before it is frozen —
> it is the firmware of this POC and the expensive thing to change later.
> Present the argument and the trade-offs; do not silently pick a design.

### 3. Four Airflow DAGs — one per service

Each DAG: pull → parse native format → map dialect to canonical → land
through Bronze → Silver → Gold.

**Four different auth protocols, one per service** (e.g. OAuth2/OIDC, mTLS,
API key + HMAC signing, service-account/JWT). Deliberate: the pipelines must
"match the vibe of each service" and still standardize the output. Auth is
**configurable**, same stance as mappings.

### 4. Medallion layers (local Delta Lake)

Bronze raw/immutable → Silver conformed/quality-enforced → Gold business-ready.

Classify every field **PCI / PII / non-PCI** and enforce per-layer handling:
what may land in Bronze, what must be masked or tokenized before Silver, what
may surface in Gold and on the dashboard.

Model one service as having an **opaque encryption scheme** — fields arriving
as blobs, mapped by metadata, decryption **stubbed at a defined interface**.
Do not build key management. This proves the architecture tolerates an opaque
source without dragging PCI-DSS scope into throwaway infrastructure. It is a
named interface, not a gap — at a fintech it will be the first objection
raised, so it must be visibly designed for.

### 5. Grading harness

Generate an **independent SSOT of expected values**, run pipeline output
against it, grade **A–F**. This converts the qualitative success criterion
into something mechanically checkable. Gold output should match each team's
own reported figures.

Report per-service and overall. Make failures legible — *which* field,
*which* mapping rule, *why*.

### 6. Dashboard

Batch, computed before the quarterly meeting, with enough lead time to show
projections for the rest of the current quarter and the next.

**Projections must be visually distinct from concrete data:** asterisk-marked,
carrying a variability range, color-coded. At billion-dollar decision scale,
never let an estimate read as a fact.

Non-functional targets: low response time, high availability. **Define numeric
SLOs rather than adjectives**, then state whether the POC meets them.

---

## Open questions — decide and justify, do not silently assume

1. Are WPS and the Bronze→Silver→Gold pipeline the same system, or distinct?
   (The source notes name them separately; they may be one thing under two
   names.) Design for the answer you argue for.
2. Where exactly does the canonical dictionary live — config repo, schema
   registry, database, catalog? Argue the choice.
3. Dashboard tool: open. Pick one that is free and locally runnable, and say
   why.
4. Numeric SLOs for latency and availability.

---

## How to work

- **Report the plan before building.** Surface disagreements early.
- Keep a running **TRUST-BOUNDARY.md** from the first commit. Every stub,
  assumption, and synthetic shortcut goes in as it is created — do not
  reconstruct it at the end.
- Prefer **structure and realism over volume** everywhere.
- **If a mapping is about to be hardcoded, stop.** That is the thesis
  failing. Fix the config surface instead.
- The final deliverable is a **walkthrough** someone else can follow, ending
  at: meetings → investments → next quarter.