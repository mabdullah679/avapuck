# WPS — Walkthrough

> Follow this top to bottom. It ends where the brief says it must:
> **meetings → investments → next quarter.**
>
> Two companion documents carry the honesty burden and should be read
> alongside it: `TRUST-BOUNDARY.md` (what is stubbed, synthetic, assumed, or
> unverified) and `docs/FLAGGABLES.md` (what you will be challenged on, and
> the honest answer to each).

---

## 0. Setup — once, about two minutes

```bash
python3.12 -m venv .venv
.venv/bin/pip install pyyaml pyarrow deltalake streamlit pandas
```

Python 3.12 rather than the system 3.14, and **delta-rs rather than Spark** —
delta-rs writes genuine Delta transaction logs with no JVM at all. At this
corpus size Spark buys nothing but failure modes, and the machine's Java 26 is
ahead of what Spark 4 supports. Gold is still real Delta, readable downstream.

Airflow is optional and only needed for §5:

```bash
.venv/bin/pip install "apache-airflow==3.0.2" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.12.txt"
```

---

## 1. Generate the corpus

```bash
.venv/bin/python -m wps.corpus.generate
```

```
periods      : 2025CQ3 .. 2026CQ3 (5)
merchants    : 90
contracts    : 118  (multi-jurisdiction merchants: 28)
truth rows   : 567
accounts     : 50,950 (ground truth only, never exported)
service_a    :   567 fixed-width records   -> ocio_quarterly.dat
service_b    :   417 XML reporting blocks  -> merchant_platform_export.xml
service_c    :   325 CSV rows              -> risk_identity_quarterly.csv
service_d    :  1650 MET records           -> legacy_setl_extract.psv
expected     :   567 ground-truth rows     -> harness/expected/ground_truth.json
```

**Look at the four files.** They are the point.

```bash
head -c 440 data/raw/service_a/ocio_quarterly.dat   # COBOL fixed-width, implied decimals
sed -n '1,25p' data/raw/service_b/merchant_platform_export.xml
head -6  data/raw/service_c/risk_identity_quarterly.csv
head -10 data/raw/service_d/legacy_setl_extract.psv
```

Four formats, four null conventions, four merchant-ID schemes, two fiscal
calendars, four currency precisions. Every one of these disagreements was
seeded deliberately — they are the translation layer's subject matter, not
noise around it.

The generator builds **one ground truth** and then derives four disagreeing
views from it. That ordering is what makes §4's grade mean something.

---

## 2. Read the SSOT before running anything

This is the part worth slowing down for. The configuration *is* the argument.

| File | What it settles |
|---|---|
| `config/canonical/dictionary.yaml` | What each business term MEANS, once. Contested metrics carry a canonical definition plus *why* the services disagree. |
| `config/canonical/rules.yaml` | Named business rules. Five definitions of "active account", each with a stated owner and a written `known_divergence`. |
| `config/canonical/operators.yaml` | **The closed transform vocabulary.** 20 named operators. No expression language, no `eval`. |
| `config/canonical/jurisdictions.yaml` | Fiscal calendars and currency precision. JPY has 0 minor units; BHD has 3. |
| `config/contracts/quarterly_performance/v1.0.yaml` | The frozen public surface: guarantees, assertions, binding obligations, change policy. |
| `config/bindings/service_*.yaml` | Everything the platform knows about each service. |
| `config/classification/policy.yaml` | PCI / PII / non-PCI handling, per medallion layer. |

Two checks that prove the thesis rather than assert it:

```bash
grep -rn 'service_a"\|COBOL\|GROSS-AMT\|mp:Reporting' wps/ --include="*.py" \
  | grep -v "^wps/corpus/" | grep -v "^wps/pipeline.py"
```

One hit, and it is a docstring saying this is true. **No service-specific fact
exists anywhere in the pipeline code.** Two files are excluded and both are
honest exclusions: `wps/corpus/` is the synthetic data generator, which must
know each dialect in order to produce it, and `wps/pipeline.py` is a six-line
table of where each service's extract lands on disk.

Even service *precedence* — which source is trusted when two can derive the
same canonical figure — is `canonical_precedence` in each binding, not an
ordered list in code. The settlement ledger wins for money because it stores
native minor units for every currency; that judgement is written where the
business can read and change it.

```bash
.venv/bin/python -c "
from wps.config import load_bundle
from wps.operators import validate_registry
b = load_bundle(); validate_registry(b)
print('closed vocabulary validated, both directions:', len(b.operators['operators']), 'operators')"
```

The vocabulary is checked in both directions at load time: a declared operator
with no implementation and an implementation with no declaration are both
errors. A binding reaching for an undeclared operator is refused before a
single record is read.

---

## 3. Run the pipeline

```bash
.venv/bin/python -m wps.run
```

```
contract 1.0.0  dictionary 1.0.0  bundle 5e7fb368d94e4f86
BRONZE    2959 records  {'service_a': 567, 'service_b': 417, 'service_c': 325, 'service_d': 1650}
SILVER    1584 rows     {'service_a': 567, 'service_b': 417, 'service_c': 325, 'service_d': 275}
GOLD       567 actual rows + 230 projected rows -> lake/gold/quarterly_performance
        703 reconciliation flags -> lake/_audit/reconciliation_flags
elapsed 0.6s
```

What happened at each layer:

- **Bronze** — raw and immutable, source fidelity preserved. PII lands raw
  because transforming on the way in would destroy the ability to prove what
  the source actually sent. PCI lands only as ciphertext.
- **Silver** — conformed to the dictionary, PII and PCI tokenized. A
  classification breach fails the batch; it is never a warning.
- **Gold** — reconciled across services. Note Service D collapsing 1,650
  metric-narrow records onto 275 rows at the canonical grain.

---

## 4. Grade it

```bash
.venv/bin/python harness/grade.py
```

```
scope                            checks   passed     pct  not rep.  degraded grade
service_a                          2835     2835 100.00%       567       160   A
service_b                          2487     2487 100.00%        15       115   A
service_c                          1594     1594 100.00%       356         0   A
service_d                          1069     1069 100.00%       581         0   A
GOLD (canonical output)            2795     2795 100.00%        40       160   A
GOLD (refusal to invent)           2114     2114 100.00%         0         0   A
OVERALL                           12894    12894 100.00%      1559       435   A
```

Three columns matter more than the grade:

**`not reported` (1,559)** — the source genuinely sent no value. Excluded from
the percentage rather than counted as a failure, and shown so it stays visible.

**`degraded` (435)** — the reporting service's declared decimal ceiling is
coarser than the currency's precision. Services A and B carry two decimal
places; **BHD has three**. The exact figure is unrecoverable *from those
sources*, so affected rows carry `precision_degraded` and Gold prefers a
source that can represent the currency where one exists. This was a live
defect found during the build, not a hypothetical.

**`GOLD (refusal to invent)`** — 2,114 checks that Gold reports **NULL** for
canonical `active_accounts`, because no service exports the account-level
detail the canonical rule needs. A number there would score as a *failure*.
Refusing to impute is graded as correctness, which is the only way to stop an
imputed figure from quietly becoming evidence.

---

## 5. Run it under Airflow (optional)

```bash
export AIRFLOW_HOME="$PWD/airflow_home" AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
       AIRFLOW__CORE__LOAD_EXAMPLES=False
.venv/bin/airflow db migrate
.venv/bin/airflow dags test wps_service_c
.venv/bin/airflow dags test wps_gold_assembly
```

Both complete with `state=success`. Five DAGs: four per-service plus Gold
assembly.

`dags/wps_service_dags.py` contains **four DAGs and no four blocks of code** —
each is generated from its binding, so a fifth service adds one config file and
nothing else. Writing four near-identical DAG modules by hand would have moved
the fragmentation into the orchestration layer: the same failure, one directory
over.

Each service DAG runs authenticate → pull → parse → map → Bronze → Silver, then
signals Gold. **Gold is deliberately not per-service** — reconciling across
services is its whole job, so no single service's pipeline can build it. That
is a considered departure from the brief's sketch and is recorded in
`TRUST-BOUNDARY.md`.

The four auth protocols (OAuth2 client-credentials, mTLS, API key + HMAC,
service-account JWT) are configured in `config/auth/profiles.yaml`, dispatched
by protocol, and **fail closed**. All four handshakes are simulated —
`TRUST-BOUNDARY.md` §2.2.

---

## 6. The dashboard

```bash
.venv/bin/streamlit run dashboard/app.py
```

Read it in this order:

1. **The projection warning at the top.** Every estimate on this page is
   asterisked, rendered in amber, and carries a range. At this decision scale,
   an estimate must never read as a fact.
2. **The stat tiles.** Three measured, one projected — and the projected one
   shows its range, never a bare number.
3. **The trend chart.** Closed quarters are a solid line with filled markers.
   Projected quarters are dashed with hollow markers inside a widening
   variability band. The distinction is structural, not decorative.
4. **The reconciliation panel — this is the demo.** Pick `active_accounts`,
   period `2026CQ3`. The top contract shows Service A reporting **242**,
   Service B **85**, Service C **175** — for the same merchant, same contract,
   same quarter. Each bar names the rule that produced it: `incl_any_open`,
   `incl_30d_txn`, `incl_verified_only`. Below, in red: **no canonical value**,
   because no source exports what the canonical rule needs.

   That single panel is the whole thesis. Before: three numbers in three
   systems, no way to tell whether anyone is wrong. After: three numbers side
   by side, each attributable to a named and owned rule, with the platform
   stating plainly what it cannot reconcile.
5. **Data quality and SLOs.** Render measured at ~240 ms against a p95 target
   of 800 ms. **Availability is marked "not met"** — a single local process is
   not a highly available serving layer, and saying so is the point.

---

## 7. Where this lands — meetings → investments → next quarter

The chain the brief asks for, made concrete:

**Gold → the meeting.** One row per merchant, per contract, per calendar
quarter, in every jurisdiction's own settlement currency with a USD companion
for adding them up. Every row stamped with `contract_version`,
`bundle_hash`, and `dictionary_version` — so any figure in the room can be
traced to the exact configuration that produced it.

**The meeting → the investment decision.** The reconciliation panel changes
what the meeting *is*. The old question — "whose number is right?" — is
unanswerable and consumes the room. The new question — "which definition do we
want to decide on?" — is a business question with an owner, and it takes
minutes. Where the platform cannot reconcile, it says so, and that gap is
itself the finding: it names exactly what each service team must start
exporting.

**The decision → next quarter.** Projections carry the current quarter to close
and the next one forward, with explicit ranges. A committee can see both the
central estimate and how much confidence to place in it. Nothing on the page
lets an estimate be mistaken for a measurement.

**And past the boundary.** Gold is real Delta, so Databricks reads it directly.
That handoff is **defined, not exercised** — no Databricks was run, because it
has no always-free tier and this POC's budget is zero.

---

## What this proves, and what it does not

**Proves.** A config-driven translation layer absorbs four mutually
incompatible dialects and emits one standardized quarterly model, with no
mapping logic in code. Adding a service is a config file. Semantic conflicts
are reconciled where possible, named where not, and never imputed.

**Does not prove.** Scale — the corpus is deliberately small and rich.
Correctness against conflicts nobody anticipated — the harness proves the
pipeline reverses conflicts *we seeded*. Production auth, key management, or
PCI compliance — all stubbed at named interfaces. High availability — not met,
and stated.

The honest next step is a real-data pilot on one service, which is exactly what
the trust boundary is written to make safe to propose.
