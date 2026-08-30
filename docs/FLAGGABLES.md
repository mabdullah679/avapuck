# FLAGGABLES

> Companion to `TRUST-BOUNDARY.md`. That file lists what is stubbed. **This
> file lists what someone in the room will push on, and what the honest answer
> is.** It exists so the person presenting this is never surprised by a
> question they could have been handed the answer to.
>
> Ordered by how likely the question is, and how much it costs to be caught
> without an answer.

---

## F1 — "Is it *really* all configuration, or is that a slogan?"

**Likelihood: certain.** This is the thesis, so it is the thing to attack.

**The honest answer.** Mappings are configuration. The *primitives that
mappings are built from* are code. `config/canonical/operators.yaml` declares
a **closed vocabulary** of ~20 named operators; bindings may only compose
those. There is no expression language, no `eval`, no inline code in any
binding.

**What that buys.** Adding Service E is one new file in `config/bindings/`
and zero lines of pipeline code. Changing how a service reports "active
accounts" is a one-line config edit. The four bindings in this repo contain
every service-specific fact the platform knows; grep the pipeline for
`service_a` and you find nothing.

**Where the line actually is.** A service needing a transform the vocabulary
cannot express requires a **new named, documented, tested operator**. That is
a code change — but a *governed, reviewable, single-purpose* one, not a
special case buried in a DAG.

**Why that line is correct rather than a compromise.** An open expression
language is code wearing a config costume. A business cannot audit a lambda.
It can audit a list of named rules. Choosing the closed vocabulary is choosing
auditability over expressiveness on purpose.

---

## F2 — "You standardized our numbers. Which means you changed them."

**Likelihood: certain, from the service teams rather than the executive.**
This is the objection that kills translation-layer projects politically, long
after they work technically.

**The honest answer.** Gold changes nothing. It **adds**. Every contested
metric carries the canonical value *and* each team's own reported figure *and*
the named rule that produced each *and* the variance between them. Service B's
812 is still 812 in Gold, labelled `incl_30d_txn`, sitting next to the
canonical figure and the reason they differ.

**Why it was built this way.** The alternative — one canonical number, teams
conform — is a cleaner schema and a worse outcome. A team that cannot find its
own number stops trusting the platform, and a platform the source teams
distrust does not survive its second quarter.

---

## F3 — "What happens to PCI data?"

**Likelihood: certain at a fintech. Usually the first question.**

**The honest answer.** No plaintext PCI value exists anywhere in this repo, at
any layer, at any point. Service D's settlement-account field arrives as an
opaque blob and is tokenized **without ever being decrypted** —
`config/classification/policy.yaml` forbids the `decrypt_ref` operator on
`pci`-classified fields outright, and the batch fails loudly on violation
rather than degrading.

**What is stubbed.** The decryption interface itself
(`wps.io.decryption:DecryptionProvider`). The POC ships a reversible synthetic
provider for demonstration. **No cryptography, no key management, no HSM, no
rotation.**

**Why stubbing it is the right call rather than a shortcut.** Building real
key management into throwaway POC infrastructure would drag that
infrastructure into PCI-DSS scope for no benefit. The architecture proves it
tolerates an opaque source; the keys stay where they belong. This is a named
interface, not a gap — but say "stubbed" out loud, because someone will check.

---

## F4 — "Three of your four services can't produce the canonical number."

**Likelihood: high, if anyone reads the bindings closely. Reward them for it.**

**The honest answer.** Correct, and it is recorded per service rather than
hidden:

| Service | Its rule | Why canon is not derivable |
|---|---|---|
| A | `incl_any_open` | Sends no per-account transaction recency at all. |
| B | `incl_30d_txn` | Reports a 30-day window; canon needs 90, and the account detail is not exported. |
| C | `incl_verified_only` | Excludes pending-KYC accounts without reporting how many. Direction of the undercount is known; magnitude is not. |
| D | `incl_nonzero_balance` | Measures **balance**, not activity — a different quantity wearing the same name. |

**What the pipeline does about it.** Records each native value under its named
rule and marks the canonical variant not-derivable. It does **not** impute.

**Why not imputing is the point.** A reconciled number that was quietly
guessed is worse than a visible gap, because it looks like evidence. At the
decision scale these dashboards feed, a confident wrong number costs more than
an honest absent one. The gap is also *actionable*: it tells you exactly what
to ask each service team to start exporting.

---

## F5 — "Which quarter is this, actually?"

**Likelihood: medium. Devastating if discovered after a decision.**

**The honest answer.** The string `FY26Q3` means **Jul–Sep 2026** to Service A
and **Oct–Dec 2026** to Service B. Same literal characters, different three
months, nothing in the data revealing which. Only the reporting service's
fiscal calendar resolves it.

**What the pipeline does.** The canonical period is **always** a calendar
quarter. Fiscal labels are translated inbound per service and never survive
into Gold — there is a contract assertion (`period_is_calendar`) that fails
the batch if one does.

**Why this is worth raising unprompted.** It is the single most plausible
undetected error in a manual reconciliation, it is invisible in a spreadsheet,
and demonstrating that the platform catches it is a stronger argument for the
platform than any throughput number.

---

## F6 — "Your Japanese figures look a hundred times too small."

**Likelihood: medium.** They are correct; the instinct is wrong.

**The honest answer.** JPY has **zero** minor units and BHD has **three**.
A pipeline that assumes the near-universal two inflates every Japanese figure
100× and every Bahraini figure 10×. `to_minor_units` reads each currency's
declared precision from `config/canonical/jurisdictions.yaml` rather than
assuming.

**The sharpest illustration, worth showing live.** In Service D's file, one
column carries four different scales at once — thousandths of a dinar,
cents, pence, and whole yen — distinguishable only by the currency code three
fields away. Any uniform divisor silently corrupts three of four jurisdictions.

**This defect was actually present and was caught during the build.** The
Service D binding originally hardcoded a 3-decimal assumption. It is fixed and
covered by the `minor_units_native` assertion. Mentioning that it happened is
better than implying it could not.

---

## F7 — "Zero, or nothing?"

**Likelihood: low in the room, high in consequence.**

**The honest answer.** Every binding declares its own null vocabulary, and
whether zero means zero. Four services, four conventions: Service A uses
`*NONE*` and blanks; Service B omits the element, or sends it empty, or uses
`xsi:nil`; Service C uses `-`, `n/a`, `TBC`; Service D uses `~`, `-1` (not
collected) and `-9` (suppressed) — sentinels that a naive sum would treat as
*negative revenue*.

A contract assertion (`null_is_not_zero`) fails the batch if an unreported
metric ever lands as 0.

**The one genuine ambiguity we could not resolve.** Service C also writes `0`
for counts suppressed under a small-count disclosure rule. Their extract does
not distinguish suppressed-zero from true-zero. **This is flagged, not
guessed at** — resolving it needs a conversation with that team, not a
heuristic. It is the clearest example of a limit the data itself imposes.

---

## F8 — "Would this survive real volume?"

**Likelihood: medium.**

**The honest answer.** Unknown, and this POC does not claim otherwise. The
corpus is deliberately small and structurally rich — ~90 merchants, 118
contracts, 6 quarters, ~680 fact rows over ~61,000 underlying accounts — to
prove a *mapping layer*, not a *scale story*. Storage is real Delta Lake
(delta-rs), so the format carries forward, but nothing here has been run at
production volume and no scale claim should be made from it.

---

## F9 — "Where's Databricks?"

**Likelihood: high, because it is in the original brief.**

**The honest answer.** Deliberately out of scope, and the constraint that put
it there is a cost constraint, not a technical one: Databricks has no
always-free tier (14-day trial; Community Edition deprecated), and this POC's
budget is zero. Gold is written as **real Delta tables**, so it is directly
readable by Databricks downstream. The handoff is **defined but not
exercised** — say "defined", not "working".

---

## F10 — "Is the dashboard production-ready?"

**Likelihood: medium.**

**The honest answer.** No, and it was never meant to be. It is a single local
Streamlit process. It demonstrates the consumption layer; it does not
implement a highly available serving one.

Numeric SLOs, stated rather than adjectival:

| SLO | Target | Verdict |
|---|---|---|
| Render p95 | ≤ 800 ms | Expected to meet (precomputed Gold, no query-time aggregation) |
| Render p99 | ≤ 1.5 s | Expected to meet |
| Availability | 99.5%/month, business hours | **Does not meet.** Single process. |
| Gold freshness at meeting time | ≤ 24 h | Meets |
| Full batch, 4 services | ≤ 30 min | Meets |

The availability row is the one to volunteer rather than wait to be asked.

---

## F11 — "How do you know the output is right?"

**Likelihood: medium.**

**The honest answer.** The corpus generator builds a **ground truth** first
and then derives four deliberately disagreeing service views from it. The
grading harness compares pipeline output against that ground truth, which was
never derived from the pipeline. So agreement is evidence rather than
tautology.

**The limit.** It proves the pipeline correctly reverses conflicts **we
ourselves seeded**. It does not prove the pipeline handles a conflict nobody
thought of. That is what a real-data pilot is for, and it is the honest next
step to propose.

---

## F12 — "Who can change the SSOT?"

**Likelihood: low, but it is the governance question.**

**The honest answer.** The model is: write-only to its creator, read-only to
everyone else, all access authenticated. In this POC that maps to git
CODEOWNERS plus branch protection plus a content-hashed bundle that the
pipeline verifies. **The POC verifies the hash; it does not enforce the write
control** — that is a repository-platform control, not application code.

Contracts are immutable once frozen. Every Gold row stamps
`contract_version`, `binding_hash` and `dictionary_version`, so any past
quarter can be replayed under exactly the rules in force at the time. That
replay capability is a design property, not a tested feature — it has not
been exercised.
