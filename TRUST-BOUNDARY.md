# TRUST BOUNDARY

> **Read this before presenting anything from this repo onward.**
>
> This is a proof of concept. Everything below is a thing that is stubbed,
> synthetic, assumed, or unverified. It is maintained as the work happens,
> not reconstructed at the end. If something is not on this list, it is
> because it genuinely runs — not because we forgot.
>
> Status legend: **STUB** (named interface, no implementation) ·
> **SYNTHETIC** (fabricated data) · **ASSUMED** (a decision taken without
> confirmation) · **UNVERIFIED** (built but not proven at scale/production
> conditions) · **OUT OF SCOPE** (deliberately not built)

Last updated: 2026-08-30

See also `docs/FLAGGABLES.md` — the questions this work will be challenged on,
with the honest answer to each.

---

## 1. Data

| # | Item | Status | What this means when presenting |
|---|---|---|---|
| 1.1 | The entire corpus — all four services, all merchants, all figures | **SYNTHETIC** | 100% fabricated by `wps/corpus/`. No real merchant, contract, volume, or person appears anywhere. Nothing here is a real financial figure. Do not quote a number from this POC as a fact about the business. |
| 1.2 | Corpus volume | **SYNTHETIC** | Deliberately small and structurally rich (thousands of records, not millions). This proves a mapping layer; it does **not** prove scale behaviour. |
| 1.3 | FX rates | **SYNTHETIC** | Fixed rate table in `config/lookups/fx_rates.yaml`. Not live rates, not a rate feed, no rate-as-of-date sourcing beyond the declared quarter. |
| 1.4 | Semantic conflicts between services | **SYNTHETIC, deliberate** | The disagreements the pipeline reconciles were seeded on purpose. They are modelled on realistic dialect conflicts but are not measurements of any real team's actual reporting divergence. |

## 2. Security, auth, and PCI/PII

| # | Item | Status | What this means when presenting |
|---|---|---|---|
| 2.1 | Service D opaque field decryption | **STUB** | Encrypted blobs arrive and are routed through a **named interface** (`wps/io/decryption.py :: DecryptionProvider`). The POC ships a reversible synthetic provider for demo only. **No real cryptography, no key management, no HSM, no key rotation.** This is designed-for, not built. |
| 2.2 | All four auth protocols (OAuth2/OIDC, mTLS, API key + HMAC, service-account JWT) | **STUB** | Auth is *configured* per service in `config/auth/profiles.yaml` and the config surface is real. The handshakes are simulated locally — no identity provider, no certificate authority, no live token exchange, no credential storage. What is proven is that auth is **configurable per service**, not that these protocols are correctly implemented. |
| 2.3 | SSOT "write-only to creator, read-only to everyone else" | **ASSUMED / partially modelled** | Modelled as git CODEOWNERS + branch protection + a signed compiled bundle. The POC verifies the bundle hash; it does **not** enforce write access — that is a repository-platform control, not application code. |
| 2.4 | PCI/PII field classification and per-layer handling | **UNVERIFIED** | Classification and masking/tokenization rules are declared in config and enforced by the pipeline. They have **not** been reviewed by a compliance function and constitute no claim of PCI-DSS compliance. This infrastructure is throwaway and was deliberately kept out of PCI scope. |
| 2.5 | Tokenization | **STUB** | Deterministic local hashing, not a vault-backed tokenization service. |

## 3. Platform and serving

| # | Item | Status | What this means when presenting |
|---|---|---|---|
| 3.1 | Dashboard availability SLO (99.5% monthly) | **DOES NOT MEET** | The dashboard is a single local process. It demonstrates the consumption layer; it does not implement a highly available serving layer. Stated plainly rather than claimed. |
| 3.2 | Databricks | **OUT OF SCOPE** | Gold is written as real Delta tables and is therefore readable downstream, but nothing was run against Databricks. The handoff is defined, not exercised. |
| 3.3 | Airflow | **UNVERIFIED at production** | DAGs are real and run locally in standalone mode. No managed deployment (MWAA/Composer/Astronomer), no HA scheduler, no production retry/alerting posture. |
| 3.4 | Latency SLOs (p95 ≤ 800ms, p99 ≤ 1.5s) | **UNVERIFIED until measured** | Targets are numeric and measured against precomputed Gold snapshots on one local machine. Not a multi-user, networked, or contended measurement. |

## 4. Design decisions taken without external confirmation

| # | Decision | Status |
|---|---|---|
| 4.1 | WPS and the Bronze→Silver→Gold pipeline are **one system**, not two | **ASSUMED** — argued from the brief. `consultation-notes.md` §6 recorded the opposite reading (WPS as a physical plant); that section is superseded and annotated. |
| 4.2 | Merchant credibility score is a **Gold-layer computed output**, not an ingested input | **ASSUMED** — the source notes are ambiguous. Chosen because a config-declared scoring formula is auditable; an ingested score would be a black box and would prove nothing about the thesis. |
| 4.3 | "Next 2 quarters" is a **reporting horizon**, not a delivery deadline | **ASSUMED** — the brief states there is no deadline. |
| 4.4 | Four jurisdictions (US, UK, JP, BH) with differing fiscal calendars and currency precision | **ASSUMED** — chosen to make the international-contract dimension load-bearing rather than cosmetic. Real jurisdiction mix is unknown. |
| 4.5 | Projection method for forward quarters | **ASSUMED** — a declared, simple statistical method with an explicit variability range. Not a validated forecasting model. Every projected figure is marked. |
| 4.6 | **No service can supply canonical `active_accounts`** | **STATED GAP** — see the table in §7. The pipeline records each service's native figure under its own named rule and marks the canonical variant not-derivable **rather than imputing**. A reconciled number that was quietly guessed is worse than a visible gap, because it looks like evidence. |
| 4.7 | Service C's suppressed zeros | **UNRESOLVED AMBIGUITY** — Service C writes `0` both for a true zero and for counts suppressed under a small-count disclosure rule, and the extract does not distinguish them. Flagged rather than guessed at; resolving it requires a conversation with that team, not a heuristic. |
| 4.8 | Merchant identity cross-reference | **SYNTHETIC** — `config/lookups/merchant_xref.yaml` is written by the corpus generator. In production this is a governed reference dataset, not a generated file. |
| 4.9 | Contract replay under a prior version | **UNVERIFIED** — every Gold row stamps `contract_version`, `binding_hash` and `dictionary_version`, which makes replay possible in principle. It has not been exercised. Design property, not tested feature. |

## 5. The honest limit of "config over code"

| # | Item | Status |
|---|---|---|
| 5.1 | The transform vocabulary is a **closed set of named operators** (`config/canonical/operators.yaml`) | **STATED LIMIT** | Bindings compose declared operators; there is no arbitrary expression language and no `eval`. A service needing a transform the vocabulary cannot express requires **adding a new named, documented, tested operator** — a governed code change, visible in review. This is the real boundary of the "no hardcoded mappings" claim, and it is a deliberate line, not an oversight. Mappings are config; the *primitives* mappings are built from are code. |

## 6. Not built

- Key management, HSM, key rotation, envelope encryption
- Real identity providers, certificate authorities, credential vaults
- Streaming ingestion (batch only, by design)
- Schema-registry or catalog integration
- Access control on the dashboard
- Backfill/replay tooling beyond what contract versioning makes possible in principle
- Anything downstream of the Gold handoff

---

## 7. Canonical `active_accounts` is not derivable from any source

The seeded semantic conflict resolves only partially, on purpose, and this is
the most important honest limit in the build. Each service applies its own
inclusion rule and none of the four exports the account-level detail that
canonical `canon_v1` requires.

| Service | Rule applied | Why canon is not derivable from it | Direction |
|---|---|---|---|
| A | `incl_any_open` | No per-account transaction recency in the extract at all. | Overcounts |
| B | `incl_30d_txn` | Reports a 30-day window; canon needs 90. Account detail not exported. | Undercounts |
| C | `incl_verified_only` | Excludes pending-KYC accounts without reporting how many were excluded. | Undercounts, magnitude unknown |
| D | `incl_nonzero_balance` | Measures balance, not activity — a different quantity under the same name. | Not comparable |

Gold therefore carries every service's native figure with its rule named, and
marks the canonical variant not-derivable where it is. **Nothing is imputed.**

This gap is also the platform's most actionable output: it states exactly what
each service team would need to start exporting for the canonical figure to
become computable.

## 8. What the grading harness does and does not prove

The corpus generator builds a ground truth first and derives four
deliberately disagreeing service views from it. The harness grades pipeline
output against that ground truth, which was never derived from the pipeline —
so agreement is evidence, not tautology.

**It proves** the pipeline correctly reverses the conflicts we seeded.
**It does not prove** the pipeline handles a conflict nobody anticipated.
Only a real-data pilot does that, and that is the honest next step.
