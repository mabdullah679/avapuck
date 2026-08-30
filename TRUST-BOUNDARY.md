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
| 4.6 | Service A cannot supply canonical `active_accounts` | **STATED GAP** — its extract carries no per-account transaction recency, so `canon_v1` is not recomputable from that source. Gold records Service A's native figure under its own named rule and marks the canonical variant not-derivable **rather than guessing**. A reconciled number that was quietly imputed would be worse than an honest gap. |

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
