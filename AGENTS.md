# AGENTS.md — working agreement for this repository

You are working on a data pipeline that handles **encrypted personal data and
payment card numbers**. Most design decisions here exist to protect that data,
and several look like extra work until you know why. This file is the why.

Read `TRUST-BOUNDARY.md` next. It is the difference between describing this
system accurately and overselling it. Read `docs/DECISIONS.md` for the
reasoning behind the platform layer (envelope, verdict, contracts, watchdog,
Jenkins, secrets).

---

## Orientation, in order

| Read | For |
|---|---|
| `TRUST-BOUNDARY.md` | What is real vs. stubbed. **Never claim a stub works.** |
| `docs/DECISIONS.md` | Every platform decision, the alternative rejected, and why |
| `README.md` | Setup, credentials, running it |
| `docs/ORCHESTRATION-SPEC.md` | Orchestrator, Spark submission, task graph, data handoff, local dev |
| `docs/ARCHITECTURE.md` | Why the data flows this way |
| `docs/EXECUTION-FLOW.md` | One run end to end, with the trace |
| `docs/RUNBOOK.md` | Operating it, and failures actually hit |
| `docs/CONSUMING.md` | How a downstream team reacts to a verdict |
| `contracts/README.md` | The message and stage-event contracts, and the two CI tiers |

---

## The shape of it

**One DAG, `trips_pipeline`, eight stages and a verdict:**

```
extract → card_split → encrypt → publish → mask → register → load → report → emit_verdict
                                                                              (all_done)
```

**The stage list lives in `pipeline/stages.py`**, and both the DAG and
`pipeline/run_pipeline.py` walk it. Add a stage there; `tests/test_stages.py`
fails until the DAG chain matches. Do not describe the pipeline a second time
anywhere in code.

Every task brackets itself with `stage_start` / `stage_end` on the
`airflow.task` logger, and returns a **stage event** that
`contracts/stage_event.schema.json` describes. The verdict consumes those
events; a stage that stops reporting a field fails the contract, not the
verdict silently.

**Everything is metadata-driven.** `pipeline/metadata/schema.py` infers each
dataset's columns, sensitivity, primary key and treatments into a manifest;
`pipeline/metadata/contract.py` holds later runs to it. **No stage names a
column literally.** If you catch yourself writing `if col == "card_info"`,
stop — add it to the classifier instead.

**Every Kafka message is `{envelope, payload, signature?}`**, built and parsed
only through `pipeline/messaging/`. Do not construct a message dict by hand.

---

## Invariants. Breaking one is a defect, not a trade-off.

### 1. Plaintext must never land
Sensitive values may exist in exactly two places: `data/csv/` (extract output,
local and transient) and inside the mask task's memory between decrypt and
mask. `spark_encrypt.py :: assert_no_plaintext` refuses to write otherwise.
**Do not weaken it to make a test pass.**

### 2. Tokens are verified, never decoded
`pipeline/common/auth.py` pins RS256 and requires signature, issuer, audience,
expiry, not-before. Each stage has its own identity: `spark-job` encrypts and
**cannot decrypt**; `hive-job` is the reverse; `orchestrator-job` may sign a
verdict and nothing else. `pipeline/messaging/signing.py :: VerdictVerifier`
applies the same rules to verdicts, plus a payload digest check.

### 3. Masking lives in Ranger policy JSON
`config/ranger/*.json` is real Apache Ranger policy. A sensitive column with no
policy **fails the run**. Generation is authoring: a generated file is never
regenerated over an edit. Never express a masking rule in Python.

### 4. Cost is capped, and the cap is measured
BigQuery bills **bytes scanned, not rows returned**. Name columns, never
`SELECT *`; `maximum_bytes_billed` makes BigQuery refuse an over-budget query.
A custom `BQ_SQL_FILE` must reference `@row_limit`.

### 5. Nothing sensitive is published in the clear
`row.encrypted` carries ciphertext; `row.flat` carries never-sensitive
columns. `_assert_no_plaintext_published` refuses the send otherwise, and
`contracts/row_flat.schema.json` rejects any `_encrypted` or `_masked` key.
**Do not "simplify" that away.**

### 6. Re-running replaces, never duplicates
Every stage keys off the run slot; the warehouse upserts on the manifest's key;
the envelope carries `idempotency_key` (per unit of work) and `event_id` (per
emission). A consumer drops a repeated `event_id` and upserts on the
`idempotency_key`. That rule lives in `pipeline/messaging/dedupe.py` and
nowhere else.

### 7. The verdict is always emitted, and is the only predicate
`emit_verdict` runs with `trigger_rule=all_done`. `pipeline/verdict.py` is
pure and decides everything; consumers read `proceed` and nothing else. If you
need downstream to behave differently, change that function and its tests.
Never add a condition to a consumer.

### 8. One place for every value
`.env.local` is the single source of configuration and secrets. Never add a
`${VAR:-default}` to `docker-compose.yml`; `tests/test_secret_hygiene.py`
fails if you do, and also if a compose variable is missing from `.env.example`.

---

## Things that will surprise you

**TLS is on, and verification is enforced.** `verify_tls=False` appears
nowhere. A client without `certs/ca.crt` fails closed. `PyJWKClient` fetches
over urllib, so it needs its own SSL context (`_jwks_ssl_context`).

**Degradations are reported, never hidden.** Spark unreachable → `engine:
pyarrow`; HiveServer2 down → `registered: false`; BigQuery failing thrice →
`fell_back_to_csv: true`; delivery copy failing → `collected: <reason>`. The
verdict names each as a reason and still proceeds. **Never assume which ran —
read the verdict.**

**A dataset's first run writes its contract.** `config/datasets/<name>.json`
gains a `schema` block; the next run whose inference disagrees stops with the
differences listed. Accept a change with
`python -m pipeline.metadata.contract accept <dataset> <landed csv>` and commit.

**Dates are UTC everywhere.** The run slot names the PDF and the verdict. The
data window the extract actually read is a different thing and is printed
separately.

**Spark needs three things aligned.** A JVM in the Airflow image (Java 21);
executor Python matching the 3.12 driver; `/data` mounted at the same path on
the workers. Break any one and it degrades to pyarrow, reported.

**The `beeline` on PATH is pyspark's**; Hive registration talks JDBC through
jaydebeapi with the jar set copied from `apache/hive:4.0.0`.

**Schema manifests are hidden files** (`.<name>.schema.json`) because Hive
reads every file in a partition directory.

---

## Before you open a PR

```bash
make ci
```

That is the pre-push hook and the Jenkins pipeline, verbatim. If you added a
stub or a shortcut, add it to `TRUST-BOUNDARY.md` **in the same change**. If
you took a decision that has an alternative, add it to `docs/DECISIONS.md`.

---

## How to describe this system honestly

**True:** AES-256-GCM with the key held in a separate service. Masked under
Ranger policy JSON. JWT verified by signature, issuer, audience and expiry.
TLS with certificate verification enforced. Encryption on a real Spark
cluster. Real, queryable Hive tables. A signed, always-emitted run verdict,
with a watchdog for its absence. Machine-checked contracts on every message.

**Also true, and must be said alongside it:** the TLS CA is self-signed and
its key sits beside the certs. The Ranger admin service is not deployed. Card
splitting leaves ~6 digits encrypted. One master key, no KMS. SASL
authenticates Kafka but does not encrypt the wire. Single broker.

**Do not say:** "production-ready", "PCI compliant", "audited". No compliance
review has happened. See `TRUST-BOUNDARY.md`.
