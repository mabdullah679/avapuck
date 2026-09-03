# AGENTS.md — working agreement for this repository

You are working on a data pipeline that handles **encrypted personal data and
payment card numbers**. Most design decisions here exist to protect that data,
and several look like extra work until you know why. This file is the why.

Read `TRUST-BOUNDARY.md` next. It is the difference between describing this
system accurately and overselling it.

---

## Orientation, in order

| Read | For |
|---|---|
| `TRUST-BOUNDARY.md` | What is real vs. stubbed. **Never claim a stub works.** |
| `README.md` | Setup, credentials, running it |
| `docs/ARCHITECTURE.md` | Why the data flows this way |
| `docs/EXECUTION-FLOW.md` | One run end to end, with the trace |
| `docs/RUNBOOK.md` | Operating it, and failures actually hit |
| `docs/AGENTS.md` | Longer-form background on the invariants |

---

## The shape of it

**One DAG, `trips_pipeline`, eight tasks:**

```
extract → card_split → encrypt → publish → mask → register → load → report
```

Every task brackets itself with `stage_start` / `stage_end` on the
`airflow.task` logger, so the UI shows each step's status and duration while a
run is in flight — not only after it finishes.

Chained by `>>`; each task consumes what the previous one wrote. An earlier
version ran six asset-scheduled DAGs — that history is in
`docs/EXECUTION-FLOW.md`, and the trade is recorded in the DAG's docstring.

**Everything is metadata-driven.** `pipeline/metadata/schema.py` infers each
dataset's columns, sensitivity, primary key and special treatments into a
manifest, and every stage reads that manifest. **No stage names a column
literally.** If you catch yourself writing `if col == "card_info"`, stop — that
is the design leaving the building. Add it to the classifier instead, so it
holds for any dataset.

---

## Invariants. Breaking one is a defect, not a trade-off.

### 1. Plaintext must never land
Sensitive values may exist in exactly two places: `data/csv/` (extract output,
local and transient) and inside the mask task's memory between decrypt and
mask. Everywhere else is ciphertext or masked.
`spark_encrypt.py :: assert_no_plaintext` refuses to write otherwise.
**Do not weaken it to make a test pass.**

### 2. Tokens are verified, never decoded
`pipeline/common/auth.py` pins RS256 and requires signature, issuer, audience,
expiry, not-before. `jwt.decode(..., verify_signature=False)` is worse than no
auth, because it looks like auth in review.

Each stage has its own identity and scope: `spark-job` encrypts and **cannot
decrypt**; `hive-job` is the reverse. Enforced by the IdP, not by convention.

### 3. Masking lives in Ranger policy JSON
`config/ranger/*.json` is real Apache Ranger policy. A sensitive column with no
policy covering it **fails the run** rather than being written unmasked.
Generation is authoring: a generated file is never regenerated over an edit,
but a dataset that gains a column gets policy *appended* for the uncovered
column only.

Never express a masking rule in Python.

### 4. Cost is capped, and the cap is measured
BigQuery bills **bytes scanned, not rows returned** — `LIMIT 100` reduces the
bill by exactly zero. What controls cost: naming columns (never `SELECT *`)
and `maximum_bytes_billed`, which makes BigQuery *refuse* an over-budget query.
A custom `BQ_SQL_FILE` must reference `@row_limit`; the extractor rejects one
that does not.

### 5. Nothing sensitive is published in the clear
The `publish` stage sends ciphertext to `rpos_encrypted` and never-sensitive
columns to `rpos_flat`. Kafka is durable, replicated and retained — plaintext
there is a larger exposure than the warehouse ever was.
`_assert_no_plaintext_published` refuses the send if the manifest's sensitive
columns are ever routed to the flat topic. **Do not "simplify" that away.**

### 6. Re-running replaces, never duplicates
Every stage keys off the logical timestamp; the warehouse upserts on the
manifest's key. Verify by re-running a slice and expecting `0 inserted /
N updated`.

---

## Things that will surprise you

**TLS is on, and verification is enforced.** `verify_tls=False` appears nowhere.
A client without `certs/ca.crt` fails closed — that is correct, not a bug to
work around. `PyJWKClient` fetches over urllib, not requests, so it needs its
own SSL context (`_jwks_ssl_context`).

**Both fallbacks are silent by design, and reported.** The encrypt stage falls
back to pyarrow if Spark is unreachable (`engine: pyarrow`), and Hive
registration skips if HiveServer2 is down (`registered: false`). Both say so in
the stage result. **Never assume which ran — read the trace.**

**Spark needs three things aligned.** A JVM in the Airflow image (Java 21 —
Spark 4 rejects 25); executor Python matching the 3.12 driver, chosen by
`spark.pyspark.python` from the *driver* side; and `/data` mounted at the same
path on the workers. Break any one and it silently degrades to pyarrow.

**The `beeline` on PATH is pyspark's**, calls `spark-class`, and cannot reach
HiveServer2. Hive registration talks JDBC through jaydebeapi with the jar set
copied from `apache/hive:4.0.0`.

**Schema manifests are hidden files** (`.<name>.schema.json`). Hive reads every
file in a partition directory and chokes parsing JSON as Parquet. The leading
dot is load-bearing.

**Frozen-archive datasets.** `BQ_DATA_START`/`BQ_DATA_END` bound the window a
logical date maps into, and they are a property of the *source table*. Point
`BQ_DATASET` elsewhere without moving them and the window lands on an empty day
— the extract then fails closed, which is correct.

---

## Before you open a PR

```bash
docker run --rm --entrypoint python -v "$PWD:/w" -w /w \
  -e PIPELINE_DATA_ROOT=/w/data -e PYTHONPATH=/w \
  avapuck-airflow -m pytest tests/ -q
```

63 tests must pass. If you added a stub or a shortcut, add it to
`TRUST-BOUNDARY.md` **in the same change**. That file is maintained as work
happens, not reconstructed at the end, and it is what makes this repository
safe to hand to someone else.

---

## How to describe this system honestly

**True:** AES-256-GCM with the key held in a separate service. Masked under
Ranger policy JSON. JWT verified by signature, issuer, audience and expiry.
TLS enabled with certificate verification enforced. Encryption runs on a real
Spark cluster. Hive tables are real and queryable.

**Also true, and must be said alongside it:** the TLS CA is self-signed and its
private key sits beside the certs it signs. The Ranger *admin service* is not
deployed — policy JSON is real, the engine applying it is local unless you
deployed Ranger. Card splitting leaves only ~6 digits encrypted.

**Do not say:** "production-ready", "PCI compliant", "audited". No compliance
review has happened. See `TRUST-BOUNDARY.md`.
