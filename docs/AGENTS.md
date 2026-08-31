# Working agreement — for AI agents and new developers

You are working on a data pipeline that handles **encrypted personal data**.
Most of the design decisions here exist to protect that data, and several of
them look like extra work until you know why. This file is the why.

Read `TRUST-BOUNDARY.md` next. It is the difference between describing this
system accurately and overselling it.

---

## The five invariants

Breaking any one is a defect, not a trade-off. Each has a test and a runtime
guard, so you will usually be stopped rather than merely wrong.

### 1. Plaintext must never land

Sensitive values may exist in exactly two places: `data/csv/` (the extract
output, local and transient) and inside the mask task's memory between decrypt
and mask. Everywhere else is ciphertext or masked.

`pipeline/transform/spark_encrypt.py :: assert_no_plaintext` refuses to write
Parquet otherwise. **Do not weaken it to make a test pass.** It fires on three
distinct checks — structural, decoded-substring, and identity — because a single
naive check is either too loose or noisy enough to be ignored.

### 2. Tokens are verified, never decoded

`pipeline/common/auth.py` pins the algorithm to RS256 and requires signature,
issuer, audience, expiry and not-before. `jwt.decode(..., verify_signature=False)`
is the most common auth bug in pipelines like this, and it is *worse* than no
auth because it looks like auth in review.

Each stage has its own identity with its own scope. `spark-job` can encrypt and
**cannot decrypt**; `hive-job` is the reverse. That separation is enforced by
the IdP, not by convention.

### 3. Masking lives in Ranger policy

`config/ranger/hive_masking_policies.json` is real Apache Ranger policy JSON.
Adding a sensitive column means adding a policy — the pipeline **refuses to
write** a sensitive column with no policy covering it.

Never express a masking rule in Python. If you find yourself writing
`if column == "bike_id": value = mask(value)`, stop: that is the rule leaving
the place the business can audit it.

### 4. Cost is capped, and the cap is measured

BigQuery bills **bytes scanned, not rows returned**. `LIMIT 100` reduces the
bill by exactly zero. What controls cost: naming columns (never `SELECT *`) and
`maximum_bytes_billed`, which makes BigQuery *refuse* an over-budget query
rather than run it.

Before changing `BQ_MAX_BYTES_BILLED`, measure with `make -f Makefile.k8s cost`.
A cap below the achievable cost is not a safety control, it is a permanent
outage — we shipped that bug once already.

### 5. Re-running replaces, never duplicates

Every stage keys off the run's logical timestamp; the warehouse upserts on
`trip_id`. A re-run of the 08:00 slice reads the same window and replaces the
same rows. Verify with: re-run a slice and expect `0 inserted / N updated`.

---

## Things that will surprise you

**The dataset is a frozen archive.** `austin_bikeshare` ends 2024-06-30. A
pipeline reading "yesterday" returns zero rows forever while still being billed
for the scan. `window_for()` maps each logical timestamp onto a real archive
window. **The date in a report is the logical date, not the date of the data.**

**The table is not partitioned.** A date predicate prunes nothing. Column
selection is the only cost lever, and every run is a full scan of the selected
columns (~232 MiB).

**`masked_by` is on every row on purpose.** It records whether real Ranger or
the local policy engine did the masking. Those are different claims; keep them
distinguishable.

**Spark has two submission paths.** In Kubernetes the job connects to the
in-cluster master directly. On a laptop it shells out to `docker exec`, because
the host JDK is often too new for Spark 4 (which needs Java 17/21). The engine
that actually ran is recorded in the stage result — never assume.

---

## Before you open a PR

```bash
.venv/bin/python -m pytest tests/ -q      # 27 invariant tests must pass
make -f Makefile.k8s verify               # in-cluster security checks
```

If you added a stub or a shortcut, add it to `TRUST-BOUNDARY.md` **in the same
change**. That file is maintained as work happens, not reconstructed at the end,
and it is the thing that makes this repository safe to hand to someone else.

## How to describe this system honestly

Say "AES-256-GCM encryption with the key held in a separate service" — true.
Say "masked under Ranger policy" — true, with the caveat that the policy engine
is local unless you deployed Ranger admin. Do **not** say "production-ready",
"PCI compliant", or "TLS-secured": TLS is not enabled, and no compliance review
has happened. See `TRUST-BOUNDARY.md` §2.
