# Architecture

```
 ┌──────────────────────┐
 │ BigQuery public data │  100 rows/day, named columns, partition-pruned,
 │  austin_bikeshare    │  maximum_bytes_billed enforced by BigQuery
 └──────────┬───────────┘
            │  JWT (extract-job)
            ▼
      data/csv/trips_YYYY-MM-DD.csv          ← plaintext, local only, transient
            │
            ▼  ┌─────────────────────────┐
   Spark job ─►│ HTTPS crypto service    │   AES-256-GCM, per-value nonce,
            │  │  keys never leave here  │   field name bound as AAD
            │  └─────────────────────────┘
            ▼
      data/parquet/dt=…/trips.parquet        ← CIPHERTEXT ONLY. Asserted
            │                                  before write.
            ▼  ┌─────────────────────────┐
   Hive job ──►│ crypto service /decrypt │
            │  └─────────────────────────┘
            │  ┌─────────────────────────┐
            └─►│ Apache Ranger policy    │   MASK_SHOW_LAST_4, MASK_HASH,
               └─────────────────────────┘   MASK_SHOW_FIRST_8, MASK_NULL
            ▼
      data/hive/trips/dt=…/trips_masked.parquet   ← MASKED values at rest
            │
            ▼  Postgres UPSERT on trip_id (idempotent)
      warehouse.trips + warehouse.load_audit
            │
            ▼
      data/reports/trips_report_YYYY-MM-DD.pdf    ← charts from masked data
```

## Why the data flows this way

**Encrypt before landing, mask before the warehouse.** Parquet holds only
ciphertext, so a stolen file yields nothing. The warehouse holds only masked
values, so a stolen database dump yields nothing either. Plaintext exists in
exactly two places: the CSV (local, transient) and inside the Hive task's
memory between decrypt and mask.

**The crypto service is a service, not a library.** In-process encryption puts
the key in Spark executor memory, in driver stack traces, in heap dumps, and in
whatever the JVM swaps to disk. Behind HTTPS the key lives in one process and
every use is authenticated and auditable.

**Masking is Ranger policy, never code.** `config/ranger/hive_masking_policies.json`
is the single place a masking rule is expressed. The pipeline reads and applies
it; it decides nothing. Changing what analysts see is a policy edit, not a
deployment.

**Blind indexes keep encrypted columns joinable.** Encryption makes a column
opaque — no joins, no GROUP BY. A keyed HMAC alongside each encrypted value
gives Hive a deterministic handle so equality still works without decryption.
The tradeoff (determinism leaks equality) is stated in TRUST-BOUNDARY.md §2.6.

## Why five DAGs instead of one

Each stage is independently re-runnable. If the Hive mask fails at 3am you
re-run that stage alone rather than re-querying BigQuery and paying again. A
single DAG is simpler to read and materially worse to operate.

## Idempotency

Every stage keys off the Airflow logical date, writes to a date-partitioned
path, and the Postgres load upserts on the `trip_id` primary key. Re-running
any date replaces that date's data. Verified: a second load of the same date
reports 0 inserted / 100 updated, and the row count does not change.

## Identity

Every job gets a short-lived RS256 JWT from the IdP and every service verifies
it by **signature, issuer, audience, expiry and not-before** — never by
decoding. The algorithm is pinned to RS256, so `alg:none` and HMAC-confusion
attacks fail. Audience grants stop the extract job's token being replayed
against the warehouse loader.

Asymmetric rather than a shared HMAC secret: with HS256 anything that can
verify a token can also mint one, so a compromised consumer becomes an
identity forger.
