# Architecture

```
 ┌──────────────────────┐
 │ BigQuery public data │  100 rows/run, named columns, maximum_bytes_billed
 │  or any CSV          │  enforced. Held to the dataset's CONTRACT.
 └──────────┬───────────┘
            │  JWT (extract-job)
            ▼
      data/csv/<dataset>_<date>.csv              ← plaintext, local only, transient
            │
            ▼  card split: first-6 · encrypted middle · last-4 (manifest-driven)
            ▼  ┌─────────────────────────┐
   encrypt ───►│ HTTPS crypto service    │   AES-256-GCM, per-value nonce,
            │  │  keys never leave here  │   field name bound as AAD
            │  └─────────────────────────┘
            ▼
      data/parquet/dt=…/<dataset>.parquet         ← CIPHERTEXT ONLY. Asserted before write.
            │
            ├──► Kafka  rpos_encrypted (row.encrypted)   ciphertext + blind indexes
            │           rpos_flat      (row.flat)        never-sensitive columns
            │           every message in the platform ENVELOPE
            ▼  ┌─────────────────────────┐
   mask ──────►│ crypto service /decrypt │  (hive-job; cannot encrypt)
            │  └─────────────────────────┘
            │  ┌─────────────────────────┐
            └─►│ Apache Ranger policy    │   MASK_SHOW_LAST_4, MASK_HASH, …
               └─────────────────────────┘
            ▼
      data/hive/<dataset>/dt=…/<dataset>_masked.parquet   ← MASKED at rest
            │
            ├──► Hive external table (non-fatal if Hive is down; reported)
            ▼
      Postgres UPSERT on the manifest key, stamped with run_slot (idempotent)
            │
            ▼
      pipeline-reports/<dataset>_report_<slot>Z.pdf     ← one per run, UTC, masked data only
            │
            ▼
      Kafka  pipeline_runs (run.verdict)   ← SIGNED. proceed: true|false. ALWAYS emitted.
            │
            ├──► dags/verdict_listener_dag.py   asset-scheduled reference consumer
            └──► services/watchdog             unhealthy when a verdict is late or negative
```

## Why the data flows this way

**Encrypt before landing, mask before the warehouse.** Parquet holds only
ciphertext, so a stolen file yields nothing. The warehouse holds only masked
values, so a stolen dump yields nothing either. Plaintext exists in exactly
two places: the landed CSV (local, transient) and inside the mask task's
memory between decrypt and mask.

**The crypto service is a service, not a library.** In-process encryption puts
the key in Spark executor memory, in driver stack traces, in heap dumps, and in
whatever the JVM swaps to disk. Behind HTTPS the key lives in one process and
every use is authenticated and auditable.

**Masking is Ranger policy, never code.** `config/ranger/*.json` is the single
place a masking rule is expressed. The pipeline reads and applies it; it
decides nothing. A dataset with no policy gets one generated for review, once.

**Blind indexes keep encrypted columns joinable.** A keyed HMAC alongside each
encrypted value gives Hive a deterministic handle so equality still works
without decryption. The tradeoff (determinism leaks equality) is in
`TRUST-BOUNDARY.md` 2.6.

**The manifest is inferred once and committed as a contract.** Every stage
reads the manifest the extract wrote, so all agree on the schema by
construction. The first run of a dataset writes that schema into
`config/datasets/<dataset>.json`; a later run that disagrees fails naming the
difference (`docs/DECISIONS.md` D10). Drift is gated, not absorbed.

## The platform layer

**One envelope, two identifiers.** Every Kafka message is
`{envelope, payload, signature?}`. `idempotency_key` is deterministic per unit
of work (event type, dataset, run slot, row key) and `event_id` per emission
(plus run id and try). A consumer drops a repeated `event_id` and upserts on
the `idempotency_key`. Non-negotiable 5, extended to Kafka. D2.

**The verdict is a pure function, always emitted, signed.** `pipeline/verdict.py`
turns the eight stage events into `proceed`, `status`, `reasons`, `outputs`
and the data window. `emit_verdict` runs with `trigger_rule=all_done`, so a
failed run says `proceed=false` and a *missing* verdict means only "the DAG did
not run". The IdP signs a digest of the payload; consumers verify against the
JWKS they already trust. D4, D5, D6.

**Downstream is asset-scheduled on the control topic.** The reference consumer
DAG's asset has a watcher backed by the Kafka `MessageQueueTrigger`; the
trigger's filter accepts only a contract-valid verdict with `proceed=true`.
The asset cascade the single-DAG consolidation gave up returns at the team
boundary. D7.

**Absence is watched from outside.** `services/watchdog` consumes the control
topic and turns unhealthy when a verdict is late or negative. A dead scheduler
cannot report itself. D8.

**Contracts are machine-checked.** `contracts/` holds JSON Schema for the
envelope, each payload, and the stage event. Producers validate before
sending; consumers validate on parse; CI validates the examples and diffs each
schema against `main`. D10, D11.

## Why one DAG rather than six

Each stage used to be its own asset-scheduled DAG, so a manual repair of one
cascaded downstream on its own. That was traded for a single graph that shows
the whole run, triggers once, and backfills once. The cost is real: re-running
`mask` by hand no longer makes `load` follow; clear the downstream tasks too.

## Idempotency

Every stage keys off the run slot, writes to a date-partitioned path, and the
Postgres load upserts on the manifest's primary key with the slot stamped on
every row. Re-running a slot replaces its data: a second load reports
`0 inserted / 100 updated`. On Kafka, a re-run publishes the same
`idempotency_key` with a new `event_id`.

## Identity

Every job gets a short-lived RS256 JWT from the IdP and every service verifies
it by **signature, issuer, audience, expiry and not-before**, never by decoding.
Audience grants stop the extract job's token being replayed against the
warehouse loader, and stop the orchestrator's signing token being used for
encryption. Asymmetric rather than a shared HMAC secret: with HS256 anything
that can verify a token can also mint one.
