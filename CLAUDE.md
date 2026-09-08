# BigQuery → Spark → Kafka → Hive → Postgres → PDF → signed verdict

Four-hourly pipeline: pull 100 rows/run from a BigQuery public dataset, or
take an uncapped CSV upload from the console (`services/console/`), encrypt
sensitive fields with AES-256-GCM via an HTTPS service, publish to Kafka in
the platform envelope, land Parquet, mask in Hive under Apache Ranger policy,
load Postgres, emit one PDF per run, and publish one signed `run.verdict`
that downstream teams react to. Around it: a Kafka tap into Postgres, a
watchdog for absent verdicts, a Spark worker scaler (the one service with
the Docker socket), and a traffic audit that detects and blocks.

`pipeline/metadata/schema.py` infers each dataset's columns, sensitivity and
key; `pipeline/metadata/contract.py` holds later runs to that inference as a
committed contract. Every stage reads the manifest instead of naming columns.
Trips is one dataset through that path, not a special case.

Read `docs/DECISIONS.md` for why the platform layer is shaped as it is,
`docs/ARCHITECTURE.md` for the data flow, and `TRUST-BOUNDARY.md` for what is
stubbed or unverified. `docs/RUNBOOK.md` is the operator guide;
`docs/CONSUMING.md` is for downstream teams. `AGENTS.md` is the working
agreement for anyone changing code.

The stage list lives once, in `pipeline/stages.py`; the DAG and
`pipeline/run_pipeline.py` both walk it. Payment-card columns get a split
stage driven by the manifest's `card_split` treatment marker, never a column
name. See `TRUST-BOUNDARY.md` §2.8 for the limits of that scheme.

## Non-negotiables

1. **Sensitive data is encrypted before it lands anywhere.** Plaintext PII must
   never reach Parquet, Kafka, Hive, Postgres, logs, or the PDF. The only
   plaintext window is inside the HTTPS crypto service. Which columns are
   sensitive comes from the schema manifest; `assert_no_plaintext` enforces it
   before every write, and `_assert_no_plaintext_published` before every send.
2. **Every job authenticates with a JWT from the IdP.** No job trusts another
   job's word about identity. Tokens are short-lived and verified by signature,
   issuer, audience, and expiry — never merely decoded. The run verdict is
   signed by the IdP and verified the same way.
3. **Masking policy is Apache Ranger policy JSON**, authored once and enforced
   at the Hive boundary. Masking rules never live in transformation code. A
   dataset with no policy gets one GENERATED into `config/ranger/` for review,
   written once and never regenerated over an edit. The same pattern governs
   the dataset contract in `config/datasets/`.
4. **100 rows per BigQuery run, hard-capped**, enforced in the SQL and
   asserted after, because that is a bill. Uploads through the console are
   bounded only by `UPLOAD_MAX_ROWS` (0 = none), and every stage streams in
   chunks of `PIPELINE_BATCH_ROWS` so a large upload and a 100-row run take
   the same path. `docs/DECISIONS.md` D16.
5. **Idempotent by run slot.** A re-run of the same slot replaces that slot's
   partition and never double-loads Postgres. On Kafka the envelope carries an
   `idempotency_key` per unit of work and an `event_id` per emission; a
   consumer upserts on the first and drops a repeat of the second.
6. **The verdict is always emitted and is the only predicate.** `emit_verdict`
   runs with `trigger_rule=all_done`; `pipeline/verdict.py` decides; consumers
   read `proceed` and nothing else.
7. **One place for every value.** `.env.local` is the single source of
   configuration and secrets. No `${VAR:-default}` in compose.

## Definition of done

`airflow dags test` runs the chain end to end, the PDF is produced, the
verdict lands on `pipeline_runs` and the watchdog reports healthy, `make ci`
passes, and `TRUST-BOUNDARY.md` honestly states what is stubbed.
