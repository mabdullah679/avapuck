# BigQuery → Spark → Hive → Postgres → PDF

Daily pipeline: pull 100 rows/day from a BigQuery public dataset, encrypt
sensitive fields with AES-256 via an HTTPS service, land Parquet, mask in Hive
under Apache Ranger policy, load Postgres, emit a PDF report.

The source may also be an arbitrary CSV: `pipeline/metadata/schema.py` infers
the dataset's columns, sensitivity and key, and every stage reads that manifest
instead of naming columns. Trips is one dataset through that path, not a
special case. See `TRUST-BOUNDARY.md` §7 for what inference can get wrong.

Read `docs/ARCHITECTURE.md` for the design and `TRUST-BOUNDARY.md` for what is
stubbed or unverified. `docs/RUNBOOK.md` is the operator guide.

Payment-card columns get an extra stage: `pipeline/transform/card_split.py`
splits a PAN into first-6 / encrypted-middle / last-4 before the encrypt
stage. Which columns are split comes from the manifest's `card_split`
treatment marker, never a column name. See `TRUST-BOUNDARY.md` §2.8 for the
limits of that scheme.

## Non-negotiables

1. **Sensitive data is encrypted before it lands anywhere.** Plaintext PII must
   never reach Parquet, Hive, Postgres, logs, or the PDF. The only plaintext
   window is inside the HTTPS crypto service. Which columns are sensitive comes
   from the schema manifest; `assert_no_plaintext` enforces it before every
   write, whatever the dataset.
2. **Every job authenticates with a JWT from the IdP.** No job trusts another
   job's word about identity. Tokens are short-lived and verified by signature,
   issuer, audience, and expiry — never merely decoded.
3. **Masking policy is Apache Ranger policy JSON**, authored once and enforced
   at the Hive boundary. Masking rules never live in transformation code. A
   dataset with no policy gets one GENERATED into `config/ranger/` for review,
   written once and never regenerated over an edit — generation is authoring,
   not a runtime decision.
4. **100 rows per run, hard-capped**, enforced in the SQL and asserted after.
   BigQuery bills by bytes scanned; an unbounded query is a billing incident.
   The CSV source path enforces the same cap on the way in.
5. **Idempotent by execution date.** A re-run of the same logical date replaces
   that date's partition and never double-loads Postgres. Datasets with no
   natural key are keyed on a deterministic hash of the row plus the date, so
   this holds without the source supplying a key.

## Definition of done

`airflow dags test` runs the chain end to end, the PDF is produced, and
`TRUST-BOUNDARY.md` honestly states what is stubbed.
