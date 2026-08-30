# BigQuery → Spark → Hive → Postgres → PDF

Daily pipeline: pull 100 rows/day from a BigQuery public dataset, encrypt
sensitive fields with AES-256 via an HTTPS service, land Parquet, mask in Hive
under Apache Ranger policy, load Postgres, emit a PDF report.

Read `docs/ARCHITECTURE.md` for the design and `TRUST-BOUNDARY.md` for what is
stubbed or unverified. `docs/RUNBOOK.md` is the operator guide.

## Non-negotiables

1. **Sensitive data is encrypted before it lands anywhere.** Plaintext PII must
   never reach Parquet, Hive, Postgres, logs, or the PDF. The only plaintext
   window is inside the HTTPS crypto service.
2. **Every job authenticates with a JWT from the IdP.** No job trusts another
   job's word about identity. Tokens are short-lived and verified by signature,
   issuer, audience, and expiry — never merely decoded.
3. **Masking policy is Apache Ranger policy JSON**, authored once and enforced
   at the Hive boundary. Masking rules never live in transformation code.
4. **100 rows per run, hard-capped**, enforced in the SQL and asserted after.
   BigQuery bills by bytes scanned; an unbounded query is a billing incident.
5. **Idempotent by execution date.** A re-run of the same logical date replaces
   that date's partition and never double-loads Postgres.

## Definition of done

`airflow dags test` runs the chain end to end, the PDF is produced, and
`TRUST-BOUNDARY.md` honestly states what is stubbed.
