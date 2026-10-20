# AvaPuck — Privacy-First Data Platform

## Recruiter-friendly overview
AvaPuck is a complete data-processing platform designed to show how sensitive information can move safely through a company. It accepts data from files or public datasets, protects private fields, sends information between services, loads results into a warehouse, creates a report, and produces a signed decision for downstream teams. The project highlights data engineering, privacy, security, workflow automation, messaging, and operational safeguards.

## Project description
A privacy-focused, containerized data pipeline using Airflow, Spark, Kafka, encryption, policy-based masking, Hive, PostgreSQL, PDF reporting, and signed run verdicts.

---

# avapuck

A privacy-first data platform, small enough to run on a laptop: pull 100 rows
per run from a BigQuery public dataset, or upload any CSV of any size through
the console, encrypt sensitive fields with AES-256-GCM through an HTTPS
service, publish to Kafka, mask under Apache Ranger policy at the Hive
boundary, load a Postgres warehouse, emit a PDF, and then publish one
**signed verdict** that tells downstream teams whether to proceed.

```
BigQuery/CSV ─▶ card split ─▶ encrypt ─▶ publish ─▶ decrypt+mask ─▶ Hive ─▶ Postgres ─▶ PDF ─▶ verdict
   or upload     6·enc·4    AES-256-GCM  every row   Ranger policy               per run     signed,
   (uncapped)               on executors (envelope)                             slot, UTC   always
```

One Airflow DAG, `trips_pipeline`, eight stages plus a terminal `emit_verdict`
task that runs even when a stage failed. Every four hours, or whenever the
console starts a run. Each stage consumes what the one before it wrote and
streams rows in chunks, so a hundred-row scheduled run and a two-gigabyte
upload take the same path.

**Read `docs/DECISIONS.md` for design rationale and `TRUST-BOUNDARY.md` for known limits.**
