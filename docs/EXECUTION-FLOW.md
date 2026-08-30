# Execution flow — one run, end to end, traceable

Every 4 hours the pipeline produces one PDF from 100 BigQuery rows. This is
the path a single run takes, and how to follow it after the fact.

## The correlation id

Each run has one id, derived deterministically from its logical timestamp:

```
trips-20260830T1600-b7edae
└─┬─┘ └──────┬─────┘ └──┬─┘
pipeline    4h slot   digest
```

**Deterministic, not random.** A re-run of the same slice produces the *same*
id, so a repair is visibly the same logical unit of work rather than a new
one. And any stage can compute it from its own context without being told —
which matters, because under asset scheduling the stages never talk to each
other.

One `grep` reconstructs the whole path:

```bash
grep "trips-20260830T1600-b7edae" airflow_home/logs -r
```

## The path

```
     ┌─────────────────────────────────────────────────────────────────┐
     │ 00:00 / 04:00 / 08:00 / 12:00 / 16:00 / 20:00   (cron 0 */4 * * *)│
     └────────────────────────────┬────────────────────────────────────┘
                                  ▼
  ╔═══ trips_01_extract ══════════════════════════════════════════════╗
  ║  identity : extract-job  (JWT, aud=crypto-service)                ║
  ║  reads    : BigQuery, ONE 4-hour archive window, 100 rows         ║
  ║  guards   : dry run first · maximum_bytes_billed · 0 rows = FAIL  ║
  ║  writes   : data/csv/trips_<date>.csv        ← PLAINTEXT, local   ║
  ║  emits    : trips_csv                                             ║
  ║  TRACE    : rows, archive_window, bytes_billed, bq_job_id         ║
  ╚═══════════════════════════════════╤═══════════════════════════════╝
                    asset trips_csv updated
                                      ▼
  ╔═══ trips_02_encrypt ══════════════════════════════════════════════╗
  ║  identity : spark-job  (scope crypto.encrypt — CANNOT decrypt)    ║
  ║  runs on  : Spark 4.0.0 cluster, 2 workers                        ║
  ║  calls    : crypto service /encrypt, batched per column           ║
  ║  crypto   : AES-256-GCM, random nonce per value, field as AAD     ║
  ║  guard    : refuses to write if any plaintext survives            ║
  ║  writes   : data/parquet/dt=<date>/trips.parquet ← CIPHERTEXT ONLY║
  ║  emits    : trips_parquet_encrypted                               ║
  ║  TRACE    : rows, engine, algorithm, identity                     ║
  ╚═══════════════════════════════════╤═══════════════════════════════╝
                asset trips_parquet_encrypted updated
                                      ▼
  ╔═══ trips_03_mask ═════════════════════════════════════════════════╗
  ║  identity : hive-job  (scope crypto.decrypt — CANNOT encrypt)     ║
  ║  calls    : crypto service /decrypt                               ║
  ║  policy   : config/ranger/hive_masking_policies.json              ║
  ║  guards   : every sensitive column must have a policy             ║
  ║             masked output must differ from its plaintext          ║
  ║  writes   : data/hive/.../trips_masked.parquet   ← MASKED at rest ║
  ║  then     : registers the Hive external table (non-fatal if down) ║
  ║  emits    : trips_hive_masked                                     ║
  ║  TRACE    : rows, masked_by, policies, identity                   ║
  ╚═══════════════════════════════════╤═══════════════════════════════╝
                  asset trips_hive_masked updated
                                      ▼
  ╔═══ trips_04_load ═════════════════════════════════════════════════╗
  ║  writes   : warehouse.trips  (UPSERT on trip_id → idempotent)     ║
  ║  audit    : warehouse.load_audit — one row per run                ║
  ║  emits    : trips_warehouse                                       ║
  ║  TRACE    : inserted, updated, run_id                             ║
  ╚═══════════════════════════════════╤═══════════════════════════════╝
                   asset trips_warehouse updated
                                      ▼
  ╔═══ trips_05_report ═══════════════════════════════════════════════╗
  ║  reads    : warehouse.trips (masked only — no privileged path)    ║
  ║  writes   : data/reports/trips_report_<date>.pdf                  ║
  ║  emits    : trips_report_pdf                                      ║
  ║  TRACE    : rows, pdf                                             ║
  ╚═══════════════════════════════════════════════════════════════════╝
```

## Where the plaintext is

Two places, both deliberate and both brief:

1. **`data/csv/`** — the extract output. Local, transient, never leaves the host.
2. **Inside the mask task's memory**, between the decrypt response and the mask
   call. Microseconds, never written.

Everywhere else is ciphertext (Parquet) or masked values (Hive, Postgres, PDF).

## Identity at each hop

No stage trusts another stage's word about who it is. Each obtains its own
short-lived RS256 JWT and each service verifies **signature + issuer +
audience + expiry + nbf** against the IdP's JWKS, with the algorithm pinned.

| Stage | Identity | Scope | Cannot |
|---|---|---|---|
| extract | `extract-job` | `bigquery.read` | encrypt or decrypt |
| encrypt | `spark-job` | `crypto.encrypt` | **decrypt** |
| mask | `hive-job` | `crypto.decrypt` | encrypt |
| load | `postgres-job` | `warehouse.write` | reach the crypto service at all |

That separation is enforced by the IdP: `postgres-job` asking for a
`crypto-service` audience is refused outright, so a leaked warehouse token
cannot be replayed against the crypto service.

## Answering "where did this number come from?"

Given a figure in a PDF:

```bash
TRACE=trips-20260830T1600-b7edae

grep -r "$TRACE" airflow_home/logs | grep TRACE     # every stage boundary
```

That yields, in order: the BigQuery job id and the archive window that produced
the rows; the crypto key version and engine that encrypted them; the Ranger
policy engine and policy count that masked them; the Postgres `run_id` and
insert/update split; the PDF path.

The warehouse keeps its own record independently:

```sql
SELECT * FROM warehouse.load_audit ORDER BY started_at DESC LIMIT 5;
SELECT DISTINCT masked_by FROM warehouse.trips;   -- which engine masked it
```

## Failure semantics

A stage that fails **emits no asset**, so nothing downstream runs — the chain
stops rather than processing stale or absent data. Fix the cause, clear the
failed run, and the asset fires on success, carrying the rest of the chain
with it. Nothing needs re-triggering by hand.
