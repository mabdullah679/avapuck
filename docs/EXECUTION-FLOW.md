# Execution flow — one run, end to end, traceable

Every 4 hours the pipeline produces one PDF and one signed verdict from 100
rows. This is the path a single run takes, and how to follow it after the fact.

## The correlation id

Each run has one id, derived deterministically from its run slot:

```
trips-20260906T0400-b7edae
└─┬─┘ └──────┬─────┘ └──┬─┘
pipeline    4h slot   digest
```

**Deterministic, not random.** A re-run of the same slot produces the *same*
id, so a repair is visibly the same logical unit of work. It appears in every
stage event, every Kafka envelope, every warehouse row for the run, and on the
PDF. One `grep` reconstructs the path:

```bash
grep -r "trips-20260906T0400-b7edae" airflow_home/logs
```

## The path

```
     ┌───────────────────────────────────────────────────────────────────┐
     │ 00:00 / 04:00 / 08:00 / 12:00 / 16:00 / 20:00 UTC  (0 */4 * * *)   │
     └────────────────────────────┬──────────────────────────────────────┘
                                  ▼
  ╔═══ extract_to_csv ══════════════════════════════════════════════════╗
  ║  identity : extract-job                                              ║
  ║  reads    : BigQuery (3 attempts, then CSV fallback), or a CSV       ║
  ║  guards   : dry run · maximum_bytes_billed · 0 rows = FAIL           ║
  ║  contract : config/datasets/<dataset>.json -- written on first sight,║
  ║             FAILS on drift afterwards                                 ║
  ║  writes   : data/csv/<dataset>_<date>.csv        ← PLAINTEXT, local  ║
  ║  event    : rows, mode, bq_attempts, fell_back_to_csv, window_*      ║
  ╚══════════════════════════════════╤═══════════════════════════════════╝
                                     ▼
  ╔═══ split_card_numbers ═══════════════════════════════════════════════╗
  ║  identity : spark-job  (scope crypto.encrypt -- CANNOT decrypt)      ║
  ║  driven by: manifest treatment=card_split -- NEVER a column name     ║
  ║  splits   : PAN → first-6 · encrypted middle · last-4                ║
  ║  writes   : data/cards/<dataset>_card_info_<date>.csv                ║
  ║  event    : rows, cards (0 is normal), dataset                       ║
  ╚══════════════════════════════════╤═══════════════════════════════════╝
                                     ▼
  ╔═══ encrypt_to_parquet ═══════════════════════════════════════════════╗
  ║  identity : spark-job                                                ║
  ║  runs on  : Spark 4.0.0 cluster (2 workers), pyarrow if unreachable  ║
  ║  crypto   : AES-256-GCM, random nonce per value, field as AAD        ║
  ║  guard    : assert_no_plaintext refuses the write                    ║
  ║  writes   : data/parquet/dt=<date>/<dataset>.parquet ← CIPHERTEXT   ║
  ║  event    : rows, engine, spark_configured, encrypted_columns        ║
  ╚══════════════════════════════════╤═══════════════════════════════════╝
                                     ▼
  ╔═══ publish_kafka ════════════════════════════════════════════════════╗
  ║  principal: pipeline  (WRITE only; cannot read any topic)            ║
  ║  sends    : row.encrypted → rpos_encrypted · row.flat → rpos_flat    ║
  ║  envelope : event_id · idempotency_key · run_slot · trace_id · key   ║
  ║  guards   : contract validated before send; sensitive column routed  ║
  ║             to the flat topic refuses the send                        ║
  ║  producer : acks=all, idempotent                                     ║
  ║  event    : rows, topic_encrypted, topic_public                      ║
  ╚══════════════════════════════════╤═══════════════════════════════════╝
                                     ▼
  ╔═══ decrypt_and_mask ═════════════════════════════════════════════════╗
  ║  identity : hive-job  (scope crypto.decrypt -- CANNOT encrypt)       ║
  ║  policy   : config/ranger/<dataset>_masking_policies.json            ║
  ║  guards   : every sensitive column must have a policy;               ║
  ║             masked output must differ from its plaintext             ║
  ║  writes   : data/hive/<dataset>/dt=<date>/<dataset>_masked.parquet   ║
  ║  event    : rows, masked_by, policies                                ║
  ╚══════════════════════════════════╤═══════════════════════════════════╝
                                     ▼
  ╔═══ register_hive_table ══════════════════════════════════════════════╗
  ║  JDBC to HiveServer2. Non-fatal when down: the data is already right.║
  ║  event    : registered (true|false), table, reason                   ║
  ╚══════════════════════════════════╤═══════════════════════════════════╝
                                     ▼
  ╔═══ load_postgres ════════════════════════════════════════════════════╗
  ║  UPSERT on the manifest key; every row stamped run_slot + trace_id   ║
  ║  audit    : warehouse.load_audit -- one row per run                  ║
  ║  event    : inserted, updated, run_id, dataset                       ║
  ╚══════════════════════════════════╤═══════════════════════════════════╝
                                     ▼
  ╔═══ build_pdf ════════════════════════════════════════════════════════╗
  ║  reads    : warehouse.<dataset> WHERE run_slot = this run (masked)   ║
  ║  prints   : run slot Z · generated Z · data window + its source ·    ║
  ║             the verdict so far (status + reasons) · trace id         ║
  ║  writes   : data/reports/<dataset>_report_<slot>Z.pdf → pipeline-reports/║
  ║  event    : rows, pdf, collected                                     ║
  ╚══════════════════════════════════╤═══════════════════════════════════╝
                                     ▼   trigger_rule = all_done
  ╔═══ emit_verdict ═════════════════════════════════════════════════════╗
  ║  pulls every stage event from XCom; validates each against           ║
  ║  contracts/stage_event.schema.json; pipeline/verdict.py decides      ║
  ║  identity : orchestrator-job → IdP /sign → RS256 JWS over the digest ║
  ║  sends    : run.verdict → pipeline_runs, keyed by dataset            ║
  ║  ALWAYS. A failed run publishes proceed=false.                       ║
  ╚══════════════════════════════════╤═══════════════════════════════════╝
                                     ▼
            ┌────────────────────────┴─────────────────────────┐
            ▼                                                  ▼
  on_run_verdict (dags/verdict_listener_dag.py)      services/watchdog
  asset watcher → accept_verdict filter → verify     consumes every verdict,
  signature → dedupe → record                        503 when late or negative
```

## Where the plaintext is

Two places, both deliberate and both brief:

1. **`data/csv/`**: the extract output. Local, transient, never leaves the host.
2. **Inside the mask task's memory**, between the decrypt response and the
   mask call. Microseconds, never written.

Everywhere else is ciphertext (Parquet, `rpos_encrypted`) or masked values
(Hive, Postgres, PDF). `rpos_flat` carries only columns that were never
sensitive.

## Identity at each hop

| Stage | Identity | Scope | Cannot |
|---|---|---|---|
| extract | `extract-job` | `bigquery.read` | encrypt or decrypt |
| card split, encrypt | `spark-job` | `crypto.encrypt` | **decrypt** |
| mask | `hive-job` | `crypto.decrypt` | encrypt |
| load | `postgres-job` | `warehouse.write` | reach the crypto service |
| report | `report-job` | `warehouse.read` | write |
| verdict | `orchestrator-job` | `verdict.sign` | encrypt, decrypt, or reach the warehouse |
| Kafka publish | `pipeline` (SASL) | write 3 topics | read any topic |
| Kafka consume | `consumer` (SASL) | read flat + control | read ciphertext |

Enforced by the IdP's audience grants and the broker's ACLs, not by convention.

## Answering "where did this number come from?"

Given a figure in a PDF: the header carries the run slot and the trace id.

```bash
TRACE=trips-20260906T0400-b7edae
grep -r "$TRACE" airflow_home/logs | grep TRACE      # every stage boundary
```

That yields, in order: the BigQuery job id and the data window; the engine
that encrypted; the policy engine and count that masked; the Postgres
`run_id` and insert/update split; the PDF path; the verdict's event id.

The warehouse and the control topic keep their own records:

```sql
SELECT * FROM warehouse.load_audit WHERE trace_id = 'trips-20260906T0400-b7edae';
SELECT * FROM consumer.received_verdicts WHERE trace_id = 'trips-20260906T0400-b7edae';
```

## Failure semantics

A failed task stops the chain: nothing downstream processes stale data. Then
`emit_verdict` runs anyway and publishes `proceed=false` with the missing
stages named. `dagrun_timeout` bounds a stuck run at three hours. Every task
failure logs `ALERT kind=task_failed …`.

So three signals are distinct: **failed** (red task, negative verdict),
**stuck** (timeout, then negative verdict), **absent** (no verdict at all,
which only the watchdog can see).
