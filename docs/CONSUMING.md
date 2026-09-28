# Consuming the platform

For a team that wants to react when the pipeline finishes a run. The whole
interface is: one Kafka topic, one message shape, one boolean, one library.

## The contract

Every message on every topic is:

```json
{
  "envelope": {
    "schema_version": "1",
    "event_type": "run.verdict",
    "event_id":        "…32 hex…",   // unique per emission -> DROP a repeat
    "idempotency_key": "…32 hex…",   // per unit of work    -> UPSERT on it
    "dataset": "trips",
    "logical_date": "2026-09-06",
    "run_slot": "2026-09-06T04:00:00Z",
    "trace_id": "trips-20260906T0400-b7edae",
    "run_id": "scheduled__2026-09-06T04:00:00+00:00",
    "try_number": 1,
    "key": null,
    "producer": "pipeline",
    "produced_at": "2026-09-06T04:09:12Z"
  },
  "payload": {
    "proceed": true,                     // the ONLY field you react to
    "status": "degraded",                // complete | degraded | failed -- for people
    "reasons": ["hive table not registered: HiveServer2 unreachable"],
    "stages": { "...per stage: ok, reported, detail; error when the stage raised..." },
    "outputs": {
      "warehouse_table": "warehouse.trips",
      "hive_table": null,
      "pdf": "/data/reports/trips_report_20260906T0400Z.pdf",
      "rows_published": 100,
      "rows_loaded": 100,
      "topics": {"encrypted": "rpos_encrypted", "flat": "rpos_flat"}
    },
    "data_window": {"start": "2025-03-02T00:00:00", "end": "2025-03-03T00:00:00",
                    "source": "archive_mapping"},
    "computed_at": "2026-09-06T04:09:12Z"
  },
  "signature": "eyJhbGciOiJSUzI1NiIs…"   // RS256 JWS from the IdP over sha256(payload)
}
```

Schemas: `contracts/run_verdict.schema.json`, `contracts/envelope.schema.json`.

## The rules, and where they are enforced

1. **React to `proceed` and nothing else.** The pipeline owns the predicate
   (`pipeline/verdict.py`). If you find yourself checking `status` or a
   reason to decide whether to run, that is predicate drift: ask for the
   verdict rule to change instead.
2. **Verify the signature.** `pipeline.messaging.signing.VerdictVerifier`
   checks signature, issuer, audience, subject (`orchestrator-job`) and that
   the payload digest matches. An altered payload fails.
3. **Dedupe through the library.** `pipeline.messaging.dedupe.accept(store,
   consumer_name, message)` returns `process` and `replay`. Drop when
   `process` is false. When `replay` is true, the same slot was re-run:
   upsert your result on `idempotency_key`, do not add to it.
4. **Parse through the library.** `pipeline.messaging.envelope.parse(raw)`
   validates against the contract and refuses anything else.

## The reference consumer

`dags/verdict_listener_dag.py` is the smallest complete example:

```python
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import DAG, Asset, AssetWatcher, task

verdicts = Asset("pipeline_run_verdicts", watchers=[AssetWatcher(
    name="control_topic",
    trigger=MessageQueueTrigger(
        scheme="kafka", topics=["pipeline_runs"], kafka_config_id="kafka_control",
        apply_function="pipeline.messaging.listen.accept_verdict"))])

with DAG(dag_id="on_run_verdict", schedule=[verdicts], ...) as dag:
    @task
    def record(**ctx):
        for ev in ctx["triggering_asset_events"].values():   # the message is in extra["payload"]
            ...
```

`accept_verdict` runs in the triggerer and wakes the DAG only for a
contract-valid `run.verdict` with `proceed=true`. The task then verifies,
dedupes and records. What it records is in `consumer.received_verdicts`.

Outside Airflow the same three calls apply to any Kafka client:

```python
from pipeline.messaging import parse, accept
from pipeline.messaging.dedupe import PostgresSeenStore
from pipeline.messaging.signing import verifier_from_env

msg = parse(record.value)                      # contract
verifier_from_env().verify(msg)                # signature
if accept(store, "my-team", msg).process:      # dedupe
    ...
```

## What you may read

| Principal | Topics | Note |
|---|---|---|
| `consumer` | `rpos_flat`, `pipeline_runs` | the reference consumer's identity |
| `ui` | `rpos_flat`, `pipeline_runs`, describe everything | Kafbat UI, read-only |
| `watchdog` | `pipeline_runs` | absence detection |
| nobody by default | `rpos_encrypted` | ciphertext; ask for a principal with decrypt rights and a reason |

To find the rows for a run: `rpos_flat` messages whose `envelope.run_slot`
matches the verdict's, or `warehouse.<dataset> WHERE run_slot = …`, or the
tap's table, `monitor.kafka_log`, which holds every flat and control message
as a row and is what the console's Kafka log tab shows.

Every row of every run is published, uploads included, so a large upload is
millions of messages. Read by `run_slot`, not from the beginning.

## Changing the contract

Contracts change downward, never sideways. A payload schema may add fields to
its own payload; nobody may add, remove or retype an envelope field without
bumping `schema_version` and updating every payload schema's example. The CI
platform tier (`make ci-platform`) prints the list of schemas that break;
that list is the blast radius, shown before merge. `contracts/README.md`.
