# Contracts

Machine-checked shapes for everything that crosses a boundary in this
platform. `docs/DECISIONS.md` D10 is the reasoning; this file is the map.

| File | What it governs | Who reads it |
|---|---|---|
| `envelope.schema.json` | The wrapper on **every** Kafka message. The platform contract. | Every producer and consumer, via `pipeline/messaging` |
| `row_encrypted.schema.json` | Payload on the ciphertext topic | Consumers with decrypt rights |
| `row_flat.schema.json` | Payload on the flat topic | Analytics consumers |
| `run_verdict.schema.json` | Payload on the control topic, signed | Every downstream DAG, the watchdog |
| `stage_event.schema.json` | The dict each pipeline stage returns | `pipeline/verdict.py`, the tests |
| `examples/*.json` | One valid instance per schema | The CI service tier |

Dataset data contracts are **not** here. They live at
`config/datasets/<dataset>.json`, one per dataset, because they already
existed as the sensitivity override file and a second location would drift.
See `pipeline/metadata/contract.py`.

## The two tiers

**Service tier** (`make ci-service`): the tests pass, and every file in
`examples/` validates against its schema. A producer or consumer change that
breaks its own contract fails here.

**Platform tier** (`make ci-platform`): every payload schema embeds the
envelope by `$ref` and does not redefine its fields; every schema is
backward-compatible with the version on `main` (no required field removed, no
type changed). A change to the envelope fails every payload schema whose
example no longer validates, and that red list is the blast radius, shown
before merge.

## Changing a contract

1. Edit the schema. If the envelope changes shape, bump `schema_version`.
2. Update `examples/`. The service tier fails until they validate.
3. Run `make ci`. The platform tier names what you broke.
4. Commit. Jenkins runs the same two targets.

Changes cascade downward, never sideways: a payload schema may add fields to
its own payload; it may not add, remove or retype an envelope field. That is
enforced, not requested.
