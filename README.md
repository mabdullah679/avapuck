# avapuck

A privacy-first data platform small enough to run on a laptop: it takes 100 rows per run from a BigQuery public dataset (or any CSV uploaded through a console), encrypts sensitive fields, moves them through Kafka and Hive under Ranger masking policy into Postgres, writes one PDF, and publishes one signed verdict telling downstream teams whether to proceed. Spec and rationale: [`docs/ORCHESTRATION-SPEC.md`](docs/ORCHESTRATION-SPEC.md), [`docs/DECISIONS.md`](docs/DECISIONS.md).

Built to show data engineering, privacy and security design, workflow automation, messaging, and operational safeguards. What is stubbed or unverified is listed honestly in [`TRUST-BOUNDARY.md`](TRUST-BOUNDARY.md); it is a proof of concept, not for live cardholder data.

```
BigQuery / CSV upload
        |
   [extract] -> [card_split] -> [encrypt] -> [publish] -> [mask] -> [hive_register] -> [load] -> [report]
                                AES-256-GCM    Kafka      Ranger        Hive          Postgres     PDF
                                (crypto svc)   envelope   policy
                                                                                          |
                                         emit_verdict (always runs) -> signed run.verdict on Kafka
```

Not local: live BigQuery needs a GCP service-account key you supply (`secrets/gcp-sa.json`). Without it the pipeline uses an offline fixture.

## Prerequisites

- To run the stack: Docker with the Compose plugin (the stack is about 15 containers; allow several GB of RAM and a long first image pull). Nothing else is needed on the host: no Python, Java or Kafka client.
- To run the tests: Python 3.12 (CI uses 3.12). No Docker, no cloud, no credentials.
- Ports used on the host: 8447 console, 8443 IdP, 8444 crypto, 8446 watchdog, 8448 scaler, 8090 Kafka UI, 8085 Airflow, 8080 Spark master, 5433 Postgres, 8095 Jenkins (opt-in).

## Run it

1. `./setup.sh` creates `.env.local` from [`.env.example`](.env.example) (dev defaults), generates certificates, starts the stack, creates Kafka topics and ACLs, and runs one slot. Safe to re-run. `./setup.sh --help` lists options.
2. Open the console at <http://localhost:8447> (auto-login on a dev machine), upload a CSV, and watch the Runs tab.
3. Generate a test CSV with `python3 scripts/make_test_csv.py --rows 1000 > small.csv`.
4. Stop with `./setup.sh --down`; `--clean` also deletes data volumes.

If a port is taken or a service will not start, see [`docs/RUNBOOK.md`](docs/RUNBOOK.md) "Common failures". The compose project name is `avapuck`, so a second checkout on the same machine shares the first one's volumes.

## What it does

| Action | Result |
|---|---|
| Scheduled run (every 4 hours) | 100 BigQuery rows (hard cap in the SQL, asserted after) or the offline fixture, through all eight stages |
| Upload a CSV in the console | One uncapped run (unless `UPLOAD_MAX_ROWS` is set), streamed in `PIPELINE_BATCH_ROWS` chunks |
| Sensitive column | Encrypted before it lands anywhere; `assert_no_plaintext` in [`spark_encrypt.py`](pipeline/transform/spark_encrypt.py) and `_assert_no_plaintext_published` in [`kafka_publish.py`](pipeline/publish/kafka_publish.py) refuse plaintext |
| Masking | Ranger policy JSON in [`config/ranger/`](config/ranger/), generated once for a new dataset for review |
| Any run, even a failed one | `emit_verdict` (`trigger_rule=all_done`) publishes a signed `run.verdict`; consumers read `proceed` only |
| Re-run of the same slot | Replaces that slot's partition; never double-loads Postgres |
| Contract change in a later export | Run stops at extract with the column differences listed ([`pipeline/metadata/contract.py`](pipeline/metadata/contract.py)) |
| Verdict not seen on time | Watchdog reports unhealthy ([`services/watchdog/`](services/watchdog/)) |

## Interfaces

| Service | Endpoints | Auth |
|---|---|---|
| Console (8447) | `/api/login`, `/api/upload`, `/api/run`, `/api/runs`, `/api/pipelines`, `/api/kafka-log`, `/api/spark`, `/api/spark/scale`, `/api/traffic`, `/healthz` | session cookie issued after IdP login (login and `/healthz` open) |
| IdP (8443) | `/token`, `/sign`, `/.well-known/jwks.json`, `/health` | client credentials; tokens verified by signature, issuer, audience, expiry |
| Crypto (8444) | `POST /encrypt`, `POST /decrypt`, `/health` | bearer JWT with a crypto scope |
| Watchdog (8446), Tap | `/status`, `/healthz` | none in code |
| Scaler (8448) | `POST /scale`, `/status`, `/healthz` | none in code; see [`TRUST-BOUNDARY.md`](TRUST-BOUNDARY.md) 4.8 |

## Tests

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements-ci.txt
make ci
```

`make ci` = `make ci-service` (pytest plus JSON-schema validation of the contract examples) then `make ci-platform` (every payload schema embeds the envelope, and each contract is backward-compatible with `main`). Result on 2026-11-18: 318 passed, 4 skipped (the skips need `google-cloud-bigquery`, an extract secret, or a service-account key). The suite needs no external services and does not exercise the running Docker stack. The same two tiers run in GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml), plus a gitleaks scan) and in local Jenkins ([`Jenkinsfile`](Jenkinsfile)).

## Layout

| Path | Role |
|---|---|
| [`pipeline/`](pipeline/) | The stages; [`stages.py`](pipeline/stages.py) is the single stage list, [`verdict.py`](pipeline/verdict.py) decides the verdict |
| [`dags/`](dags/) | Airflow DAGs: `trips_pipeline` and the verdict listener |
| [`services/`](services/) | IdP, crypto, console, Kafka tap, watchdog, scaler |
| [`contracts/`](contracts/) | JSON Schemas for the envelope and payloads, with examples |
| [`config/`](config/) | Per-dataset contracts, Ranger policies, BigQuery SQL, traffic profile |
| [`docker/`](docker/), [`docker-compose.yml`](docker-compose.yml) | Images and the stack |
| [`scripts/`](scripts/) | CI contract check, git hooks, cert generation, test-CSV generator |
| [`tests/`](tests/) | The pytest suite |
| [`docs/`](docs/) | [Architecture](docs/ARCHITECTURE.md), [decisions](docs/DECISIONS.md), [runbook](docs/RUNBOOK.md), [consuming the verdict](docs/CONSUMING.md), [execution flow](docs/EXECUTION-FLOW.md) |
| [`AGENTS.md`](AGENTS.md), [`CLAUDE.md`](CLAUDE.md) | Working agreements for changing the code |

## Design notes

- **Encryption at the edge, not in the warehouse.** Encrypting inside Hive or Postgres would leave plaintext in Kafka and Parquet. Here the only plaintext window is inside the crypto service ([D1-D2](docs/DECISIONS.md)).
- **Masking is policy, not code.** Transformation code that masks columns drifts from what security signed off; Ranger JSON is authored once and enforced at the Hive boundary.
- **The verdict is a boolean from a pure function.** Downstream teams reacting to log text or task states would break on rewording; they read `proceed` ([D4-D5](docs/DECISIONS.md)).
- **CI is both Jenkins and GitHub Actions.** Both call the same `make` targets, so neither can drift from the other ([D11](docs/DECISIONS.md)).
