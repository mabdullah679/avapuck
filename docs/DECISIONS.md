# Decision record

Decisions taken on 2026-09-05 before the platform work began, each one put to
the owner as a single plain-language question and confirmed. This file is the
authority for *why* the code is shaped the way it is; `TRUST-BOUNDARY.md`
remains the authority for what is stubbed or unverified.

Format: decision, the alternative that was rejected, and the reason.

---

## D1. Three artifacts, three meanings

A green DAG means "my steps ran". A signed `run.verdict` message on the
control topic means "what the platform promises downstream". The PDF prints
the same verdict, so the human artifact and the machine artifact agree.

Rejected: making the DAG go red when a downstream consumer fails. A producer
cannot know when the world is finished; it can only say what it delivered.

## D2. One envelope, two identifiers

Every Kafka message on every topic is `{envelope, payload, signature?}`. The
envelope carries an `event_id`, unique per emission (derived from the
idempotency key, the Airflow run id and the try number), and an
`idempotency_key`, deterministic per logical unit of work (event type,
dataset, run slot, row key). A consumer drops a repeated `event_id` and
upserts on the `idempotency_key`.

Rejected: a single dedupe key. Non-negotiable 5 says a re-run of a slot
replaces rather than duplicates; a single key either drops the repaired run
or double-counts it. Two keys with two verbs encode both.

Consequence: the flat `{**meta, **row}` payload the publisher used to send is
gone. A dataset with a column called `dataset` or `logical_date` no longer
overwrites the message metadata.

## D3. Shared library now, package later

`pipeline/messaging/` holds envelope construction, contract validation,
signing, and the consumer-side dedupe helper. The reference consumer DAG in
this repo imports it. External teams get it as a package when they exist; the
module boundary is drawn so extraction is a packaging step, not a refactor.

## D4. The verdict is a pure function with a boolean

`pipeline/verdict.py` takes the stage events and returns `proceed`, `status`,
`reasons`, per-stage detail and the run's outputs. Consumers key off `proceed`
only. `status` and `reasons` are for people and for the PDF.

`proceed` is true when extract, card split, encrypt, publish, mask, load and
report all succeeded. An unregistered Hive table, a failed delivery copy, a
BigQuery-to-CSV fallback, or a pyarrow fallback when Spark was configured
make the status `degraded` but still `proceed`. That codifies the run's
existing non-fatal semantics rather than changing them. Hive is the one
judgment call and it lives in one function with the heaviest tests in the
repo.

Rejected: holding downstream on any degraded step. A metastore hiccup would
stop every team for data that is correct.

## D5. The verdict is always emitted

The terminal task runs with `trigger_rule="all_done"`, so a failed run still
publishes a verdict with `proceed=false`. A *missing* verdict therefore means
exactly one thing: the DAG did not run. That is what the watchdog watches.

## D6. One control topic, one writer, signed

`pipeline_runs`, one partition, long retention, keyed by dataset. Only the
`pipeline` principal may write it. Each verdict is additionally signed: the
orchestrator authenticates to the IdP as `orchestrator-job` and the IdP
returns an RS256 JWS over the payload digest. Consumers verify against the
IdP's JWKS, the same way services already verify each other.

The owner chose signing now over deferring it. ACLs guard the topic; the
signature guards the message wherever it travels afterwards.

## D7. Downstream reacts through Airflow 3 event-driven scheduling

The reference consumer DAG is scheduled on an `Asset` with an `AssetWatcher`
backed by the common-messaging `MessageQueueTrigger` for Kafka. The trigger's
`apply_function` accepts only `run.verdict` messages with `proceed=true`; the
DAG's first task verifies the signature and dedupes through the shared
library. The demo consumer records the verdict it received and stops.

Rejected: a deferrable sensor plus `TriggerDagRunOperator`. Works, but is the
Airflow 2 shape; the asset watcher is native to the pinned 3.0.2 and is the
asset cascade the single-DAG consolidation gave up, returning at the team
boundary where it belongs.

Rejected: having the demo consumer build a summary table. The owner chose the
minimal reference.

## D8. Absence is watched from outside Airflow

A dead scheduler cannot report itself. `services/watchdog/` consumes the
control topic, tracks the last verdict per dataset, and serves `/healthz`
that fails when the age exceeds the schedule interval plus grace, or the last
verdict said `proceed=false`. The compose healthcheck makes that visible; an
optional webhook URL posts the alert out. The mother DAG also gets
`dagrun_timeout` and a failure callback that logs a structured `ALERT` line,
so stuck, failed and absent are three distinguishable states.

Rejected: Prometheus and Alertmanager. The production form; two more
containers than a demo needs. Recorded in TRUST-BOUNDARY as not done.

## D9. Kafka UI is Kafbat UI, always on, read-only

Own SASL principal `ui` with Describe on all topics and groups, Read on the
flat topic and the control topic, and no Read on the ciphertext topic,
matching the posture the `consumer` principal already has. Read-only mode on,
login form on. Always on, because the point is showing the client a verdict
land.

## D10. Contracts are JSON Schema; the data contract reuses the override file

`contracts/` holds the envelope schema, the three payload schemas and a
stage-event schema. The stage-event schema is the per-step contract: it is
what the verdict consumes, so a stage that stops reporting a field fails a
contract test instead of the verdict silently. Each dataset's data contract is
the existing `config/datasets/<name>.json`, extended with columns, types,
sensitivity and key. Inference proposes, the committed file is the contract:
the first run of a dataset writes it (marked `_generated`, never regenerated),
and a later run whose inferred manifest disagrees **fails** naming the
difference. A dataset may also declare an `event_time_column`, from which the
PDF's data window is derived.

Rejected: warn-only. Keeps today's behaviour, where Postgres silently grows a
column, and makes the contract documentary rather than structural.

## D11. CI is local Jenkins, not GitHub Actions

Jenkins runs as a compose service under the opt-in `ci` profile, seeded from
code (Configuration as Code plus Job DSL), with the repo bind-mounted read-only
as a file:// git remote. The controller image bakes Python and the test
requirements so the built-in node is the agent; no docker socket, no
docker-in-docker. The `Jenkinsfile` is two sequential stages that only call
`make ci-service` and `make ci-platform`, so `make ci` gives the identical
answer without Jenkins. Polling plus a post-commit hook trigger builds; a
pre-push hook installed by `setup.sh` refuses a push that fails `make ci`.

Rejected: GitHub Actions, to save runner minutes. Rejected: mounting the
docker socket, because it is root on the host.

Known limit: with no GitHub status integration nothing stops a push except
the local hook.

## D12. Secrets: one file, no third-party manager

The Google Secret Manager scripts, make targets and document are removed; a
client wires their own secrets manager into the one place values live. That
place is a gitignored `.env.local`. One committed `.env.example` lists every
variable with its dev value. `docker-compose.yml` carries **no** built-in
defaults, so a missing variable fails loudly by name. Kafka's JAAS and admin
client files are rendered from the same variables at broker start. File-based
secrets (TLS material, a GCP key) stay files; `.env.local` holds their paths.

Rejected: keeping compose defaults as a safety net. A value that can come from
two places is the drift this decision exists to remove.

## D13. Dates are UTC and labelled; the PDF is per run

Airflow schedules in UTC, so the run slot, the generation time and the data
window all print with a `Z` suffix. One PDF per run, named with the slot, so
each PDF pairs with exactly one verdict. Every warehouse row records the
`run_slot` that loaded it so the page can select one run. The page prints the
data window the extract actually read, closing TRUST-BOUNDARY 1.9.

Rejected: local time on the page (would disagree with Airflow's own UI and
logs); one accumulating PDF per day (six runs overwrote one file and the page
silently summed them).

## D14. Cleanup

Kubernetes docs, manifests, Makefile and Dockerfiles: deleted (stale by the
spec's own account; git keeps them). `scripts/run_chain.py`: deleted;
`pipeline/run_pipeline.py` stays and both it and the DAG iterate the same
stage list in `pipeline/stages.py`, so a runner cannot drift again.
`docs/AGENTS.md`: deleted; the root `AGENTS.md` stays. `docs/SECRETS.md` and
`.env.local.example`: deleted under D12.
