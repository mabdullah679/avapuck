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

---

# Round two: the console, uncapped uploads, scaling, traffic

Decided 2026-09-08, the same way: one question at a time, confirmed.

## D15. A console, and BigQuery is not a toggle in it

`services/console/` is a small web app: drop or pick a CSV, press Run, and
watch it land. Run means **one fresh run of the existing `trips_pipeline`**
with the upload in the DAG run's conf, its own run slot, PDF and signed
verdict. No second pipeline, no per-run cluster.

The scheduled source stays an environment variable, `EXTRACT_MODE`, fixed
when the stack starts. The console shows it and says why it cannot change it:
BigQuery is a billable source and there is no RBAC yet, so an API call to
switch it would be an unauthenticated switch on a bill. **When RBAC exists,
an admin UI gets an "update BigQuery toggle" API call** backed by the IdP's
roles; until then the env var is simpler and safer for a proof of concept.
This paragraph is that documentation.

Rejected: a separate upload DAG (two histories for one pipeline); a fresh
Spark cluster per run (needs Docker control per run, adds minutes).

## D16. Uploads are uncapped; the data path streams

Non-negotiable 4 said 100 rows per run. Its reason is BigQuery's bill, which
a local file does not incur. The owner chose to lift the cap for uploads,
which changed the non-negotiable's wording: **100 rows per BigQuery run;
uploads are bounded only by `UPLOAD_MAX_ROWS`, 0 meaning none.**

That dragged the data path with it. Every stage now walks rows in chunks of
`PIPELINE_BATCH_ROWS` through `pipeline/common/batching.py`; every crypto
call is one chunk wide; Parquet is written incrementally to one file with a
fixed all-string schema; Postgres loads through `execute_values`. On Spark,
the **executors** run the same `encrypt_rows` over their partitions and call
the crypto service themselves with the spark-job identity, which is what
lets a multi-gigabyte upload spread its crypto work across workers. The
100-row scheduled run takes exactly the same path with one chunk.

The owner also chose **publish every row, always**, over a summary above a
threshold. A 2 GB upload is therefore millions of Kafka messages on a single
local broker. That is honest to the contract; the verdict, the Kafka UI and
the tap table show the volume plainly, and TRUST-BOUNDARY says the broker
is single-node.

Rejected: a separate scale-test DAG (would have proven Spark, not the
pipeline); synthetic load only.

## D17. One scaler service holds the Docker socket

Scaling `spark-worker` containers up and down inside Docker Compose needs
something that can start and stop containers. The only way is the Docker
socket, which is root on the host. It was refused for Jenkins; the owner
accepted it here on the condition that exactly one small service has it and
the fact is written down. `services/scaler/` polls the Spark master's own
status, applies the pure rule in `services/scaler/policy.py` (up on waiting
apps or utilisation above a threshold, down when idle and only ever removing
workers it created, cooldown between actions), and clones an existing
worker's container definition to add one. A real deployment replaces it with
the orchestrator's autoscaler.

Rejected: a fixed pool with Spark dynamic allocation (no container count
change to show); a manual button that prints a command.

## D18. Traffic audit: detect and block

`config/traffic_profile.json` says what expected traffic is, per category of
request: per-client and total limits per window. `pipeline/common/traffic.py`
counts what arrives and **blocks**: a client over its limit is refused for
`TRAFFIC_BLOCK_SECONDS`; a category over its total is refused to every
non-allowlisted client until the window drains, which is the distributed
case. The console audits with nothing allowlisted; the IdP allowlists the
compose network so the pipeline's own token requests can never be cut off.
Every transition logs an ALERT line and posts to the watchdog's webhook.

The owner chose blocking over alert-only, knowing a wrong profile locks out
a real user. The profile ships as a first guess, marked as such; the
console's Traffic tab is where it gets tuned.

## D19. The Kafka tap is a Postgres table

`services/tap/` consumes the flat and control topics as the `tap` principal,
which can read those two and nothing else, and writes one row per message to
`monitor.kafka_log`. The console shows that table, filtered and paged.
Durable across restarts, queryable with SQL, and messages that fail their
contract are kept and marked `invalid` rather than dropped.

Rejected: an in-memory ring buffer.

## D20. Console login goes through the IdP

A `console-admin` client in the IdP. The console forwards the typed password
as that client's secret, verifies the token it gets back against the JWKS
(signature, issuer, audience `console`, scope `console.admin`), and issues an
HMAC-signed session cookie. The password is never stored. This is the hook
RBAC attaches to: more clients, more scopes, the same verification.

Rejected for now: localhost-only with no login.

## D21. The scale test procedure

Small file first, then 2 GB, once the small one is confirmed end to end.
`scripts/make_test_csv.py` generates either, with sensitive-shaped columns
and Luhn-valid test cards so every stage does real work. The procedure and
what to watch are in `docs/RUNBOOK.md`. Until it has been run, everything
about scale is UNVERIFIED and TRUST-BOUNDARY says so.

---

# Round three: the console in use (2026-09-08, same day, after the first live run)

## D22. Run any pipeline; three presets; "connected" reuses the watchdog

The Run panel lists **every DAG Airflow knows**, mother and child alike, from
`GET /api/pipelines`. Each entry carries its kind (mother = scheduled and
producing verdicts; child = asset-triggered), whether Airflow's API answers
200 for it, its last run, and whether it is *connected*. Three presets filter
the dropdown: all, responding 200, connected.

"Connected" is **not a new system**. The owner asked for a loop that tags a
connection from a node and the data it transmits and re-checks one condition
forever, unless one already existed. One did: the watchdog consumes every
verdict, records which principal signed it and when, and re-evaluates one
condition on a timer. The console defines connected as: the pipeline's last
run succeeded within the watchdog's window (interval + grace), and, for a
mother, the watchdog has seen a positive verdict inside that window. The
rule is `connection_state` in `services/console/app.py`, one function for
every DAG.

Rejected: a separate heartbeat service. It would have duplicated the
watchdog's loop with a second definition of "alive".

## D23. The local browser stays signed in

Sessions last 30 days by default (`CONSOLE_SESSION_SECONDS`), and
`CONSOLE_DEV_AUTOLOGIN=true` lets a browser on a loopback or private address
get a session without typing anything: the console signs in to the IdP
itself with `CLIENT_SECRET_CONSOLE_ADMIN` and verifies the token exactly as
a typed login would. The IdP stays in the loop; only the typing is skipped.
The login form remains for when auto-login is off, and its fields carry
autocomplete hints so the browser offers to save the password.

This is a development convenience and is labelled as such everywhere it
appears. Anywhere the console is reachable from a network you do not own,
`CONSOLE_DEV_AUTOLOGIN` must be false, and TRUST-BOUNDARY says so.

Rejected: pre-filling the password into the page. That puts the secret in
every page load.

## D24. setup.sh is idempotent, and persistence is only ever brought up to date

Re-running `./setup.sh` must be safe and fast. Builds and pulls are cached by
Docker; the script does nothing to defeat that. Certificates are generated
once. `--merge-env` appends variables a newer template added, with dev
defaults, never touching existing values. The warehouse schema is applied on
every run with `IF NOT EXISTS` statements, because Postgres only runs init
SQL on a fresh volume and an older volume must catch up without being
rebuilt. Both DAGs are unpaused every run. Airflow is waited for until the
DAG is actually parsed before a run is attempted, and the PDF is looked up
by the slot it was run for, not by whatever is newest in the folder. A
paused or stopped Docker Desktop is restarted rather than reported.

## Live findings, first day on the real stack (2026-09-08)

Every one of these was found by running, not by reading. Each is fixed and
recorded here so the reasoning survives.

| Found | Cause | Fix |
|---|---|---|
| Decrypt refused right after encrypt | The crypto service had four uvicorn workers; with no `CRYPTO_MASTER_KEY` each worker made its own random key | One worker. Scaling the crypto service requires fixed keys first; compose says so |
| Console login failed after the IdP was rebuilt; the crypto service would have too | The IdP's key id was a constant, so every verifier's cached JWKS "matched" a key that no longer existed | The kid is derived from the key; a new key is a new kid and verifiers refetch on their own |
| BigQuery fell back to CSV every run | `GOOGLE_APPLICATION_CREDENTIALS` in `.env.local` was a host-relative path, meaningless in the container | The DAG resolves a relative path against the repo root, then `/secrets/`; setup warns |
| Watchdog and tap could not read their topics | They started before `kafka-init` granted their ACLs and cached the refusal | They depend on `kafka-init` completing |
| The scaler cloned a worker seconds after boot | It read the master before the compose workers had registered and saw fewer than min | A startup grace period |
| The console could not start a run | Airflow's token endpoint answers 201 Created | Accept 200 and 201 |
| The consumer DAG never woke | Airflow's serializer passes the trigger's empty args and kwargs to the filter as stray arguments; and the DAG was created paused | The filter accepts and ignores extras; new DAGs are created unpaused; setup unpauses both |
| The first scheduled run failed in two stages | It began on the old container the moment the DAG was unpaused, and the container was replaced mid-run | Not a bug in the current stack; recorded so nobody chases it |
| `setup.sh` claimed a PDF that was days old | It listed the newest file in the folder; the run had actually failed before the DAG was parsed | Wait for the DAG to be parsed; look up the PDF by slot; surface the run's errors |
| The owner's first real upload, the BLM lease-sale export, went red after eight minutes with a verdict that said only "extract did not complete", seven times | The contract gate refused it in 0.06s (the committed contract was written from the demo copy of that export, which carries a synthetic `card_info` column the real export lacks). The refusal is a decision, and Airflow retried it twice with backoff. The reason lived only in the task log: a stage that raises left nothing behind for the verdict, so the console and the watchdog could not show it | A stage that raises records a `failure_record` to XCom before the exception leaves it; the verdict names it (`extract failed: …`) and carries it per stage as `error`; `stages.NON_RETRYABLE_ERRORS` (the contract refusal) fails the task in one try. Verified live: the same upload re-run went red in ten seconds with the sentence in the Runs tab and on `/status` |
| That refused run's verdict was filed under dataset `trips` | With no stage event to name the dataset, the verdict fell back to the pipeline's name, so the watchdog blamed the wrong dataset | The run's conf (`dataset`) is consulted before the pipeline name |
| A console upload was committed to git | `data/incoming/` was not ignored; a test upload (synthetic, 1000 generated rows) went in with the console round | `data/incoming/` ignored and the file untracked. Uploads are plaintext by definition; the history still holds that synthetic file |

Two things the same run showed and this round did NOT change, pending a
decision: Airflow marks such a run `success`, because the run's state follows
its leaf task and `emit_verdict` succeeds whenever a verdict was published
(the console shows that state beside the verdict's `failed`); and the
console derives the dataset name from the file name, so an export of a
dataset the schedule already ingests is, correctly, held to that dataset's
contract, with no warning before the run starts.
