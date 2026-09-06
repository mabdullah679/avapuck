# TRUST BOUNDARY

**Read this before relying on anything this pipeline produces.**

Everything below is stubbed, synthetic, assumed, or unverified. Maintained as
the work happened, not reconstructed afterwards. If something is not on this
list it is because it genuinely runs and was checked.

Status: **STUB** (named interface, no real implementation) · **SYNTHETIC**
(fabricated data) · **ASSUMED** (decision taken without confirmation) ·
**UNVERIFIED** (built but not proven under real conditions)

Last updated: 2026-09-06

---

## 0. What the 2026-09-06 platform change did and did not verify

The envelope, verdict, signing, contracts, consumer DAG, watchdog, Kafka UI,
Jenkins and single-env-file work landed in one change. **Docker was not
running on the machine that made it.** So:

| Verified | How |
|---|---|
| The test suite (227 tests) | `make test` on the host |
| Both CI tiers | `make ci` on the host |
| Both DAGs parse under Airflow 3.0.2 | imported with the pinned Airflow in the venv |
| The Kafka and messaging providers resolve against Airflow 3.0.2 | `pip` resolution and import |
| The IdP `/sign` endpoint and verdict verification | FastAPI test client, real RS256 keys |
| `docker-compose.yml` renders with `.env.example` | `docker compose config` |

| **UNVERIFIED** | What would verify it |
|---|---|
| The full stack starting with the strict env file | `./setup.sh` |
| The Kafka entrypoint rendering JAAS and the ACLs for `ui` and `watchdog` | `kafka-init` output |
| Kafbat UI logging in and reading `rpos_flat` and `pipeline_runs` | open <http://localhost:8090> |
| The watchdog consuming, verifying and turning unhealthy | `curl localhost:8446/status` after a run and after 4h30m of silence |
| The consumer DAG waking from the `MessageQueueTrigger` on a real verdict | `consumer.received_verdicts` gaining a row |
| The per-run PDF against a live Postgres with `run_slot` | a run producing `<dataset>_report_<slot>Z.pdf` |
| The Jenkins image building and the seeded job going green | `./setup.sh --ci` |
| `demo/report.pdf` | still the OLD per-day layout; regenerate after a live run |

Until each row above moves to the first table, describe the corresponding
feature as **built and unit-tested, not exercised**.

## 1. BigQuery — live and verified (2026-08-30)

| # | Item | Status |
|---|---|---|
| 1.1 | **Live BigQuery extraction** | **WORKS.** Verified 2026-08-30 with a real service-account credential. 100 rows returned, bytes billed recorded, job id recorded. |
| 1.2 | Cost per run | **MEASURED.** About 24 MiB scanned against `thelook_ecommerce`, roughly $0.0002. |
| 1.3 | **The source table is not partitioned** | **CORRECTED ASSUMPTION.** A date predicate prunes nothing; column selection is the only lever, and every run is a full scan of the named columns. |
| 1.4 | The byte cap is 300 MiB | **DELIBERATE.** Catches a `SELECT *` regression without refusing every real query. |
| 1.5 | **Frozen archives** | **STATED.** The extractor maps each run slot deterministically onto a day inside `BQ_DATA_START..END`, so re-runs stay idempotent against data that never changes. |
| 1.6 | Empty results | **FATAL.** An extract returning 0 rows raises rather than writing an empty CSV. |
| 1.7 | Report dates vs data dates | **CLOSED (2026-09-06).** The PDF now prints the run slot, the generation time and the data window actually read, all UTC, each labelled with its source. The window is derived from the archive mapping (BigQuery), from a declared `event_time_column` (CSV), or is the run date and says so. |
| 1.8 | `gcloud` CLI | **DENIED** in `.claude/settings.json`. The Secret Manager integration that depended on it was removed on 2026-09-06 (D12); credentials reach the pipeline via the key file and `.env.local`. |

## 2. Security

| # | Item | Status |
|---|---|---|
| 2.1 | AES-256-GCM encryption | **WORKS.** Round-trip, tamper detection, AAD field-binding, nonce uniqueness verified. |
| 2.2 | **Key management** | **STUB.** One master key from an env var. No KMS, no HSM, no rotation, no envelope encryption. A restart without `CRYPTO_MASTER_KEY` generates a new key and **all prior ciphertext becomes permanently unreadable**. The same is true of `IDP_SIGNING_KEY` for previously signed verdicts. |
| 2.3 | JWT auth | **WORKS.** RS256, verified by signature + issuer + audience + expiry + nbf against the IdP's JWKS. Algorithm pinned. Audience grants and scopes enforced. |
| 2.4 | The IdP itself | **STUB.** A local FastAPI service with hardcoded client registrations. No user directory, no MFA, no revocation, no refresh tokens, no audit trail. |
| 2.5 | **TLS** | **ENABLED, self-signed dev CA.** Every client verifies against `PIPELINE_CA_BUNDLE`; `verify_tls=False` appears nowhere. The CA's private key sits in `certs/` beside the certs it signs. Postgres, Hive, Spark and Kafka traffic is plain TCP. |
| 2.6 | Blind indexes | **WORKS, with a stated tradeoff.** Deterministic HMAC-SHA256 leaks equality; on a low-cardinality column that enables a dictionary attack. |
| 2.7 | Dev secrets | **INSECURE BY DESIGN, in one place.** `.env.example` carries dev passwords for every service; `.env.local` is the only place a value lives; compose has no defaults (test-enforced). Change every value before anywhere shared. |
| 2.8 | **Card split** | **WORKS, with a serious limit.** Only the middle ~6 digits of a 16-digit PAN are encrypted, one of them a Luhn check digit: brute-forceable offline in seconds. Fine for the published test PANs used here; **not for live cardholder data**. |
| 2.9 | **Kafka topics** | **WORKS, deliberately narrower than the request.** `rpos_encrypted` carries ciphertext, `rpos_flat` carries never-sensitive columns. `_assert_no_plaintext_published` and `contracts/row_flat.schema.json` both refuse anything else. |
| 2.9a | **Kafka auth (SASL) and ACLs** | **ENABLED, verified for `pipeline` and `consumer` (2026-09-03); UNVERIFIED for the `ui` and `watchdog` principals added 2026-09-06.** SASL_PLAINTEXT authenticates but does not encrypt the wire; PLAIN, not SCRAM; `User:ANONYMOUS` is a super user because the controller listener is PLAINTEXT; single broker, replication factor 1. |
| 2.10 | **BigQuery retry and CSV fallback** | **WORKS.** Three attempts inside the task, then `CSV_FALLBACK_PATH`. Never silent: the stage event carries `fell_back_to_csv` and the verdict names it as a reason. |
| 2.11 | **Signed verdicts** | **BUILT and unit-tested; UNVERIFIED against the running stack.** The orchestrator obtains a token as `orchestrator-job` (audience `idp`, scope `verdict.sign`) and the IdP returns an RS256 JWS over the payload digest; `VerdictVerifier` checks signature, issuer, audience, subject and digest. Verified end to end with the real IdP app under a test client. No key rotation: a `kid` change invalidates nothing old, because the JWKS is refetched on a `kid` miss, but there is only ever one key. |
| 2.12 | **Envelope and dedupe** | **BUILT and unit-tested.** Two deterministic ids per message; `dedupe.accept` drops a repeated `event_id` and flags a replayed `idempotency_key`. The Postgres-backed store's SQL is tested against a fake cursor, **not a live database**. Producer idempotence (`enable_idempotence=True`) is configured, not observed under broker retries. |

## 3. Apache Ranger

| # | Item | Status |
|---|---|---|
| 3.1 | Masking policy format | **REAL.** Genuine Ranger policy JSON in the shape the REST API accepts. |
| 3.2 | **Policy enforcement** | **STUB in every run so far.** `LocalPolicyProvider` applies Ranger's documented masking semantics locally. `RangerAdminProvider` is written and **has never been exercised**. Every masked row carries `masked_by` so the two are distinguishable. |
| 3.3 | Ranger Hive plugin | **NOT INSTALLED.** This pipeline masks before writing, which is stronger at rest but a different mechanism from query-time masking. |

## 4. Hive, Spark, Airflow

| # | Item | Status |
|---|---|---|
| 4.1 | Hive tables | **WORKS** (verified 2026-09-03). External table over masked Parquet, `MSCK REPAIR`, queryable through Tez. Registration is non-fatal when Hive is down and the verdict reports it as degraded. Hive does not backfill added columns into pre-existing partitions. |
| 4.2 | Spark | **WORKS** (verified 2026-09-03). The Airflow container is the driver against a real Spark 4.0.0 cluster. Three things must align (JVM in the Airflow image, executor Python 3.12, `/data` on the workers) or it degrades to pyarrow and the verdict says so. |
| 4.3 | Which engine ran | **RECORDED, NOT ASSUMED.** `engine` and `spark_configured` on the encrypt event; `masked_by` on every row. |
| 4.4 | Scale | **UNVERIFIED.** 100 rows/run. Nothing has been run at volume. |
| 4.5 | Airflow topology | `airflow standalone`, LocalExecutor, one container. No HA. Fine for a laptop. |
| 4.6 | **Event-driven consumer DAG** | **BUILT; UNVERIFIED against a broker.** Uses `AssetWatcher` + `MessageQueueTrigger` (providers `apache-kafka 1.16.0`, `common-messaging 2.0.4`, resolved against Airflow 3.0.2 by pip and imported successfully). The trigger runs `pipeline.messaging.listen.accept_verdict` in the triggerer; the message reaches the DAG as `asset_event.extra["payload"]` (confirmed in Airflow's source, not observed live). |
| 4.7 | Kubernetes | **REMOVED 2026-09-06.** The manifests predated TLS, card split, Kafka and the single DAG and were never updated. The last commit containing them is `f7d9375`. Docker Compose is the only deployment path. |

## 5. Assumptions taken without confirmation

| # | Assumption |
|---|---|
| 5.1 | The build account's GCP project id appears in git history. Not a credential; a client supplies their own `GCP_PROJECT_ID`. |
| 5.2 | Dataset choice for the fixture (`austin_bikeshare` shape) and for live runs (`thelook_ecommerce`). Not requested by name. |
| 5.3 | Which columns are "sensitive" is inferred by heuristic (§7) and now frozen per dataset by the contract (§10). A real classification exercise would involve a data steward. |
| 5.4 | The warehouse serves the `analyst` group; the `data_steward` policy path exists and is unused. |
| 5.5 | Hive registration failing is `degraded`, not `failed`, so downstream proceeds. Confirmed by the owner (D4); still an assumption about what downstream needs. |
| 5.6 | The reference consumer uses the pipeline's own Postgres role. A real consumer gets its own. |
| 5.7 | The watchdog's deadline is interval + 30 minutes of grace. Chosen, not measured against real run durations. |

## 6. What was actually verified

**Live, end to end, on real data (2026-08-30 and 2026-09-03):** real BigQuery
query executed; real PII encrypted, masked and loaded; no plaintext in Parquet
or Postgres by direct search; tampered ciphertext rejected; ciphertext moved
between columns fails to decrypt; unauthenticated and wrong-audience requests
refused; load idempotent (0 inserted / 100 updated); Spark cluster genuinely
executes the encrypt stage; Hive returns masked rows via Tez; Kafka enforces
SASL and per-topic ACLs; a dataset the pipeline had never seen ran end to end
with its schema inferred and its policy generated.

**On the host, 2026-09-06:** 227 tests pass; both CI tiers pass; both DAGs
parse under Airflow 3.0.2; compose renders with the example env. See §0 for
what that does not cover.

## 7. Schema inference — what it is and is not

Inference (`pipeline/metadata/schema.py`) is a heuristic. Its known limits:

| # | Limitation |
|---|---|
| 7.1 | **Sensitivity is a guess.** A sensitive column with an opaque name and an unremarkable value shape is classified `public` and lands unencrypted. Every verdict carries its reason; `config/datasets/<name>.json` overrides it; the contract (§10) freezes it so at least it cannot change silently. |
| 7.2 | **It over-classifies by design.** The opposite bias would leak. |
| 7.3 | **Type inference reads 500 rows.** A later value that does not fit fails the load loudly. With the contract, a changed inferred type now fails at extract instead. |
| 7.4 | **A synthetic key hashes the whole row.** Two identical rows collapse into one. |
| 7.5 | **Generated Ranger policy is a starting point.** Marked `_generated`; never regenerated. |
| 7.6 | **Trips is unaffected.** Inference reproduces the hand-authored sensitive set; verified by test. |
| 7.7 | **A sensitive primary key is keyed on its blind index**, checked by `assert_no_plaintext`. |

## 8. Not built at all

Backfill tooling · data quality framework · lineage tracking · Prometheus /
Alertmanager (the watchdog is the whole of monitoring) · a schema registry
(`contracts/` is the whole of contract management) · Kafka wire encryption
(SASL_SSL) · SCRAM · multi-broker Kafka · KMS / key rotation for the crypto
master key and the IdP signing key · Kerberos for Hive · HDFS · multi-tenancy
· GDPR erasure workflow · report delivery beyond the `pipeline-reports/`
folder · GitHub status integration for Jenkins (the pre-push hook is the only
gate) · a consumer that does anything with a verdict beyond recording it.

## 9. Consumer, watchdog, UI

| # | Item | Status |
|---|---|---|
| 9.1 | Reference consumer DAG | **BUILT, UNVERIFIED live** (4.6). Records the verdict and stops, by decision (D7). |
| 9.2 | Watchdog | **BUILT, UNVERIFIED live.** Verifies every verdict's signature, tracks the latest slot per dataset, 503 when late or negative, webhook optional. Starts from the earliest offset so a restart replays history. Its own liveness is the compose healthcheck plus restart policy; a dead Docker host is out of scope. |
| 9.3 | Kafbat UI | **UNVERIFIED.** `ghcr.io/kafbat/kafka-ui:v1.5.0`, read-only flag plus a read-only principal, login form. Configured from documentation, not from a run. |

## 10. Contracts and CI

| # | Item | Status |
|---|---|---|
| 10.1 | Message contracts | **WORKS** (unit-tested). Every message is validated before send and on parse. Examples validate in CI. |
| 10.2 | Stage-event contract | **WORKS** (unit-tested). The verdict task validates every event; a malformed one yields `proceed=false` and a red task. |
| 10.3 | Dataset contracts | **WORKS** (unit-tested). First sight writes `schema` into `config/datasets/<name>.json`; drift fails the extract. `config/datasets/trips.json` is committed from the fixture. The BLM dataset's contract is written on its next run. |
| 10.4 | Backward-compatibility check | **NARROW BY DESIGN.** Catches removed required fields, changed types, narrowed enums and consts, walking `properties` and `allOf`. Does not reason about `oneOf`, `pattern` tightening, or `additionalProperties`. |
| 10.5 | Jenkins | **UNVERIFIED.** Image, plugins, CasC seed and Jenkinsfile written from documentation. `make ci` on the host is verified and is the same two targets. |
| 10.6 | Push gate | **LOCAL ONLY.** The pre-push hook is the whole gate; `SKIP_CI=1` bypasses it. No GitHub status integration. |
