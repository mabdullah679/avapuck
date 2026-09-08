# TRUST BOUNDARY

**Read this before relying on anything this pipeline produces.**

Everything below is stubbed, synthetic, assumed, or unverified. Maintained as
the work happened, not reconstructed afterwards. If something is not on this
list it is because it genuinely runs and was checked.

Status: **STUB** (named interface, no real implementation) · **SYNTHETIC**
(fabricated data) · **ASSUMED** (decision taken without confirmation) ·
**UNVERIFIED** (built but not proven under real conditions)

Last updated: 2026-09-08

---

## 0. What has and has not been verified against the running stack

The platform layer (2026-09-06) and the console layer (2026-09-08) were
built without Docker running. **On 2026-09-08 the stack was then brought up
and exercised**, which found nine defects (listed in `docs/DECISIONS.md`,
"Live findings") and moved most rows from the second table to the first.

| Verified live, 2026-09-08 | How |
|---|---|
| The full stack starting with the strict env file, twice, the second time idempotently | `./setup.sh --merge-env`, then `--merge-env --no-run`: cached builds, schema applied, DAGs unpaused |
| Airflow's passwords file, the console obtaining an API token, a run started from the console | four console-triggered runs of `trips_pipeline`, all `success` |
| The **1000-row uncapped upload** through every stage | `small_orders`: contract generated, `engine=spark`, 1000 rows published, masked, loaded with `run_slot`, per-run PDF |
| **Spark executors calling the crypto service** | `engine=spark` on a run where the only crypto client on the driver is unused by that path; the worker containers' env did the calls |
| Kafka principals `ui`, `watchdog`, `tap` and their ACLs | `kafka-init` output; tap table at 4300 flat rows and 7 verdicts; watchdog consuming |
| The watchdog live, including signature verification | healthy, datasets tracked, two verdicts signed by a previous ephemeral IdP key **rejected** as unverifiable, which is the design |
| **The consumer DAG waking from the Kafka `MessageQueueTrigger`** | `asset_triggered__…` run `success`; `consumer.received_verdicts` holds the verdict with `signed_by=orchestrator-job`; `consumed_events` has one row |
| The per-run PDF against a live Postgres | `small_orders_report_20260908T1742Z.pdf` and three more, one per run |
| The IdP key id changing with the key, and verifiers refetching on their own | console login and crypto encrypt both succeeded after an IdP rebuild with no verifier restart |
| The scaler acting through the Docker socket | it cloned a worker at boot (the startup race, now fixed with a grace period) and removed it when idle after cooldown |
| The console's pipelines endpoint and the three presets | mother and child listed; both `connected` after a run |
| Dev auto-login from a loopback client | `GET /api/me` with no cookie returned the admin session |
| Kafbat UI | container healthy, login page answers 200; **not visually inspected** |
| The test suite (306 tests), both CI tiers, both DAGs parsing, compose rendering | on the host |

| **Still UNVERIFIED** | What would verify it |
|---|---|
| **The 2 GB upload**, and every stage at that volume | the procedure in `docs/RUNBOOK.md`; only 1000 rows have been run |
| The scaler scaling **up under load** (only the boot-time race has been observed) | the Spark tab during the medium and big runs |
| Compose's behaviour on the next `up` with scaled clones present | scale up, then `./setup.sh --no-run` |
| The traffic profile's numbers, and a real block | a real session in the console; the numbers are a guess |
| The watchdog turning unhealthy on silence | wait 4h30m with the scheduler stopped |
| Kafbat UI reading the topics after login | open <http://localhost:8090> and look |
| The Jenkins image building and the seeded job going green | `./setup.sh --ci` |
| Live BigQuery on this stack (every run so far fell back to CSV because of the relative credentials path, now fixed) | a scheduled run with `bq_attempts=1` and `fell_back_to_csv=false` |
| `demo/report.pdf` | still the OLD per-day layout; copy a live per-run PDF over it |

Until each row above moves to the first table, describe the corresponding
feature as **built and unit-tested, not exercised**.

## 1. BigQuery — live and verified (2026-08-30)

| # | Item | Status |
|---|---|---|
| 1.1 | **Live BigQuery extraction** | **WORKS.** Verified 2026-08-30 with a real service-account credential. |
| 1.2 | Cost per run | **MEASURED.** About 24 MiB scanned, roughly $0.0002. |
| 1.3 | **The source table is not partitioned** | **CORRECTED ASSUMPTION.** Column selection is the only lever; every run is a full scan of the named columns. |
| 1.4 | The byte cap is 300 MiB | **DELIBERATE.** |
| 1.5 | **Frozen archives** | **STATED.** The extractor maps each run slot deterministically onto a day inside `BQ_DATA_START..END`. |
| 1.6 | Empty results | **FATAL.** |
| 1.7 | Report dates vs data dates | **CLOSED (2026-09-06).** The PDF prints run slot, generation time and the data window actually read, all UTC, each labelled with its source. |
| 1.8 | **BigQuery is not switchable from the console** | **DELIBERATE (D15).** `EXTRACT_MODE` is an env var fixed at stack start. An admin "update BigQuery toggle" API call waits for RBAC. |

## 2. Security

| # | Item | Status |
|---|---|---|
| 2.1 | AES-256-GCM encryption | **WORKS.** Round-trip, tamper detection, AAD field-binding, nonce uniqueness verified. |
| 2.2 | **Key management** | **STUB.** One master key from an env var. No KMS, no rotation. A restart without `CRYPTO_MASTER_KEY` makes all prior ciphertext unreadable. Same for `IDP_SIGNING_KEY` and signed verdicts, and `CONSOLE_SESSION_SECRET` and sessions. |
| 2.3 | JWT auth | **WORKS.** RS256, verified by signature + issuer + audience + expiry + nbf against the JWKS. Algorithm pinned. |
| 2.4 | The IdP itself | **STUB.** A local FastAPI service with hardcoded client registrations. No user directory, no MFA, no revocation, no audit trail. The console's login is one more client (`console-admin`) with one password. |
| 2.5 | **TLS** | **ENABLED, self-signed dev CA.** The CA's private key sits in `certs/`. Postgres, Hive, Spark, Kafka and the console's own HTTP are plain. |
| 2.6 | Blind indexes | **WORKS, with a stated tradeoff.** Determinism leaks equality. |
| 2.7 | Dev secrets | **INSECURE BY DESIGN, in one place.** `.env.example` carries dev passwords for every service; `.env.local` is the only place a value lives; compose has no defaults (test-enforced). |
| 2.8 | **Card split** | **WORKS, with a serious limit.** Only the middle ~6 digits of a 16-digit PAN are encrypted. Not for live cardholder data. |
| 2.9 | **Kafka topics** | **WORKS, deliberately narrower than the request.** Ciphertext to one topic, never-sensitive columns to the other; two guards refuse anything else. Every row of every run is published, uploads included (D16). |
| 2.9a | **Kafka auth (SASL) and ACLs** | **ENABLED, verified for `pipeline` and `consumer`; UNVERIFIED for `ui`, `watchdog`, `tap`.** SASL_PLAINTEXT does not encrypt the wire; PLAIN, not SCRAM; single broker, RF 1. |
| 2.10 | **BigQuery retry and CSV fallback** | **WORKS.** Never silent; the verdict names it. |
| 2.11 | **Signed verdicts** | **BUILT and unit-tested; UNVERIFIED live.** No signing-key rotation. |
| 2.12 | **Envelope and dedupe** | **BUILT and unit-tested.** Producer idempotence configured, not observed under broker retries. |
| 2.13 | **Console sessions and dev auto-login** | **WORKS live.** HMAC-signed cookie, 30 days, HttpOnly, SameSite=Lax, over plain HTTP on localhost. No CSRF token beyond SameSite; no logout-everywhere beyond rotating the secret. One admin user; RBAC is the stated next step (D20). **`CONSOLE_DEV_AUTOLOGIN=true` means anyone who can reach the console from a loopback or private address is the admin** (D23). It is a development convenience, the dev template ships it on, and it must be off anywhere the console is reachable from a network you do not own. A private address is not proof of a trusted client. |
| 2.14 | **Traffic audit** | **BUILT and unit-tested; the profile is a GUESS.** In-process counters per service, per client IP as the socket sees it. Blocks on breach (D18). It is not a firewall: a flood large enough to exhaust the container before the middleware answers is not stopped by it, and the crypto service is deliberately not audited (its callers are in-network executors). `X-Forwarded-For` is honoured when present, which a direct client could set; nothing sits in front of these services to strip it. |
| 2.15 | **Uploads** | **BUILT and unit-tested.** File names are sanitised; content is not inspected beyond CSV parsing downstream. No virus scanning, no size limit beyond disk. Anyone with the one console password can start an uncapped run. |

## 3. Apache Ranger

| # | Item | Status |
|---|---|---|
| 3.1 | Masking policy format | **REAL.** |
| 3.2 | **Policy enforcement** | **STUB in every run so far.** `LocalPolicyProvider` applies Ranger's semantics locally. `RangerAdminProvider` has never been exercised. Every masked row carries `masked_by`. |
| 3.3 | Ranger Hive plugin | **NOT INSTALLED.** Masks before writing, not at query time. |

## 4. Hive, Spark, Airflow, scale

| # | Item | Status |
|---|---|---|
| 4.1 | Hive tables | **WORKS** (verified 2026-09-03). Non-fatal when Hive is down; the verdict reports it. |
| 4.2 | Spark | **WORKS for the 100-row driver-side path** (verified 2026-09-03). **The executor-side crypto path (D16) is UNVERIFIED**: the worker containers carry the spark-job secret and the CA, and the partition function is tested in-process, but no executor has called the crypto service yet. |
| 4.3 | Which engine ran | **RECORDED, NOT ASSUMED.** |
| 4.4 | **Scale** | **UNVERIFIED, and now the point.** Uploads are uncapped (D16) and every stage streams in chunks, but nothing has been run above 100 rows outside the unit tests. Known single-threaded points at volume: the Parquet write after Spark (one writer, one file); card split, mask and load on the driver; the publish of two messages per row to one broker. The 2 GB procedure is in the runbook and has not been executed. |
| 4.5 | Airflow topology | `airflow standalone`, LocalExecutor, one container. `max_active_runs=1`: an upload queues behind a scheduled run. |
| 4.6 | **Event-driven consumer DAG** | **BUILT; UNVERIFIED against a broker.** |
| 4.7 | Kubernetes | **REMOVED 2026-09-06.** Last commit containing it: `f7d9375`. |
| 4.8 | **The scaler holds the Docker socket** | **ROOT-EQUIVALENT ON THE HOST, BY DECISION (D17).** One service, `services/scaler`, runs as root with `/var/run/docker.sock` mounted. Anything that compromises it owns the host. **Its actions are verified live**: it cloned a worker (at boot, before the compose workers had registered; a startup grace now prevents that) and removed it once idle. Compose's behaviour on the next `up` with clones present is **UNVERIFIED** (they carry the project and service labels so `down` removes them). A real deployment replaces this with the orchestrator's autoscaler. |
| 4.9 | Scaler policy | **TESTED as a rule; scale-down observed live; scale-up under real load UNVERIFIED.** Thresholds are defaults, not measurements. |

## 5. Assumptions taken without confirmation

| # | Assumption |
|---|---|
| 5.1 | The build account's GCP project id appears in git history. Not a credential. |
| 5.2 | Dataset choice for the fixture and for live runs. |
| 5.3 | Sensitivity is inferred by heuristic (§7) and frozen per dataset by the contract (§10). |
| 5.4 | The warehouse serves the `analyst` group. |
| 5.5 | Hive registration failing is `degraded`, not `failed`. Confirmed by the owner (D4). |
| 5.6 | The reference consumer, the tap and the console use the pipeline's own Postgres role. A real deployment gives each its own. |
| 5.7 | The watchdog's deadline is interval + 30 minutes. |
| 5.8 | `PIPELINE_BATCH_ROWS=5000` is a reasonable chunk. Not measured. |
| 5.9 | The traffic profile's limits (`config/traffic_profile.json`). A first guess, marked as such in the file. |
| 5.10 | Uploaded files are trusted to be CSV. The extract stage fails loudly on anything else; nothing checks earlier. |

## 6. What was actually verified

**Live, end to end, on real data (2026-08-30 and 2026-09-03):** real BigQuery
query executed; real PII encrypted, masked and loaded; no plaintext in Parquet
or Postgres by direct search; tampered ciphertext rejected; unauthenticated
and wrong-audience requests refused; load idempotent; Spark cluster executes
the encrypt stage; Hive returns masked rows via Tez; Kafka enforces SASL and
per-topic ACLs; a dataset the pipeline had never seen ran end to end.

**On the host, 2026-09-08:** 288 tests pass; both CI tiers pass; both DAGs
parse under Airflow 3.0.2; compose renders with the example env; the
generated scale-test CSV classifies as intended. See §0 for what that does
not cover.

## 7. Schema inference — what it is and is not

| # | Limitation |
|---|---|
| 7.1 | **Sensitivity is a guess.** Every verdict carries its reason; the contract (§10) freezes it so it cannot change silently. |
| 7.2 | **It over-classifies by design.** The generated test data's `city` column is treated as a quasi-identifier, for instance. |
| 7.3 | **Type inference reads 500 rows.** At 2 GB that is 0.003% of the file; a later value that does not fit fails the contract at the next run, or the load loudly. |
| 7.4 | **A synthetic key hashes the whole row.** Two identical rows collapse into one. |
| 7.5 | **Generated Ranger policy is a starting point.** |
| 7.6 | **Trips is unaffected.** |
| 7.7 | **A sensitive primary key is keyed on its blind index.** |
| 7.8 | **Row counting reads the whole file at extract.** Minutes at 2 GB, before any stage does work. |

## 8. Not built at all

Backfill tooling · data quality framework · lineage tracking · Prometheus /
Alertmanager (the watchdog and the traffic audit are the whole of monitoring)
· a schema registry · Kafka wire encryption (SASL_SSL) · SCRAM · multi-broker
Kafka · KMS / key rotation · Kerberos for Hive · HDFS · multi-tenancy · GDPR
erasure workflow · report delivery beyond the folder · GitHub status
integration for Jenkins · **RBAC** (one admin login; the BigQuery toggle
waits on it) · upload virus scanning or size limits · TLS on the console ·
a real edge firewall in front of the traffic audit · a consumer that does
anything with a verdict beyond recording it.

## 9. Consumer, watchdog, UI, tap, console, scaler

| # | Item | Status |
|---|---|---|
| 9.1 | Reference consumer DAG | **WORKS live** (2026-09-08). Woke from the Kafka `MessageQueueTrigger`, verified the signature, deduped, recorded. It took three fixes to the filter's signature to get there, all caused by how Airflow serialises the trigger's empty args; the filter now finds the message by shape. Records the verdict and stops (D7). |
| 9.2 | Watchdog | **WORKS live**, including rejecting verdicts signed by a key the IdP no longer has. Turning unhealthy on silence is untested (needs 4h30m). |
| 9.3 | Kafbat UI | **Healthy and answering; not visually inspected.** |
| 9.4 | **Kafka tap** | **WORKS live.** Scrape-only by ACL; every flat and control message into `monitor.kafka_log`, invalid ones kept and marked. 4300 rows after five small runs; at 2 GB that table grows by two rows per source row. No retention policy on it. |
| 9.5 | **Console** | **WORKS live**: login through the IdP, auto-login, upload, run trigger, the Runs tab joined to verdicts, the pipelines dropdown with its presets, the Spark tab, the Kafka log tab. The Traffic tab has shown only calm traffic. |
| 9.6 | **Scaler** | **WORKS live for the actions it took** (4.8); scale-up under load unobserved. |

## 10. Contracts and CI

| # | Item | Status |
|---|---|---|
| 10.1 | Message contracts | **WORKS** (unit-tested). |
| 10.2 | Stage-event contract | **WORKS** (unit-tested). Extract events gained optional `source`, `source_file`, `row_limit`; the change is additive. |
| 10.3 | Dataset contracts | **WORKS** (unit-tested). A console upload of a new dataset writes its contract on first sight, like any other. |
| 10.4 | Backward-compatibility check | **NARROW BY DESIGN.** |
| 10.5 | Jenkins | **UNVERIFIED.** `make ci` on the host is verified and is the same two targets. |
| 10.6 | Push gate | **LOCAL ONLY.** |
