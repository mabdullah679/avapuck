# TRUST BOUNDARY

**Read this before relying on anything this pipeline produces.**

Everything below is stubbed, synthetic, assumed, or unverified. Maintained as
the work happened, not reconstructed afterwards. If something is not on this
list it is because it genuinely runs and was checked.

Status: **STUB** (named interface, no real implementation) · **SYNTHETIC**
(fabricated data) · **ASSUMED** (decision taken without confirmation) ·
**UNVERIFIED** (built but not proven under real conditions)

Last updated: 2026-08-30

---

## 1. BigQuery — now live and verified

| # | Item | Status |
|---|---|---|
| 1.1 | **Live BigQuery extraction** | **WORKS.** Verified 2026-08-30 against `bigquery-public-data.austin_bikeshare.bikeshare_trips` with a real service-account credential. 100 rows returned, 232.00 MiB billed, job id recorded. All five DAGs run green in `EXTRACT_MODE=live`. |
| 1.2 | Cost per run | **MEASURED, not estimated.** 232.00 MiB billed per run — matching the dry-run estimate of 231.68 MiB. ~6.8 GiB/month at one run/day, about 0.7% of the 1 TiB monthly free tier. |
| 1.3 | **The table is NOT partitioned** | **CORRECTED ASSUMPTION.** The original design assumed a date predicate would prune partitions. It does not — there are none. Measured: `SELECT *` 252.06 MiB, 9 named columns 231.68 MiB, 9 columns **plus a date predicate** 231.68 MiB. The predicate changes the bill by exactly zero. Column selection is the only lever, and every run is a full scan. |
| 1.4 | The cap now sits at 300 MiB, not 50 | **DELIBERATE.** The original 50 MiB cap was set against the wrong cost model and would have refused every real query. A cap below the only achievable cost is not a safety control, it is a permanent outage. 300 MiB still catches a `SELECT *` regression on a larger table. |
| 1.5 | **The dataset is a frozen archive** | **STATED.** It ends **2024-06-30**. A daily pipeline reading "yesterday" returns zero rows forever while still being billed for the scan. The extractor therefore maps each logical date deterministically onto a day inside the archive span (2013-12-12 … 2024-06-30), so consecutive schedule days read consecutive archive days and re-runs stay idempotent. **The dates in the reports are logical, not the dates the data is from** — `window_date` on every extract result records which archive day was actually read. |
| 1.6 | Empty results | **NOW FATAL.** An extract returning 0 rows raises rather than writing an empty CSV. Being billed for a scan that yields nothing, silently, is the worst of both outcomes. |
| 1.7 | Column names were guessed and wrong | **FIXED.** The schema was written from memory as `bikeid`; the real column is `bike_id`. BigQuery rejected the query. The column list is now verified against the live schema, with a comment saying not to edit it from memory. |
| 1.8 | `gcloud` CLI | **STILL DENIED** in `.claude/settings.json`, so `make secrets-bootstrap` / `make secrets-sync` have never run. Secret Manager integration remains written and unexercised. Credentials reached the pipeline via the key file instead. |
| 1.9 | Report dates vs data dates | **KNOWN COSMETIC GAP.** The PDF header shows the logical date (e.g. 2026-08-27) while the data is from the mapped archive day (e.g. 2016-02-08). Not misleading about the numbers, but a reader would reasonably assume otherwise. The mapping is recorded in `window_date` but not yet surfaced on the page. |

## 2. Security

| # | Item | Status |
|---|---|---|
| 2.1 | AES-256-GCM encryption | **WORKS.** Verified: round-trip, tamper detection, AAD field-binding, nonce uniqueness. |
| 2.2 | **Key management** | **STUB.** The crypto service holds one master key from an env var. No KMS, no HSM, no rotation, no envelope encryption, no key hierarchy. A restart without `CRYPTO_MASTER_KEY` set generates a new key and **all prior ciphertext becomes permanently unreadable**. |
| 2.3 | JWT auth | **WORKS.** RS256, verified by signature + issuer + audience + expiry + nbf against the IdP's JWKS. Algorithm pinned, so `alg:none` and HMAC-confusion attacks fail. Audience grants and scopes enforced. |
| 2.4 | The IdP itself | **STUB.** A local FastAPI service with hardcoded client registrations and dev secrets in `docker-compose.yml`. Not a real identity provider — no user directory, no MFA, no revocation, no refresh tokens, no audit trail. |
| 2.5 | **TLS** | **NOT ENABLED.** Services talk plain HTTP over the Docker network, and clients pass `verify_tls=False`. The requirement says "HTTPS service"; this is HTTP. On a shared network the AES keys in transit would be exposed. Terminating TLS is a config change, not a redesign, but **it has not been done**. |
| 2.6 | Blind indexes | **WORKS, with a stated tradeoff.** Deterministic HMAC-SHA256 lets Hive join on encrypted columns. Determinism *leaks equality*: anyone reading the index can tell which rows share a value, and on a low-cardinality column (`subscriber_type` has 7 values) that enables a dictionary attack. Keyed to prevent offline precomputation, and applied only to `bikeid` and `start_station_name`. |
| 2.7 | Dev secrets in compose | **INSECURE BY DESIGN.** `docker-compose.yml` has literal fallback secrets (`dev-spark-secret` etc.) for local runs. Fine locally, unacceptable anywhere else. |

## 3. Apache Ranger

| # | Item | Status |
|---|---|---|
| 3.1 | Masking policy format | **REAL.** `config/ranger/hive_masking_policies.json` is genuine Ranger policy JSON in the shape the Ranger REST API accepts. |
| 3.2 | **Policy enforcement** | **STUB in every run so far.** `LocalPolicyProvider` reads that JSON and applies Ranger's documented masking semantics locally. The real `RangerAdminProvider` is written and **has never been exercised** — the `ranger` compose profile has not been started (it needs Solr + a Ranger DB + several minutes of startup). Every masked row carries `masked_by = "local-policy-engine"`, which is the distinction being kept honest: query that column to see which engine ran. |
| 3.3 | Ranger Hive plugin | **NOT INSTALLED.** Real Ranger enforces masking *at query time* inside Hive. This pipeline masks *before writing*, which is a stronger guarantee at rest but a different mechanism. A query-time policy change would not retroactively alter already-written rows. |

## 4. Hive and Spark

| # | Item | Status |
|---|---|---|
| 4.1 | Hive tables | **WORKS.** HiveServer2 4.0.0 runs against a Postgres metastore. The external table is created, `MSCK REPAIR` picks up partitions, and `SELECT` returns masked rows through Tez. Verified: 200 rows across 2 partitions. Registration is wired into the mask DAG and is **non-fatal if Hive is down**, because the Parquet is already written and correct. |
| 4.2 | Spark | **WORKS, with a documented fallback.** A real Spark 4.0.0 standalone cluster (1 master, 2 workers, 4 cores) runs the encrypt stage via `pipeline/transform/spark_submit.sh`, which submits **inside the master container** — the host carries Java 26 and Spark 4 needs 17/21, so a host-side submit fails at `JavaSparkContext`. When `SPARK_MASTER_URL` is unset the stage falls back to pyarrow and **says so** in its return value (`engine: pyarrow`). At 100 rows Spark is genuine overhead; the fallback exists so the data path is testable without a cluster. |
| 4.3 | Which engine ran | **RECORDED, NOT ASSUMED.** Every encrypt result carries `engine`; every masked row carries `masked_by`. Neither is inferred. |
| 4.4 | Scale | **UNVERIFIED.** 100 rows/day. Nothing here has been run at volume, and the batched-per-column crypto calls are sized for this, not for millions of rows. |

## 4b. The Airflow scheduler does not run on this host

| # | Item | Status |
|---|---|---|
| 4b.1 | **`airflow scheduler` crashes on macOS** | **BLOCKED, not a pipeline defect.** Airflow 3 removed `SequentialExecutor` and silently substitutes `LocalExecutor`, which forks worker processes. On macOS a forked child that touches CoreFoundation -- which the Google auth libraries do, via keychain lookups -- dies with `SIGSEGV`. Observed 279-502 segfaults per scheduler start. `AIRFLOW__CORE__MP_START_METHOD=spawn` and `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` did not fix it. |
| 4b.2 | What this means | The **DAGs are correct**: `airflow dags test <dag_id>` executes every stage successfully, and the asset wiring is registered and visible in `airflow assets list`. What cannot be demonstrated on this host is the scheduler *automatically* propagating assets between DAGs. On Linux (or Docker) the scheduler runs normally and no code changes. |
| 4b.3 | The workaround | `scripts/run_chain.py` reproduces the asset semantics exactly -- run a stage, verify its output asset materialised, only then run the consumer -- calling the **same stage functions the DAGs call**, so there is no parallel implementation that could drift. This is what has been used to verify the chain end to end. |

## 5. Assumptions taken without confirmation

| # | Assumption |
|---|---|
| 5.1 | GCP project ID is `avapuck`, inferred from the service-account email domain. **Not confirmed** — only the project *number* (58996819807) was supplied. |
| 5.2 | Dataset choice: `bigquery-public-data.austin_bikeshare.bikeshare_trips`. Picked for size, partition-friendliness and having genuinely maskable fields. Not requested by name. |
| 5.3 | Which columns are "sensitive". Station names and bike IDs are treated as PII/quasi-identifiers. Defensible, but a real classification exercise would involve a data steward. |
| 5.4 | The warehouse serves the `analyst` group. The `data_steward` policy path exists in the Ranger config but no run has used it. |
| 5.5 | Stages are chained by **Airflow Assets**, not `TriggerDagRunOperator`. Only `trips_01_extract` has a clock schedule (`0 */4 * * *`); every other stage is scheduled by the asset it consumes. This decouples the stages and makes a manual repair trigger downstream automatically. |
| 5.6 | The archive window mapping gives each 4-hour run its own slice. Six runs/day therefore read six different windows and produce genuinely new rows, rather than re-reading one day six times. |

## 6. What was actually verified

**Live, end to end, on real Austin bikeshare data (2026-08-30):**
- Real BigQuery query executed: 100 rows, 232.00 MiB billed
- All 5 DAGs `state=success` with `EXTRACT_MODE=live`
- Real station names and bike ids encrypted, masked, and loaded
- No real plaintext in Parquet or Postgres — verified by direct search


Stated plainly so the unverified items above are not read as covering everything:

- All 5 Airflow DAGs execute to `state=success` via `airflow dags test`
- Full chain produces CSV → Parquet → masked Parquet → Postgres → PDF
- **No plaintext in Parquet** — asserted programmatically before every write
- **No plaintext in Postgres** — verified by direct query
- Tampered ciphertext is rejected (GCM authentication)
- Ciphertext moved between columns fails to decrypt (AAD binding)
- Unauthenticated and wrong-audience requests are refused
- Load is idempotent: re-running a date gives 0 inserted / 100 updated
- 27 tests pass
- Spark cluster genuinely executes the encrypt stage (`engine: spark`)
- Hive returns masked rows via beeline/Tez (`SELECT count(*)` = 200)
- macOS bind-mount hazard found and fixed: `chown -R` inside a container
  silently empties the mount view; recreating the container restores it

## 7. Not built at all

Backfill tooling · data quality framework (Great Expectations etc.) · lineage
tracking · monitoring and alerting · CI · Kerberos for Hive · HDFS (local
filesystem instead) · multi-tenancy · GDPR erasure workflow · report delivery
(the PDF is written to disk and goes nowhere)
