# Deployment — Kubernetes / minikube

Written to be followed by someone who has never seen this machine. Every step
is verifiable; if a check fails, the fix is next to it.

---

## 1. Prerequisites

| Tool | Minimum | Check |
|---|---|---|
| Docker Desktop | 20 GB memory allocated | `docker info --format '{{.MemTotal}}'` |
| minikube | 1.36+, **matching your CPU arch** | `minikube version` |
| kubectl | 1.30+ | `kubectl version --client` |
| GCP service account | `bigquery.jobUser` + `bigquery.dataViewer` | see §3 |

```bash
make -f Makefile.k8s preflight     # checks all of the above and stops on the first problem
```

> **Apple Silicon:** install minikube via Homebrew (`brew install minikube`).
> The download-page default is an **amd64** binary that runs under Rosetta —
> slow, and it fights the arm64 images. Check with
> `file $(which minikube)`; you want `arm64`.

> **Docker memory:** the stack needs ~12 GB. Docker Desktop defaults to 8 GB.
> Raise it in **Settings → Resources → Memory**, which restarts the daemon and
> all running containers.

---

## 2. Configure for your environment

Everything environment-specific lives in **`k8s/base/02-config.yaml`**:

```yaml
GCP_PROJECT_ID: "your-project-id"        # the ID string, NOT the project number
GCP_SERVICE_ACCOUNT_EMAIL: "sa@your-project.iam.gserviceaccount.com"
BQ_DATASET: "bigquery-public-data.austin_bikeshare"
BQ_TABLE: "bikeshare_trips"
BQ_MAX_BYTES_BILLED: "314572800"         # MEASURE before changing — see §6
```

Get your project ID (the number will not work in the client libraries):

```bash
gcloud projects describe <PROJECT_NUMBER> --format='value(projectId)'
```

---

## 3. Provide the one credential

Everything else is generated. This is the only secret you supply.

```bash
gcloud iam service-accounts keys create secrets/gcp-sa.json \
  --iam-account=<sa-email> --project=<project-id>
chmod 600 secrets/gcp-sa.json
```

Grant **only** `roles/bigquery.jobUser` and `roles/bigquery.dataViewer`. This
pipeline reads one public table; anything broader is exposure with no upside.

`secrets/` is gitignored in full, and `tests/test_secret_hygiene.py` fails the
build if a credential is ever tracked or left world-readable.

---

## 4. Bring it up

```bash
make -f Makefile.k8s up
```

That does, in order: start the cluster (6 CPU / 16 GB) → build four images
**inside minikube's Docker daemon** (so no registry is needed) → generate and
apply secrets → apply manifests → wait for readiness.

First run takes 10–15 minutes, mostly image pulls. Expect:

```
crypto-…              1/1 Running
hive-metastore-…      1/1 Running
hive-metastore-db-0   1/1 Running
hiveserver2-…         1/1 Running
idp-…                 1/1 Running
postgres-0            1/1 Running
spark-master-…        1/1 Running
spark-worker-… (x2)   1/1 Running
```

---

## 5. Run it

```bash
make -f Makefile.k8s run-now     # do not wait for the 4-hourly schedule
make -f Makefile.k8s logs
```

A successful run ends with `CHAIN COMPLETE trace=trips-…`. Every stage logs
that same trace id — see `docs/EXECUTION-FLOW.md`.

```bash
make -f Makefile.k8s verify      # in-cluster security checks
```

---

## 6. Check what a run costs — before it runs

```bash
make -f Makefile.k8s cost
```

BigQuery bills **bytes scanned, not rows returned**; `LIMIT 100` reduces the
bill by zero. A dry run is free and reports exactly what a real run would scan.

For the default dataset that is **~232 MiB per run** (~6.8 GiB/month at
4-hourly, well inside the 1 TiB/month free tier). If you point at a different
table, **measure before setting the cap** — and note that a cap set *below* the
achievable cost is a permanent outage, not a safety control.

---

## 7. Day-two operations

```bash
make -f Makefile.k8s status       # pods, cronjob, recent runs
make -f Makefile.k8s shell-pg     # psql into the warehouse
make -f Makefile.k8s shell-hive   # beeline into HiveServer2
make -f Makefile.k8s ui-spark     # Spark master UI on :8080
make -f Makefile.k8s down         # remove workloads, KEEP data
make -f Makefile.k8s destroy      # delete the cluster entirely
```

Get the PDF out of the cluster:

```bash
POD=$(kubectl -n trips get pods -l app=spark-master -o jsonpath='{.items[0].metadata.name}')
kubectl -n trips cp "$POD:/data/reports" ./reports
```

---

## 8. Failures we have actually hit

Not hypothetical — every row here cost us time.

| Symptom | Cause | Fix |
|---|---|---|
| `spark-master` CrashLoopBackOff, `NumberFormatException` | Kubernetes injects `SPARK_MASTER_PORT=tcp://…` from the same-named Service; Spark parses it as an int | already fixed — the manifest sets `SPARK_MASTER_PORT` explicitly. **Do not remove it.** |
| `ClassNotFoundException: org.postgresql.Driver` | stock `apache/hive` has no Postgres JDBC driver | `docker/Dockerfile.hive.k8s` bakes it in |
| `Read-only file system: '/opt/pipeline/data'` | code wrote next to the source instead of the mounted volume | set `PIPELINE_DATA_ROOT`; already wired |
| `JAVA_GATEWAY_EXITED` | pyspark needs a local JVM even for a *remote* cluster | run the Spark stage from `trips/spark` (has JDK 17) via `k8s/jobs/21-spark-encrypt-job.yaml` |
| `FileNotFoundError: 'docker'` | stage shelled out to `docker exec`; no docker inside a pod | native cluster path preferred in-cluster; docker path kept for laptops |
| `image can't be pulled` | image built on the host daemon, not minikube's | `eval $(minikube -p trips docker-env)` before building |
| `0 rows` from BigQuery | the dataset is a frozen archive ending 2024-06-30 | `window_for()` maps logical dates onto the archive; see `docs/AGENTS.md` |
| Pods `Pending`, `Insufficient memory` | Docker Desktop below 20 GB | raise it in Settings → Resources |

---

## 9. What is NOT deployed

Read `TRUST-BOUNDARY.md` for the full list. The two that will matter first:

- **Apache Ranger admin is not deployed.** Masking uses genuine Ranger policy
  JSON applied by a local engine. Every row records `masked_by` so you can
  always tell which one ran.
- **TLS is not enabled.** In-cluster traffic is plain HTTP. Terminating TLS is
  a configuration change, not a redesign — but it has not been done.
