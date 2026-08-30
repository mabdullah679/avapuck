---
name: wps-run
description: Bring up and exercise the WPS POC stack — Delta Lake medallion layers, Airflow DAGs, the grading harness, and the dashboard. Use when asked to run, start, rebuild, or verify the WPS pipeline end to end, or when a loop iteration needs to confirm the happy path still works.
---

# Running the WPS POC

Zero-cost, fully local. Nothing here should ever call a paid cloud service.

## Environment facts (verified 2026-08-29)

| Tool | Version | Note |
|---|---|---|
| Python | 3.14.6 (`/opt/homebrew/bin/python3`) | **Newer than PySpark/delta-spark support.** See below. |
| Java | OpenJDK 26 (Temurin) | Present; Spark needs a JVM. |
| Docker | 29.5.3 | Fallback when the local venv will not resolve. |
| Git | system | Repo is not initialised yet. |

### The Python version trap

Python 3.14 is likely ahead of what `pyspark` and `delta-spark` publish wheels
for. **Do not burn a long unattended stretch fighting a resolver.**

Order of attack, stop at the first that works:

1. Try a venv on the default `python3`. If `pip install` resolves, proceed.
2. If it fails, look for an older interpreter (`ls /opt/homebrew/bin/python3.*`)
   and build the venv on 3.11 or 3.12.
3. If neither, run Spark/Airflow in Docker.
4. If all three fail, **write the blocker into TRUST-BOUNDARY.md and move to
   work that does not need Spark** — the corpus generator, the SSOT schema, the
   mapping config, and the grading harness are all plain Python.

Never leave a loop stuck on dependency resolution. Record and route around.

## Long-running processes

The Airflow scheduler/webserver and Spark jobs do not exit on their own. In an
unattended loop a foreground call blocks until timeout.

- Start them with `run_in_background: true`.
- Poll with `Monitor` (already permitted) rather than a foreground `sleep`.
- Always capture logs to a file so a later iteration can read what happened.

## Layout

```
data/raw/{service_a,service_b,service_c,service_d}/   synthetic source files
config/contracts/                                     SSOT + mapping config
dags/                                                 one DAG per service
lake/{bronze,silver,gold}/                            local Delta tables
harness/                                              A–F grading
dashboard/                                            local dashboard
TRUST-BOUNDARY.md                                     running honesty log
```

## Verifying the happy path

The POC's success criterion is a walkthrough, so the check is end to end:

1. Corpus exists for all four services, in four different formats.
2. Each DAG parses its native format and maps to canonical terms.
3. Bronze → Silver → Gold all populate.
4. Grading harness runs and emits a per-service and overall A–F grade.
5. Dashboard renders, with projections visually distinct from actuals.

A green run that skipped a step is a failed run. Say which step was skipped.

## Loop discipline

- Append to `TRUST-BOUNDARY.md` **as stubs are created**, never reconstruct at
  the end.
- If a mapping is about to be hardcoded, stop — that is the thesis failing.
  Fix the config surface instead.
- Prefer structure and realism over volume in the corpus.
- Scope ends when the happy path runs and the trust boundary is honest.
  Further iteration is scope creep.
