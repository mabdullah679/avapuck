"""One Airflow DAG per service -- GENERATED FROM CONFIGURATION.

There are four DAGs here and no four blocks of code. Each service's DAG is
built from its binding: the schedule, the auth protocol, the source format and
every mapping come from `config/`, so onboarding a fifth service means adding
one binding file and nothing else. Writing four near-identical DAG modules by
hand would have moved the fragmentation into the orchestration layer, which is
the same failure the platform exists to prevent, one directory over.

Each DAG runs: authenticate -> pull -> parse -> map -> Bronze -> Silver, then
signals the shared Gold assembly. Gold is deliberately NOT per-service: its
whole job is to reconcile across services, so it cannot be built by any one of
them. That is a departure from the brief's per-service "through to Gold"
sketch, and it is recorded in TRUST-BOUNDARY.md.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:                                    # Airflow 3.x
    from airflow.sdk import DAG, task
except ImportError:                     # Airflow 2.x
    from airflow import DAG
    from airflow.decorators import task

from wps.config import load_bundle
from wps.pipeline import SOURCE_PATHS

DEFAULT_ARGS = {
    "owner": "wps-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# Quarterly close, run early enough to leave lead time before the meeting.
SCHEDULE = "0 6 5 1,4,7,10 *"

_bundle = load_bundle()


def build_dag(service_id: str, binding: dict) -> DAG:
    fmt = binding["source"]["format"]
    protocol = binding["auth_profile"].rsplit("/", 1)[-1]

    with DAG(
        dag_id=f"wps_{service_id}",
        description=(f"{binding['service_name']} — {fmt} over "
                     f"{protocol}, mapped to quarterly_performance "
                     f"v{binding['binds_contract_version']}"),
        default_args=DEFAULT_ARGS,
        schedule=SCHEDULE,
        start_date=datetime(2025, 7, 1),
        catchup=False,
        max_active_runs=1,
        tags=["wps", service_id, fmt, binding["binds_contract"]],
    ) as dag:

        @task(task_id="authenticate")
        def authenticate_task():
            """Fails closed. Each service uses a different protocol, declared
            in config/auth/profiles.yaml -- none of it is in this file."""
            from wps.io.auth import authenticate
            cred = authenticate(service_id, load_bundle())
            return cred.redacted()

        @task(task_id="pull")
        def pull_task(cred: dict):
            """In production this fetches the extract using the credential
            above. In the POC the synthetic corpus stands in for the remote
            endpoint; the seam is the same."""
            path = SOURCE_PATHS[service_id]
            if not path.exists():
                raise FileNotFoundError(
                    f"no extract for {service_id} at {path}; "
                    f"run `python -m wps.corpus.generate`")
            return {"path": str(path), "bytes": path.stat().st_size,
                    "auth": cred["protocol"]}

        @task(task_id="parse_and_map")
        def parse_and_map_task(pulled: dict):
            """Parse the native dialect and map it to canonical terms. Both
            steps are driven entirely by the binding."""
            from wps.engine import map_service
            bundle = load_bundle()
            mapped = map_service(service_id, Path(pulled["path"]), bundle)
            errors = sum(len(m.errors) for m in mapped)
            if errors:
                raise ValueError(f"{errors} field-level mapping failures for "
                                 f"{service_id}; see lake/_audit/silver_failures")
            return {"records": len(mapped), "binding_hash": bundle.binding_hashes[service_id]}

        @task(task_id="land_bronze")
        def bronze_task(mapped: dict):
            """Raw, immutable, source fidelity. PCI may land only as ciphertext."""
            from wps.medallion import land_bronze
            return land_bronze(load_bundle())[service_id]

        @task(task_id="conform_silver")
        def silver_task(bronze_count: int):
            """Conformed, quality-enforced, PII and PCI tokenized. A
            classification breach fails the batch rather than warning."""
            from wps.medallion import land_silver
            counts, _, failures = land_silver(load_bundle())
            if failures:
                raise ValueError(f"{len(failures)} rows failed conformance")
            return counts[service_id]

        @task(task_id="signal_gold_ready")
        def signal_task(silver_count: int):
            """Gold reconciles ACROSS services, so no single service DAG can
            build it. This marks this service's contribution complete."""
            return {"service_id": service_id, "silver_rows": silver_count,
                    "contract_version": _bundle.contract_version}

        signal_task(silver_task(bronze_task(parse_and_map_task(pull_task(authenticate_task())))))

    return dag


for _service_id, _binding in sorted(_bundle.bindings.items()):
    globals()[f"wps_{_service_id}"] = build_dag(_service_id, _binding)
