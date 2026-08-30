"""Gold assembly and grading.

Separate from the per-service DAGs because reconciliation is inherently
cross-service: Gold's job is to put four services' figures beside each other
and name the rule behind each, which no single service's pipeline can do.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from airflow.sdk import DAG, task
except ImportError:
    from airflow import DAG
    from airflow.decorators import task

with DAG(
    dag_id="wps_gold_assembly",
    description="Reconcile all services into Gold, project forward, grade the result",
    default_args={"owner": "wps-platform", "retries": 1,
                  "retry_delay": timedelta(minutes=10)},
    schedule="0 8 5 1,4,7,10 *",
    start_date=datetime(2025, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["wps", "gold", "quarterly_performance"],
) as dag:

    @task(task_id="build_gold")
    def build_gold_task():
        from wps.config import load_bundle
        from wps.medallion import AS_OF, _write, build_gold, land_silver
        from wps.projections import project
        bundle = load_bundle()
        _, rows, _ = land_silver(bundle)
        gold, flags = build_gold(bundle, rows)
        proj = project(gold, AS_OF)
        cols = sorted({k for r in gold + proj for k in r})
        _write("gold/quarterly_performance",
               [{c: r.get(c) for c in cols} for r in gold + proj])
        if flags:
            _write("_audit/reconciliation_flags", flags)
        return {"actual": len(gold), "projected": len(proj), "flags": len(flags)}

    @task(task_id="grade")
    def grade_task(built: dict):
        """Grade against the independent expected-value SSOT. A run that
        cannot be graded is not a green run."""
        import subprocess
        r = subprocess.run([sys.executable, str(ROOT / "harness" / "grade.py")],
                           capture_output=True, text=True)
        print(r.stdout)
        if r.returncode != 0:
            raise ValueError("grading harness returned below a B grade")
        return built

    grade_task(build_gold_task())
