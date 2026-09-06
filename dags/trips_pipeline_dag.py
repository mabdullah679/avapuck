"""Trips pipeline — one DAG, eight stages and a verdict, every 4 hours.

STRUCTURE
=========

A single DAG owns the whole run. Each task consumes what the one before it
wrote, so the `>>` chain IS the data dependency -- there is no ordering here
that could be violated and still produce a correct result.

    schedule="0 */4 * * *"
    ──────▶ extract ──▶ card_split ──▶ encrypt ──▶ publish ──▶ mask
                                                                │
       emit_verdict ◀── report ◀── load ◀── register ◀──────────┘
       (all_done)

  extract        100 rows from BigQuery (or a CSV source) -> data/csv, held
                 to the dataset's contract
  card_split     PANs -> prefix / encrypted middle / suffix -> data/cards
  encrypt        AES-256-GCM on every sensitive column -> data/parquet
  publish        each row to two Kafka topics, in the platform envelope
  mask           decrypt, apply Ranger policy, write masked -> data/hive
  register       expose the masked Parquet as a Hive external table
  load           upsert into Postgres, stamped with this run's slot
  report         one PDF for THIS run, with the verdict so far
  emit_verdict   the signed run.verdict on the control topic -- ALWAYS

THE STAGE LIST LIVES IN pipeline/stages.py
------------------------------------------
This file owns orchestration: the schedule, retries, XCom, the UI logging.
What each stage does, with which inputs, is `pipeline.stages`, which the
plain runner (pipeline/run_pipeline.py) walks too. A test asserts this DAG's
task ids match that list, so the two cannot drift (docs/DECISIONS.md D14).

WHY ONE DAG RATHER THAN SIX CHAINED BY ASSETS
---------------------------------------------
An earlier version ran six separate DAGs scheduled on Airflow Assets, so a
manual repair of one stage cascaded downstream on its own. That was traded
for a single graph that shows the whole run, triggers once, and backfills
once. The cost: re-running `mask` by hand no longer makes `load` follow --
clear the downstream tasks too. The asset cascade returns at the team
boundary instead: the consumer DAG is asset-scheduled on the control topic.

IDEMPOTENCY
-----------
Every stage keys off the run's slot. The extractor maps that slot to a fixed
4-hour archive window, so re-running the 08:00 slice always reads the same
rows; the Postgres load upserts on the dataset's key; the Kafka envelope
carries an idempotency key. Re-running any run replaces its data rather than
duplicating it.

FAILURE SEMANTICS
-----------------
A failed task stops the chain -- nothing downstream processes stale data --
but `emit_verdict` still runs (trigger_rule=all_done) and publishes
proceed=false. The whole run is bounded by dagrun_timeout, and every task
failure logs a structured ALERT line. So: stuck, failed and absent are three
different signals, and the watchdog outside Airflow watches for absent.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Airflow runs tasks in a subprocess that does not inherit the shell's
# environment reliably, and GOOGLE_APPLICATION_CREDENTIALS in .env.local is a
# RELATIVE path -- which resolves against the worker's cwd, not the repo. Both
# are fixed here, once, at parse time, for runs outside Docker. In the
# compose stack every variable arrives through the container environment.
try:
    from dotenv import dotenv_values
    for _k, _v in dotenv_values(ROOT / ".env.local").items():
        if _v and _k not in os.environ:
            os.environ[_k] = _v
    _cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if _cred and not os.path.isabs(_cred):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(ROOT / _cred)
except ImportError:
    pass

from airflow.sdk import DAG, task

from pipeline import stages
from pipeline.common.trace import stage_end, stage_start

# Task logs go to the Airflow UI; this is the logger every stage writes to.
log = logging.getLogger("airflow.task")

DATA = Path(os.environ.get("PIPELINE_DATA_ROOT") or (ROOT / "data"))
SCHEDULE = "0 */4 * * *"


def _alert_on_failure(context):
    """One greppable line per task failure. The watchdog covers absence; this
    covers loud failure, and the two must not be confused with each other."""
    ti = context.get("task_instance")
    log.error("ALERT kind=task_failed dag=%s task=%s run_id=%s try=%s exception=%s",
              getattr(ti, "dag_id", "?"), getattr(ti, "task_id", "?"),
              getattr(ti, "run_id", "?"), getattr(ti, "try_number", "?"),
              str(context.get("exception"))[:300])


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
    "email_on_failure": False,
    "on_failure_callback": _alert_on_failure,
}


def logical_ts(ctx: dict) -> datetime:
    """The run's logical timestamp, however Airflow chose to supply it.

    `ctx["logical_date"]` is NOT always present. A run triggered from the UI
    Run button (or `airflow dags trigger`) on Airflow 3 arrives without it,
    while `airflow dags test` supplies it -- so a KeyError here fails only on
    the UI path, then sits in `up_for_retry` for the retry delay looking like
    the scheduler is stuck. Fall back the same way trace_from_context does.
    Always returns an aware UTC datetime.
    """
    ts = (ctx.get("logical_date")
          or ctx.get("data_interval_start")
          or (ctx.get("dag_run") and getattr(ctx["dag_run"], "logical_date", None))
          or (ctx.get("dag_run") and getattr(ctx["dag_run"], "run_after", None)))
    if ts is None:
        ts = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def run_env(ctx: dict) -> stages.RunEnv:
    """Everything a stage needs, derived from the Airflow context once."""
    dag_run = ctx.get("dag_run")
    ti = ctx.get("task_instance") or ctx.get("ti")
    return stages.RunEnv.build(
        data_root=DATA,
        run_slot=logical_ts(ctx),
        run_id=getattr(dag_run, "run_id", None),
        try_number=int(getattr(ti, "try_number", 1) or 1),
    )


def _run(stage: str, ctx: dict, events: dict | None = None) -> dict:
    """Run one stage with UI banners around it and return its event to XCom."""
    env = run_env(ctx)
    t0 = stage_start(stage, env.trace_id)
    ev = stages.run_stage(stage, env, events or {})
    stage_end(stage, env.trace_id, t0,
              **{k: v for k, v in ev.items() if k not in ("trace_id", "stage")})
    return ev


def _pull_events(ctx: dict, upto: str | None = None) -> dict:
    """Every stage's event so far, keyed by stage name. None where absent."""
    ti = ctx["task_instance"] if "task_instance" in ctx else ctx["ti"]
    names = []
    for name, _ in stages.STAGES:
        if name == upto:
            break
        names.append(name)
    pulled = ti.xcom_pull(task_ids=[stages.TASK_ID_FOR[n] for n in names]) or []
    return {n: (ev if isinstance(ev, dict) else None) for n, ev in zip(names, pulled)}


with DAG(
    dag_id="trips_pipeline",
    description=("BigQuery/CSV -> card split -> encrypt -> publish -> mask -> load "
                 "-> PDF -> signed verdict, every 4 hours"),
    schedule=SCHEDULE,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Stuck is not absent. A run that outlives this is killed and its verdict
    # says proceed=false; a run that never starts is the watchdog's to notice.
    dagrun_timeout=timedelta(hours=3),
    tags=["trips", "pii", "4-hourly"],
) as dag:

    @task(task_id=stages.TASK_ID_FOR["extract"], execution_timeout=timedelta(minutes=10))
    def extract(**ctx):
        return _run("extract", ctx)

    @task(task_id=stages.TASK_ID_FOR["card_split"])
    def card_split(**ctx):
        return _run("card_split", ctx)

    @task(task_id=stages.TASK_ID_FOR["encrypt"])
    def encrypt(**ctx):
        return _run("encrypt", ctx)

    @task(task_id=stages.TASK_ID_FOR["publish"])
    def publish(**ctx):
        return _run("publish", ctx)

    @task(task_id=stages.TASK_ID_FOR["mask"])
    def mask(**ctx):
        return _run("mask", ctx)

    @task(task_id=stages.TASK_ID_FOR["hive_register"])
    def register(**ctx):
        return _run("hive_register", ctx)

    @task(task_id=stages.TASK_ID_FOR["load"])
    def load(**ctx):
        return _run("load", ctx, _pull_events(ctx, upto="load"))

    @task(task_id=stages.TASK_ID_FOR["report"])
    def report(**ctx):
        return _run("report", ctx, _pull_events(ctx, upto="report"))

    @task(task_id=stages.VERDICT_TASK_ID, trigger_rule="all_done", retries=1)
    def emit_verdict(**ctx):
        """The signed yes/no on the control topic. Runs whatever happened
        upstream, so downstream always hears something and the watchdog can
        treat silence as the one remaining failure mode."""
        env = run_env(ctx)
        t0 = stage_start("verdict", env.trace_id)
        result = stages.emit_verdict(env, _pull_events(ctx))
        stage_end("verdict", env.trace_id, t0, proceed=result["proceed"],
                  status=result["status"], event_id=result["event_id"],
                  topic=result["topic"], reasons=len(result["verdict"]["reasons"]))
        return result

    (
        extract()
        >> card_split()
        >> encrypt()
        >> publish()
        >> mask()
        >> register()
        >> load()
        >> report()
        >> emit_verdict()
    )
