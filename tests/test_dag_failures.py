"""A stage that raises leaves its reason behind, and a refusal is not retried.

Seen live 2026-09-08: a console upload hit the dataset contract gate. The
gate answered in 0.06s; Airflow retried it twice with backoff, the run went
red after eight minutes, and the verdict said "extract did not complete"
with no trace of the actual sentence. These tests hold the DAG's task
wrapper to the fix: record to XCom first, then fail fast on a decision and
re-raise unchanged on anything a retry might cure.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import stages                                    # noqa: E402
from pipeline.common.trace import stage_end, stage_start        # noqa: E402
from pipeline.metadata.contract import ContractMismatch         # noqa: E402


class AirflowFailException(Exception):
    """Stands in for airflow.exceptions.AirflowFailException: fail, no retry."""


def _dag_helpers() -> dict:
    """The DAG's task wrapper and XCom helpers, without importing Airflow."""
    src = (ROOT / "dags" / "trips_pipeline_dag.py").read_text()
    start = src.index("FAILURE_XCOM_KEY = ")
    end = src.index("with DAG(")
    ns = {"datetime": datetime, "timedelta": timedelta, "timezone": timezone, "Path": Path,
          "stages": stages, "stage_start": stage_start, "stage_end": stage_end,
          "log": logging.getLogger("test.dag"), "DATA": Path("/tmp/avapuck-test-data"),
          "AirflowFailException": AirflowFailException}
    exec(compile(src[start:end], "trips_pipeline_dag", "exec"), ns)
    return ns


class TI:
    """The slice of a task instance the wrapper touches."""
    def __init__(self, store: dict | None = None):
        self.task_id = "extract_to_csv"
        self.try_number = 1
        self.pushed: list[tuple[str, dict]] = []
        self.store = store or {}

    def xcom_push(self, key, value):
        self.pushed.append((key, value))

    def xcom_pull(self, task_ids=None, key="return_value"):
        return [self.store.get((t, key)) for t in task_ids]


class DagRun:
    run_id = "upload__abc__1"
    conf = {"csv_path": "/data/incoming/abc_x.csv", "dataset": "blm", "row_limit": 0}


def _ctx(ti=None):
    return {"task_instance": ti or TI(), "dag_run": DagRun(),
            "logical_date": datetime(2026, 9, 8, 21, 45, tzinfo=timezone.utc)}


def test_a_contract_refusal_is_recorded_then_fails_without_retry(monkeypatch):
    ns = _dag_helpers()
    exc = ContractMismatch("blm", Path("/opt/pipeline/config/datasets/blm.json"),
                           ["column 'card_info': in the contract, missing from the data"])
    monkeypatch.setattr(stages, "run_stage", lambda *a, **k: (_ for _ in ()).throw(exc))
    ti = TI()
    with pytest.raises(AirflowFailException) as info:
        ns["_run"]("extract", _ctx(ti))
    assert "card_info" in str(info.value) and info.value.__cause__ is exc
    assert ti.pushed == [(ns["FAILURE_XCOM_KEY"], stages.failure_record("extract", exc))]
    assert ti.pushed[0][1]["retryable"] is False


def test_an_infrastructure_error_is_recorded_then_left_for_airflow_to_retry(monkeypatch):
    ns = _dag_helpers()
    exc = ConnectionError("kafka: broker unreachable")
    monkeypatch.setattr(stages, "run_stage", lambda *a, **k: (_ for _ in ()).throw(exc))
    ti = TI()
    with pytest.raises(ConnectionError) as info:
        ns["_run"]("publish", _ctx(ti))
    assert info.value is exc                       # unchanged, so retries apply
    assert ti.pushed[0][1]["retryable"] is True


def test_success_records_nothing_and_returns_the_event(monkeypatch):
    ns = _dag_helpers()
    ev = {"trace_id": "t", "stage": "extract", "rows": 1}
    monkeypatch.setattr(stages, "run_stage", lambda *a, **k: ev)
    ti = TI()
    assert ns["_run"]("extract", _ctx(ti)) is ev
    assert ti.pushed == []


def test_recording_failure_never_hides_the_original_error(monkeypatch):
    ns = _dag_helpers()
    exc = RuntimeError("the real problem")
    monkeypatch.setattr(stages, "run_stage", lambda *a, **k: (_ for _ in ()).throw(exc))

    class BrokenTI(TI):
        def xcom_push(self, key, value):
            raise OSError("api server away")

    with pytest.raises(RuntimeError, match="the real problem"):
        ns["_run"]("load", _ctx(BrokenTI()))


def test_pull_failures_keys_by_stage_and_skips_absent_stages():
    ns = _dag_helpers()
    rec = stages.failure_record("extract", ContractMismatch("blm", Path("c.json"), ["x"]))
    ti = TI(store={("extract_to_csv", ns["FAILURE_XCOM_KEY"]): rec,
                   ("publish_kafka", ns["FAILURE_XCOM_KEY"]): "not a record"})
    got = ns["_pull_failures"](_ctx(ti))
    assert got == {"extract": rec}


def test_the_verdict_task_passes_failures_through():
    src = (ROOT / "dags" / "trips_pipeline_dag.py").read_text()
    assert "stages.emit_verdict(env, _pull_events(ctx), failures=_pull_failures(ctx))" in src
