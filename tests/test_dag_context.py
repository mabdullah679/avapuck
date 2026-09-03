"""The DAG must work however Airflow supplies the run timestamp.

`airflow dags test` puts `logical_date` in the task context; a run triggered
from the UI Run button (or `airflow dags trigger`) on Airflow 3 does not.
Reading `ctx["logical_date"]` directly therefore failed ONLY on the UI path,
and the task then sat in `up_for_retry` for the retry delay -- which looks
like a stuck scheduler rather than a bug.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _logical_ts():
    """Import the helper without importing Airflow itself."""
    import importlib.util
    src = (ROOT / "dags" / "trips_pipeline_dag.py").read_text()
    start = src.index("def logical_ts(ctx: dict):")
    end = src.index("def _crypto(", start)
    ns = {}
    exec(compile(src[start:end], "logical_ts", "exec"), ns)
    return ns["logical_ts"]


class TestLogicalTs:
    def test_uses_logical_date_when_present(self):
        ts = datetime(2026, 9, 13, tzinfo=timezone.utc)
        assert _logical_ts()({"logical_date": ts}) == ts

    def test_falls_back_to_data_interval_start(self):
        """The UI Run button path: no logical_date in the context."""
        ts = datetime(2026, 9, 13, tzinfo=timezone.utc)
        assert _logical_ts()({"data_interval_start": ts}) == ts

    def test_falls_back_to_dag_run_attributes(self):
        ts = datetime(2026, 9, 13, tzinfo=timezone.utc)

        class DagRun:
            logical_date = ts

        assert _logical_ts()({"dag_run": DagRun()}) == ts

    def test_never_raises_on_an_empty_context(self):
        """A missing timestamp must not KeyError -- that failure mode cost a
        whole retry cycle and looked like a scheduler problem."""
        got = _logical_ts()({})
        assert isinstance(got, datetime)

    def test_no_task_reads_logical_date_directly(self):
        """Guard against the bug being reintroduced in a new task."""
        src = (ROOT / "dags" / "trips_pipeline_dag.py").read_text()
        code = "\n".join(l for l in src.split("\n")
                         if not l.strip().startswith("#")
                         and '`ctx["logical_date"]`' not in l)
        assert 'ctx["logical_date"]' not in code, (
            'a task reads ctx["logical_date"] directly; use logical_ts(ctx) '
            "so the UI Run button path does not KeyError")
