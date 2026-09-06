"""The stage list is the one description of the pipeline. Both runners use it."""
from __future__ import annotations

import ast
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import stages, verdict  # noqa: E402


def test_stage_names_match_the_verdict_stage_list():
    assert stages.STAGE_NAMES == verdict.STAGES


def test_every_stage_has_a_runner():
    assert set(stages.RUNNERS) == set(stages.STAGE_NAMES)


def _dag_task_ids() -> list[str]:
    """Task ids in chain order, read from the DAG file without importing Airflow."""
    src = (ROOT / "dags" / "trips_pipeline_dag.py").read_text()
    tree = ast.parse(src)
    # The chain expression `extract() >> card_split() >> ... >> emit_verdict()`
    # is the last top-level expression inside the `with DAG(...)` block.
    with_block = next(n for n in tree.body if isinstance(n, ast.With))
    chain = with_block.body[-1].value
    names = []
    while isinstance(chain, ast.BinOp):
        names.append(chain.right.func.id)
        chain = chain.left
    names.append(chain.func.id)
    names.reverse()
    # Map each decorated function name to its task_id keyword.
    task_ids = {}
    for fn in with_block.body:
        if isinstance(fn, ast.FunctionDef):
            for dec in fn.decorator_list:
                if isinstance(dec, ast.Call):
                    for kw in dec.keywords:
                        if kw.arg == "task_id":
                            task_ids[fn.name] = kw.value
    out = []
    for fn_name in names:
        node = task_ids[fn_name]
        if isinstance(node, ast.Subscript):          # stages.TASK_ID_FOR["extract"]
            out.append(stages.TASK_ID_FOR[node.slice.value])
        elif isinstance(node, ast.Attribute):        # stages.VERDICT_TASK_ID
            out.append(getattr(stages, node.attr))
        else:
            out.append(node.value)
    return out


def test_dag_chain_is_the_stage_list_plus_the_verdict():
    assert _dag_task_ids() == [t for _, t in stages.STAGES] + [stages.VERDICT_TASK_ID]


def test_runner_walks_the_same_list():
    """The plain runner exposes exactly the stage names as --stage choices."""
    src = (ROOT / "pipeline" / "run_pipeline.py").read_text()
    assert "stages.STAGE_NAMES" in src
    assert "stages.run_stage(" in src
    assert "stages.emit_verdict(" in src


def test_run_env_normalises_to_utc_and_derives_the_trace():
    env = stages.RunEnv.build(Path("/tmp/x"), datetime(2026, 9, 6, 4))
    assert env.run_slot.tzinfo is timezone.utc
    assert env.run_id == "manual__2026-09-06T04:00:00+00:00"
    assert env.trace_id.startswith("trips-20260906T0400-")
    assert env.date.isoformat() == "2026-09-06"


def test_emit_verdict_publishes_even_when_events_are_missing_and_flags_malformed():
    """Silence is the one thing the verdict task must never produce."""
    from pipeline.messaging.contracts import ContractError

    sent = []

    class Pub:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def send(self, topic, key, message): sent.append((topic, key, message))
        def flush(self): pass

    class Signer:
        def sign(self, envelope, payload): return "a.b.c"

    env = stages.RunEnv.build(Path("/tmp/x"), datetime(2026, 9, 6, 4, tzinfo=timezone.utc),
                              run_id="scheduled__x")
    events = {"extract": {"trace_id": "t", "stage": "extract", "rows": "not-an-int"}}
    with pytest.raises(ContractError, match="violated their contract"):
        stages.emit_verdict(env, events, dataset="trips", signer=Signer(), publisher=Pub())
    assert len(sent) == 1
    topic, key, msg = sent[0]
    assert key == "trips"
    assert msg["payload"]["proceed"] is False
    assert any("extract" in r and "contract" in r for r in msg["payload"]["reasons"])


def test_find_input_names_the_directory_it_searched(tmp_path):
    with pytest.raises(FileNotFoundError, match=str(tmp_path)):
        stages.find_input(tmp_path, "*.parquet", "mask")
