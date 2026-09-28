"""The verdict is the platform's single point of correctness. Test it hardest.

Every rule in pipeline/verdict.py has a case here. A change to what "proceed"
means must change a test in this file first.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import verdict as V                              # noqa: E402
from pipeline.messaging import build, EVENT_RUN_VERDICT         # noqa: E402
from pipeline.messaging.contracts import validate_message, validate_stage_event  # noqa: E402
from tests.helpers_events import good_events                    # noqa: E402

NOW = datetime(2026, 9, 6, 4, 9, 0, tzinfo=timezone.utc)


def _events(**overrides):
    ev = good_events()
    for stage, patch in overrides.items():
        if patch is None:
            ev[stage] = None
        else:
            ev[stage] = {**ev[stage], **patch}
    return ev


# ── The happy path ────────────────────────────────────────────────────────

def test_all_stages_ok_is_complete_and_proceeds():
    v = V.compute(_events(), now=NOW)
    assert v.proceed is True
    assert v.status == V.STATUS_COMPLETE
    assert v.reasons == []
    assert all(s["ok"] and s["reported"] for s in v.stages.values())
    assert v.computed_at == "2026-09-06T04:09:00Z"


def test_stage_order_is_the_pipeline_order():
    v = V.compute(_events(), now=NOW)
    assert list(v.stages) == V.STAGES


def test_outputs_point_at_what_was_produced():
    v = V.compute(_events(), now=NOW)
    assert v.outputs == {
        "warehouse_table": "warehouse.trips",
        "hive_table": "trips_warehouse.trips",
        "pdf": "/data/reports/trips_report_20260906T0400Z.pdf",
        "rows_published": 100,
        "rows_loaded": 100,
        "topics": {"encrypted": "rpos_encrypted", "flat": "rpos_flat"},
    }


def test_data_window_comes_from_the_extract_event():
    v = V.compute(_events(), now=NOW)
    assert v.data_window == {"start": "2025-03-02T00:00:00",
                             "end": "2025-03-03T00:00:00",
                             "source": "archive_mapping"}


# ── Degradations: proceed, but say so ─────────────────────────────────────

def test_hive_not_registered_is_degraded_but_proceeds():
    v = V.compute(_events(hive_register={"registered": False, "table": None,
                                         "reason": "HiveServer2 unreachable"}), now=NOW)
    assert v.proceed is True
    assert v.status == V.STATUS_DEGRADED
    assert any("hive table not registered" in r and "HiveServer2" in r for r in v.reasons)
    assert v.stages["hive_register"]["ok"] is False
    assert v.outputs["hive_table"] is None


def test_hive_register_missing_entirely_is_degraded_not_failed():
    v = V.compute(_events(hive_register=None), now=NOW)
    assert v.proceed is True
    assert v.status == V.STATUS_DEGRADED
    assert any("hive_register did not report" in r for r in v.reasons)


def test_bigquery_fallback_to_csv_is_degraded():
    v = V.compute(_events(extract={"fell_back_to_csv": True, "bq_attempts": 3,
                                   "mode": "CsvSourceExtractor"}), now=NOW)
    assert v.proceed is True
    assert v.status == V.STATUS_DEGRADED
    assert any("fell back to CSV after 3" in r for r in v.reasons)


def test_pyarrow_when_spark_configured_is_degraded():
    v = V.compute(_events(encrypt={"engine": "pyarrow", "spark_configured": True}), now=NOW)
    assert v.status == V.STATUS_DEGRADED
    assert any("pyarrow although Spark was configured" in r for r in v.reasons)


def test_pyarrow_when_spark_not_configured_is_complete():
    """No cluster is not a fallback; it is the configuration."""
    v = V.compute(_events(encrypt={"engine": "pyarrow", "spark_configured": False}), now=NOW)
    assert v.status == V.STATUS_COMPLETE
    assert v.reasons == []


def test_failed_delivery_copy_is_degraded():
    v = V.compute(_events(report={"collected": "source missing"}), now=NOW)
    assert v.proceed is True
    assert v.status == V.STATUS_DEGRADED
    assert any("delivery copy failed: source missing" in r for r in v.reasons)


def test_delivery_copy_with_null_is_degraded():
    v = V.compute(_events(report={"collected": None}), now=NOW)
    assert v.status == V.STATUS_DEGRADED


def test_multiple_degradations_are_all_named():
    v = V.compute(_events(
        extract={"fell_back_to_csv": True},
        hive_register={"registered": False, "reason": "down"},
        report={"collected": "read-only"},
    ), now=NOW)
    assert v.proceed is True
    assert v.status == V.STATUS_DEGRADED
    assert len(v.reasons) == 3


# ── Failures: do not proceed ──────────────────────────────────────────────

@pytest.mark.parametrize("stage", sorted(V.REQUIRED_FOR_PROCEED))
def test_any_required_stage_missing_fails(stage):
    v = V.compute(_events(**{stage: None}), now=NOW)
    assert v.proceed is False
    assert v.status == V.STATUS_FAILED
    assert f"{stage} did not complete" in v.reasons
    assert v.stages[stage] == {"ok": False, "reported": False}


def test_nothing_reported_fails_with_every_stage_named():
    v = V.compute({}, now=NOW)
    assert v.proceed is False
    assert v.status == V.STATUS_FAILED
    assert len([r for r in v.reasons if r.endswith("did not complete")]) == len(V.REQUIRED_FOR_PROCEED)
    assert v.outputs["warehouse_table"] is None
    assert v.outputs["rows_loaded"] is None
    assert v.data_window == {"start": None, "end": None, "source": "unknown"}


def test_failed_run_still_reports_the_stages_that_did_run():
    v = V.compute(_events(load=None, report=None), now=NOW)
    assert v.proceed is False
    assert v.stages["extract"]["reported"] is True
    assert v.stages["load"]["reported"] is False
    assert v.outputs["rows_published"] == 100     # publish happened before load failed


def test_a_degradation_never_upgrades_a_failure():
    v = V.compute(_events(load=None, hive_register={"registered": False, "reason": "x"}), now=NOW)
    assert v.status == V.STATUS_FAILED


def test_hive_register_is_the_only_optional_stage():
    """If this changes, docs/DECISIONS.md D4 has to change with it."""
    assert set(V.STAGES) - V.REQUIRED_FOR_PROCEED == {"hive_register"}


# ── Purity and contract ───────────────────────────────────────────────────

def test_compute_is_deterministic_and_does_not_mutate_its_input():
    ev = _events()
    snapshot = {k: dict(v) for k, v in ev.items()}
    a = V.compute(ev, now=NOW).to_payload()
    b = V.compute(ev, now=NOW).to_payload()
    assert a == b
    assert ev == snapshot


def test_verdict_payload_satisfies_the_run_verdict_contract():
    payload = V.compute(_events(), now=NOW).to_payload()
    msg = build(event_type=EVENT_RUN_VERDICT, dataset="trips",
                run_slot=datetime(2026, 9, 6, 4, tzinfo=timezone.utc),
                trace_id="trips-20260906T0400-1a2b3c", run_id="scheduled__x",
                try_number=1, key=None, producer="pipeline", payload=payload,
                signature="header.payload.sig")
    validate_message(msg)


def test_failed_verdict_payload_also_satisfies_the_contract():
    payload = V.compute({}, now=NOW).to_payload()
    msg = build(event_type=EVENT_RUN_VERDICT, dataset="trips",
                run_slot=datetime(2026, 9, 6, 4, tzinfo=timezone.utc),
                trace_id="t", run_id="r", try_number=1, key=None, producer="pipeline",
                payload=payload, signature="x.y.z")
    validate_message(msg)


@pytest.mark.parametrize("stage", V.STAGES)
def test_every_fixture_stage_event_satisfies_the_stage_contract(stage):
    validate_stage_event(good_events()[stage])


# ── A stage that raised is named, with its error ──────────────────────────

def test_a_stage_that_raised_names_its_error_not_merely_incomplete():
    """Seen live 2026-09-08: a contract refusal reached the console as
    "extract did not complete" x7. The error is the reason; the rest of the
    chain never ran and says so."""
    err = ("dataset 'blm' no longer matches its contract config/datasets/blm.json:\n"
           "  column 'card_info': in the contract, missing from the data\n"
           "If the change is intended, update the contract and commit it:\n"
           "  python -m pipeline.metadata.contract accept blm <landed csv>")
    v = V.compute({}, now=NOW, failures={"extract": err})
    assert v.proceed is False and v.status == "failed"
    assert v.reasons[0] == ("extract failed: dataset 'blm' no longer matches its contract "
                            "config/datasets/blm.json: column 'card_info': in the contract, "
                            "missing from the data If the change is intended, update the "
                            "contract and commit it: python -m pipeline.metadata.contract "
                            "accept blm <landed csv>")
    assert v.stages["extract"] == {"ok": False, "reported": False, "error": v.reasons[0][16:]}
    assert v.reasons[1] == "card_split did not complete"
    assert "error" not in v.stages["card_split"]


def test_a_recorded_failure_is_ignored_once_the_stage_reported():
    """A first try that failed and a second that succeeded: the event wins."""
    v = V.compute(_events(), now=NOW, failures={"extract": "try 1 lost the broker"})
    assert v.proceed is True and v.status == "complete" and v.reasons == []
    assert "error" not in v.stages["extract"]


def test_a_failed_hive_registration_is_named_too():
    v = V.compute(_events(hive_register=None), now=NOW,
                  failures={"hive_register": "metastore refused: thrift timeout"})
    assert v.proceed is True                    # still not load-bearing
    assert v.reasons == ["hive_register failed: metastore refused: thrift timeout"]


def test_error_text_is_one_bounded_line():
    v = V.compute({}, now=NOW, failures={"extract": "x\n" * 5000})
    err = v.stages["extract"]["error"]
    assert "\n" not in err and len(err) == V.MAX_ERROR_CHARS and err.endswith("…")


def test_failed_verdict_with_errors_satisfies_the_contract():
    payload = V.compute({}, now=NOW, failures={"extract": "refused"}).to_payload()
    msg = build(event_type=EVENT_RUN_VERDICT, dataset="trips",
                run_slot=datetime(2026, 9, 6, 4, tzinfo=timezone.utc),
                trace_id="t", run_id="r", try_number=1, key=None, producer="pipeline",
                payload=payload, signature="x.y.z")
    validate_message(msg)
