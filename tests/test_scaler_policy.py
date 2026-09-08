"""The scaling rule, exhaustively, without Docker or Spark."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.scaler.policy import ClusterState, Limits, decide, worker_health  # noqa: E402

L = Limits(min_workers=2, max_workers=4, up_threshold=0.8, down_threshold=0.2, cooldown_seconds=120)


def _s(alive=2, total=4, used=0, waiting=0, active=0, scaled=0):
    return ClusterState(alive_workers=alive, total_cores=total, used_cores=used,
                        waiting_apps=waiting, active_apps=active, scaled_workers=scaled)


def test_idle_cluster_holds():
    d = decide(_s(), L, None)
    assert d.action == "hold" and d.utilisation == 0


def test_waiting_app_scales_up():
    d = decide(_s(used=4, waiting=1, active=1), L, None)
    assert d.action == "up" and "waiting" in d.reason


def test_high_utilisation_scales_up():
    d = decide(_s(used=4), L, None)
    assert d.action == "up" and "utilisation 100%" in d.reason


def test_at_max_holds_even_under_load():
    d = decide(_s(alive=4, total=8, used=8, waiting=2), L, None)
    assert d.action == "hold" and "at max" in d.reason


def test_below_min_scales_up_regardless_of_load():
    assert decide(_s(alive=1, total=2), L, None).action == "up"


def test_idle_with_a_scaled_worker_scales_down():
    d = decide(_s(alive=3, total=6, used=0, scaled=1), L, None)
    assert d.action == "down"


def test_never_removes_a_compose_managed_worker():
    """Above min but nothing scaled: nothing to remove."""
    assert decide(_s(alive=3, total=6, used=0, scaled=0), L, None).action == "hold"


def test_does_not_scale_down_while_an_app_runs():
    assert decide(_s(alive=3, total=6, used=1, active=1, scaled=1), L, None).action == "hold"


def test_does_not_scale_down_to_below_min():
    assert decide(_s(alive=2, total=4, used=0, scaled=2), L, None).action == "hold"


def test_cooldown_holds_everything():
    d = decide(_s(used=4, waiting=3), L, seconds_since_last_action=30)
    assert d.action == "hold" and "cooldown" in d.reason
    assert decide(_s(used=4, waiting=3), L, seconds_since_last_action=121).action == "up"


def test_zero_cores_is_zero_utilisation_not_a_crash():
    assert decide(_s(alive=2, total=0, used=0), L, None).utilisation == 0.0


@pytest.mark.parametrize("state,health", [
    ("ALIVE", "healthy"), ("DEAD", "unhealthy"), ("DECOMMISSIONED", "unhealthy"),
    ("DECOMMISSIONING", "warning"), ("UNKNOWN", "warning"), ("", "warning"),
])
def test_worker_health_colours(state, health):
    assert worker_health({"state": state}) == health


def test_cluster_state_reads_the_masters_json():
    from services.scaler.app import cluster_state
    master = {"workers": [
        {"state": "ALIVE", "cores": 2, "coresused": 2},
        {"state": "ALIVE", "cores": 2, "coresused": 0},
        {"state": "DEAD", "cores": 2, "coresused": 0},
    ], "activeapps": [{}], "waitingapps": []}
    s = cluster_state(master, scaled=1)
    assert (s.alive_workers, s.total_cores, s.used_cores, s.active_apps, s.waiting_apps, s.scaled_workers) \
        == (2, 4, 2, 1, 0, 1)
