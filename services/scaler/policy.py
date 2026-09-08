"""The scaling rule, as a pure function. Tested; the app only applies it.

Inputs are what the Spark master reports about itself plus what the scaler
did last. Output is one of up, down, hold, with the reason.

  up    when an application is waiting for resources, or core utilisation is
        at or above the up threshold, and we are below max
  down  when utilisation is at or below the down threshold, nothing is
        running, we are above min, and there is a scaled worker to remove
        (compose-managed originals are never removed)
  hold  otherwise, and always inside the cooldown after an action, so the
        cluster settles before the next decision
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ClusterState:
    alive_workers: int
    total_cores: int
    used_cores: int
    waiting_apps: int
    active_apps: int
    scaled_workers: int          # workers this scaler created, still running


@dataclass(frozen=True)
class Limits:
    min_workers: int = 2
    max_workers: int = 6
    up_threshold: float = 0.8
    down_threshold: float = 0.2
    cooldown_seconds: int = 120


@dataclass(frozen=True)
class Decision:
    action: str          # up | down | hold
    reason: str
    utilisation: float


def utilisation(state: ClusterState) -> float:
    return (state.used_cores / state.total_cores) if state.total_cores else 0.0


def decide(state: ClusterState, limits: Limits, seconds_since_last_action: float | None) -> Decision:
    util = utilisation(state)
    if seconds_since_last_action is not None and seconds_since_last_action < limits.cooldown_seconds:
        left = int(limits.cooldown_seconds - seconds_since_last_action)
        return Decision("hold", f"cooldown: {left}s left after the last action", util)

    if state.alive_workers < limits.min_workers:
        return Decision("up", f"{state.alive_workers} alive < min {limits.min_workers}", util)

    wants_up = state.waiting_apps > 0 or util >= limits.up_threshold
    if wants_up and state.alive_workers < limits.max_workers:
        why = (f"{state.waiting_apps} app(s) waiting for resources" if state.waiting_apps
               else f"utilisation {util:.0%} >= {limits.up_threshold:.0%}")
        return Decision("up", why, util)
    if wants_up:
        return Decision("hold", f"load high but at max {limits.max_workers}", util)

    if (util <= limits.down_threshold and state.active_apps == 0
            and state.alive_workers > limits.min_workers and state.scaled_workers > 0):
        return Decision("down", f"utilisation {util:.0%} <= {limits.down_threshold:.0%} and idle", util)

    return Decision("hold", "within thresholds", util)


def worker_health(worker: dict) -> str:
    """Colour class for one worker from the master's own state string."""
    state = str(worker.get("state", "")).upper()
    if state == "ALIVE":
        return "healthy"
    if state in ("DEAD", "DECOMMISSIONED", "KILLED"):
        return "unhealthy"
    if state in ("DECOMMISSIONING", "UNKNOWN", ""):
        return "warning"
    return "warning"
