"""Spark worker scaler: grows and shrinks spark-worker containers on load.

THE ONE SERVICE WITH THE DOCKER SOCKET (docs/DECISIONS.md D17). That socket
is root on the host, which is why nothing else gets it, why this process does
nothing but scale one service, and why TRUST-BOUNDARY says so in plain words.
A real deployment replaces this with the orchestrator's own autoscaler.

Every SCALER_POLL_SECONDS it reads the Spark master's /json/ status, applies
services/scaler/policy.py, and acts:

  up    clones an existing spark-worker container -- same image, command,
        environment, binds, network, memory limit -- labelled avapuck.scaled
  down  removes the youngest avapuck.scaled worker, never a compose-managed one

/status is what the console's Spark tab shows: every worker with a colour
class, the last decision, and what the scaler is allowed to do. POST /scale
sets a target by hand.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from services.scaler.policy import ClusterState, Decision, Limits, decide, worker_health

log = logging.getLogger("scaler")
logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")

MASTER_UI = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080").rstrip("/")
POLL = float(os.environ.get("SCALER_POLL_SECONDS", "15"))
LIMITS = Limits(
    min_workers=int(os.environ.get("SPARK_WORKERS_MIN", "2")),
    max_workers=int(os.environ.get("SPARK_WORKERS_MAX", "6")),
    up_threshold=float(os.environ.get("SCALER_UP_THRESHOLD", "0.8")),
    down_threshold=float(os.environ.get("SCALER_DOWN_THRESHOLD", "0.2")),
    cooldown_seconds=int(os.environ.get("SCALER_COOLDOWN_SECONDS", "120")),
)
SERVICE_LABEL = "com.docker.compose.service"
WORKER_SERVICE = os.environ.get("SPARK_WORKER_SERVICE", "spark-worker")
SCALED_LABEL = "avapuck.scaled"
ENABLED = os.environ.get("SCALER_ENABLED", "true").lower() in ("1", "true", "yes")
# Observed on the first live start: the scaler read the master before the
# compose workers had registered, saw fewer than min, and cloned one. Hold
# every action for this long after start so the cluster settles first.
STARTUP_GRACE = float(os.environ.get("SCALER_STARTUP_GRACE_SECONDS", "120"))
STARTED_AT = time.monotonic()

app = FastAPI(title="Spark scaler", version="1.0.0")


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.master: dict = {}
        self.master_error: str | None = None
        self.docker_error: str | None = None
        self.last_action_at: float | None = None
        self.last_decision: Decision | None = None
        self.history: list[dict] = []
        self.containers: list[dict] = []
        self.target: int | None = None       # manual target, if any

    def record(self, decision: Decision, acted: bool, detail: str = "") -> None:
        with self.lock:
            self.last_decision = decision
            self.history.insert(0, {"at": datetime.now(timezone.utc).isoformat(),
                                    "action": decision.action, "reason": decision.reason,
                                    "utilisation": round(decision.utilisation, 3),
                                    "acted": acted, "detail": detail})
            del self.history[50:]
            if acted:
                self.last_action_at = time.monotonic()


STATE = State()


# ── Spark master ──────────────────────────────────────────────────────────

def read_master() -> dict:
    r = requests.get(f"{MASTER_UI}/json/", timeout=5)
    r.raise_for_status()
    return r.json()


def cluster_state(master: dict, scaled: int) -> ClusterState:
    workers = master.get("workers", [])
    alive = [w for w in workers if str(w.get("state", "")).upper() == "ALIVE"]
    return ClusterState(
        alive_workers=len(alive),
        total_cores=sum(int(w.get("cores", 0)) for w in alive),
        used_cores=sum(int(w.get("coresused", 0)) for w in alive),
        waiting_apps=len(master.get("waitingapps", []) or []),
        active_apps=len(master.get("activeapps", []) or []),
        scaled_workers=scaled,
    )


# ── Docker ────────────────────────────────────────────────────────────────

def _docker():
    import docker
    return docker.from_env()


def worker_containers(client) -> list:
    return client.containers.list(all=True, filters={"label": f"{SERVICE_LABEL}={WORKER_SERVICE}"})


def scaled_containers(client) -> list:
    return [c for c in worker_containers(client) if c.labels.get(SCALED_LABEL) == "true"]


def clone_worker(client) -> str:
    """Start one more worker with exactly the configuration of an existing one."""
    templates = [c for c in worker_containers(client) if c.labels.get(SCALED_LABEL) != "true"]
    if not templates:
        raise RuntimeError("no compose-managed spark-worker container to clone from")
    tmpl = templates[0]
    cfg, host = tmpl.attrs["Config"], tmpl.attrs["HostConfig"]
    networks = list((tmpl.attrs.get("NetworkSettings") or {}).get("Networks", {}))
    keep = ("com.docker.compose.project", "com.docker.compose.project.working_dir", SERVICE_LABEL)
    labels = {k: v for k, v in (cfg.get("Labels") or {}).items() if k in keep}
    labels[SCALED_LABEL] = "true"
    name = f"pl-spark-worker-scaled-{int(time.time())}"
    client.containers.run(
        image=cfg["Image"], command=cfg.get("Cmd"), entrypoint=cfg.get("Entrypoint"),
        environment=cfg.get("Env"), working_dir=cfg.get("WorkingDir") or None,
        user=cfg.get("User") or None, volumes=host.get("Binds") or None,
        mem_limit=host.get("Memory") or None, network=networks[0] if networks else None,
        labels=labels, name=name, detach=True,
        restart_policy={"Name": "unless-stopped"},
    )
    return name


def remove_youngest_scaled(client) -> str:
    scaled = sorted(scaled_containers(client), key=lambda c: c.attrs["Created"], reverse=True)
    if not scaled:
        raise RuntimeError("no scaled worker to remove")
    victim = scaled[0]
    victim.stop(timeout=30)
    victim.remove()
    return victim.name


# ── The loop ──────────────────────────────────────────────────────────────

def tick() -> None:
    try:
        master = read_master()
        STATE.master, STATE.master_error = master, None
    except Exception as e:  # noqa: BLE001
        STATE.master_error = str(e)[:200]
        return

    try:
        client = _docker()
        scaled = scaled_containers(client)
        STATE.containers = [{"name": c.name, "status": c.status,
                             "scaled": c.labels.get(SCALED_LABEL) == "true"}
                            for c in worker_containers(client)]
        STATE.docker_error = None
    except Exception as e:  # noqa: BLE001
        STATE.docker_error = str(e)[:200]
        return

    state = cluster_state(master, len(scaled))
    since = None if STATE.last_action_at is None else time.monotonic() - STATE.last_action_at

    if STATE.target is not None:
        # A manual target overrides the policy until reached.
        if state.alive_workers < STATE.target:
            decision = Decision("up", f"manual target {STATE.target}", 0.0)
        elif state.alive_workers > STATE.target and scaled:
            decision = Decision("down", f"manual target {STATE.target}", 0.0)
        else:
            STATE.target = None
            decision = Decision("hold", "manual target reached", 0.0)
    else:
        decision = decide(state, LIMITS, since)

    if not ENABLED and decision.action != "hold":
        STATE.record(Decision("hold", f"scaler disabled; would {decision.action}: {decision.reason}",
                              decision.utilisation), acted=False)
        return
    uptime = time.monotonic() - STARTED_AT
    if decision.action != "hold" and uptime < STARTUP_GRACE:
        STATE.record(Decision("hold", f"startup grace: {int(STARTUP_GRACE - uptime)}s left "
                                      f"(would {decision.action}: {decision.reason})",
                              decision.utilisation), acted=False)
        return
    try:
        if decision.action == "up":
            name = clone_worker(client)
            STATE.record(decision, acted=True, detail=f"started {name}")
            log.info("scaled UP: %s (%s)", name, decision.reason)
        elif decision.action == "down":
            name = remove_youngest_scaled(client)
            STATE.record(decision, acted=True, detail=f"removed {name}")
            log.info("scaled DOWN: %s (%s)", name, decision.reason)
        else:
            STATE.record(decision, acted=False)
    except Exception as e:  # noqa: BLE001
        STATE.docker_error = str(e)[:200]
        STATE.record(decision, acted=False, detail=f"failed: {e}")
        log.error("ALERT kind=scaler_action_failed action=%s error=%s", decision.action, e)


def loop_forever() -> None:
    while True:
        try:
            tick()
        except Exception as e:  # noqa: BLE001
            log.error("scaler tick failed: %s", e)
        time.sleep(POLL)


@app.on_event("startup")
def _start() -> None:
    threading.Thread(target=loop_forever, name="scaler", daemon=True).start()


class ScaleRequest(BaseModel):
    workers: int = Field(ge=0, le=64)


@app.post("/scale")
def scale(req: ScaleRequest) -> dict:
    if req.workers < LIMITS.min_workers or req.workers > LIMITS.max_workers:
        raise HTTPException(400, f"target must be within {LIMITS.min_workers}..{LIMITS.max_workers}")
    STATE.target = req.workers
    return {"target": req.workers}


@app.get("/status")
def status() -> dict:
    with STATE.lock:
        workers = []
        for w in STATE.master.get("workers", []):
            workers.append({
                "id": w.get("id"), "host": w.get("host"), "state": w.get("state"),
                "health": worker_health(w), "cores": w.get("cores"), "cores_used": w.get("coresused"),
                "memory_mb": w.get("memory"), "memory_used_mb": w.get("memoryused"),
                "last_heartbeat": w.get("lastheartbeat"),
            })
        alive = [w for w in workers if w["health"] == "healthy"]
        return {
            "enabled": ENABLED,
            "socket_mounted": STATE.docker_error is None,
            "limits": LIMITS.__dict__,
            "master": {"url": STATE.master.get("url"), "status": STATE.master.get("status"),
                       "alive_workers": len(alive),
                       "cores": sum(int(w["cores"] or 0) for w in alive),
                       "cores_used": sum(int(w["cores_used"] or 0) for w in alive),
                       "active_apps": len(STATE.master.get("activeapps", []) or []),
                       "waiting_apps": len(STATE.master.get("waitingapps", []) or []),
                       "error": STATE.master_error},
            "workers": workers,
            "containers": STATE.containers,
            "docker_error": STATE.docker_error,
            "manual_target": STATE.target,
            "last_decision": STATE.last_decision.__dict__ if STATE.last_decision else None,
            "history": STATE.history[:20],
        }


@app.get("/healthz")
def healthz() -> dict:
    if STATE.master_error:
        raise HTTPException(503, f"spark master unreachable: {STATE.master_error}")
    return {"healthy": True, "docker": STATE.docker_error is None}
