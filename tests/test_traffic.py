"""The traffic auditor detects and blocks against the written profile."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.common import traffic as T   # noqa: E402


def _profile(tmp_path, **over):
    doc = {
        "window_seconds": 60, "block_seconds": 300, "allowlist": ["10.0.0.0/8"],
        "categories": {
            "auth": {"match": ["POST /api/login"], "per_client_per_window": 3, "total_per_window": 5},
            "read": {"match": ["GET /api/"], "per_client_per_window": 100, "total_per_window": 1000},
        },
        "default": {"per_client_per_window": 50, "total_per_window": 500},
    }
    doc.update(over)
    p = tmp_path / "profile.json"
    p.write_text(json.dumps(doc))
    return T.TrafficProfile.load(p, allowlist_env="")


class Clock:
    def __init__(self): self.t = 1000.0
    def __call__(self): return self.t


def test_shipped_profile_loads_and_classifies():
    prof = T.TrafficProfile.load(ROOT / "config" / "traffic_profile.json", allowlist_env="")
    assert prof.classify("POST", "/api/login").name == "auth"
    assert prof.classify("POST", "/api/upload").name == "upload"
    assert prof.classify("GET", "/api/runs").name == "read"
    assert prof.classify("GET", "/").name == "static"
    assert prof.classify("DELETE", "/nowhere").name == "default"


def test_env_allowlist_is_added(monkeypatch, tmp_path):
    prof = _profile(tmp_path)
    assert prof.is_allowlisted("10.1.2.3") and not prof.is_allowlisted("8.8.8.8")
    p = tmp_path / "profile.json"
    prof2 = T.TrafficProfile.load(p, allowlist_env="172.16.0.0/12, 192.168.1.5")
    assert prof2.is_allowlisted("172.20.0.9") and prof2.is_allowlisted("192.168.1.5")
    assert not prof2.is_allowlisted("not-an-ip")


def test_per_client_breach_blocks_that_client_only(tmp_path):
    clock = Clock()
    a = T.TrafficAuditor(_profile(tmp_path), clock=clock)
    for _ in range(3):
        assert a.observe("1.1.1.1", "POST", "/api/login").allowed
    d = a.observe("1.1.1.1", "POST", "/api/login")
    assert not d.allowed and "per-client" in d.reason and d.retry_after == 300
    assert a.observe("2.2.2.2", "GET", "/api/runs").allowed       # someone else, another category
    assert a.snapshot()["blocked_clients"]["1.1.1.1"]["category"] == "auth"
    assert a.alerts[0]["kind"] == "traffic_client_blocked"


def test_block_expires(tmp_path):
    clock = Clock()
    a = T.TrafficAuditor(_profile(tmp_path), clock=clock)
    for _ in range(4):
        a.observe("1.1.1.1", "POST", "/api/login")
    clock.t += 301
    assert a.observe("1.1.1.1", "GET", "/api/runs").allowed
    assert any(al["kind"] == "traffic_client_unblocked" for al in a.alerts)


def test_category_total_breach_refuses_everyone_not_allowlisted(tmp_path):
    """The distributed case: six clients, one hit each, total limit five."""
    clock = Clock()
    a = T.TrafficAuditor(_profile(tmp_path), clock=clock)
    for i in range(5):
        assert a.observe(f"5.5.5.{i}", "POST", "/api/login").allowed
    d = a.observe("5.5.5.99", "POST", "/api/login")
    assert not d.allowed and "anomalous" in d.reason
    snap = a.snapshot()
    assert snap["under_attack"] is True
    assert snap["categories"]["auth"]["anomaly_until_seconds"] > 0
    assert a.alerts[0]["kind"] == "traffic_anomaly"
    # Other categories are untouched; the allowlist still gets through.
    assert a.observe("5.5.5.100", "GET", "/api/runs").allowed
    assert a.observe("10.9.9.9", "POST", "/api/login").allowed


def test_anomaly_clears_when_the_window_drains(tmp_path):
    clock = Clock()
    a = T.TrafficAuditor(_profile(tmp_path), clock=clock)
    for i in range(6):
        a.observe(f"5.5.5.{i}", "POST", "/api/login")
    clock.t += 301
    assert a.observe("7.7.7.7", "POST", "/api/login").allowed
    assert a.snapshot()["under_attack"] is False
    assert any(al["kind"] == "traffic_anomaly_cleared" for al in a.alerts)


def test_allowlisted_client_is_counted_but_never_blocked(tmp_path):
    a = T.TrafficAuditor(_profile(tmp_path), clock=Clock())
    for _ in range(10):
        assert a.observe("10.0.0.7", "POST", "/api/login").allowed
    assert a.snapshot()["categories"]["auth"]["hits_in_window"] == 10
    assert a.snapshot()["blocked_clients"] == {}


def test_alert_hook_is_called_and_failures_are_swallowed(tmp_path):
    seen = []

    def hook(rec):
        seen.append(rec["kind"])
        raise RuntimeError("webhook down")

    a = T.TrafficAuditor(_profile(tmp_path), clock=Clock(), on_alert=hook)
    for _ in range(4):
        a.observe("1.1.1.1", "POST", "/api/login")
    assert seen == ["traffic_client_blocked"]


def test_middleware_returns_429_with_retry_after(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    monkeypatch.setenv("TRAFFIC_AUDIT", "on")
    app = FastAPI()

    @app.post("/api/login")
    def login():
        return {"ok": True}

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    clock = Clock()
    T.install(app, T.TrafficAuditor(_profile(tmp_path), clock=clock))
    c = TestClient(app)
    for _ in range(3):
        assert c.post("/api/login", headers={"x-forwarded-for": "3.3.3.3"}).status_code == 200
    r = c.post("/api/login", headers={"x-forwarded-for": "3.3.3.3"})
    assert r.status_code == 429 and r.headers["retry-after"] == "300"
    assert "refused" in r.json()["detail"]
    for _ in range(10):
        assert c.get("/healthz").status_code == 200        # never audited


def test_audit_off_installs_nothing(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    monkeypatch.setenv("TRAFFIC_AUDIT", "off")
    app = FastAPI()

    @app.post("/api/login")
    def login():
        return {"ok": True}

    T.install(app, T.TrafficAuditor(_profile(tmp_path), clock=Clock()))
    c = TestClient(app)
    assert all(c.post("/api/login").status_code == 200 for _ in range(10))
