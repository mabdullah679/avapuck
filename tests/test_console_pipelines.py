"""The pipelines dropdown, its three presets, and dev auto-login."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

NOW = datetime(2026, 9, 8, 18, 0, tzinfo=timezone.utc)


def _iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


class _Resp:
    def __init__(self, status, body):
        self.status_code, self._body, self.text = status, body, json.dumps(body)

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


@pytest.fixture
def console(monkeypatch, tmp_path):
    monkeypatch.setenv("CONSOLE_SESSION_SECRET", "s3ssion")
    monkeypatch.setenv("UPLOAD_DIR", str(tmp_path / "incoming"))
    monkeypatch.setenv("AIRFLOW_ADMIN_PASSWORD", "af-pass")
    monkeypatch.setenv("CONSOLE_DEV_AUTOLOGIN", "false")
    monkeypatch.setenv("CLIENT_SECRET_CONSOLE_ADMIN", "console-secret")
    import importlib
    import services.console.app as mod
    importlib.reload(mod)
    from fastapi.testclient import TestClient
    return TestClient(mod.app), mod


def _fake_airflow(mod, monkeypatch, watchdog_datasets=None, mother_state="success",
                  mother_end=NOW - timedelta(minutes=10), child_runs=True):
    dags = {"dags": [
        {"dag_id": "trips_pipeline", "is_paused": False, "timetable_summary": "0 */4 * * *",
         "timetable_description": "Every 4 hours"},
        {"dag_id": "on_run_verdict", "is_paused": False, "timetable_summary": "Asset",
         "timetable_description": "Triggered by assets"},
    ]}

    def fake_get(url, **kw):
        if url.endswith("/api/v2/dags"):
            return _Resp(200, dags)
        if url.endswith("/dags/trips_pipeline"):
            return _Resp(200, dags["dags"][0])
        if url.endswith("/dags/on_run_verdict"):
            return _Resp(200, dags["dags"][1])
        if url.endswith("/dags/trips_pipeline/dagRuns"):
            return _Resp(200, {"dag_runs": [{"dag_run_id": "m1", "state": mother_state,
                                             "end_date": _iso(mother_end)}]})
        if url.endswith("/dags/on_run_verdict/dagRuns"):
            return _Resp(200, {"dag_runs": [{"dag_run_id": "c1", "state": "success",
                                             "end_date": _iso(NOW - timedelta(minutes=5))}] if child_runs else []})
        if url.endswith("/status"):     # the watchdog
            return _Resp(200, {"healthy": True, "interval_hours": 4, "grace_minutes": 30,
                               "datasets": watchdog_datasets if watchdog_datasets is not None else {
                                   "trips": {"seen_at": _iso(NOW - timedelta(minutes=9)), "proceed": True}}})
        raise AssertionError(url)

    def fake_post(url, **kw):
        if url.endswith("/auth/token"):
            return _Resp(201, {"access_token": "af-tok"})
        raise AssertionError(url)

    monkeypatch.setattr(mod.requests, "get", fake_get)
    monkeypatch.setattr(mod.requests, "post", fake_post)


def _session(client, mod):
    client.cookies.set(mod.SESSION_COOKIE, mod.make_session("admin"))


class TestPipelines:
    def test_lists_mother_and_child_with_presets(self, console, monkeypatch):
        client, mod = console
        _session(client, mod)
        _fake_airflow(mod, monkeypatch)
        monkeypatch.setattr(mod, "datetime", _FrozenDatetime)
        d = client.get("/api/pipelines").json()
        assert [p["dag_id"] for p in d["pipelines"]] == ["trips_pipeline", "on_run_verdict"]
        assert [p["kind"] for p in d["pipelines"]] == ["mother", "child"]
        assert d["presets"] == {"all": 2, "responding": 2, "connected": 2}
        assert d["watchdog"]["window"] == "4h + 30m"

    def test_mother_is_not_connected_without_a_fresh_verdict(self, console, monkeypatch):
        client, mod = console
        _session(client, mod)
        _fake_airflow(mod, monkeypatch, watchdog_datasets={})
        monkeypatch.setattr(mod, "datetime", _FrozenDatetime)
        d = client.get("/api/pipelines").json()
        mother = d["pipelines"][0]
        assert mother["connected"] is False and "no verdict" in mother["connection_reason"]
        assert d["presets"]["connected"] == 1     # the child still is

    def test_old_or_failed_runs_are_not_connected(self, console, monkeypatch):
        client, mod = console
        _session(client, mod)
        _fake_airflow(mod, monkeypatch, mother_end=NOW - timedelta(hours=9))
        monkeypatch.setattr(mod, "datetime", _FrozenDatetime)
        assert "outside" in client.get("/api/pipelines").json()["pipelines"][0]["connection_reason"]
        _fake_airflow(mod, monkeypatch, mother_state="failed")
        assert client.get("/api/pipelines").json()["pipelines"][0]["connected"] is False

    def test_no_runs_means_not_connected(self, console, monkeypatch):
        client, mod = console
        _session(client, mod)
        _fake_airflow(mod, monkeypatch, child_runs=False)
        monkeypatch.setattr(mod, "datetime", _FrozenDatetime)
        child = client.get("/api/pipelines").json()["pipelines"][1]
        assert child["connected"] is False and child["connection_reason"] == "no run yet"


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)


class TestRunOnAnyDag:
    def _upload(self, client):
        return client.post("/api/upload", files={"file": ("x.csv", b"id\n1\n", "text/csv")}).json()

    def test_run_targets_the_chosen_dag(self, console, monkeypatch):
        client, mod = console
        _session(client, mod)
        up = self._upload(client)
        posted = []

        def fake_post(url, **kw):
            if url.endswith("/auth/token"):
                return _Resp(201, {"access_token": "t"})
            posted.append(url)
            return _Resp(200, {"dag_run_id": kw["json"]["dag_run_id"], "state": "queued"})

        monkeypatch.setattr(mod.requests, "post", fake_post)
        monkeypatch.setattr(mod.requests, "get",
                            lambda url, **kw: _Resp(200, {"dag_id": "on_run_verdict", "is_paused": False}))
        r = client.post("/api/run", json={"file_id": up["file_id"], "dag_id": "on_run_verdict"})
        assert r.status_code == 200 and r.json()["dag_id"] == "on_run_verdict"
        assert posted[-1].endswith("/dags/on_run_verdict/dagRuns")

    def test_paused_dag_is_refused_with_409(self, console, monkeypatch):
        client, mod = console
        _session(client, mod)
        up = self._upload(client)
        monkeypatch.setattr(mod.requests, "post", lambda url, **kw: _Resp(201, {"access_token": "t"}))
        monkeypatch.setattr(mod.requests, "get",
                            lambda url, **kw: _Resp(200, {"dag_id": "x", "is_paused": True}))
        r = client.post("/api/run", json={"file_id": up["file_id"], "dag_id": "x"})
        assert r.status_code == 409 and "paused" in r.json()["detail"]

    def test_unknown_dag_is_404(self, console, monkeypatch):
        client, mod = console
        _session(client, mod)
        up = self._upload(client)
        monkeypatch.setattr(mod.requests, "post", lambda url, **kw: _Resp(201, {"access_token": "t"}))
        monkeypatch.setattr(mod.requests, "get", lambda url, **kw: _Resp(404, {}))
        assert client.post("/api/run", json={"file_id": up["file_id"], "dag_id": "nope"}).status_code == 404


class TestDevAutologin:
    def _reload(self, monkeypatch, tmp_path, enabled):
        monkeypatch.setenv("CONSOLE_SESSION_SECRET", "s3ssion")
        monkeypatch.setenv("UPLOAD_DIR", str(tmp_path / "incoming"))
        monkeypatch.setenv("CONSOLE_DEV_AUTOLOGIN", "true" if enabled else "false")
        monkeypatch.setenv("CLIENT_SECRET_CONSOLE_ADMIN", "console-secret")
        import importlib
        import services.console.app as mod
        importlib.reload(mod)
        from fastapi.testclient import TestClient
        return TestClient(mod.app), mod

    def _idp(self, mod, monkeypatch, calls):
        def fake_post(url, **kw):
            calls.append(kw["data"]["client_secret"])
            return _Resp(200 if kw["data"]["client_secret"] == "console-secret" else 401,
                         {"access_token": "tok"})

        class P:
            def require_scope(self, s): pass

        monkeypatch.setattr(mod.requests, "post", fake_post)
        monkeypatch.setattr(mod._verifier, "verify", lambda tok: P())

    def test_local_client_is_signed_in_through_the_idp(self, monkeypatch, tmp_path):
        client, mod = self._reload(monkeypatch, tmp_path, enabled=True)
        calls = []
        self._idp(mod, monkeypatch, calls)
        r = client.get("/api/me", headers={"x-forwarded-for": "127.0.0.1"})
        assert r.status_code == 200 and r.json() == {"username": "admin"}
        assert calls == ["console-secret"]                 # the IdP was asked, once
        assert mod.SESSION_COOKIE in r.cookies
        assert client.get("/api/me").status_code == 200    # and the cookie now carries it
        assert calls == ["console-secret"]

    def test_remote_client_is_not_auto_signed_in(self, monkeypatch, tmp_path):
        client, mod = self._reload(monkeypatch, tmp_path, enabled=True)
        calls = []
        self._idp(mod, monkeypatch, calls)
        assert client.get("/api/me", headers={"x-forwarded-for": "8.8.8.8"}).status_code == 401
        assert calls == []

    def test_disabled_means_nobody_is_auto_signed_in(self, monkeypatch, tmp_path):
        client, mod = self._reload(monkeypatch, tmp_path, enabled=False)
        calls = []
        self._idp(mod, monkeypatch, calls)
        assert client.get("/api/me", headers={"x-forwarded-for": "127.0.0.1"}).status_code == 401
        assert calls == []

    def test_session_default_is_thirty_days(self, monkeypatch, tmp_path):
        monkeypatch.delenv("CONSOLE_SESSION_SECONDS", raising=False)
        _, mod = self._reload(monkeypatch, tmp_path, enabled=False)
        assert mod.SESSION_TTL == 30 * 24 * 3600
