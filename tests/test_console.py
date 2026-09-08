"""The console: IdP login, signed sessions, uploads, and the run trigger."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def console(monkeypatch, tmp_path):
    monkeypatch.setenv("CONSOLE_SESSION_SECRET", "s3ssion")
    monkeypatch.setenv("UPLOAD_DIR", str(tmp_path / "incoming"))
    monkeypatch.setenv("UPLOAD_MAX_ROWS", "0")
    monkeypatch.setenv("AIRFLOW_ADMIN_PASSWORD", "af-pass")
    monkeypatch.setenv("EXTRACT_MODE", "fixture")
    import importlib
    import services.console.app as mod
    importlib.reload(mod)
    from fastapi.testclient import TestClient
    return TestClient(mod.app), mod


class _Resp:
    def __init__(self, status, body):
        self.status_code, self._body, self.text = status, body, json.dumps(body)

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def _login(client, mod, monkeypatch, password="right"):
    """Stand in for the IdP round trip and the JWKS verification."""
    def fake_post(url, **kw):
        if url.endswith("/token"):
            ok = kw["data"]["client_secret"] == "right" and kw["data"]["client_id"] == "console-admin"
            return _Resp(200 if ok else 401, {"access_token": "tok"} if ok else {"detail": "invalid_client"})
        raise AssertionError(url)

    class P:
        def require_scope(self, s):
            assert s == "console.admin"

    monkeypatch.setattr(mod.requests, "post", fake_post)
    monkeypatch.setattr(mod._verifier, "verify", lambda tok: P())
    return client.post("/api/login", json={"username": "admin", "password": password})


class TestSessions:
    def test_session_roundtrip_and_expiry(self, console):
        _, mod = console
        tok = mod.make_session("admin", now=1000)
        assert mod.read_session(tok, now=1001)["u"] == "admin"
        assert mod.read_session(tok, now=1000 + mod.SESSION_TTL + 1) is None

    def test_tampered_session_is_rejected(self, console):
        _, mod = console
        tok = mod.make_session("admin")
        payload, sig = tok.rsplit(".", 1)
        assert mod.read_session(payload + "x." + sig) is None
        assert mod.read_session("garbage") is None and mod.read_session(None) is None

    def test_api_requires_a_session(self, console):
        client, _ = console
        assert client.get("/api/me").status_code == 401
        assert client.get("/api/source").status_code == 401
        assert client.post("/api/run", json={"file_id": "a" * 12}).status_code == 401


class TestLogin:
    def test_good_password_sets_a_cookie_and_me_works(self, console, monkeypatch):
        client, mod = console
        r = _login(client, mod, monkeypatch)
        assert r.status_code == 200 and mod.SESSION_COOKIE in r.cookies
        assert client.get("/api/me").json() == {"username": "admin"}
        client.post("/api/logout")
        assert client.get("/api/me").status_code == 401

    def test_bad_password_is_refused_and_never_stored(self, console, monkeypatch):
        client, mod = console
        assert _login(client, mod, monkeypatch, password="wrong").status_code == 401
        assert client.get("/api/me").status_code == 401

    def test_unknown_user_is_refused_before_the_idp(self, console, monkeypatch):
        client, mod = console
        called = []
        monkeypatch.setattr(mod.requests, "post", lambda *a, **k: called.append(1))
        r = client.post("/api/login", json={"username": "root", "password": "x"})
        assert r.status_code == 401 and called == []


class TestSourceAndUpload:
    def test_source_says_bigquery_is_not_a_toggle(self, console, monkeypatch):
        client, mod = console
        _login(client, mod, monkeypatch)
        s = client.get("/api/source").json()
        assert s["extract_mode"] == "fixture" and s["bigquery_configured"] is False
        assert "RBAC" in s["note"] and "D15" in s["note"]

    def test_upload_streams_to_disk_and_suggests_a_dataset(self, console, monkeypatch, tmp_path):
        client, mod = console
        _login(client, mod, monkeypatch)
        body = b"id,customer_email,total\n1,a@x.com,9\n2,b@x.com,7\n"
        r = client.post("/api/upload", files={"file": ("Q3 Orders (final).csv", body, "text/csv")})
        assert r.status_code == 200, r.text
        d = r.json()
        assert d["bytes"] == len(body) and d["name"] == "Q3_Orders_final.csv"
        assert Path(d["path"]).read_bytes() == body
        assert Path(d["path"]).parent == tmp_path / "incoming"
        assert d["dataset_suggestion"] == "q3_orders_final"

    def test_empty_upload_is_refused(self, console, monkeypatch):
        client, mod = console
        _login(client, mod, monkeypatch)
        r = client.post("/api/upload", files={"file": ("empty.csv", b"", "text/csv")})
        assert r.status_code == 400

    def test_safe_name_strips_paths_and_forces_csv(self, console):
        _, mod = console
        assert mod.safe_name("../../etc/passwd") == "passwd.csv"
        assert mod.safe_name("") == "upload.csv"
        assert mod.safe_name("a b/c.CSV") == "c.CSV"


class TestRun:
    def test_run_triggers_one_dag_run_with_the_upload_in_conf(self, console, monkeypatch):
        client, mod = console
        _login(client, mod, monkeypatch)
        up = client.post("/api/upload", files={"file": ("orders.csv", b"id,v\n1,2\n", "text/csv")}).json()

        calls = []

        def fake_post(url, **kw):
            calls.append((url, kw))
            if url.endswith("/auth/token"):
                assert kw["json"] == {"username": "admin", "password": "af-pass"}
                return _Resp(201, {"access_token": "af-tok"})     # Airflow 3 says Created
            if url.endswith("/dagRuns"):
                assert kw["headers"]["Authorization"] == "Bearer af-tok"
                return _Resp(200, {"dag_run_id": kw["json"]["dag_run_id"], "state": "queued",
                                   "logical_date": kw["json"]["logical_date"]})
            raise AssertionError(url)

        monkeypatch.setattr(mod.requests, "post", fake_post)
        monkeypatch.setattr(mod.requests, "get",
                            lambda url, **kw: _Resp(200, {"dag_id": "trips_pipeline", "is_paused": False}))
        r = client.post("/api/run", json={"file_id": up["file_id"], "dataset": "orders_q3"})
        assert r.status_code == 200, r.text
        d = r.json()
        assert d["state"] == "queued" and d["dag_run_id"].startswith(f"upload__{up['file_id']}__")
        assert d["dag_id"] == "trips_pipeline"
        body = calls[-1][1]["json"]
        assert body["conf"] == {"csv_path": up["path"], "row_limit": 0, "source": "upload",
                                "uploaded_by": "admin", "dataset": "orders_q3"}
        assert body["logical_date"].endswith("+00:00")
        assert "/api/v2/dags/trips_pipeline/dagRuns" in calls[-1][0]

    def test_run_default_dataset_comes_from_the_file_name(self, console, monkeypatch):
        client, mod = console
        _login(client, mod, monkeypatch)
        up = client.post("/api/upload", files={"file": ("Lease Sale 2026.csv", b"id,v\n1,2\n", "text/csv")}).json()
        monkeypatch.setattr(mod, "trigger_run",
                            lambda conf, run_id, dag_id=None: {"dag_run_id": run_id, "state": "queued"})
        d = client.post("/api/run", json={"file_id": up["file_id"], "row_limit": 500}).json()
        assert d["conf"]["dataset"] == "lease_sale_2026" and d["conf"]["row_limit"] == 500

    def test_run_with_unknown_upload_is_404(self, console, monkeypatch):
        client, mod = console
        _login(client, mod, monkeypatch)
        assert client.post("/api/run", json={"file_id": "0" * 12}).status_code == 404

    def test_airflow_refusal_is_a_502_not_a_crash(self, console, monkeypatch):
        client, mod = console
        _login(client, mod, monkeypatch)
        up = client.post("/api/upload", files={"file": ("x.csv", b"id\n1\n", "text/csv")}).json()
        monkeypatch.setattr(mod.requests, "post", lambda url, **kw: _Resp(401, {"detail": "no"}))
        r = client.post("/api/run", json={"file_id": up["file_id"]})
        assert r.status_code == 502 and "Airflow" in r.json()["detail"]


def test_index_and_static_are_served(console):
    client, _ = console
    assert client.get("/healthz").status_code == 200
    r = client.get("/")
    assert r.status_code == 200 and "avapuck console" in r.text
    assert client.get("/static/app.js").status_code == 200
