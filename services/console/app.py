"""The console: upload a CSV, run the pipeline, watch it land.

One small FastAPI app serving a single-page UI (services/console/static) and
a JSON API (docs/DECISIONS.md D15, D20):

  login       POST /api/login     -> the IdP, as client `console-admin`; the
                                     returned token is verified against the
                                     IdP's JWKS, then a signed session cookie
                                     is issued. The console never stores the
                                     password.
  source      GET  /api/source    -> which extractor the stack was started
                                     with. BigQuery is NOT switchable here;
                                     EXTRACT_MODE is an env var until RBAC
                                     exists (D15).
  upload      POST /api/upload    -> lands the file under /data/incoming, the
                                     path Airflow sees at the same location
  run         POST /api/run       -> triggers ONE fresh run of trips_pipeline
                                     through Airflow's REST API with the
                                     upload in conf, uncapped by default (D16)
  runs        GET  /api/runs      -> recent runs with their verdicts
  kafka log   GET  /api/kafka-log -> monitor.kafka_log, filtered and paged
  spark       GET  /api/spark     -> the scaler's view; POST /api/spark/scale
  traffic     GET  /api/traffic   -> this app's own traffic audit (D18)

Every request passes the traffic auditor first; a client over profile gets
429 before any of the above runs.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated

import requests
from fastapi import Depends, FastAPI, File, HTTPException, Request, Response, UploadFile, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from pipeline.common.auth import AuthError, TokenVerifier, tls_verify_from_env
from pipeline.common.traffic import TrafficAuditor, TrafficProfile, install as install_traffic

log = logging.getLogger("console")
logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")

STATIC = Path(__file__).resolve().parent / "static"
IDP_URL = os.environ.get("IDP_URL", "https://localhost:8443").rstrip("/")
IDP_ISSUER = os.environ.get("IDP_ISSUER", "https://idp.local")
CONSOLE_CLIENT_ID = "console-admin"
CONSOLE_AUDIENCE = "console"
ADMIN_USER = os.environ.get("CONSOLE_ADMIN_USER", "admin")
SESSION_SECRET = os.environ.get("CONSOLE_SESSION_SECRET", "")
# Long-lived by default (30 days): the owner asked for the local browser to
# stay signed in. The cookie is HttpOnly and signed; rotate the secret to end
# every session at once.
SESSION_TTL = int(os.environ.get("CONSOLE_SESSION_SECONDS", str(30 * 24 * 3600)))
SESSION_COOKIE = "console_session"
# Dev auto-login (docs/DECISIONS.md D23): a request from a loopback or private
# address with no session gets one, by the console logging in to the IdP
# ITSELF with the configured admin secret. The IdP is still in the loop and
# the token is still verified; only the typing is skipped. MUST be false
# anywhere the console is reachable from a network you do not own.
DEV_AUTOLOGIN = os.environ.get("CONSOLE_DEV_AUTOLOGIN", "false").lower() in ("1", "true", "yes")
ADMIN_SECRET = os.environ.get("CLIENT_SECRET_CONSOLE_ADMIN", "")
WATCHDOG_URL = os.environ.get("WATCHDOG_URL", "http://watchdog:8446").rstrip("/")
UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", "/data/incoming"))
UPLOAD_MAX_ROWS = int(os.environ.get("UPLOAD_MAX_ROWS", "0"))
AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://airflow:8080").rstrip("/")
AIRFLOW_USER = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "")
DAG_ID = os.environ.get("PIPELINE_DAG_ID", "trips_pipeline")
SCALER_URL = os.environ.get("SCALER_URL", "http://scaler:8448").rstrip("/")
WEBHOOK = os.environ.get("WATCHDOG_WEBHOOK_URL") or None

app = FastAPI(title="avapuck console", version="1.0.0")
_verifier = TokenVerifier(issuer=IDP_ISSUER, audience=CONSOLE_AUDIENCE,
                          jwks_url=os.environ.get("IDP_JWKS_URL"))


# ── Traffic audit: first thing every request meets ────────────────────────

def _post_alert(rec: dict) -> None:
    if not WEBHOOK:
        return
    try:
        requests.post(WEBHOOK, json={"text": f"Console traffic: {rec['kind']} "
                                             + " ".join(f"{k}={v}" for k, v in rec.items()
                                                        if k not in ("kind", "at")),
                                     **rec}, timeout=5)
    except Exception as e:  # noqa: BLE001
        log.warning("webhook post failed: %s", e)


AUDITOR = TrafficAuditor(TrafficProfile.load(), on_alert=_post_alert)
install_traffic(app, AUDITOR)


# ── Sessions ──────────────────────────────────────────────────────────────

def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def make_session(username: str, now: float | None = None) -> str:
    """`<payload>.<hmac>`; payload is username and expiry. Nothing secret in it."""
    if not SESSION_SECRET:
        raise RuntimeError("CONSOLE_SESSION_SECRET is not set")
    exp = int((now or time.time()) + SESSION_TTL)
    payload = _b64(json.dumps({"u": username, "exp": exp}).encode())
    mac = hmac.new(SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).digest()
    return f"{payload}.{_b64(mac)}"


def read_session(token: str | None, now: float | None = None) -> dict | None:
    if not token or "." not in token or not SESSION_SECRET:
        return None
    payload, sig = token.rsplit(".", 1)
    want = hmac.new(SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).digest()
    try:
        if not hmac.compare_digest(want, _unb64(sig)):
            return None
        data = json.loads(_unb64(payload))
    except Exception:  # noqa: BLE001
        return None
    if data.get("exp", 0) < (now or time.time()):
        return None
    return data


def _idp_login(password: str) -> None:
    """Prove the password to the IdP and verify what comes back. Raises 401/502."""
    try:
        r = requests.post(f"{IDP_URL}/token",
                          data={"grant_type": "client_credentials",
                                "client_id": CONSOLE_CLIENT_ID,
                                "client_secret": password,
                                "audience": CONSOLE_AUDIENCE},
                          timeout=10, verify=tls_verify_from_env())
    except requests.RequestException as e:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"IdP unreachable: {e}") from e
    if r.status_code != 200:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials")
    try:
        principal = _verifier.verify(r.json()["access_token"])
    except AuthError as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"IdP token failed verification: {e}") from e
    principal.require_scope("console.admin")


def _is_local(ip: str) -> bool:
    import ipaddress
    try:
        a = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return a.is_loopback or a.is_private


def current_user(request: Request, response: Response) -> str:
    sess = read_session(request.cookies.get(SESSION_COOKIE))
    if sess:
        return sess["u"]
    from pipeline.common.traffic import client_ip
    if DEV_AUTOLOGIN and ADMIN_SECRET and _is_local(client_ip(request)):
        _idp_login(ADMIN_SECRET)
        response.set_cookie(SESSION_COOKIE, make_session(ADMIN_USER), max_age=SESSION_TTL,
                            httponly=True, samesite="lax", path="/")
        log.info("dev auto-login for %s from %s", ADMIN_USER, client_ip(request))
        return ADMIN_USER
    raise HTTPException(status.HTTP_401_UNAUTHORIZED, "not logged in")


User = Annotated[str, Depends(current_user)]


class LoginBody(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


@app.post("/api/login")
def login(body: LoginBody, response: Response) -> dict:
    """Authenticate through the IdP. The password goes to the IdP and nowhere else."""
    if body.username != ADMIN_USER:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials")
    _idp_login(body.password)
    response.set_cookie(SESSION_COOKIE, make_session(body.username), max_age=SESSION_TTL,
                        httponly=True, samesite="lax", path="/")
    return {"username": body.username, "expires_in": SESSION_TTL}


@app.post("/api/logout")
def logout(response: Response) -> dict:
    response.delete_cookie(SESSION_COOKIE, path="/")
    return {"ok": True}


@app.get("/api/me")
def me(user: User) -> dict:
    return {"username": user}


# ── Source ────────────────────────────────────────────────────────────────

@app.get("/api/source")
def source(user: User) -> dict:
    mode = os.environ.get("EXTRACT_MODE", "auto")
    return {
        "extract_mode": mode,
        "bigquery_configured": bool(os.environ.get("GCP_PROJECT_ID")),
        "upload_max_rows": UPLOAD_MAX_ROWS,
        "note": ("The scheduled source is fixed by EXTRACT_MODE when the stack starts. "
                 "BigQuery cannot be toggled here on purpose: until RBAC exists, an "
                 "admin API call to change it would be an unauthenticated switch on a "
                 "billable source. docs/DECISIONS.md D15."),
    }


# ── Upload and run ────────────────────────────────────────────────────────

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def safe_name(name: str) -> str:
    """A filesystem-safe basename that still ends in .csv."""
    base = Path(name or "").name
    stem, ext = (base[:-4], base[-4:]) if base.lower().endswith(".csv") else (base, ".csv")
    stem = _SAFE.sub("_", stem).strip("._-") or "upload"
    return f"{stem}{ext}"


@app.post("/api/upload")
async def upload(user: User, file: UploadFile = File(...)) -> dict:
    """Stream the file to disk in chunks; never hold it in memory."""
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_id = uuid.uuid4().hex[:12]
    name = safe_name(file.filename or "")
    dest = UPLOAD_DIR / f"{file_id}_{name}"
    size = 0
    with dest.open("wb") as out:
        while True:
            chunk = await file.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
            size += len(chunk)
    if size == 0:
        dest.unlink(missing_ok=True)
        raise HTTPException(400, "the file is empty")
    log.info("upload %s: %s (%d bytes) by %s", file_id, dest.name, size, user)
    return {"file_id": file_id, "path": str(dest), "name": name, "bytes": size,
            "dataset_suggestion": dataset_from_name(name)}


def dataset_from_name(name: str) -> str:
    from pipeline.metadata.schema import dataset_name_for
    return dataset_name_for(Path(name))


class RunBody(BaseModel):
    file_id: str = Field(pattern=r"^[0-9a-f]{12}$")
    dataset: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]{0,62}$")
    row_limit: int | None = Field(default=None, ge=0)
    dag_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_.-]{1,200}$")


# ── Pipelines: every DAG Airflow knows, with three presets (D22) ──────────

def _airflow_get(path: str, token: str, **params) -> tuple[int, dict]:
    r = requests.get(f"{AIRFLOW_URL}/api/v2{path}", params=params,
                     headers={"Authorization": f"Bearer {token}"}, timeout=15)
    try:
        body = r.json()
    except ValueError:
        body = {}
    return r.status_code, body


def _watchdog_view() -> dict:
    """The existing loop that tags a node's transmissions and re-checks them.

    "Connected" is not a new system: the watchdog already consumes every
    verdict, records which principal signed it and when, and re-evaluates one
    condition on a timer. The console reads that view.
    """
    try:
        r = requests.get(f"{WATCHDOG_URL}/status", timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        return {"error": str(e)[:120], "datasets": {}, "interval_hours": 4, "grace_minutes": 30}


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def classify_dag(dag: dict) -> str:
    """mother: runs on a schedule and produces verdicts. child: asset-triggered."""
    summary = f"{dag.get('timetable_summary') or ''} {dag.get('timetable_description') or ''}".lower()
    if "asset" in summary or dag.get("asset_expression"):
        return "child"
    return "mother"


def connection_state(kind: str, last_run: dict | None, watchdog: dict, now: datetime) -> tuple[bool, str]:
    """One rule, applied to every DAG: transmitted something within the
    watchdog's window and the check on it passed."""
    window = timedelta(hours=float(watchdog.get("interval_hours", 4)),
                       minutes=float(watchdog.get("grace_minutes", 30)))
    if last_run is None:
        return False, "no run yet"
    ended = _parse_ts(last_run.get("end_date")) or _parse_ts(last_run.get("start_date"))
    if ended is None:
        return False, f"last run {last_run.get('state')}, not finished"
    age = now - ended
    if age > window:
        return False, f"last run {int(age.total_seconds() // 60)} min ago, outside the {window} window"
    if last_run.get("state") != "success":
        return False, f"last run {last_run.get('state')}"
    if kind == "mother":
        fresh = [d for d, v in (watchdog.get("datasets") or {}).items()
                 if (_parse_ts(v.get("seen_at")) or now - window * 2) >= now - window and v.get("proceed")]
        if not fresh:
            return False, "no verdict seen by the watchdog within the window"
        return True, f"verdict seen for {', '.join(sorted(fresh))}"
    return True, "last run succeeded within the window"


@app.get("/api/pipelines")
def pipelines(user: User) -> dict:
    token = airflow_token()
    code, body = _airflow_get("/dags", token, limit=100)
    if code != 200:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"Airflow: {code}")
    watchdog = _watchdog_view()
    now = datetime.now(timezone.utc)
    rows = []
    for dag in body.get("dags", []):
        dag_id = dag["dag_id"]
        detail_code, _ = _airflow_get(f"/dags/{dag_id}", token)
        _, runs = _airflow_get(f"/dags/{dag_id}/dagRuns", token, limit=1, order_by="-run_after")
        last = (runs.get("dag_runs") or [None])[0]
        kind = classify_dag(dag)
        connected, why = connection_state(kind, last, watchdog, now)
        rows.append({
            "dag_id": dag_id, "kind": kind, "paused": bool(dag.get("is_paused")),
            "schedule": dag.get("timetable_summary") or dag.get("timetable_description"),
            "api_status": detail_code, "responding": detail_code == 200,
            "last_run": {"id": last["dag_run_id"], "state": last.get("state"),
                         "end_date": last.get("end_date")} if last else None,
            "connected": connected, "connection_reason": why,
        })
    rows.sort(key=lambda r: (r["kind"] != "mother", r["dag_id"]))
    return {
        "pipelines": rows,
        "presets": {
            "all": len(rows),
            "responding": sum(1 for r in rows if r["responding"]),
            "connected": sum(1 for r in rows if r["connected"]),
        },
        "watchdog": {"healthy": watchdog.get("healthy"), "error": watchdog.get("error"),
                     "window": f"{watchdog.get('interval_hours', 4)}h + {watchdog.get('grace_minutes', 30)}m"},
    }


def airflow_token() -> str:
    r = requests.post(f"{AIRFLOW_URL}/auth/token",
                      json={"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}, timeout=10)
    # Airflow 3 answers 201 Created for a new token (seen live, 2026-09-08).
    if r.status_code not in (200, 201):
        raise HTTPException(status.HTTP_502_BAD_GATEWAY,
                            f"Airflow refused the console's credentials: {r.status_code}")
    return r.json()["access_token"]


def trigger_run(conf: dict, run_id: str, dag_id: str = DAG_ID) -> dict:
    token = airflow_token()
    code, dag = _airflow_get(f"/dags/{dag_id}", token)
    if code == 404:
        raise HTTPException(404, f"no pipeline named {dag_id!r} in Airflow")
    if code != 200:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"Airflow: {code} for {dag_id}")
    if dag.get("is_paused"):
        raise HTTPException(409, f"{dag_id} is paused in Airflow; unpause it first")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    body = {"dag_run_id": run_id, "logical_date": now.isoformat(), "conf": conf,
            "note": f"console upload {conf.get('csv_path')}"}
    r = requests.post(f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns", json=body,
                      headers={"Authorization": f"Bearer {token}"}, timeout=15)
    if r.status_code not in (200, 201):
        raise HTTPException(status.HTTP_502_BAD_GATEWAY,
                            f"Airflow did not accept the run: {r.status_code} {r.text[:200]}")
    return r.json()


@app.post("/api/run")
def run(body: RunBody, user: User) -> dict:
    matches = sorted(UPLOAD_DIR.glob(f"{body.file_id}_*.csv"))
    if not matches:
        raise HTTPException(404, "no such upload; upload the file again")
    path = matches[0]
    dag_id = body.dag_id or DAG_ID
    row_limit = body.row_limit if body.row_limit is not None else UPLOAD_MAX_ROWS
    conf = {"csv_path": str(path), "row_limit": row_limit, "source": "upload",
            "uploaded_by": user, "dataset": body.dataset or dataset_from_name(path.name[13:])}
    run_id = f"upload__{body.file_id}__{int(time.time())}"
    dag_run = trigger_run(conf, run_id, dag_id)
    log.info("triggered %s on %s for %s as %s", run_id, dag_id, path.name, user)
    return {"dag_id": dag_id, "dag_run_id": dag_run.get("dag_run_id", run_id),
            "state": dag_run.get("state"), "logical_date": dag_run.get("logical_date"),
            "conf": conf, "airflow_url": f"http://localhost:8085/dags/{dag_id}/runs/{run_id}"}


# ── Runs, verdicts, Kafka log ─────────────────────────────────────────────

def _db():
    from pipeline.load.postgres_load import connection_from_env
    return connection_from_env()


def _verdicts_by_run_id(conn, run_ids: list[str]) -> dict[str, dict]:
    if not run_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT payload->>'proceed', payload->>'status', payload->'reasons',
                   run_slot, trace_id, received_at, raw::jsonb->'envelope'->>'run_id'
            FROM monitor.kafka_log
            WHERE event_type = 'run.verdict'
              AND raw::jsonb->'envelope'->>'run_id' = ANY(%s)
            ORDER BY received_at DESC
        """, (run_ids,))
        out = {}
        for proceed, st, reasons, slot, trace, at, run_id in cur.fetchall():
            out.setdefault(run_id, {"proceed": proceed == "true", "status": st,
                                    "reasons": reasons or [], "run_slot": slot,
                                    "trace_id": trace, "received_at": at})
        return out


@app.get("/api/runs")
def runs(user: User, limit: int = 25, dag_id: str = "~") -> dict:
    """Recent runs across every DAG (`~`) or one, joined to their verdicts."""
    r = requests.get(f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns",
                     params={"limit": min(limit, 100), "order_by": "-run_after"},
                     headers={"Authorization": f"Bearer {airflow_token()}"}, timeout=15)
    if r.status_code != 200:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"Airflow: {r.status_code}")
    dag_runs = r.json().get("dag_runs", [])
    verdicts: dict[str, dict] = {}
    try:
        conn = _db()
        try:
            verdicts = _verdicts_by_run_id(conn, [d["dag_run_id"] for d in dag_runs])
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001
        log.warning("verdict lookup failed: %s", e)
    rows = []
    for d in dag_runs:
        conf = d.get("conf") or {}
        rows.append({
            "dag_id": d.get("dag_id"),
            "dag_run_id": d["dag_run_id"], "state": d.get("state"),
            "logical_date": d.get("logical_date"), "start_date": d.get("start_date"),
            "end_date": d.get("end_date"), "run_type": d.get("run_type"),
            "source": conf.get("source", "configured"), "dataset": conf.get("dataset"),
            "csv_path": conf.get("csv_path"), "verdict": verdicts.get(d["dag_run_id"]),
        })
    return {"runs": rows}


@app.get("/api/kafka-log")
def kafka_log(user: User, topic: str | None = None, event_type: str | None = None,
              dataset: str | None = None, run_slot: str | None = None,
              limit: int = 50, offset: int = 0) -> dict:
    where, params = ["TRUE"], []
    for col, val in (("topic", topic), ("event_type", event_type), ("dataset", dataset)):
        if val:
            where.append(f"{col} = %s"); params.append(val)
    if run_slot:
        where.append("run_slot = %s"); params.append(run_slot)
    clause = " AND ".join(where)
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM monitor.kafka_log WHERE {clause}", params)
            total = cur.fetchone()[0]
            cur.execute(f"""
                SELECT id, topic, partition, "offset", event_type, dataset, run_slot, trace_id,
                       event_id, idempotency_key, key, producer, produced_at, payload, received_at
                FROM monitor.kafka_log WHERE {clause}
                ORDER BY id DESC LIMIT %s OFFSET %s
            """, [*params, min(limit, 500), offset])
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.execute("SELECT event_type, count(*) FROM monitor.kafka_log GROUP BY 1")
            counts = dict(cur.fetchall())
    finally:
        conn.close()
    return {"total": total, "rows": rows, "counts": counts, "limit": limit, "offset": offset}


# ── Spark ─────────────────────────────────────────────────────────────────

@app.get("/api/spark")
def spark(user: User) -> dict:
    try:
        r = requests.get(f"{SCALER_URL}/status", timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"scaler unreachable: {e}") from e


class ScaleBody(BaseModel):
    workers: int = Field(ge=0, le=64)


@app.post("/api/spark/scale")
def spark_scale(body: ScaleBody, user: User) -> dict:
    r = requests.post(f"{SCALER_URL}/scale", json={"workers": body.workers}, timeout=5)
    if r.status_code != 200:
        raise HTTPException(r.status_code, r.text[:200])
    log.info("manual scale to %d by %s", body.workers, user)
    return r.json()


# ── Traffic ───────────────────────────────────────────────────────────────

@app.get("/api/traffic")
def traffic(user: User) -> dict:
    return AUDITOR.snapshot()


# ── UI and health ─────────────────────────────────────────────────────────

@app.get("/healthz")
def healthz() -> dict:
    return {"healthy": True}


app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.exception_handler(HTTPException)
async def _plain_errors(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
