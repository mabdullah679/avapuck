"""Traffic audit: is the system being hit outside its expected profile?

A written profile (config/traffic_profile.json) says what normal looks like:
for each category of request, how many a single client may make per window
and how many everyone together may make. The auditor counts what actually
arrives and, on a breach, DETECTS AND BLOCKS (docs/DECISIONS.md D18):

  * a client over its per-client limit is refused (429) for `block_seconds`;
  * a category over its total limit is an anomaly: the category is refused
    to every non-allowlisted client until the window drains. That is the
    distributed case, where no single client is over its own limit.

Allowlisted networks are counted but never blocked, so the pipeline's own
service-to-service calls cannot be cut off by a flood at the front door.

Every transition logs an ALERT line and calls an optional hook (the console
posts it to the same webhook the watchdog uses). State is in-process and
per service; the console's /api/traffic shows it. Nothing here is a
substitute for an edge firewall; it is the audit the owner asked for, in the
place a POC can have one.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("traffic")

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROFILE_PATH = ROOT / "config" / "traffic_profile.json"


@dataclass
class Category:
    name: str
    match: list[str]                 # "METHOD /path-prefix"
    per_client_per_window: int
    total_per_window: int


@dataclass
class TrafficProfile:
    window_seconds: int
    block_seconds: int
    categories: list[Category]
    default: Category
    allowlist: list = field(default_factory=list)   # ip_network objects

    @classmethod
    def load(cls, path: Path | None = None, allowlist_env: str | None = None) -> "TrafficProfile":
        doc = json.loads(Path(path or os.environ.get("TRAFFIC_PROFILE", DEFAULT_PROFILE_PATH)).read_text())
        cats = [Category(name=n, match=list(c["match"]),
                         per_client_per_window=int(c["per_client_per_window"]),
                         total_per_window=int(c["total_per_window"]))
                for n, c in doc["categories"].items()]
        d = doc["default"]
        default = Category("default", [], int(d["per_client_per_window"]), int(d["total_per_window"]))
        nets = list(doc.get("allowlist", []))
        raw = allowlist_env if allowlist_env is not None else os.environ.get("TRAFFIC_ALLOWLIST", "")
        nets += [n.strip() for n in raw.split(",") if n.strip()]
        return cls(window_seconds=int(doc.get("window_seconds", 60)),
                   block_seconds=int(os.environ.get("TRAFFIC_BLOCK_SECONDS", doc.get("block_seconds", 300))),
                   categories=cats, default=default,
                   allowlist=[ipaddress.ip_network(n, strict=False) for n in nets])

    def classify(self, method: str, path: str) -> Category:
        probe = f"{method.upper()} {path}"
        for cat in self.categories:
            for m in cat.match:
                if probe.startswith(m):
                    return cat
        return self.default

    def is_allowlisted(self, client: str) -> bool:
        try:
            ip = ipaddress.ip_address(client)
        except ValueError:
            return False
        return any(ip in net for net in self.allowlist)


@dataclass(frozen=True)
class Decision:
    allowed: bool
    category: str
    reason: str
    retry_after: int = 0


class TrafficAuditor:
    """Sliding-window counters plus the block/anomaly state."""

    def __init__(self, profile: TrafficProfile, clock=time.monotonic, on_alert=None):
        self.profile = profile
        self.clock = clock
        self.on_alert = on_alert
        self._lock = threading.Lock()
        self._client_hits: dict[tuple[str, str], deque] = {}
        self._total_hits: dict[str, deque] = {}
        self._blocked: dict[str, tuple[float, str]] = {}     # client -> (until, category)
        self._anomaly: dict[str, float] = {}                 # category -> until
        self.alerts: deque = deque(maxlen=100)

    # ── internals ──
    def _prune(self, dq: deque, now: float) -> None:
        cutoff = now - self.profile.window_seconds
        while dq and dq[0] < cutoff:
            dq.popleft()

    def _alert(self, kind: str, **fields) -> None:
        line = " ".join(f"{k}={v}" for k, v in fields.items())
        log.error("ALERT kind=%s %s", kind, line)
        rec = {"kind": kind, "at": time.time(), **fields}
        self.alerts.appendleft(rec)
        if self.on_alert:
            try:
                self.on_alert(rec)
            except Exception as e:  # noqa: BLE001
                log.warning("traffic alert hook failed: %s", e)

    # ── the rule ──
    def observe(self, client: str, method: str, path: str) -> Decision:
        cat = self.profile.classify(method, path)
        now = self.clock()
        exempt = self.profile.is_allowlisted(client)
        with self._lock:
            # Already blocked?
            until, why = self._blocked.get(client, (0.0, ""))
            if until > now and not exempt:
                return Decision(False, cat.name, f"client blocked ({why})", int(until - now) + 1)
            if until and until <= now:
                self._blocked.pop(client, None)
                self._alert("traffic_client_unblocked", client=client)

            # Count.
            per_client = self._client_hits.setdefault((cat.name, client), deque())
            total = self._total_hits.setdefault(cat.name, deque())
            self._prune(per_client, now)
            self._prune(total, now)
            per_client.append(now)
            total.append(now)

            # Per-client breach.
            if len(per_client) > cat.per_client_per_window and not exempt:
                self._blocked[client] = (now + self.profile.block_seconds, cat.name)
                self._alert("traffic_client_blocked", client=client, category=cat.name,
                            hits=len(per_client), limit=cat.per_client_per_window,
                            block_seconds=self.profile.block_seconds)
                return Decision(False, cat.name, "over the per-client limit", self.profile.block_seconds)

            # Category-wide breach: the distributed case.
            if len(total) > cat.total_per_window:
                if self._anomaly.get(cat.name, 0) <= now:
                    self._anomaly[cat.name] = now + self.profile.block_seconds
                    self._alert("traffic_anomaly", category=cat.name, hits=len(total),
                                limit=cat.total_per_window, window=self.profile.window_seconds,
                                block_seconds=self.profile.block_seconds)
            anomaly_until = self._anomaly.get(cat.name, 0)
            if anomaly_until > now and not exempt:
                return Decision(False, cat.name, "category under anomalous load",
                                int(anomaly_until - now) + 1)
            if anomaly_until and anomaly_until <= now:
                self._anomaly.pop(cat.name, None)
                self._alert("traffic_anomaly_cleared", category=cat.name)
        return Decision(True, cat.name, "within profile")

    def snapshot(self) -> dict:
        now = self.clock()
        with self._lock:
            cats = {}
            for cat in [*self.profile.categories, self.profile.default]:
                total = self._total_hits.get(cat.name, deque())
                self._prune(total, now)
                clients = {c: len(dq) for (n, c), dq in self._client_hits.items() if n == cat.name and dq}
                cats[cat.name] = {
                    "hits_in_window": len(total),
                    "total_per_window": cat.total_per_window,
                    "per_client_per_window": cat.per_client_per_window,
                    "anomaly_until_seconds": max(0, int(self._anomaly.get(cat.name, 0) - now)),
                    "top_clients": sorted(clients.items(), key=lambda kv: -kv[1])[:5],
                }
            blocked = {c: {"category": why, "seconds_left": max(0, int(until - now))}
                       for c, (until, why) in self._blocked.items() if until > now}
            under_attack = any(v["anomaly_until_seconds"] > 0 for v in cats.values())
        return {
            "under_attack": under_attack,
            "window_seconds": self.profile.window_seconds,
            "block_seconds": self.profile.block_seconds,
            "allowlist": [str(n) for n in self.profile.allowlist],
            "categories": cats,
            "blocked_clients": blocked,
            "recent_alerts": list(self.alerts)[:20],
        }


def client_ip(request) -> str:
    """The caller's address. No proxy sits in front of these services, so the
    socket peer is the truth; X-Forwarded-For is honoured only if present."""
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def install(app, auditor: TrafficAuditor, skip_paths: tuple[str, ...] = ("/healthz", "/health")) -> None:
    """Attach the auditor to a FastAPI app as an HTTP middleware.

    TRAFFIC_AUDIT=off leaves the app unaudited: for the unit tests, whose
    in-process client has no IP address, and for nothing else.
    """
    from starlette.responses import JSONResponse

    app.state.traffic = auditor
    if os.environ.get("TRAFFIC_AUDIT", "on").lower() in ("off", "0", "false", "no"):
        log.warning("traffic audit is OFF (TRAFFIC_AUDIT=off)")
        return

    @app.middleware("http")
    async def _audit(request, call_next):
        if request.url.path in skip_paths:
            return await call_next(request)
        d = auditor.observe(client_ip(request), request.method, request.url.path)
        if not d.allowed:
            return JSONResponse(
                status_code=429,
                content={"detail": f"refused: {d.reason}", "category": d.category,
                         "retry_after": d.retry_after},
                headers={"Retry-After": str(d.retry_after)})
        return await call_next(request)
