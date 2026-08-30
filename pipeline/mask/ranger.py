"""Ranger masking — policy resolution and enforcement.

Masking rules live in config/ranger/hive_masking_policies.json, in genuine
Apache Ranger policy format, and are NEVER restated in transformation code.
This module reads that policy and applies it; it decides nothing on its own.

Two providers behind one interface:

  RangerAdminProvider  -- fetches live policy from a running Ranger admin via
                          its REST API. Used when the Ranger container is up.
  LocalPolicyProvider  -- reads the same JSON from disk and applies Ranger's
                          documented masking semantics locally.

Both consume the identical policy file, so a policy authored for one is
correct for the other. Which one ran is recorded on every output, because
"masked by real Ranger" and "masked by our reading of Ranger's semantics" are
different claims and must not be conflated.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY = ROOT / "config" / "ranger" / "hive_masking_policies.json"


class MaskingError(Exception):
    pass


@dataclass(frozen=True)
class ColumnMask:
    database: str
    table: str
    column: str
    mask_type: str
    group: str


def apply_mask(mask_type: str, value):
    """Ranger's documented masking semantics.

    Names and behaviours follow Ranger's built-in mask types so that swapping
    in the real service is a no-op. An unknown type raises rather than passing
    the value through -- silently returning unmasked data on an unrecognised
    policy is the worst possible failure mode here.
    """
    if value is None or value == "":
        return value
    s = str(value)

    if mask_type in ("MASK_NONE", "NONE"):
        return value
    if mask_type == "MASK_NULL":
        return None
    if mask_type == "MASK_HASH":
        return hashlib.sha256(s.encode()).hexdigest()
    if mask_type == "MASK_SHOW_LAST_4":
        return ("*" * max(0, len(s) - 4)) + s[-4:] if len(s) > 4 else "*" * len(s)
    if mask_type == "MASK_SHOW_FIRST_4":
        return s[:4] + ("*" * max(0, len(s) - 4)) if len(s) > 4 else s
    if mask_type == "MASK_SHOW_FIRST_8":
        return s[:8] + ("*" * max(0, len(s) - 8)) if len(s) > 8 else s
    if mask_type == "MASK_DATE_SHOW_YEAR":
        return s[:4] + "-01-01"
    if mask_type == "MASK":
        # Ranger's default: letters->x, digits->n, keep everything else.
        return "".join("x" if c.isalpha() else "n" if c.isdigit() else c for c in s)
    raise MaskingError(
        f"unknown Ranger mask type {mask_type!r}. Refusing to pass the value "
        f"through unmasked -- add the type to apply_mask() deliberately.")


class MaskingProvider(Protocol):
    name: str
    def masks_for(self, database: str, table: str, group: str) -> dict[str, str]: ...


class LocalPolicyProvider:
    """Applies the policy file directly. Used when Ranger admin is not up."""

    name = "local-policy-engine"

    def __init__(self, policy_path: Path = DEFAULT_POLICY):
        self.policy_path = policy_path
        self._policies = json.loads(policy_path.read_text())["policies"]

    def masks_for(self, database: str, table: str, group: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for p in self._policies:
            if not p.get("isEnabled", True) or p.get("policyType") != 1:
                continue
            res = p["resources"]
            if database not in res["database"]["values"]:
                continue
            if table not in res["table"]["values"]:
                continue
            for item in p.get("dataMaskPolicyItems", []):
                if group not in item.get("groups", []):
                    continue
                info = item["dataMaskInfo"]
                mtype = info.get("valueExpr") or info["dataMaskType"]
                for col in res["column"]["values"]:
                    out[col] = mtype
        return out


class RangerAdminProvider:
    """Fetches policy from a live Ranger admin over its REST API."""

    name = "ranger-admin"

    def __init__(self, url: str, service: str, user: str, password: str,
                 timeout: int = 15):
        self.url = url.rstrip("/")
        self.service = service
        self.auth = (user, password)
        self.timeout = timeout

    def masks_for(self, database: str, table: str, group: str) -> dict[str, str]:
        import requests
        r = requests.get(
            f"{self.url}/service/public/v2/api/service/{self.service}/policy",
            auth=self.auth, timeout=self.timeout,
            headers={"Accept": "application/json"})
        if r.status_code != 200:
            raise MaskingError(
                f"Ranger admin returned {r.status_code}: {r.text[:200]}")
        local = LocalPolicyProvider.__new__(LocalPolicyProvider)
        local._policies = r.json()
        local.policy_path = None
        return LocalPolicyProvider.masks_for(local, database, table, group)

    def push_policies(self, policy_path: Path = DEFAULT_POLICY) -> int:
        """Upload the authored policies. Idempotent by policy name."""
        import requests
        policies = json.loads(policy_path.read_text())["policies"]
        pushed = 0
        for p in policies:
            body = {k: v for k, v in p.items() if not k.startswith("_")}
            r = requests.post(
                f"{self.url}/service/public/v2/api/policy",
                json=body, auth=self.auth, timeout=self.timeout,
                headers={"Content-Type": "application/json"})
            if r.status_code in (200, 201):
                pushed += 1
            elif r.status_code == 400 and "already exists" in r.text.lower():
                # Update in place so re-running is safe.
                requests.put(
                    f"{self.url}/service/public/v2/api/policy/service/"
                    f"{self.service}/name/{p['name']}",
                    json=body, auth=self.auth, timeout=self.timeout,
                    headers={"Content-Type": "application/json"})
                pushed += 1
            else:
                raise MaskingError(
                    f"failed to push {p['name']}: {r.status_code} {r.text[:200]}")
        return pushed


def get_provider() -> MaskingProvider:
    """Live Ranger when reachable, local policy engine otherwise.

    Probes rather than assuming, and logs which one it chose -- the distinction
    goes on every masked output and into the trust boundary.
    """
    url = os.environ.get("RANGER_URL")
    if url:
        try:
            import requests
            r = requests.get(f"{url}/service/public/v2/api/service", timeout=3,
                             auth=(os.environ.get("RANGER_ADMIN_USER", "admin"),
                                   os.environ.get("RANGER_ADMIN_PASSWORD", "")))
            if r.status_code < 500:
                log.info("using live Ranger admin at %s", url)
                return RangerAdminProvider(
                    url=url,
                    service=os.environ.get("RANGER_SERVICE_NAME", "hive_pipeline"),
                    user=os.environ.get("RANGER_ADMIN_USER", "admin"),
                    password=os.environ.get("RANGER_ADMIN_PASSWORD", ""))
        except Exception as e:  # noqa: BLE001
            log.warning("Ranger admin unreachable (%s); using local policy engine", e)
    return LocalPolicyProvider()
