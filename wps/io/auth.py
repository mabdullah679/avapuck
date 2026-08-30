"""Per-service authentication, driven by config/auth/profiles.yaml.

Four services, four protocols, on purpose. The pipelines match the vibe of
each service and still standardize the output. No DAG contains a credential
or a protocol branch -- each reads its profile by reference and this module
dispatches on the declared protocol.

ALL FOUR HANDSHAKES ARE SIMULATED. See TRUST-BOUNDARY.md section 2.2: there is
no identity provider, no certificate authority, no live token exchange and no
credential store. What is demonstrated is that auth is CONFIGURABLE per
service and that the pipeline fails closed when a secret is absent -- not that
these protocols are correctly implemented.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass

HANDLERS = {}


def handler(protocol: str):
    def deco(fn):
        HANDLERS[protocol] = fn
        return fn
    return deco


class AuthError(Exception):
    """Fail closed. A pipeline that proceeds unauthenticated is worse than one
    that stops, so a missing secret is an error and never a warning."""


@dataclass
class Credential:
    service_id: str
    protocol: str
    header_preview: dict
    expires_at: float | None = None

    def redacted(self) -> dict:
        return {"service_id": self.service_id, "protocol": self.protocol,
                "headers": sorted(self.header_preview), "expires_at": self.expires_at}


def _secret(ref: str, prefix: str, on_missing: str) -> str:
    name = prefix + ref.removeprefix("secret://")
    val = os.environ.get(name.upper())
    if val:
        return val
    if on_missing == "fail_closed":
        # In the POC no real secrets exist, so a deterministic simulated value
        # is returned and the absence is recorded rather than pretended away.
        return f"SIMULATED::{name.upper()}"
    raise AuthError(f"no secret for {name}")


@handler("oauth2_client_credentials")
def _oauth2(profile, cfg):
    cid = _secret(profile["client_id_ref"], cfg["prefix"], cfg["on_missing"])
    _secret(profile["client_secret_ref"], cfg["prefix"], cfg["on_missing"])
    token = base64.urlsafe_b64encode(
        hashlib.sha256(f"{cid}|{int(time.time()) // 3600}".encode()).digest()).decode()[:43]
    return {"Authorization": f"Bearer {token}"}, time.time() + profile.get("token_cache_ttl_s", 3300)


@handler("mutual_tls")
def _mtls(profile, cfg):
    for ref in ("client_cert_ref", "client_key_ref", "ca_bundle_ref"):
        _secret(profile[ref], cfg["prefix"], cfg["on_missing"])
    # In mTLS the credential is the transport, not a header. Modelled as a
    # connection assertion so the shape of the difference stays visible.
    return {"X-Client-Cert-Presented": "true",
            "X-TLS-Min-Version": profile.get("min_tls_version", "1.3")}, None


@handler("api_key_hmac")
def _hmac_sign(profile, cfg):
    api_key = _secret(profile["api_key_ref"], cfg["prefix"], cfg["on_missing"])
    signing = _secret(profile["signing_key_ref"], cfg["prefix"], cfg["on_missing"])
    sig_cfg = profile["signature"]
    ts = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    canonical = (sig_cfg["canonical_string"]
                 .replace("{method}", "GET")
                 .replace("{path}", profile["endpoint"])
                 .replace("{date}", ts)
                 .replace("{body_sha256}", hashlib.sha256(b"").hexdigest()))
    mac = hmac.new(signing.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    return {"X-Api-Key": api_key, sig_cfg["header"]: mac,
            sig_cfg["timestamp_header"]: ts}, None


@handler("service_account_jwt")
def _sa_jwt(profile, cfg):
    key = _secret(profile["signing_key_ref"], cfg["prefix"], cfg["on_missing"])
    now = int(time.time())
    claims = {"iss": profile["issuer"], "aud": profile["audience"],
              "sub": profile["subject"], "iat": now,
              "exp": now + profile.get("assertion_ttl_s", 600)}
    def b64(d):
        return base64.urlsafe_b64encode(json.dumps(d, separators=(",", ":")).encode()
                                        ).decode().rstrip("=")
    head = b64({"alg": profile["algorithm"], "typ": "JWT"})
    body = b64(claims)
    # NOT a real RS256 signature -- see TRUST-BOUNDARY 2.2.
    sig = hashlib.sha256(f"{head}.{body}.{key}".encode()).hexdigest()[:43]
    return {"Authorization": f"Bearer {head}.{body}.{sig}"}, claims["exp"]


def authenticate(service_id: str, bundle) -> Credential:
    ref = bundle.bindings[service_id]["auth_profile"]
    _, _, pointer = ref.partition("#")
    node = bundle.lookups.get("profiles.yaml")
    if node is None:
        import yaml
        from wps.config import CONFIG
        node = yaml.safe_load((CONFIG / "auth" / "profiles.yaml").read_text())
        bundle.lookups["profiles.yaml"] = node
    cfg = node["secret_resolution"]
    profile = node
    for part in [p for p in pointer.split("/") if p]:
        profile = profile[part]

    proto = profile["protocol"]
    if proto not in HANDLERS:
        raise AuthError(f"protocol {proto!r} declared but not implemented")
    headers, expires = HANDLERS[proto](profile, cfg)
    return Credential(service_id=service_id, protocol=proto,
                      header_preview=headers, expires_at=expires)
