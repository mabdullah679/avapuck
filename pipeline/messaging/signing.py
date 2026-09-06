"""Signed verdicts: the IdP vouches for what the orchestrator said.

Kafka ACLs stop anyone but the `pipeline` principal writing the control topic.
That guards the topic. The signature guards the MESSAGE wherever it travels
afterwards -- into a consumer's table, a log, another broker -- and applies
non-negotiable 2 to Kafka: no job trusts another job's word about identity.

How it works (docs/DECISIONS.md D6):

  1. The orchestrator authenticates to the IdP as `orchestrator-job` and
     obtains a normal short-lived token for audience `idp`.
  2. It POSTs the verdict's payload DIGEST (sha256 of the canonical JSON) and
     the envelope's event_id to the IdP's /sign endpoint.
  3. The IdP returns a compact RS256 JWS whose claims name the digest, the
     event_id, the subject that asked, and an audience of `pipeline-consumers`.
  4. A consumer verifies the JWS against the IdP's JWKS -- signature, issuer,
     audience, subject -- then recomputes the payload digest and compares.

The payload is not embedded in the JWS; the digest binds them. A payload
altered by a single byte fails step 4 even though the JWS itself is intact.
"""
from __future__ import annotations

import hashlib
import os
import threading

import jwt
import requests
from jwt import PyJWKClient

from pipeline.common.auth import AuthError, _jwks_ssl_context
from pipeline.messaging.envelope import canonical_json

SIGNER_CLIENT_ID = "orchestrator-job"
SIGN_AUDIENCE = "idp"                    # audience of the token that may call /sign
VERDICT_AUDIENCE = "pipeline-consumers"  # audience stamped on the JWS itself
DEFAULT_LEEWAY_SECONDS = 30


def payload_digest(payload: dict) -> str:
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


class Signer:
    """Client side of /sign. Holds no key material -- that is the point."""

    def __init__(self, idp_url: str, token_client, verify_tls: bool | str = True):
        self.idp_url = idp_url.rstrip("/")
        self.tokens = token_client
        self.verify_tls = verify_tls

    def sign(self, envelope: dict, payload: dict) -> str:
        body = {
            "digest": payload_digest(payload),
            "event_id": envelope["event_id"],
            "event_type": envelope["event_type"],
            "dataset": envelope["dataset"],
            "run_slot": envelope["run_slot"],
        }
        try:
            r = requests.post(f"{self.idp_url}/sign", json=body,
                              headers=self.tokens.auth_header(SIGN_AUDIENCE),
                              timeout=10, verify=self.verify_tls)
        except requests.RequestException as e:
            raise AuthError(f"IdP unreachable for signing at {self.idp_url}: {e}") from e
        if r.status_code != 200:
            raise AuthError(f"IdP refused to sign: {r.status_code} {r.text[:200]}")
        return r.json()["jws"]


class VerdictVerifier:
    """Consumer side. Same strictness as TokenVerifier, different claim set.

    A verdict is a statement, not a session: it has `iat` but no `exp`,
    because "the run of 2026-09-06T04:00Z proceeded" does not stop being true
    at midnight. Everything else -- signature, issuer, audience, pinned
    algorithm -- is required.
    """

    def __init__(self, issuer: str, jwks_url: str | None = None,
                 expected_subject: str = SIGNER_CLIENT_ID,
                 leeway: int = DEFAULT_LEEWAY_SECONDS):
        self.issuer = issuer
        self.expected_subject = expected_subject
        self.leeway = leeway
        self._jwks_url = jwks_url or f"{issuer}/.well-known/jwks.json"
        self._client: PyJWKClient | None = None
        self._lock = threading.Lock()

    def _jwk_client(self) -> PyJWKClient:
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = PyJWKClient(self._jwks_url, cache_keys=True,
                                               ssl_context=_jwks_ssl_context())
        return self._client

    def verify(self, message: dict) -> dict:
        """Return the JWS claims if, and only if, the message is genuine."""
        jws = message.get("signature")
        if not jws:
            raise AuthError("verdict carries no signature")
        try:
            key = self._jwk_client().get_signing_key_from_jwt(jws).key
        except Exception as e:  # noqa: BLE001 -- surfaced as AuthError, never ignored
            raise AuthError(f"cannot resolve signing key: {e}") from e
        try:
            claims = jwt.decode(
                jws, key,
                algorithms=["RS256"],
                audience=VERDICT_AUDIENCE,
                issuer=self.issuer,
                leeway=self.leeway,
                options={
                    "require": ["iat", "iss", "aud", "sub", "jti", "digest", "event_id"],
                    "verify_signature": True,
                    "verify_exp": False,      # statements do not expire; see class doc
                    "verify_iat": True,
                    "verify_aud": True,
                    "verify_iss": True,
                },
            )
        except jwt.InvalidTokenError as e:
            raise AuthError(f"invalid verdict signature: {e}") from e

        env = message["envelope"]
        if claims["sub"] != self.expected_subject:
            raise AuthError(f"verdict signed for subject {claims['sub']!r}, "
                            f"expected {self.expected_subject!r}")
        if claims["event_id"] != env["event_id"]:
            raise AuthError("signature was issued for a different event_id")
        if claims["digest"] != payload_digest(message["payload"]):
            raise AuthError("payload digest does not match the signature; "
                            "the payload was altered after signing")
        return claims


def verifier_from_env() -> VerdictVerifier:
    return VerdictVerifier(
        issuer=os.environ.get("IDP_ISSUER", "https://idp.local"),
        jwks_url=os.environ.get("IDP_JWKS_URL"),
    )
