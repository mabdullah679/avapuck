"""Identity Provider — mints and publishes verification material for job JWTs.

Every pipeline job authenticates to every service with a short-lived JWT from
here. The design rule that matters: **services verify signatures against this
IdP's public JWKS; they never decode a token and trust its claims.** A decoded
token is attacker-controlled JSON. Verification is what makes it an identity.

Asymmetric RS256 rather than a shared HMAC secret is deliberate. With HS256,
every service that can *verify* a token can also *mint* one, so a compromised
consumer becomes an identity forger. With RS256 the private key never leaves
this process.
"""
from __future__ import annotations

import base64
import os
import time
import uuid
from typing import Annotated

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Depends, FastAPI, Form, Header, HTTPException, status
from pydantic import BaseModel, Field

ISSUER = os.environ.get("IDP_ISSUER", "https://idp.local")
KEY_ID = "wps-idp-key-1"
TOKEN_TTL_SECONDS = int(os.environ.get("IDP_TOKEN_TTL", "300"))   # 5 minutes

# Registered pipeline jobs and the audiences each may request. A job asking for
# an audience outside its grant is refused -- this is what stops the extract
# job's token from being replayed against the Postgres loader.
CLIENTS: dict[str, dict] = {
    "extract-job":  {"audiences": ["crypto-service"], "scopes": ["bigquery.read"]},
    "spark-job":    {"audiences": ["crypto-service"], "scopes": ["crypto.encrypt"]},
    "hive-job":     {"audiences": ["crypto-service", "ranger"],
                     "scopes": ["crypto.decrypt", "ranger.policy.read"]},
    "postgres-job": {"audiences": ["warehouse"], "scopes": ["warehouse.write"]},
    "report-job":   {"audiences": ["warehouse"], "scopes": ["warehouse.read"]},
    # The orchestrator may ask THIS service to sign a run verdict, and nothing
    # else. Its token audience is the IdP itself; the JWS it gets back carries
    # a different audience (pipeline-consumers) so the two cannot be confused.
    "orchestrator-job": {"audiences": ["idp"], "scopes": ["verdict.sign"]},
}

# The audience stamped on signed verdicts. Consumers verify against it.
VERDICT_AUDIENCE = "pipeline-consumers"


def _load_or_create_key() -> rsa.RSAPrivateKey:
    """Prefer an injected key; generate an ephemeral one only for local dev.

    An ephemeral key means tokens do not survive a restart -- which is correct
    for a dev default, because it fails loudly rather than silently accepting
    tokens signed by a key nobody can account for.
    """
    pem = os.environ.get("IDP_SIGNING_KEY")
    if pem:
        return serialization.load_pem_private_key(pem.encode(), password=None)
    path = os.environ.get("IDP_SIGNING_KEY_FILE")
    if path and os.path.exists(path):
        with open(path, "rb") as fh:
            return serialization.load_pem_private_key(fh.read(), password=None)
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


PRIVATE_KEY = _load_or_create_key()
PUBLIC_KEY = PRIVATE_KEY.public_key()

app = FastAPI(title="Pipeline IdP", version="1.0.0")


def _b64u(n: int) -> str:
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    expires_in: int
    scope: str


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "issuer": ISSUER}


@app.get("/.well-known/jwks.json")
def jwks() -> dict:
    """Public verification material. Services fetch and cache this."""
    nums = PUBLIC_KEY.public_numbers()
    return {"keys": [{
        "kty": "RSA", "use": "sig", "alg": "RS256", "kid": KEY_ID,
        "n": _b64u(nums.n), "e": _b64u(nums.e),
    }]}


@app.get("/.well-known/openid-configuration")
def discovery() -> dict:
    return {
        "issuer": ISSUER,
        "jwks_uri": f"{ISSUER}/.well-known/jwks.json",
        "token_endpoint": f"{ISSUER}/token",
        "grant_types_supported": ["client_credentials"],
        "id_token_signing_alg_values_supported": ["RS256"],
    }


@app.post("/token", response_model=TokenResponse)
def token(
    client_id: Annotated[str, Form()],
    client_secret: Annotated[str, Form()],
    audience: Annotated[str, Form()],
    grant_type: Annotated[str, Form()] = "client_credentials",
) -> TokenResponse:
    if grant_type != "client_credentials":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "unsupported_grant_type")

    client = CLIENTS.get(client_id)
    expected = os.environ.get(f"CLIENT_SECRET_{client_id.upper().replace('-', '_')}")

    # Constant-time comparison, and an identical error for unknown-client and
    # wrong-secret -- distinguishing them tells an attacker which client ids
    # are real.
    import hmac
    ok = client is not None and expected is not None and hmac.compare_digest(
        client_secret, expected)
    if not ok:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid_client")

    if audience not in client["audiences"]:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"client {client_id!r} may not request audience {audience!r}")

    now = int(time.time())
    claims = {
        "iss": ISSUER,
        "sub": client_id,
        "aud": audience,
        "iat": now,
        "nbf": now,
        "exp": now + TOKEN_TTL_SECONDS,
        "jti": str(uuid.uuid4()),          # replay tracking
        "scope": " ".join(client["scopes"]),
    }
    encoded = jwt.encode(claims, PRIVATE_KEY, algorithm="RS256",
                         headers={"kid": KEY_ID})
    return TokenResponse(access_token=encoded, expires_in=TOKEN_TTL_SECONDS,
                         scope=claims["scope"])


# ── Verdict signing ──────────────────────────────────────────────────────
#
# The orchestrator does not hold a signing key; it asks the IdP to vouch for
# a run verdict, the same way every job asks it to vouch for an identity. The
# IdP signs a DIGEST of the payload plus the event id, never the payload
# itself, so the statement is bound to exactly one message and one content.
# Consumers verify with the same JWKS they already trust. docs/DECISIONS.md D6.

class SignRequest(BaseModel):
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    event_type: str
    dataset: str = Field(min_length=1)
    run_slot: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class SignResponse(BaseModel):
    jws: str
    kid: str = KEY_ID


def _verify_own_token(authorization: str | None, audience: str) -> dict:
    """Verify a bearer token this IdP issued. Signature, issuer, audience,
    expiry, algorithm pinned -- the same rules services apply, applied here."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token")
    token = authorization.split(None, 1)[1].strip()
    try:
        return jwt.decode(
            token, PUBLIC_KEY, algorithms=["RS256"], audience=audience,
            issuer=ISSUER, leeway=30,
            options={"require": ["exp", "iat", "nbf", "iss", "aud", "sub"]})
    except jwt.InvalidTokenError as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"invalid token: {e}") from e


@app.post("/sign", response_model=SignResponse)
def sign(req: SignRequest,
         authorization: Annotated[str | None, Header()] = None) -> SignResponse:
    claims_in = _verify_own_token(authorization, audience="idp")
    if "verdict.sign" not in claims_in.get("scope", "").split():
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            f"{claims_in['sub']!r} lacks scope verdict.sign")
    if req.event_type != "run.verdict":
        raise HTTPException(status.HTTP_400_BAD_REQUEST,
                            f"only run.verdict is signed, not {req.event_type!r}")

    now = int(time.time())
    claims = {
        "iss": ISSUER,
        "sub": claims_in["sub"],           # who asked: orchestrator-job
        "aud": VERDICT_AUDIENCE,
        "iat": now,
        "jti": str(uuid.uuid4()),
        # A statement, not a session: no exp. See VerdictVerifier.
        "digest": req.digest,
        "event_id": req.event_id,
        "event_type": req.event_type,
        "dataset": req.dataset,
        "run_slot": req.run_slot,
    }
    return SignResponse(jws=jwt.encode(claims, PRIVATE_KEY, algorithm="RS256",
                                       headers={"kid": KEY_ID}))
