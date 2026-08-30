"""JWT verification, and the client that obtains tokens.

THE RULE THIS FILE EXISTS TO ENFORCE: a token is verified by signature,
issuer, audience, expiry and not-before -- never merely decoded. `jwt.decode`
with `verify_signature=False` is the single most common auth bug in pipelines
of this shape, and it is worse than having no auth at all, because it looks
like auth in code review.

Verification here is strict by construction:
  * algorithms is pinned to ["RS256"] so a token with alg:none or a swapped
    HMAC alg is rejected rather than trusted
  * audience and issuer are required, not optional
  * expiry and not-before are enforced with a small leeway for clock skew
  * the signing key is fetched from the IdP's JWKS by the token's `kid`
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

import jwt
import requests
from jwt import PyJWKClient

DEFAULT_LEEWAY_SECONDS = 30


class AuthError(Exception):
    """Raised on any verification failure. Never swallowed, never downgraded."""


@dataclass(frozen=True)
class Principal:
    """A verified identity. Constructing one requires passing verification."""
    subject: str
    audience: str
    scopes: frozenset[str]
    token_id: str
    expires_at: int

    def require_scope(self, scope: str) -> None:
        if scope not in self.scopes:
            raise AuthError(
                f"principal {self.subject!r} lacks required scope {scope!r} "
                f"(has: {sorted(self.scopes)})")


class TokenVerifier:
    """Verifies tokens against the IdP's published JWKS.

    The JWKS is cached by PyJWKClient; a `kid` miss triggers a refetch, which
    is what makes key rotation work without a restart.
    """

    def __init__(self, issuer: str, audience: str, jwks_url: str | None = None,
                 leeway: int = DEFAULT_LEEWAY_SECONDS):
        self.issuer = issuer
        self.audience = audience
        self.leeway = leeway
        self._jwks_url = jwks_url or f"{issuer}/.well-known/jwks.json"
        self._client: PyJWKClient | None = None
        self._lock = threading.Lock()

    def _jwk_client(self) -> PyJWKClient:
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = PyJWKClient(self._jwks_url, cache_keys=True)
        return self._client

    def verify(self, token: str) -> Principal:
        try:
            signing_key = self._jwk_client().get_signing_key_from_jwt(token).key
        except Exception as e:  # noqa: BLE001 — surfaced as AuthError, never ignored
            raise AuthError(f"cannot resolve signing key: {e}") from e

        try:
            claims = jwt.decode(
                token,
                signing_key,
                algorithms=["RS256"],        # pinned: defeats alg confusion
                audience=self.audience,      # required
                issuer=self.issuer,          # required
                leeway=self.leeway,
                options={
                    "require": ["exp", "iat", "nbf", "iss", "aud", "sub"],
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_nbf": True,
                    "verify_aud": True,
                    "verify_iss": True,
                },
            )
        except jwt.ExpiredSignatureError as e:
            raise AuthError("token expired") from e
        except jwt.InvalidAudienceError as e:
            raise AuthError(f"token audience is not {self.audience!r}") from e
        except jwt.InvalidIssuerError as e:
            raise AuthError(f"token issuer is not {self.issuer!r}") from e
        except jwt.InvalidTokenError as e:
            raise AuthError(f"invalid token: {e}") from e

        return Principal(
            subject=claims["sub"],
            audience=self.audience,
            scopes=frozenset(claims.get("scope", "").split()),
            token_id=claims.get("jti", ""),
            expires_at=int(claims["exp"]),
        )


class TokenClient:
    """Obtains tokens from the IdP, with refresh before expiry.

    Tokens are cached per (client_id, audience) and refreshed early rather than
    on failure -- a job that discovers expiry mid-run has already failed a
    request it did not need to fail.
    """

    REFRESH_SKEW_SECONDS = 60

    def __init__(self, idp_url: str, client_id: str, client_secret: str,
                 verify_tls: bool | str = True):
        self.idp_url = idp_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.verify_tls = verify_tls
        self._cache: dict[str, tuple[str, float]] = {}
        self._lock = threading.Lock()

    def token_for(self, audience: str) -> str:
        with self._lock:
            hit = self._cache.get(audience)
            if hit and hit[1] - self.REFRESH_SKEW_SECONDS > time.time():
                return hit[0]

        try:
            r = requests.post(
                f"{self.idp_url}/token",
                data={"grant_type": "client_credentials",
                      "client_id": self.client_id,
                      "client_secret": self.client_secret,
                      "audience": audience},
                timeout=10,
                verify=self.verify_tls,
            )
        except requests.RequestException as e:
            raise AuthError(f"IdP unreachable at {self.idp_url}: {e}") from e

        if r.status_code != 200:
            # The body may echo the client id; never log the secret.
            raise AuthError(f"IdP refused token for audience {audience!r}: "
                            f"{r.status_code} {r.text[:200]}")

        payload = r.json()
        token, ttl = payload["access_token"], int(payload["expires_in"])
        with self._lock:
            self._cache[audience] = (token, time.time() + ttl)
        return token

    def auth_header(self, audience: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token_for(audience)}"}


def verifier_from_env(audience: str) -> TokenVerifier:
    return TokenVerifier(
        issuer=os.environ.get("IDP_ISSUER", "https://idp.local"),
        audience=audience,
        jwks_url=os.environ.get("IDP_JWKS_URL"),
    )
