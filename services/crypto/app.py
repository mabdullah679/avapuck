"""HTTPS crypto service — AES-256-GCM encrypt/decrypt for sensitive fields.

WHY A SERVICE AND NOT A LIBRARY: if the pipeline encrypted in-process, the key
would live in Spark executor memory, in driver logs on a stack trace, in heap
dumps, and in whatever the JVM swapped to disk. Putting it behind an HTTPS
boundary means the key exists in exactly one process, and every use of it is
an authenticated, auditable call.

WHY GCM AND NOT CBC: GCM is authenticated encryption -- it detects tampering.
AES-CBC without a separate MAC is malleable: an attacker who can flip
ciphertext bits can make predictable changes to the plaintext, and the
decryptor cannot tell. For PII in a regulated pipeline that is not an academic
concern.

WHAT THIS DOES NOT DO: manage keys. See TRUST-BOUNDARY.md.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from typing import Annotated, Literal

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

import sys
sys.path.insert(0, "/opt/pipeline")
from pipeline.common.auth import AuthError, Principal, verifier_from_env

KEY_VERSION = os.environ.get("CRYPTO_KEY_VERSION", "v1")
NONCE_BYTES = 12          # 96 bits, the GCM standard
CIPHER_PREFIX = "enc"


def _master_key() -> bytes:
    raw = os.environ.get("CRYPTO_MASTER_KEY")
    if not raw:
        # Dev fallback only. A generated key means ciphertext does not survive
        # a restart, which fails loudly instead of silently decrypting wrong.
        return AESGCM.generate_key(bit_length=256)
    key = base64.b64decode(raw)
    if len(key) != 32:
        raise RuntimeError(
            f"CRYPTO_MASTER_KEY must decode to 32 bytes for AES-256, got {len(key)}")
    return key


def _blind_index_key() -> bytes:
    raw = os.environ.get("BLIND_INDEX_KEY")
    return base64.b64decode(raw) if raw else secrets.token_bytes(32)


MASTER_KEY = _master_key()
BLIND_KEY = _blind_index_key()
AEAD = AESGCM(MASTER_KEY)

app = FastAPI(title="Pipeline Crypto Service", version="1.0.0")
_verifier = verifier_from_env(audience="crypto-service")


def principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
    """Every endpoint requires a verified JWT. No exceptions, no dev bypass."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,
                            "missing bearer token")
    try:
        return _verifier.verify(authorization.split(" ", 1)[1].strip())
    except AuthError as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(e)) from e


class EncryptRequest(BaseModel):
    field: str = Field(description="logical field name, bound into the AAD")
    values: list[str | None]


class EncryptResponse(BaseModel):
    field: str
    key_version: str
    ciphertexts: list[str | None]
    blind_indexes: list[str | None]


class DecryptRequest(BaseModel):
    field: str
    ciphertexts: list[str | None]


class DecryptResponse(BaseModel):
    field: str
    values: list[str | None]


def _aad(field: str) -> bytes:
    """Bind ciphertext to its field name.

    Without this, ciphertext from `start_station_name` could be pasted into
    `end_station_name` and would decrypt cleanly -- a silent integrity break
    that GCM alone does not prevent. With it, a moved value fails to decrypt.
    """
    return f"{KEY_VERSION}|{field}".encode()


def _blind_index(field: str, value: str) -> str:
    """Deterministic HMAC for equality joins on encrypted columns.

    Encrypted columns are opaque -- no joins, no GROUP BY. This gives Hive a
    deterministic handle so equality still works WITHOUT decryption.

    The tradeoff, stated rather than hidden: determinism leaks equality. Anyone
    reading the index can tell which rows share a value, and on a low-cardinality
    field that enables a dictionary attack. It is keyed with a separate HMAC key
    to prevent offline precomputation, and applied only to fields where joining
    matters. See TRUST-BOUNDARY.md.
    """
    mac = hmac.new(BLIND_KEY, f"{field}|{value}".encode(), hashlib.sha256)
    return base64.urlsafe_b64encode(mac.digest()[:16]).decode().rstrip("=")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "key_version": KEY_VERSION, "algorithm": "AES-256-GCM"}


@app.post("/encrypt", response_model=EncryptResponse)
def encrypt(req: EncryptRequest,
            who: Annotated[Principal, Depends(principal)]) -> EncryptResponse:
    who.require_scope("crypto.encrypt")
    aad = _aad(req.field)
    cts: list[str | None] = []
    bis: list[str | None] = []
    for value in req.values:
        if value is None or value == "":
            cts.append(None)
            bis.append(None)
            continue
        # A fresh random nonce per value. Nonce reuse under the same key is
        # catastrophic for GCM -- it leaks the authentication subkey.
        nonce = secrets.token_bytes(NONCE_BYTES)
        ct = AEAD.encrypt(nonce, value.encode(), aad)
        blob = base64.b64encode(nonce + ct).decode()
        cts.append(f"{CIPHER_PREFIX}:{KEY_VERSION}:{blob}")
        bis.append(_blind_index(req.field, value))
    return EncryptResponse(field=req.field, key_version=KEY_VERSION,
                           ciphertexts=cts, blind_indexes=bis)


@app.post("/decrypt", response_model=DecryptResponse)
def decrypt(req: DecryptRequest,
            who: Annotated[Principal, Depends(principal)]) -> DecryptResponse:
    who.require_scope("crypto.decrypt")
    aad = _aad(req.field)
    out: list[str | None] = []
    for blob in req.ciphertexts:
        if blob is None or blob == "":
            out.append(None)
            continue
        try:
            prefix, version, payload = blob.split(":", 2)
            if prefix != CIPHER_PREFIX:
                raise ValueError(f"not a ciphertext envelope: {prefix!r}")
            if version != KEY_VERSION:
                raise ValueError(
                    f"ciphertext key version {version!r} != service {KEY_VERSION!r}")
            raw = base64.b64decode(payload)
            nonce, ct = raw[:NONCE_BYTES], raw[NONCE_BYTES:]
            out.append(AEAD.decrypt(nonce, ct, aad).decode())
        except Exception as e:  # noqa: BLE001
            # Fail the request. A per-value fallback to None would turn an
            # integrity failure into missing data, which reads as a data
            # quality issue rather than the security event it is.
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"decryption failed for field {req.field!r}: {type(e).__name__}") from e
    return DecryptResponse(field=req.field, values=out)
