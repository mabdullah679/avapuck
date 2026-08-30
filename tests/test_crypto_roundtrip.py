"""End-to-end: IdP mints a token, crypto service verifies it, AES-256-GCM round-trips."""
from __future__ import annotations

import base64
import os
import sys

import pytest
from fastapi.testclient import TestClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

os.environ.setdefault("IDP_ISSUER", "https://idp.test")
os.environ.setdefault("CLIENT_SECRET_SPARK_JOB", "spark-secret")
os.environ.setdefault("CLIENT_SECRET_HIVE_JOB", "hive-secret")
os.environ.setdefault("CRYPTO_MASTER_KEY", base64.b64encode(b"k" * 32).decode())
os.environ.setdefault("BLIND_INDEX_KEY", base64.b64encode(b"b" * 32).decode())


@pytest.fixture(scope="module")
def idp():
    from services.idp.app import app
    return TestClient(app)


@pytest.fixture(scope="module")
def crypto(idp, monkeypatch_session):
    import services.crypto.app as mod
    # Point the verifier at the in-process IdP's JWKS.
    jwks = idp.get("/.well-known/jwks.json").json()

    class _Key:
        def __init__(self, k): self.key = k

    from jwt.algorithms import RSAAlgorithm
    pub = RSAAlgorithm.from_jwk(jwks["keys"][0])
    mod._verifier._client = type("C", (), {
        "get_signing_key_from_jwt": staticmethod(lambda t: _Key(pub))})()
    return TestClient(mod.app)


@pytest.fixture(scope="module")
def monkeypatch_session():
    from _pytest.monkeypatch import MonkeyPatch
    mp = MonkeyPatch()
    yield mp
    mp.undo()


def _token(idp, client_id, secret, audience="crypto-service"):
    r = idp.post("/token", data={"grant_type": "client_credentials",
                                 "client_id": client_id, "client_secret": secret,
                                 "audience": audience})
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


def test_encrypt_decrypt_roundtrip(idp, crypto):
    tok_enc = _token(idp, "spark-job", "spark-secret")
    tok_dec = _token(idp, "hive-job", "hive-secret")
    values = ["Zilker Park", "Congress & 6th", None, "Riverside"]

    r = crypto.post("/encrypt", json={"field": "start_station_name", "values": values},
                    headers={"Authorization": f"Bearer {tok_enc}"})
    assert r.status_code == 200, r.text
    body = r.json()
    cts = body["ciphertexts"]

    # Ciphertext must not contain the plaintext anywhere.
    for v, ct in zip(values, cts):
        if v is None:
            assert ct is None
        else:
            assert v not in ct
            assert ct.startswith("enc:v1:")

    # Same plaintext twice -> different ciphertext (random nonce), but the
    # blind index IS deterministic, which is the whole point of having one.
    r2 = crypto.post("/encrypt", json={"field": "start_station_name",
                                       "values": ["Zilker Park"]},
                     headers={"Authorization": f"Bearer {tok_enc}"})
    assert r2.json()["ciphertexts"][0] != cts[0], "nonce reuse! ciphertext is deterministic"
    assert r2.json()["blind_indexes"][0] == body["blind_indexes"][0]

    d = crypto.post("/decrypt", json={"field": "start_station_name", "ciphertexts": cts},
                    headers={"Authorization": f"Bearer {tok_dec}"})
    assert d.status_code == 200, d.text
    assert d.json()["values"] == values


def test_aad_binds_ciphertext_to_its_field(idp, crypto):
    """Ciphertext moved to another column must NOT decrypt."""
    tok_enc = _token(idp, "spark-job", "spark-secret")
    tok_dec = _token(idp, "hive-job", "hive-secret")
    ct = crypto.post("/encrypt", json={"field": "start_station_name", "values": ["Zilker"]},
                     headers={"Authorization": f"Bearer {tok_enc}"}).json()["ciphertexts"]
    bad = crypto.post("/decrypt", json={"field": "end_station_name", "ciphertexts": ct},
                      headers={"Authorization": f"Bearer {tok_dec}"})
    assert bad.status_code == 400, "ciphertext decrypted under the WRONG field name"


def test_tampered_ciphertext_is_rejected(idp, crypto):
    """GCM authenticates: a flipped bit must fail, not silently decrypt."""
    tok_enc = _token(idp, "spark-job", "spark-secret")
    tok_dec = _token(idp, "hive-job", "hive-secret")
    ct = crypto.post("/encrypt", json={"field": "bikeid", "values": ["12345"]},
                     headers={"Authorization": f"Bearer {tok_enc}"}).json()["ciphertexts"][0]
    prefix, ver, payload = ct.split(":", 2)
    raw = bytearray(base64.b64decode(payload))
    raw[-1] ^= 0x01
    tampered = f"{prefix}:{ver}:{base64.b64encode(bytes(raw)).decode()}"
    r = crypto.post("/decrypt", json={"field": "bikeid", "ciphertexts": [tampered]},
                    headers={"Authorization": f"Bearer {tok_dec}"})
    assert r.status_code == 400, "TAMPERED CIPHERTEXT WAS ACCEPTED"


def test_unauthenticated_is_refused(crypto):
    r = crypto.post("/encrypt", json={"field": "x", "values": ["a"]})
    assert r.status_code == 401


def test_wrong_scope_is_refused(idp, crypto):
    """extract-job may not encrypt; it has no crypto.encrypt scope."""
    r = idp.post("/token", data={"grant_type": "client_credentials",
                                 "client_id": "extract-job",
                                 "client_secret": os.environ.get(
                                     "CLIENT_SECRET_EXTRACT_JOB", "x"),
                                 "audience": "crypto-service"})
    if r.status_code != 200:
        pytest.skip("extract-job secret not configured")
    tok = r.json()["access_token"]
    bad = crypto.post("/encrypt", json={"field": "x", "values": ["a"]},
                      headers={"Authorization": f"Bearer {tok}"})
    assert bad.status_code in (401, 403)


def test_audience_grant_is_enforced(idp):
    """postgres-job may not request a crypto-service token at all."""
    r = idp.post("/token", data={"grant_type": "client_credentials",
                                 "client_id": "postgres-job",
                                 "client_secret": os.environ.get(
                                     "CLIENT_SECRET_POSTGRES_JOB", "x"),
                                 "audience": "crypto-service"})
    assert r.status_code in (401, 403)
