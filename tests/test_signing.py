"""A verdict is genuine only if the IdP signed THIS payload for THIS event."""
from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.common.auth import AuthError                                  # noqa: E402
from pipeline.messaging import build, EVENT_RUN_VERDICT                    # noqa: E402
from pipeline.messaging.signing import (SIGNER_CLIENT_ID, VERDICT_AUDIENCE,  # noqa: E402
                                        VerdictVerifier, payload_digest)

ISSUER = "https://idp.test"
KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
SLOT = datetime(2026, 9, 6, 4, tzinfo=timezone.utc)

PAYLOAD = {"proceed": True, "status": "complete", "reasons": [], "stages": {},
           "outputs": {"warehouse_table": "warehouse.trips", "hive_table": None, "pdf": None,
                       "rows_published": 100, "rows_loaded": 100, "topics": {}},
           "data_window": {"start": None, "end": None, "source": "logical_date"},
           "computed_at": "2026-09-06T04:09:00Z"}


def _jws(envelope, payload, *, key=KEY, sub=SIGNER_CLIENT_ID, aud=VERDICT_AUDIENCE,
         iss=ISSUER, event_id=None, digest=None):
    """What the IdP's /sign endpoint produces, built locally."""
    claims = {"iss": iss, "sub": sub, "aud": aud, "iat": int(time.time()),
              "jti": str(uuid.uuid4()),
              "digest": digest or payload_digest(payload),
              "event_id": event_id or envelope["event_id"],
              "event_type": "run.verdict", "dataset": envelope["dataset"],
              "run_slot": envelope["run_slot"]}
    return jwt.encode(claims, key, algorithm="RS256", headers={"kid": "test-key"})


def _message(**sign_kw):
    msg = build(event_type=EVENT_RUN_VERDICT, dataset="trips", run_slot=SLOT, trace_id="t",
                run_id="r", try_number=1, key=None, producer="pipeline", payload=PAYLOAD)
    msg["signature"] = _jws(msg["envelope"], PAYLOAD, **sign_kw)
    return msg


class _Key:
    def __init__(self, key): self.key = key


@pytest.fixture
def verifier(monkeypatch):
    v = VerdictVerifier(issuer=ISSUER)
    # Stand in for the JWKS fetch: the verifier resolves the IdP's public key.
    monkeypatch.setattr(v, "_jwk_client",
                        lambda: type("C", (), {"get_signing_key_from_jwt":
                                                lambda self, t: _Key(KEY.public_key())})())
    return v


def test_genuine_verdict_verifies(verifier):
    claims = verifier.verify(_message())
    assert claims["sub"] == SIGNER_CLIENT_ID
    assert claims["aud"] == VERDICT_AUDIENCE


def test_altered_payload_is_rejected(verifier):
    msg = _message()
    msg["payload"]["proceed"] = False          # one flipped bit after signing
    with pytest.raises(AuthError, match="digest does not match"):
        verifier.verify(msg)


def test_signature_for_a_different_event_is_rejected(verifier):
    msg = _message(event_id="0" * 32)
    with pytest.raises(AuthError, match="different event_id"):
        verifier.verify(msg)


def test_wrong_key_is_rejected(verifier):
    with pytest.raises(AuthError, match="invalid verdict signature"):
        verifier.verify(_message(key=OTHER_KEY))


def test_wrong_audience_is_rejected(verifier):
    with pytest.raises(AuthError, match="invalid verdict signature"):
        verifier.verify(_message(aud="crypto-service"))


def test_wrong_issuer_is_rejected(verifier):
    with pytest.raises(AuthError, match="invalid verdict signature"):
        verifier.verify(_message(iss="https://evil.example"))


def test_wrong_subject_is_rejected(verifier):
    """A valid token from another registered job must not pass as a verdict."""
    with pytest.raises(AuthError, match="subject"):
        verifier.verify(_message(sub="spark-job"))


def test_missing_signature_is_rejected(verifier):
    msg = _message()
    del msg["signature"]
    with pytest.raises(AuthError, match="no signature"):
        verifier.verify(msg)


def test_alg_none_is_rejected(verifier):
    msg = _message()
    claims = jwt.decode(msg["signature"], options={"verify_signature": False})
    msg["signature"] = jwt.encode(claims, key=None, algorithm="none")
    with pytest.raises(AuthError):
        verifier.verify(msg)


def test_digest_is_over_canonical_json():
    assert payload_digest({"b": 1, "a": 2}) == payload_digest({"a": 2, "b": 1})
    assert payload_digest({"a": 1}) != payload_digest({"a": 2})


# ── The IdP side, through the real app ────────────────────────────────────

@pytest.fixture
def idp_client(monkeypatch):
    httpx = pytest.importorskip("httpx")   # noqa: F841 -- TestClient needs it
    from fastapi.testclient import TestClient
    monkeypatch.setenv("CLIENT_SECRET_ORCHESTRATOR_JOB", "s3cret")
    monkeypatch.setenv("CLIENT_SECRET_SPARK_JOB", "spark-secret")
    import importlib
    import services.idp.app as idp
    importlib.reload(idp)
    return TestClient(idp.app), idp


def _token(client, client_id, secret, audience):
    r = client.post("/token", data={"grant_type": "client_credentials",
                                    "client_id": client_id, "client_secret": secret,
                                    "audience": audience})
    return r


def test_idp_signs_for_the_orchestrator_and_the_result_verifies(idp_client):
    client, idp = idp_client
    tok = _token(client, "orchestrator-job", "s3cret", "idp")
    assert tok.status_code == 200, tok.text
    msg = build(event_type=EVENT_RUN_VERDICT, dataset="trips", run_slot=SLOT, trace_id="t",
                run_id="r", try_number=1, key=None, producer="pipeline", payload=PAYLOAD)
    r = client.post("/sign", json={"digest": payload_digest(PAYLOAD),
                                   "event_id": msg["envelope"]["event_id"],
                                   "event_type": "run.verdict", "dataset": "trips",
                                   "run_slot": msg["envelope"]["run_slot"]},
                    headers={"Authorization": f"Bearer {tok.json()['access_token']}"})
    assert r.status_code == 200, r.text
    msg["signature"] = r.json()["jws"]

    v = VerdictVerifier(issuer=idp.ISSUER)
    v._jwk_client = lambda: type("C", (), {"get_signing_key_from_jwt":
                                            lambda self, t: _Key(idp.PUBLIC_KEY)})()
    assert v.verify(msg)["sub"] == "orchestrator-job"


def test_idp_refuses_to_sign_for_a_job_without_the_scope(idp_client):
    client, idp = idp_client
    tok = _token(client, "spark-job", "spark-secret", "crypto-service")
    assert tok.status_code == 200
    r = client.post("/sign", json={"digest": "0" * 64, "event_id": "0" * 32,
                                   "event_type": "run.verdict", "dataset": "trips",
                                   "run_slot": "2026-09-06T04:00:00Z"},
                    headers={"Authorization": f"Bearer {tok.json()['access_token']}"})
    # The spark-job token's audience is crypto-service, not idp: refused at auth.
    assert r.status_code == 401


def test_idp_refuses_to_sign_without_a_token(idp_client):
    client, _ = idp_client
    r = client.post("/sign", json={"digest": "0" * 64, "event_id": "0" * 32,
                                   "event_type": "run.verdict", "dataset": "trips",
                                   "run_slot": "2026-09-06T04:00:00Z"})
    assert r.status_code == 401


def test_orchestrator_cannot_get_a_crypto_token(idp_client):
    """Signing rights must not leak into encrypt/decrypt rights."""
    client, _ = idp_client
    r = _token(client, "orchestrator-job", "s3cret", "crypto-service")
    assert r.status_code == 403
