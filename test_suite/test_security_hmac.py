"""Security category — inbound webhook hardening (25 tests).

Covers the ingress contract of :mod:`adapters.source_adapter`: HMAC-SHA256
verification, timestamp replay protection, header/body integrity, constant-time
comparison and the sliding-window IP rate limiter.

Requests are served in-process through ``httpx.ASGITransport`` against a
framework-free ASGI harness (:mod:`test_suite.ingress_harness`) that drives the
real ``verify_hmac_sha256`` / ``check_replay_window`` / ``WebhookPayload``
primitives. No socket is ever opened and no web framework is imported.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from test_suite.config_loader import load_config
from test_suite.gateway_support import SlidingWindowRateLimiter
from test_suite.ingress_harness import AsgiIngress, MemoryStorage, source_adapter

CONFIG = load_config()
SECRET_BYTES = str(CONFIG["hmac_secret"]).encode("utf-8")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _app():
    storage = MemoryStorage()
    return AsgiIngress(storage, config=CONFIG), storage


def _sign(body: bytes, secret: bytes = SECRET_BYTES) -> str:
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def _body(**overrides) -> bytes:
    payload = {
        "event_id": "e1",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "n8n",
        "payload": {"k": 1},
    }
    payload.update(overrides)
    return json.dumps(payload).encode("utf-8")


def _post(app, body: bytes, headers=None):
    async def _run():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post("/webhook", content=body, headers=headers or {})

    return asyncio.run(_run())


def _rate_limited_app(limiter: SlidingWindowRateLimiter) -> AsgiIngress:
    return AsgiIngress(MemoryStorage(), config=CONFIG, limiter=limiter, auth=False)


# --------------------------------------------------------------------------- #
# Valid HMAC signatures (3)
# --------------------------------------------------------------------------- #
def test_valid_signature_small_payload():
    app, storage = _app()
    body = _body(payload={"k": 1})
    response = _post(app, body, {"X-Signature": _sign(body)})
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
    assert storage.claims == ["e1"]


def test_valid_signature_medium_payload():
    app, _ = _app()
    body = _body(payload={"blob": "x" * 1024})
    assert _post(app, body, {"X-Signature": _sign(body)}).status_code == 200


def test_valid_signature_large_payload():
    app, _ = _app()
    body = _body(payload={"blob": "y" * 50_000})
    assert _post(app, body, {"X-Signature": _sign(body)}).status_code == 200


# --------------------------------------------------------------------------- #
# Invalid signatures / broken digests (4)
# --------------------------------------------------------------------------- #
def test_signature_with_one_byte_flipped_is_rejected():
    body = _body()
    signature = _sign(body)
    flipped = signature[:-1] + ("0" if signature[-1] != "0" else "1")
    assert _post(_app()[0], body, {"X-Signature": flipped}).status_code == 401


def test_signature_with_wrong_secret_is_rejected():
    body = _body()
    forged = "sha256=" + hmac.new(b"wrong-secret", body, hashlib.sha256).hexdigest()
    assert _post(_app()[0], body, {"X-Signature": forged}).status_code == 401


def test_signature_empty_header_is_rejected():
    body = _body()
    assert _post(_app()[0], body, {"X-Signature": ""}).status_code == 401


def test_signature_malformed_digest_is_rejected():
    body = _body()
    assert _post(_app()[0], body, {"X-Signature": "sha256=nothexdigest"}).status_code == 401


# --------------------------------------------------------------------------- #
# Replay attacks (4)
# --------------------------------------------------------------------------- #
def test_replay_timestamp_in_future_is_rejected():
    future = datetime.now(timezone.utc) + timedelta(seconds=120)
    body = _body(event_ts=future.isoformat())
    assert _post(_app()[0], body, {"X-Signature": _sign(body)}).status_code == 401


def test_replay_timestamp_older_than_300s_is_rejected():
    stale = datetime.now(timezone.utc) - timedelta(seconds=301)
    body = _body(event_ts=stale.isoformat())
    assert _post(_app()[0], body, {"X-Signature": _sign(body)}).status_code == 401


def test_replay_missing_timestamp_fails_contract():
    body = json.dumps({"event_id": "e1", "source": "n8n", "payload": {}}).encode()
    assert _post(_app()[0], body, {"X-Signature": _sign(body)}).status_code == 422


def test_replay_string_timestamp_format_is_rejected():
    body = _body(event_ts="not-a-timestamp")
    assert _post(_app()[0], body, {"X-Signature": _sign(body)}).status_code == 422


# --------------------------------------------------------------------------- #
# Header integrity (4)
# --------------------------------------------------------------------------- #
def test_missing_x_signature_header_is_rejected():
    body = _body()
    assert _post(_app()[0], body, {}).status_code == 401


def test_empty_user_agent_still_authenticates():
    body = _body()
    headers = {"X-Signature": _sign(body), "User-Agent": ""}
    assert _post(_app()[0], body, headers).status_code == 200


def test_x_forwarded_for_spoof_cannot_bypass_hmac():
    body = _body()
    headers = {"X-Signature": "sha256=" + "0" * 64, "X-Forwarded-For": "127.0.0.1"}
    assert _post(_app()[0], body, headers).status_code == 401


def test_untrusted_headers_are_ignored_with_valid_signature():
    body = _body()
    headers = {
        "X-Signature": _sign(body),
        "X-Forwarded-For": "10.0.0.1",
        "X-Real-IP": "10.0.0.2",
    }
    assert _post(_app()[0], body, headers).status_code == 200


# --------------------------------------------------------------------------- #
# Request-body boundary cases (5)
# --------------------------------------------------------------------------- #
def test_empty_body_is_rejected():
    body = b""
    assert _post(_app()[0], body, {"X-Signature": _sign(body)}).status_code == 422


def test_body_larger_than_one_megabyte_is_handled():
    body = _body(payload={"blob": "z" * (1024 * 1024 + 16)})
    assert len(body) > 1024 * 1024
    response = _post(_app()[0], body, {"X-Signature": _sign(body)})
    assert response.status_code in (200, 413)


def test_invalid_utf8_body_is_rejected():
    body = b"\xff\xfe\x00\x01"
    assert _post(_app()[0], body, {"X-Signature": _sign(body)}).status_code == 422


def test_broken_json_body_is_rejected():
    body = b"{not: valid json"
    assert _post(_app()[0], body, {"X-Signature": _sign(body)}).status_code == 422


def test_empty_json_object_is_rejected():
    body = b"{}"
    assert _post(_app()[0], body, {"X-Signature": _sign(body)}).status_code == 422


# --------------------------------------------------------------------------- #
# Timing-attack resistance (2)
# --------------------------------------------------------------------------- #
def test_verify_hmac_uses_constant_time_compare(monkeypatch):
    calls = {"n": 0}
    real = source_adapter.hmac.compare_digest

    def spy(left, right):
        calls["n"] += 1
        return real(left, right)

    monkeypatch.setattr(source_adapter.hmac, "compare_digest", spy)
    body = b"payload"
    signature = _sign(body, SECRET_BYTES)
    assert source_adapter.verify_hmac_sha256(body, signature, SECRET_BYTES) is True
    assert calls["n"] == 1


def test_verify_hmac_rejects_length_mismatch_without_compare(monkeypatch):
    calls = {"n": 0}
    real = source_adapter.hmac.compare_digest

    def spy(left, right):
        calls["n"] += 1
        return real(left, right)

    monkeypatch.setattr(source_adapter.hmac, "compare_digest", spy)
    assert source_adapter.verify_hmac_sha256(b"x", "sha256=abc", SECRET_BYTES) is False
    assert calls["n"] == 0


# --------------------------------------------------------------------------- #
# Inbound IP rate limiting (3)
# --------------------------------------------------------------------------- #
def test_rate_limit_allows_burst_of_ten():
    limiter = SlidingWindowRateLimiter(limit=10, window_s=1.0)
    assert all(limiter.allow("1.2.3.4", now=100.0) for _ in range(10))


def test_rate_limit_returns_429_when_burst_exceeded():
    limiter = SlidingWindowRateLimiter(limit=10, window_s=60.0)
    app = _rate_limited_app(limiter)
    statuses = [
        _post(app, b"{}", {"X-Real-IP": "9.9.9.9"}).status_code for _ in range(11)
    ]
    assert statuses[:10] == [200] * 10
    assert statuses[10] == 429


def test_rate_limit_recovers_after_window():
    limiter = SlidingWindowRateLimiter(limit=2, window_s=1.0)
    assert limiter.allow("ip", now=1.0) is True
    assert limiter.allow("ip", now=1.0) is True
    assert limiter.allow("ip", now=1.0) is False
    assert limiter.allow("ip", now=2.5) is True
