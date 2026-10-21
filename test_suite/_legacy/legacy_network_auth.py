"""Network/gateway category — HMAC and replay authentication (3 tests)."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

pytestmark = pytest.mark.network


def _sign(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _body(event_id: str = "e1", ts: datetime | None = None) -> bytes:
    moment = ts or datetime.now(timezone.utc)
    return json.dumps(
        {
            "event_id": event_id,
            "event_ts": moment.isoformat(),
            "source": "n8n",
            "payload": {"k": 1},
        }
    ).encode()


def _post(app, body: bytes, signature):
    async def _run():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {"X-Signature": signature} if signature is not None else {}
            return await client.post("/webhook", content=body, headers=headers)

    return asyncio.run(_run())


def test_missing_signature_is_rejected(webhook_app):
    response = _post(webhook_app, _body(), None)
    assert response.status_code == 401
    assert webhook_app.state.storage.claims == []


def test_forged_signature_is_rejected(webhook_app):
    body = _body()
    forged = "sha256=" + "0" * 64
    response = _post(webhook_app, body, forged)
    assert response.status_code == 401
    assert webhook_app.state.storage.claims == []


def test_replay_window_is_rejected(webhook_app, config):
    stale = datetime.now(timezone.utc) - timedelta(seconds=301)
    body = _body(ts=stale)
    signature = _sign(body, config["hmac_secret"])
    response = _post(webhook_app, body, signature)
    assert response.status_code == 401
    assert webhook_app.state.storage.claims == []