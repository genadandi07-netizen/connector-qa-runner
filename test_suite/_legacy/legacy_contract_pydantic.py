"""Contract category — HTTP 422 before business logic, Pydantic v2 (2 tests)."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from datetime import datetime, timezone

import httpx
import pytest
from pydantic import ValidationError

from adapters.source_adapter import WebhookPayload

pytestmark = pytest.mark.contract


def _sign(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_422_returned_before_business_logic(webhook_app, config):
    body = {
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "n8n",
        "payload": {},
    }
    raw = json.dumps(body).encode()
    signature = _sign(raw, config["hmac_secret"])

    async def _run():
        transport = httpx.ASGITransport(app=webhook_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post(
                "/webhook", content=raw, headers={"X-Signature": signature}
            )

    response = asyncio.run(_run())
    assert response.status_code == 422
    assert webhook_app.state.storage.claims == []


def test_pydantic_model_contract():
    valid = WebhookPayload.model_validate(
        {
            "event_id": "e1",
            "event_ts": "2026-10-09T00:00:00Z",
            "source": "n8n",
            "payload": {},
        }
    )
    assert valid.event_id == "e1"

    with pytest.raises(ValidationError):
        WebhookPayload.model_validate(
            {"event_ts": "2026-10-09T00:00:00Z", "source": "n8n", "payload": {}}
        )
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate(
            {
                "event_id": "e1",
                "event_ts": "2026-10-09T00:00:00Z",
                "source": "n8n",
                "payload": {},
                "extra": 1,
            }
        )