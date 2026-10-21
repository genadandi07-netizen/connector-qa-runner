"""SourceAdapter — ingress port.

FastAPI webhook with HMAC-SHA256 verification, a >300 s replay window and a
Pydantic v2 payload contract that rejects bad input with HTTP 422 *before*
business logic. Extensible to Telegram Bot / Workers via subclass.

Anti-Corruption Layer note: this module does **not** declare its own payload
model. The ingress contract lives in :mod:`schemas` (the single source of
truth) and is imported here as ``InboundWebhookPayload`` — the historical
``WebhookPayload`` name is kept as a compatibility alias.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError

from schemas import InboundWebhookPayload
from test_suite.config_loader import load_config

SIGNATURE_HEADER = "X-Signature"
SIGNATURE_PREFIX = "sha256="

# Backward-compatible alias: the model itself is defined in ``schemas.py``.
WebhookPayload = InboundWebhookPayload


def _normalize_signature(signature_header: Optional[str]) -> Optional[str]:
    if not signature_header:
        return None
    signature = signature_header.strip()
    if signature.startswith(SIGNATURE_PREFIX):
        signature = signature[len(SIGNATURE_PREFIX):]
    return signature or None


def verify_hmac_sha256(body: bytes, signature_header: str, secret: bytes) -> bool:
    """Constant-time HMAC-SHA256 verification (forged payload -> False)."""
    provided = _normalize_signature(signature_header)
    if not provided:
        return False
    expected = hmac.new(secret, body, hashlib.sha256).hexdigest()
    if len(provided) != len(expected):
        return False
    return hmac.compare_digest(provided, expected)


def check_replay_window(
    event_ts: datetime,
    now: datetime,
    max_age_s: int = 300,
) -> bool:
    """True when ``event_ts`` is fresh (not older than ``max_age_s``)."""
    if event_ts.tzinfo is None:
        event_ts = event_ts.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    delta_seconds = (now - event_ts).total_seconds()
    if delta_seconds < -60:
        return False
    return delta_seconds <= max_age_s


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def create_app(storage, config: Optional[Dict[str, Any]] = None) -> FastAPI:
    """Build the FastAPI ingress app around a storage adapter.

    Pipeline order: HMAC -> replay -> Pydantic contract -> storage.claim_event.
    """
    cfg = config or load_config()
    secret = str(cfg["hmac_secret"]).encode("utf-8")
    max_age = int(cfg["replay"]["max_age_s"])

    app = FastAPI(title="connector-qa-runner-source-adapter")
    counters = {"auth_failures": 0}
    app.state.auth_failures = 0

    @app.post("/webhook")
    async def webhook(request: Request):
        raw_body = await request.body()
        signature = request.headers.get(SIGNATURE_HEADER, "")

        if not verify_hmac_sha256(raw_body, signature, secret):
            counters["auth_failures"] += 1
            app.state.auth_failures = counters["auth_failures"]
            raise HTTPException(status_code=401, detail="invalid signature")

        try:
            data = json.loads(raw_body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise HTTPException(status_code=422, detail="invalid json")

        if not isinstance(data, dict):
            raise HTTPException(status_code=422, detail="payload must be an object")

        event_ts = _parse_ts(data.get("event_ts"))
        if event_ts is not None and not check_replay_window(
            event_ts, datetime.now(timezone.utc), max_age
        ):
            counters["auth_failures"] += 1
            app.state.auth_failures = counters["auth_failures"]
            raise HTTPException(status_code=401, detail="replay window exceeded")

        try:
            event = WebhookPayload.model_validate(data)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors())

        claim = await storage.claim_event(event.event_id)
        return {
            "status": "accepted",
            "event_id": event.event_id,
            "first_processed": bool(claim.get("first_processed", False)),
        }

    return app