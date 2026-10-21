"""Framework-free ASGI harness for the ingress security pipeline.

The production ingress lives in :mod:`adapters.source_adapter` and is a FastAPI
application. Importing FastAPI is comparatively expensive (pydantic + starlette
model construction) and unnecessary for exercising the security primitives,
which are pure and framework-agnostic.

This module therefore imports ``source_adapter`` with a minimal placeholder for
the (unused) web-framework import, and re-runs its **real** security functions::

    verify_hmac_sha256  ->  _parse_ts / check_replay_window  ->  WebhookPayload

They are driven through a tiny raw ASGI callable that reproduces the
documented pipeline order (HMAC -> replay -> contract -> ``claim_event``) and
the same status codes (401 / 422 / 429 / 200). No socket is opened and no
third-party web framework is imported.
"""
from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _load_source_adapter():
    """Import the real ``source_adapter`` without paying for FastAPI.

    The stub only satisfies the module-level ``from fastapi import ...``; the
    primitives under test never reference it.
    """
    if "fastapi" not in sys.modules:
        stub = types.ModuleType("fastapi")

        class _UnusedFastAPI:  # pragma: no cover - never instantiated
            def __init__(self, *args, **kwargs):  # noqa: D401
                pass

            def post(self, *args, **kwargs):
                def decorator(fn):
                    return fn

                return decorator

        class _UnusedHTTPException(Exception):
            pass

        stub.FastAPI = _UnusedFastAPI
        stub.HTTPException = _UnusedHTTPException
        stub.Request = object
        sys.modules["fastapi"] = stub

    from adapters import source_adapter

    return source_adapter


source_adapter = _load_source_adapter()


class MemoryStorage:
    """In-process storage double: records claims, never touches disk."""

    def __init__(self) -> None:
        self.claims = []

    async def claim_event(self, event_id: str) -> Dict[str, Any]:
        self.claims.append(event_id)
        return {"event_id": event_id, "first_processed": True}


class AsgiIngress:
    """Minimal ASGI ingress mirroring ``source_adapter.create_app``."""

    def __init__(
        self,
        storage: MemoryStorage,
        config: Dict[str, Any],
        limiter: Optional[Any] = None,
        auth: bool = True,
    ) -> None:
        self.storage = storage
        self.secret = str(config["hmac_secret"]).encode("utf-8")
        self.max_age = int(config["replay"]["max_age_s"])
        self.limiter = limiter
        self.auth = auth
        self.auth_failures = 0

    async def __call__(self, scope, receive, send) -> None:
        body = b""
        more = True
        while more:
            message = await receive()
            body += message.get("body", b"")
            more = message.get("more_body", False)

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        status, payload = await self._dispatch(headers, body)

        encoded = json.dumps(payload).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(encoded)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": encoded})

    async def _dispatch(self, headers, body):
        if self.limiter is not None:
            client_ip = headers.get("x-real-ip", "unknown")
            if not self.limiter.allow(client_ip):
                return 429, {"detail": "too many requests"}
            if not self.auth:
                return 200, {"status": "accepted"}

        signature = headers.get("x-signature", "")
        if not source_adapter.verify_hmac_sha256(body, signature, self.secret):
            self.auth_failures += 1
            return 401, {"detail": "invalid signature"}

        try:
            data = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return 422, {"detail": "invalid json"}

        if not isinstance(data, dict):
            return 422, {"detail": "payload must be an object"}

        event_ts = source_adapter._parse_ts(data.get("event_ts"))
        if event_ts is not None and not source_adapter.check_replay_window(
            event_ts, datetime.now(timezone.utc), self.max_age
        ):
            self.auth_failures += 1
            return 401, {"detail": "replay window exceeded"}

        try:
            event = source_adapter.WebhookPayload.model_validate(data)
        except Exception:
            return 422, {"detail": "invalid payload"}

        claim = await self.storage.claim_event(event.event_id)
        return 200, {
            "status": "accepted",
            "event_id": event.event_id,
            "first_processed": bool(claim.get("first_processed", False)),
        }
