"""Egress ACL category — build_request, auth rotation, TokenBucket (19 tests).

Exercises the Anti-Corruption Layer of :mod:`adapters.egress_adapter`:

* :meth:`EgressAdapter.build_request` — URL / ``Authorization: Bearer`` /
  body mapping and subclass extensibility,
* :class:`TokenBucketRateLimiter` — proactive throttling bound to
  ``config.py`` / ``test_suite/config.json``,
* ``asyncio.Lock``-guarded single-flight token rotation on 401,
* the single-source-of-truth ``WebhookPayload`` import in
  :mod:`adapters.source_adapter`.

Everything runs in-process through ``httpx.MockTransport``; no socket is ever
opened.
"""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from pydantic import BaseModel, ValidationError

from adapters import source_adapter
from adapters.egress_adapter import (
    DEFAULT_RATE_LIMIT_BURST,
    DEFAULT_RATE_LIMIT_RPS,
    EgressAdapter,
    TokenBucketRateLimiter,
    send as legacy_send,
)
from schemas import AmoLeadCreateSchema, InboundWebhookPayload, WebhookPayload
from test_suite.config_loader import load_config

CFG = load_config()
EVENT = {"event_id": "e-acl", "source": "n8n", "payload": {"k": 1}}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _capture_client(handler):
    """AsyncClient + list of every request handed to the MockTransport."""
    seen = []

    def _wrapped(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    return httpx.AsyncClient(transport=httpx.MockTransport(_wrapped)), seen


class _SpyLimiter(TokenBucketRateLimiter):
    """Real limiter that counts how often ``send`` waits for a permit."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.calls = 0

    async def acquire(self, tokens: float = 1.0) -> float:
        self.calls += 1
        return await super().acquire(tokens)


class _ManualClock:
    """Deterministic monotonic clock advanced by the injected sleeper."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


# --------------------------------------------------------------------------- #
# build_request() — ACL request assembly (5)
# --------------------------------------------------------------------------- #
def test_build_request_maps_url_headers_and_body():
    adapter = EgressAdapter(config=CFG)
    url, headers, body = adapter.build_request(EVENT)

    assert url == CFG["egress"]["url"]
    assert headers["Idempotency-Key"] == "e-acl"
    assert headers["Authorization"] == f"Bearer {CFG['egress']['token']}"
    assert body == {"event_id": "e-acl", "source": "n8n", "payload": {"k": 1}}


def test_build_request_defaults_missing_source_and_payload():
    adapter = EgressAdapter(config=CFG)
    _, _, body = adapter.build_request({"event_id": "e-plain"})

    assert body == {"event_id": "e-plain", "source": "n8n", "payload": {}}


def test_build_request_omits_authorization_without_token():
    cfg = {"egress": {"url": "https://example.invalid/hub", "token": ""}}
    adapter = EgressAdapter(config=cfg)
    _, headers, _ = adapter.build_request(EVENT)

    assert "Authorization" not in headers
    assert headers["Idempotency-Key"] == "e-acl"


def test_send_stamps_authorization_header_on_the_wire():
    client, seen = _capture_client(lambda _r: httpx.Response(200))

    async def _run():
        async with client:
            adapter = EgressAdapter(config=CFG, client=client)
            return await adapter.send(EVENT)

    result = asyncio.run(_run())

    assert result["status"] == "ok"
    assert seen[0].headers["Authorization"] == f"Bearer {CFG['egress']['token']}"


def test_build_request_is_the_acl_seam_for_subclasses():
    """A subclass adapts events to amoCRM without touching transport logic."""

    class AmoLeadEgress(EgressAdapter):
        def build_request(self, event):
            url = "https://example.invalid/api/v4/leads"
            lead = AmoLeadCreateSchema.model_validate(event["payload"])
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            }
            return url, headers, lead.model_dump()

    client, seen = _capture_client(lambda _r: httpx.Response(201))

    async def _run():
        async with client:
            adapter = AmoLeadEgress(config=CFG, client=client)
            return await adapter.send(
                {"event_id": "e-amo", "payload": {"name": "Deal", "price": 100}}
            )

    result = asyncio.run(_run())

    assert result["status"] == "ok"
    assert str(seen[0].url) == "https://example.invalid/api/v4/leads"
    assert json.loads(seen[0].content) == {
        "name": "Deal",
        "price": 100,
        "custom_fields_values": [],
    }
    assert seen[0].headers["Authorization"].startswith("Bearer ")


# --------------------------------------------------------------------------- #
# TokenBucketRateLimiter — proactive rate limiting (7)
# --------------------------------------------------------------------------- #
def test_rate_limiter_defaults_match_specification():
    limiter = TokenBucketRateLimiter()

    assert limiter.rate_limit_rps == DEFAULT_RATE_LIMIT_RPS == 2.0
    assert limiter.burst == DEFAULT_RATE_LIMIT_BURST == 5


def test_rate_limiter_parameters_bind_to_config():
    adapter = EgressAdapter(config=CFG)

    assert adapter.rate_limiter.rate_limit_rps == CFG["rate_limit"]["rate_limit_rps"]
    assert adapter.rate_limiter.burst == CFG["rate_limit"]["burst"]

    # The same knobs projected by config.py's runtime shape.
    from config import get_settings

    runtime = get_settings().as_runtime_config()
    assert runtime["rate_limit"] == CFG["rate_limit"]

    # A config without the block falls back to the documented defaults.
    fallback = EgressAdapter(config={"egress": {"url": "https://example.invalid"}})
    assert fallback.rate_limiter.rate_limit_rps == DEFAULT_RATE_LIMIT_RPS
    assert fallback.rate_limiter.burst == DEFAULT_RATE_LIMIT_BURST


def test_token_bucket_allows_burst_without_delay():
    async def _run():
        limiter = TokenBucketRateLimiter(rate_limit_rps=2.0, burst=5)
        for _ in range(5):
            assert await limiter.acquire() == 0.0
        return limiter.tokens

    # All 5 burst tokens consumed instantly (only micro-drift may refill).
    assert asyncio.run(_run()) < 1e-3


def test_token_bucket_delays_when_rps_exceeded():
    async def _run():
        limiter = TokenBucketRateLimiter(rate_limit_rps=100.0, burst=2)
        await limiter.acquire()
        await limiter.acquire()
        import time as _time

        start = _time.monotonic()
        await limiter.acquire()
        return _time.monotonic() - start

    elapsed = asyncio.run(_run())
    # Burst (2 tokens) is exhausted: the 3rd acquire must really wait
    # (deficit 1 / 100 rps = 10 ms nominal; scheduler oversleep only adds).
    assert elapsed >= 0.004


def test_token_bucket_requested_delay_matches_configured_rps():
    """The *requested* wait is derived from the configured RPS, not a default.

    Deterministic (injected clock/sleeper) so it cannot be masked by event
    loop scheduling noise: with ``rate_limit_rps=100`` the deficit of one
    token costs 10 ms, whereas the 2.0-rps default would cost 500 ms.
    """
    async def _run():
        clock = _ManualClock()
        delays = []

        async def _record(delay: float) -> None:
            delays.append(delay)
            clock.now += delay

        limiter = TokenBucketRateLimiter(
            rate_limit_rps=100.0, burst=2, clock=clock, sleep=_record
        )
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()  # bucket empty -> must wait deficit/rps
        return delays

    delays = asyncio.run(_run())
    assert delays, "exhausted bucket must trigger a wait"
    assert sum(delays) == pytest.approx(0.01, abs=1e-6)
    assert all(delay < 0.4 for delay in delays)  # 2.0-rps default would be 0.5


def test_token_bucket_refills_deterministically_with_injected_clock():
    async def _run():
        clock = _ManualClock()

        async def _virtual_sleep(delay: float) -> None:
            clock.now += delay

        limiter = TokenBucketRateLimiter(
            rate_limit_rps=2.0, burst=1, clock=clock, sleep=_virtual_sleep
        )
        await limiter.acquire()
        assert limiter.tokens == 0.0
        waited = await limiter.acquire()
        return waited, clock.now

    waited, advanced = asyncio.run(_run())
    assert waited == pytest.approx(0.5)  # 1 token / 2 rps of virtual time
    assert advanced == pytest.approx(0.5)


def test_send_waits_on_rate_limiter_before_every_post(dlq_dir):
    client, seen = _capture_client(lambda _r: httpx.Response(503))

    async def _run():
        async with client:
            adapter = EgressAdapter(
                config=CFG,
                client=client,
                dlq_dir=dlq_dir,
                sleep=lambda _d: asyncio.sleep(0),
            )
            adapter.rate_limiter = _SpyLimiter(rate_limit_rps=1000.0, burst=100)
            return adapter, await adapter.send(EVENT)

    adapter, result = asyncio.run(_run())

    assert result["status"] == "dlq"
    assert len(seen) == 3  # max_attempts
    # One proactive acquire before *each* dispatched request.
    assert adapter.rate_limiter.calls == 3


# --------------------------------------------------------------------------- #
# 401 -> single-flight token rotation under asyncio.Lock (3)
# --------------------------------------------------------------------------- #
def test_401_rotates_token_once_and_retries_with_new_token(dlq_dir):
    calls = {"n": 0}
    seen_auth = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_auth.append(request.headers.get("Authorization"))
        calls["n"] += 1
        return httpx.Response(401 if calls["n"] == 1 else 200)

    refreshed = {"n": 0}

    async def refresher():
        refreshed["n"] += 1
        return "rotated-token"

    client, _ = _capture_client(handler)

    async def _run():
        async with client:
            adapter = EgressAdapter(
                config=CFG, client=client, dlq_dir=dlq_dir, token_refresher=refresher
            )
            return adapter, await adapter.send(EVENT)

    adapter, result = asyncio.run(_run())

    assert result["status"] == "ok"
    assert result["attempts"] == 2
    assert refreshed["n"] == 1
    assert adapter.token == "rotated-token"
    assert seen_auth[0] == f"Bearer {CFG['egress']['token']}"
    assert seen_auth[1] == "Bearer rotated-token"


def test_concurrent_token_refreshes_are_single_flight():
    """Eight racing 401 handlers share exactly one refresh (no race)."""
    calls = {"n": 0}

    async def slow_refresher():
        calls["n"] += 1
        await asyncio.sleep(0.01)  # hold the lock long enough to race
        return f"tok-{calls['n']}"

    async def _run():
        adapter = EgressAdapter(config=CFG, token_refresher=slow_refresher)
        tokens = await asyncio.gather(
            *(adapter.refresh_token(seen_version=0) for _ in range(8))
        )
        return adapter, tokens

    adapter, tokens = asyncio.run(_run())

    assert calls["n"] == 1  # only the lock winner refreshed
    assert set(tokens) == {"tok-1"}
    assert adapter.token == "tok-1"
    assert adapter.token_version == 1


def test_refresh_without_invalidator_still_rotates_and_bumps_version():
    async def refresher():
        return "fresh"

    adapter = EgressAdapter(config=CFG, token_refresher=refresher)
    assert asyncio.run(adapter.refresh_token()) == "fresh"
    assert adapter.token_version == 1


# --------------------------------------------------------------------------- #
# WebhookPayload — single source of truth in schemas.py (3)
# --------------------------------------------------------------------------- #
def test_source_adapter_imports_webhook_payload_from_schemas():
    # The ingress model is *re-exported*, not redefined, by the adapter.
    assert source_adapter.WebhookPayload is InboundWebhookPayload

    own_models = [
        value
        for value in vars(source_adapter).values()
        if isinstance(value, type)
        and issubclass(value, BaseModel)
        and value is not BaseModel
        and value.__module__ == source_adapter.__name__
    ]
    assert own_models == [], f"duplicate models defined in source_adapter: {own_models}"


def test_generic_and_ingress_webhook_payloads_are_distinct_contracts():
    generic = WebhookPayload.model_validate(
        {"event_id": "e1", "source": "n8n", "timestamp": 1, "data": {"k": 1}}
    )
    assert generic.timestamp == 1

    ingress = InboundWebhookPayload.model_validate(
        {"event_id": "e1", "event_ts": "2024-01-01T00:00:00+00:00", "source": "n8n"}
    )
    assert ingress.payload == {}
    assert WebhookPayload is not InboundWebhookPayload


def test_inbound_webhook_payload_contract_is_strict():
    valid = {
        "event_id": "e1",
        "event_ts": "2024-01-01T00:00:00+00:00",
        "source": "n8n",
        "payload": {"k": 1},
    }
    assert InboundWebhookPayload.model_validate(valid).event_id == "e1"

    with pytest.raises(ValidationError):  # event_id / event_ts required
        InboundWebhookPayload.model_validate({"source": "n8n"})
    with pytest.raises(ValidationError):  # extra="forbid"
        InboundWebhookPayload.model_validate({**valid, "extra": True})


# --------------------------------------------------------------------------- #
# Backward-compatible functional entry point (1)
# --------------------------------------------------------------------------- #
def test_module_level_send_delegates_to_adapter(dlq_dir):
    client, seen = _capture_client(lambda _r: httpx.Response(200))

    async def _run():
        async with client:
            return await legacy_send(
                EVENT,
                conn=None,
                dlq_dir=dlq_dir,
                config=CFG,
                client=client,
                sleep=lambda _d: asyncio.sleep(0),
            )

    result = asyncio.run(_run())

    assert result["status"] == "ok"
    assert seen[0].headers["Authorization"] == f"Bearer {CFG['egress']['token']}"
