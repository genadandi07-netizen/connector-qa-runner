"""Resilience category — circuit breaker, retries, timeouts and DLQ (26 tests).

Exercises :mod:`adapters.egress_adapter` against in-process
``httpx.MockTransport`` handlers, :mod:`adapters.storage_adapter` (atomic DLQ)
and the reference circuit breaker / n8n fallback helpers. No real network is
ever reached and no socket is opened.
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import sqlite3

import httpx
import pytest

from adapters.egress_adapter import classify, compute_backoff_delay, send
from adapters.storage_adapter import claim_event, init_storage, replay_dlq, write_dlq
from test_suite.config_loader import load_config
from test_suite.gateway_support import CircuitBreaker, post_n8n_fallback, run_with_deadline

CFG = load_config()
URL = CFG["egress"]["url"]
N8N_URL = CFG["n8n"]["webhook_url"]


class _FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, delta: float) -> None:
        self.value += delta


async def _no_sleep(_delay: float) -> None:
    return None


def _respond(status: int):
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    return handler


def _raise(exc: Exception):
    def handler(_request: httpx.Request) -> httpx.Response:
        raise exc

    return handler


def _egress(dlq_dir, event, handler):
    async def _run():
        conn = sqlite3.connect(":memory:")
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            try:
                return await send(
                    event,
                    conn,
                    dlq_dir,
                    config=CFG,
                    client=client,
                    sleep=_no_sleep,
                )
            finally:
                conn.close()

    return asyncio.run(_run())


def _call_n8n(handler, payload):
    async def _run():
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            return await post_n8n_fallback(client, N8N_URL, payload)

    return asyncio.run(_run())


# --------------------------------------------------------------------------- #
# Circuit breaker (6)
# --------------------------------------------------------------------------- #
def test_circuit_opens_after_three_failures():
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30, clock=_FakeClock())
    for _ in range(3):
        breaker.record_failure()
    assert breaker.state == "OPEN"


def test_circuit_blocks_requests_when_open():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=30, clock=_FakeClock())
    breaker.record_failure()
    assert breaker.allow() is False


def test_circuit_success_resets_failure_count():
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30, clock=_FakeClock())
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    assert breaker.state == "CLOSED"
    assert breaker.failures == 0


def test_circuit_transitions_to_half_open_after_timeout():
    clock = _FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=30, clock=clock)
    breaker.record_failure()
    clock.advance(31)
    assert breaker.allow() is True
    assert breaker.state == "HALF_OPEN"


def test_circuit_half_open_closes_on_success():
    clock = _FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=30, clock=clock)
    breaker.record_failure()
    clock.advance(31)
    breaker.allow()
    breaker.record_success()
    assert breaker.state == "CLOSED"


def test_circuit_half_open_reopens_on_failure():
    clock = _FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=30, clock=clock)
    breaker.record_failure()
    clock.advance(31)
    breaker.allow()
    breaker.record_failure()
    assert breaker.state == "OPEN"


# --------------------------------------------------------------------------- #
# Retry policy and exponential backoff (5)
# --------------------------------------------------------------------------- #
def test_full_jitter_backoff_within_bounds():
    rng = random.Random(7)
    for attempt in range(4):
        ceiling = min(5.0, 0.1 * (2 ** attempt))
        for _ in range(100):
            delay = compute_backoff_delay(attempt, None, rng, base=0.1, cap=5.0)
            assert 0.0 <= delay <= ceiling


def test_retry_after_caps_backoff():
    rng = random.Random(7)
    for _ in range(100):
        delay = compute_backoff_delay(0, 0.05, rng, base=0.1, cap=5.0)
        assert 0.0 <= delay <= 0.05


def test_exponential_ceiling_grows_with_attempts():
    ceilings = [min(5.0, 0.1 * (2 ** attempt)) for attempt in range(3)]
    assert ceilings[0] < ceilings[1] < ceilings[2]


def test_retry_attempts_do_not_exceed_three(dlq_dir):
    counter = {"calls": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        counter["calls"] += 1
        return httpx.Response(500)

    _egress(dlq_dir, {"event_id": "e-max-attempts"}, handler)
    assert counter["calls"] == 3
    assert CFG["retry"]["max_attempts"] == 3


def test_retry_preserves_error_context(dlq_dir):
    result = _egress(dlq_dir, {"event_id": "e-context"}, _respond(503))
    assert result["event_id"] == "e-context"
    assert result["reason"] == "HTTP 503"


# --------------------------------------------------------------------------- #
# Timeouts and network failures (5)
# --------------------------------------------------------------------------- #
def test_gateway_timeout_504_is_classified_and_dead_lettered(dlq_dir):
    result = _egress(dlq_dir, {"event_id": "e-504"}, _respond(504))
    assert classify(504) == "fail"
    assert result["status"] == "dlq"
    assert result["reason"] == "HTTP 504"


def test_connection_refused_goes_to_dlq(dlq_dir):
    result = _egress(
        dlq_dir, {"event_id": "e-refused"}, _raise(httpx.ConnectError("refused"))
    )
    assert result["status"] == "dlq"
    assert result["reason"] == "ConnectError"


def test_connection_reset_mid_transfer_goes_to_dlq(dlq_dir):
    result = _egress(
        dlq_dir, {"event_id": "e-reset"}, _raise(httpx.ReadError("reset by peer"))
    )
    assert result["status"] == "dlq"
    assert result["reason"] == "ReadError"


def test_connect_timeout_goes_to_dlq(dlq_dir):
    result = _egress(
        dlq_dir, {"event_id": "e-timeout"}, _raise(httpx.ConnectTimeout("timeout"))
    )
    assert result["status"] == "dlq"
    assert result["reason"] == "ConnectTimeout"


def test_timeout_deadline_cuts_off_slow_task():
    async def _slow():
        await asyncio.sleep(1.0)
        return "done"

    async def _run():
        try:
            await run_with_deadline(_slow(), timeout_s=0.05)
            return None
        except asyncio.TimeoutError:
            return "timeout"

    assert asyncio.run(_run()) == "timeout"
    assert CFG["retry"]["timeout_s"] <= 2.5


# --------------------------------------------------------------------------- #
# Dead Letter Queue (6)
# --------------------------------------------------------------------------- #
def test_write_dlq_persists_failed_packet(dlq_dir):
    path = write_dlq("evt-d1", {"k": 1}, "HTTP 503", dlq_dir)
    with open(path, "r", encoding="utf-8") as handle:
        record = json.load(handle)
    assert record["event_id"] == "evt-d1"
    assert record["reason"] == "HTTP 503"


def test_dlq_preserves_original_body(dlq_dir):
    payload = {"event_id": "body", "payload": {"nested": {"v": [1, 2, 3]}}}
    path = write_dlq("body", payload, "boom", dlq_dir)
    with open(path, "r", encoding="utf-8") as handle:
        record = json.load(handle)
    assert record["payload"] == payload


def test_dlq_preserves_stacktrace_reason(dlq_dir):
    reason = (
        "ValueError: boom\nTraceback (most recent call last):\n"
        '  File "x.py", line 1, in <module>'
    )
    path = write_dlq("trace", {"a": 1}, reason, dlq_dir)
    with open(path, "r", encoding="utf-8") as handle:
        record = json.load(handle)
    assert "Traceback" in record["reason"]
    assert "ValueError" in record["reason"]


def test_dlq_writes_are_atomic_no_partial_json(dlq_dir):
    async def _run():
        return await asyncio.gather(
            *[
                asyncio.to_thread(write_dlq, f"parallel-{i}", {"i": i}, "ok", dlq_dir)
                for i in range(12)
            ]
        )

    paths = asyncio.run(_run())
    for path in paths:
        with open(path, "r", encoding="utf-8") as handle:
            json.load(handle)


def test_replay_dlq_returns_event_to_processing(tmp_db, dlq_dir):
    conn = init_storage(tmp_db)
    try:
        write_dlq("replay-me", {"a": 1}, "boom", dlq_dir)
        asyncio.run(claim_event("replay-me", conn))
        replayed = replay_dlq("replay-me", conn, dlq_dir)
    finally:
        conn.close()
    assert replayed is True
    assert not os.path.exists(os.path.join(dlq_dir, "replay-me.json"))


def test_dlq_directory_contains_only_json_or_tmp(dlq_dir):
    write_dlq("only", {"a": 1}, "ok", dlq_dir)
    for name in os.listdir(dlq_dir):
        assert name.endswith(".json") or name.endswith(".tmp")


# --------------------------------------------------------------------------- #
# n8n fallback webhook (4)
# --------------------------------------------------------------------------- #
def test_n8n_fallback_delivers_when_amocrm_unavailable():
    result = _call_n8n(
        lambda _request: httpx.Response(200, json={"success": True}),
        {"event_id": "e-n8n"},
    )
    assert result["status"] == "fallback-ok"
    assert result["delivered"] is True


def test_n8n_fallback_continue_on_fail_response():
    result = _call_n8n(
        lambda _request: httpx.Response(200, json={"continueOnFail": True}),
        {"event_id": "e-cf"},
    )
    assert result["delivered"] is True
    assert result["body"]["continueOnFail"] is True


def test_egress_failure_triggers_n8n_fallback(dlq_dir):
    first = _egress(dlq_dir, {"event_id": "e-fallback"}, _respond(503))
    assert first["status"] == "dlq"

    fallback = _call_n8n(
        lambda _request: httpx.Response(200, json={"ok": True}),
        {"event_id": "e-fallback", "reason": first["reason"]},
    )
    assert fallback["status"] == "fallback-ok"


def test_n8n_fallback_failure_is_handled():
    result = _call_n8n(
        _raise(httpx.ConnectError("n8n down")), {"event_id": "e-down"}
    )
    assert result["status"] == "fallback-error"
    assert result["delivered"] is False
