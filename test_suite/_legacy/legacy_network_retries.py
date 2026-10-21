"""Network/gateway category — retries, backoff and DLQ exhaustion (4 tests)."""
from __future__ import annotations

import asyncio
import json
import os
import random

import httpx
import pytest
import respx

from adapters.egress_adapter import compute_backoff_delay, send
from adapters.storage_adapter import init_storage

pytestmark = pytest.mark.network

URL = "https://api.hubapi.com/crm/v3/objects/contacts"


async def _no_sleep(_delay: float) -> None:
    return None


def _run_send(tmp_db, dlq_dir, config, event):
    async def _inner():
        conn = init_storage(tmp_db)
        async with httpx.AsyncClient() as client:
            try:
                return await send(
                    event,
                    conn,
                    dlq_dir,
                    config=config,
                    client=client,
                    sleep=_no_sleep,
                )
            finally:
                conn.close()

    return asyncio.run(_inner())


@respx.mock
def test_429_with_retry_after_then_success(tmp_db, dlq_dir, config):
    respx.post(URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "1"}),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    result = _run_send(tmp_db, dlq_dir, config, {"event_id": "e-429"})
    assert result["status"] == "ok"
    assert result["attempts"] == 2


def test_full_jitter_stays_within_bounds():
    rng = random.Random(1234)
    for attempt in range(5):
        ceiling = min(5.0, 0.1 * (2 ** attempt))
        for _ in range(200):
            delay = compute_backoff_delay(attempt, None, rng, base=0.1, cap=5.0)
            assert 0.0 <= delay <= ceiling
    capped = compute_backoff_delay(4, 2.0, random.Random(0), base=0.1, cap=5.0)
    assert 0.0 <= capped <= 2.0


@respx.mock
def test_5xx_exhaustion_writes_dlq(tmp_db, dlq_dir, config):
    respx.post(URL).mock(return_value=httpx.Response(503))
    result = _run_send(tmp_db, dlq_dir, config, {"event_id": "e-503"})
    assert result["status"] == "dlq"
    path = os.path.join(dlq_dir, "e-503.json")
    assert os.path.exists(path)
    with open(path, "r", encoding="utf-8") as handle:
        assert json.load(handle)["reason"] == "HTTP 503"


@respx.mock
def test_attempts_do_not_exceed_max(tmp_db, dlq_dir, config):
    route = respx.post(URL).mock(return_value=httpx.Response(500))
    _run_send(tmp_db, dlq_dir, config, {"event_id": "e-500"})
    assert route.call_count == config["retry"]["max_attempts"]