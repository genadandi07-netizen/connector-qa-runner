"""Network/gateway category — ConnectTimeout / ReadTimeout to DLQ (2 tests)."""
from __future__ import annotations

import asyncio
import json
import os

import httpx
import pytest
import respx

from adapters.egress_adapter import send
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
def test_connect_timeout_goes_to_dlq(tmp_db, dlq_dir, config):
    respx.post(URL).mock(side_effect=httpx.ConnectTimeout("connect boom"))
    result = _run_send(tmp_db, dlq_dir, config, {"event_id": "e-ct"})
    assert result["status"] == "dlq"
    assert result["reason"] == "ConnectTimeout"
    path = os.path.join(dlq_dir, "e-ct.json")
    assert os.path.exists(path)
    with open(path, "r", encoding="utf-8") as handle:
        assert "ConnectTimeout" in json.load(handle)["reason"]


@respx.mock
def test_read_timeout_goes_to_dlq(tmp_db, dlq_dir, config):
    respx.post(URL).mock(side_effect=httpx.ReadTimeout("read boom"))
    result = _run_send(tmp_db, dlq_dir, config, {"event_id": "e-rt"})
    assert result["status"] == "dlq"
    assert result["reason"] == "ReadTimeout"
    path = os.path.join(dlq_dir, "e-rt.json")
    assert os.path.exists(path)