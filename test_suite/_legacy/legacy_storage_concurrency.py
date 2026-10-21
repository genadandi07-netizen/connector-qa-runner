"""Storage/idempotency category — asyncio.gather concurrency stress (3 tests)."""
from __future__ import annotations

import asyncio

import pytest

from adapters.storage_adapter import claim_event, init_storage

pytestmark = pytest.mark.storage


@pytest.mark.parametrize("concurrency", [30, 40, 50])
def test_concurrent_claim_single_winner(tmp_db, concurrency):
    async def _run():
        connections = [init_storage(tmp_db) for _ in range(concurrency)]

        async def _claim(conn):
            return await claim_event("same-event-id", conn)

        try:
            return await asyncio.gather(*[_claim(conn) for conn in connections])
        finally:
            for conn in connections:
                conn.close()

    results = asyncio.run(_run())
    winners = [item for item in results if item["first_processed"]]
    assert len(winners) == 1
    assert len(results) == concurrency