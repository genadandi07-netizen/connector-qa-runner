"""Storage/idempotency category — atomic DLQ writes via os.replace (3 tests)."""
from __future__ import annotations

import asyncio
import glob
import json
import os

import pytest

from adapters.storage_adapter import write_dlq

pytestmark = pytest.mark.storage


def test_no_partial_json_after_parallel_failures(dlq_dir):
    class Unserializable:
        pass

    async def _run():
        tasks = []
        for index in range(20):
            if index % 2 == 0:
                payload = {"ok": index}
                reason = "ok"
            else:
                payload = Unserializable()
                reason = "serialization-error"
            tasks.append(
                asyncio.to_thread(write_dlq, f"evt-{index}", payload, reason, dlq_dir)
            )
        return await asyncio.gather(*tasks, return_exceptions=True)

    results = asyncio.run(_run())
    assert any(isinstance(item, Exception) for item in results)

    json_files = glob.glob(os.path.join(dlq_dir, "*.json"))
    assert json_files
    for path in json_files:
        with open(path, "r", encoding="utf-8") as handle:
            json.load(handle)


def test_existing_dlq_records_are_valid(dlq_dir):
    path = write_dlq("evt-valid", {"a": 1}, "ok", dlq_dir)
    with open(path, "r", encoding="utf-8") as handle:
        record = json.load(handle)
    assert record["event_id"] == "evt-valid"
    assert record["reason"] == "ok"


def test_only_tmp_or_complete_json_present(dlq_dir):
    write_dlq("evt-a", {"a": 1}, "ok", dlq_dir)
    for name in os.listdir(dlq_dir):
        assert name.endswith(".json") or name.endswith(".tmp")
        if name.endswith(".json"):
            with open(os.path.join(dlq_dir, name), "r", encoding="utf-8") as handle:
                json.load(handle)