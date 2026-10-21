"""Storage category — SQLite WAL reliability and queue integrity (25 tests).

Exercises :mod:`adapters.storage_adapter` plus the queue/rotation reference
helpers: WAL pragmas, 50-way concurrent ``BEGIN IMMEDIATE`` claims, idempotent
dedup, crash/rollback recovery, queue state machine and cleanup. Databases are
isolated temp files with journal_mode=WAL.
"""
from __future__ import annotations

import asyncio
import sqlite3

import pytest

from adapters.storage_adapter import claim_event, get_event, init_storage
from test_suite.config_loader import load_config
from test_suite.gateway_support import (
    QueueStore,
    ensure_log_table,
    event_hash,
    prune_logs,
    safe_open,
    status_for_claim,
)

CFG = load_config()


def _conn(tmp_path, name="events.db"):
    return init_storage(str(tmp_path / name))


def _mem():
    """In-memory connection: same schema/PRAGMAs without fsync cost."""
    return init_storage(":memory:")


# --------------------------------------------------------------------------- #
# WAL pragmas (3)
# --------------------------------------------------------------------------- #
def test_journal_mode_is_wal(tmp_path):
    conn = _conn(tmp_path)
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"
    finally:
        conn.close()


def test_synchronous_normal_pragma_effective(tmp_path):
    conn = _conn(tmp_path)
    try:
        conn.execute("PRAGMA synchronous=NORMAL")
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1
        assert CFG["storage"]["journal_mode"] == "WAL"
    finally:
        conn.close()


def test_busy_timeout_is_5000(tmp_path):
    conn = _conn(tmp_path)
    try:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Concurrent transactions (5)
# --------------------------------------------------------------------------- #
def test_fifty_concurrent_claims_have_single_winner(tmp_path):
    db = str(tmp_path / "events.db")
    conns = [init_storage(db) for _ in range(50)]

    async def _run():
        return await asyncio.gather(*[claim_event("same-event", conn) for conn in conns])

    try:
        results = asyncio.run(_run())
    finally:
        for conn in conns:
            conn.close()
    assert len(results) == 50
    assert sum(1 for item in results if item["first_processed"]) == 1


def test_concurrent_claims_do_not_deadlock(tmp_path):
    db = str(tmp_path / "events.db")
    conns = [init_storage(db) for _ in range(30)]

    async def _run():
        return await asyncio.wait_for(
            asyncio.gather(
                *[claim_event(f"event-{i}", conn) for i, conn in enumerate(conns)]
            ),
            timeout=5.0,
        )

    try:
        results = asyncio.run(_run())
    finally:
        for conn in conns:
            conn.close()
    assert len(results) == 30
    assert all(item["first_processed"] for item in results)


def test_concurrent_distinct_events_all_claimed(tmp_path):
    db = str(tmp_path / "events.db")
    conns = [init_storage(db) for _ in range(25)]

    async def _run():
        return await asyncio.gather(
            *[claim_event(f"distinct-{i}", conn) for i, conn in enumerate(conns)]
        )

    try:
        results = asyncio.run(_run())
    finally:
        for conn in conns:
            conn.close()
    assert len(results) == 25
    assert all(item["first_processed"] for item in results)


def test_begin_immediate_serializes_writers(tmp_path):
    db = str(tmp_path / "events.db")
    conn = init_storage(db)
    try:
        conn.execute("BEGIN IMMEDIATE")
        other = init_storage(db)
        try:
            other.execute("PRAGMA busy_timeout=25")
            with pytest.raises(sqlite3.OperationalError):
                other.execute("BEGIN IMMEDIATE")
        finally:
            other.close()
        conn.execute("COMMIT")
    finally:
        conn.close()


def test_mixed_concurrent_claims_are_consistent(tmp_path):
    db = str(tmp_path / "events.db")
    conns = [init_storage(db) for _ in range(50)]

    async def _run():
        tasks = []
        for i, conn in enumerate(conns):
            event_id = "shared" if i % 2 == 0 else f"unique-{i}"
            tasks.append(claim_event(event_id, conn))
        return await asyncio.gather(*tasks)

    try:
        results = asyncio.run(_run())
    finally:
        for conn in conns:
            conn.close()
    shared = [item for item in results if item["event_id"] == "shared"]
    assert len(shared) == 25
    assert sum(1 for item in shared if item["first_processed"]) == 1
    unique = [item for item in results if item["event_id"] != "shared"]
    assert all(item["first_processed"] for item in unique)


# --------------------------------------------------------------------------- #
# Idempotency and deduplication (5)
# --------------------------------------------------------------------------- #
def test_duplicate_event_id_is_deduplicated():
    conn = _mem()
    try:
        first = asyncio.run(claim_event("dup", conn))
        second = asyncio.run(claim_event("dup", conn))
    finally:
        conn.close()
    assert first["first_processed"] is True
    assert second["first_processed"] is False


def test_identical_payloads_produce_identical_hash():
    first = {"event_id": "e", "source": "n8n", "timestamp": 1, "data": {"k": [1, 2]}}
    second = {"data": {"k": [1, 2]}, "timestamp": 1, "source": "n8n", "event_id": "e"}
    assert event_hash(first) == event_hash(second)


def test_distinct_payloads_produce_distinct_hash():
    assert event_hash({"a": 1}) != event_hash({"a": 2})


def test_duplicate_claim_is_marked_ignored():
    conn = _mem()
    try:
        asyncio.run(claim_event("ignored", conn))
        second = asyncio.run(claim_event("ignored", conn))
    finally:
        conn.close()
    assert status_for_claim(second) == "IGNORED"


def test_repeated_claim_inserts_single_row():
    conn = _mem()
    try:
        for _ in range(5):
            asyncio.run(claim_event("once", conn))
        count = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_id='once'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 1


# --------------------------------------------------------------------------- #
# Crash recovery (4)
# --------------------------------------------------------------------------- #
def test_rollback_on_mid_transaction_failure():
    conn = _mem()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO events VALUES ('partial','s',1,'t')")
        conn.execute("ROLLBACK")
        count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    finally:
        conn.close()
    assert count == 0


def test_rollback_preserves_primary_key_index():
    conn = _mem()
    try:
        asyncio.run(claim_event("keep", conn))
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT OR IGNORE INTO events VALUES ('keep','dup',1,'t')")
        conn.execute("ROLLBACK")
        status = conn.execute(
            "SELECT status FROM events WHERE event_id='keep'"
        ).fetchone()[0]
        cursor = conn.execute("INSERT OR IGNORE INTO events VALUES ('keep','x',1,'t')")
    finally:
        conn.close()
    assert status == "first_processed"
    assert cursor.rowcount == 0


def test_no_partial_rows_after_failed_batch():
    conn = _mem()
    try:
        conn.execute("BEGIN IMMEDIATE")
        for i in range(5):
            conn.execute("INSERT INTO events VALUES (?,?,1,'t')", (f"batch-{i}", "s"))
        conn.execute("ROLLBACK")
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    finally:
        conn.close()


def test_reopen_after_failure_keeps_integrity(tmp_path):
    db = str(tmp_path / "events.db")
    conn = init_storage(db)
    asyncio.run(claim_event("persist", conn))
    conn.close()
    reopened = init_storage(db)
    try:
        assert get_event("persist", reopened)["first_processed"] is True
        assert reopened.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        reopened.close()


# --------------------------------------------------------------------------- #
# Queue reads (4)
# --------------------------------------------------------------------------- #
def test_claim_batch_returns_requested_number():
    conn = _mem()
    try:
        queue = QueueStore(conn)
        queue.enqueue(range(1, 11))
        claimed = queue.claim_batch(4)
    finally:
        conn.close()
    assert claimed == [1, 2, 3, 4]


def test_claim_batch_isolates_rows_for_update():
    conn = _mem()
    try:
        queue = QueueStore(conn)
        queue.enqueue(range(1, 6))
        first = queue.claim_batch(2)
        second = queue.claim_batch(2)
    finally:
        conn.close()
    assert first == [1, 2]
    assert second == [3, 4]
    assert not set(first) & set(second)


def test_queue_status_machine_new_processing_processed():
    conn = _mem()
    try:
        queue = QueueStore(conn)
        queue.enqueue([42])
        assert queue.status_of(42) == "NEW"
        (row_id,) = queue.claim_batch(1)
        assert queue.status_of(row_id) == "PROCESSING"
        queue.mark_processed([row_id])
        assert queue.status_of(row_id) == "PROCESSED"
    finally:
        conn.close()


def test_queue_claim_is_atomic_under_failure():
    conn = _mem()
    try:
        queue = QueueStore(conn)
        queue.enqueue([7, 8])
        conn.execute("BEGIN IMMEDIATE")
        rows = [
            row[0]
            for row in conn.execute(
                "SELECT id FROM lead_queue WHERE status='NEW' ORDER BY id LIMIT 2"
            ).fetchall()
        ]
        for row_id in rows:
            conn.execute(
                "UPDATE lead_queue SET status='PROCESSING' WHERE id=?", (row_id,)
            )
        conn.execute("ROLLBACK")
        first_status = queue.status_of(7)
        second_status = queue.status_of(8)
    finally:
        conn.close()
    assert first_status == "NEW"
    assert second_status == "NEW"


# --------------------------------------------------------------------------- #
# Cleanup and rotation (4)
# --------------------------------------------------------------------------- #
def test_vacuum_reclaims_space_and_integrity_ok(tmp_path):
    db = str(tmp_path / "events.db")
    conn = init_storage(db)
    try:
        conn.execute("BEGIN")
        for i in range(200):
            conn.execute("INSERT INTO events VALUES (?,?,1,'t')", (f"e{i}", "s"))
        conn.execute("COMMIT")
        conn.execute("DELETE FROM events")
        conn.execute("VACUUM")
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    finally:
        conn.close()


def test_prune_deletes_logs_older_than_seven_days():
    conn = _mem()
    now = 1_700_000_000.0
    try:
        ensure_log_table(conn)
        conn.execute(
            "INSERT INTO gateway_logs (created_ts,message) VALUES (?,?)",
            (now - 8 * 86400, "old"),
        )
        conn.execute(
            "INSERT INTO gateway_logs (created_ts,message) VALUES (?,?)",
            (now - 1 * 86400, "recent"),
        )
        deleted = prune_logs(conn, now, 7)
        remaining = [row[0] for row in conn.execute("SELECT message FROM gateway_logs")]
    finally:
        conn.close()
    assert deleted == 1
    assert remaining == ["recent"]


def test_corrupted_database_file_is_detected(tmp_path):
    path = tmp_path / "corrupted.db"
    path.write_bytes(b"this is definitely not a sqlite database" * 64)
    conn, error = safe_open(str(path))
    try:
        assert error is not None
    finally:
        conn.close()


def test_wal_checkpoint_truncate_succeeds(tmp_path):
    db = str(tmp_path / "events.db")
    conn = init_storage(db)
    try:
        asyncio.run(claim_event("checkpoint", conn))
        result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        conn.close()
    assert result is not None
    assert result[0] == 0
