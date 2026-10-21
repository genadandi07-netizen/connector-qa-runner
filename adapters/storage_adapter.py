"""StorageAdapter — state, dedup and DLQ port.

SQLite in WAL mode with ``BEGIN IMMEDIATE`` and ``busy_timeout=5000`` guarantees
exactly one ``first_processed`` per ``event_id`` under 30-50 concurrent
coroutines. DLQ writes are atomic through ``os.replace`` so partial files never
survive. Extensible to ChromaDB via subclass.

Dependency direction: this module implements the ``StoragePort`` protocol that
``qa_runner_core`` consumes. It must never import ``qa_runner_core`` (or any
runner module) — the flow is strictly Core/Runner -> Adapters -> Storage.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def init_storage(db_path: str) -> sqlite3.Connection:
    """Open a WAL-mode connection with the agreed PRAGMAs."""
    conn = sqlite3.connect(
        db_path,
        isolation_level=None,
        timeout=5.0,
        check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events ("
        "event_id TEXT PRIMARY KEY, "
        "status TEXT, "
        "first_processed INTEGER DEFAULT 0, "
        "created_at TEXT"
        ")"
    )
    return conn


def _claim_event_sync(
    event_id: str,
    conn: sqlite3.Connection,
    now: Optional[str] = None,
) -> Dict[str, Any]:
    """Atomic claim under BEGIN IMMEDIATE; returns the single winner."""
    timestamp = now or datetime.now(timezone.utc).isoformat()
    deadline = time.monotonic() + 5.0
    while True:
        try:
            conn.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() and time.monotonic() < deadline:
                time.sleep(0.002)
                continue
            raise
    try:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO events "
            "(event_id, status, first_processed, created_at) VALUES (?, ?, ?, ?)",
            (event_id, "first_processed", 1, timestamp),
        )
        first_processed = cursor.rowcount == 1
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    return {"event_id": event_id, "first_processed": first_processed}


async def claim_event(
    event_id: str,
    conn: sqlite3.Connection,
    now: Optional[str] = None,
) -> Dict[str, Any]:
    """Async wrapper around the blocking claim; one connection per coroutine."""
    return await asyncio.to_thread(_claim_event_sync, event_id, conn, now)


def get_event(event_id: str, conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    """Read operation (CRUD completeness)."""
    row = conn.execute(
        "SELECT event_id, status, first_processed, created_at FROM events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "event_id": row[0],
        "status": row[1],
        "first_processed": bool(row[2]),
        "created_at": row[3],
    }


def write_dlq(
    event_id: str,
    payload: Any,
    reason: str,
    dlq_dir: str,
) -> str:
    """Atomically persist a dead-letter record (temp file + fsync + os.replace)."""
    os.makedirs(dlq_dir, exist_ok=True)
    tmp_path = os.path.join(
        dlq_dir, f"{event_id}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    final_path = os.path.join(dlq_dir, f"{event_id}.json")
    record = {
        "event_id": event_id,
        "reason": reason,
        "payload": payload,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    descriptor = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, final_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return final_path


def replay_dlq(event_id: str, conn: sqlite3.Connection, dlq_dir: str) -> bool:
    """Return a DLQ event to processing; removes the DLQ file on success."""
    path = os.path.join(dlq_dir, f"{event_id}.json")
    if not os.path.exists(path):
        return False
    with open(path, "r", encoding="utf-8") as handle:
        json.load(handle)
    conn.execute("DELETE FROM events WHERE event_id = ?", (event_id,))
    result = _claim_event_sync(event_id, conn)
    if result["first_processed"]:
        os.remove(path)
        return True
    return False