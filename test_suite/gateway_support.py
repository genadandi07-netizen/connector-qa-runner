"""Deterministic reference support for the connector-gateway test suite.

The production adapters (``adapters/source_adapter.py``,
``adapters/storage_adapter.py``, ``adapters/egress_adapter.py``) cover ingress
authentication, SQLite WAL state/DLQ and the downstream HTTP port. Several
acceptance criteria in the ROPE specification exercise behaviour that is not
embodied by a single adapter — sliding-window IP rate limiting, a three-state
circuit breaker, input sanitisation, the amoCRM REST v4 projection, validation
error normalisation, queue scheduling/rotation and the n8n fallback webhook.

This module provides small, dependency-light reference implementations of those
contracts so the suite can assert on exact, deterministic behaviour. Nothing
here opens a socket, spawns a process or touches the network: all HTTP helpers
operate on an injected ``httpx.AsyncClient`` (backed by ``respx`` or an
``ASGITransport`` in the tests).
"""
from __future__ import annotations

import asyncio
import hashlib
import html
import json
import re
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence

import httpx

from schemas import OneCOrderCreateSchema

# --------------------------------------------------------------------------- #
# Event identity / hashing
# --------------------------------------------------------------------------- #
def event_hash(payload: Any) -> str:
    """Stable SHA-256 of a JSON payload (key order independent)."""
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def status_for_claim(claim: Dict[str, Any]) -> str:
    """Project a storage claim onto the queue state machine vocabulary."""
    return "PROCESSED" if claim.get("first_processed") else "IGNORED"


# --------------------------------------------------------------------------- #
# Inbound IP rate limiting (sliding window)
# --------------------------------------------------------------------------- #
class SlidingWindowRateLimiter:
    """Sliding-window limiter: at most ``limit`` hits per ``window_s`` seconds."""

    def __init__(self, limit: int = 10, window_s: float = 1.0) -> None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        self.limit = int(limit)
        self.window_s = float(window_s)
        self._hits: Dict[str, List[float]] = {}

    def allow(self, key: str, now: Optional[float] = None) -> bool:
        """Return True when ``key`` may act; record the hit when allowed."""
        moment = time.monotonic() if now is None else float(now)
        cutoff = moment - self.window_s
        bucket = self._hits.setdefault(key, [])
        while bucket and bucket[0] <= cutoff:
            bucket.pop(0)
        if len(bucket) >= self.limit:
            return False
        bucket.append(moment)
        return True

    def retry_after(self, key: str, now: Optional[float] = None) -> float:
        """Seconds until the oldest hit leaves the window for ``key``."""
        moment = time.monotonic() if now is None else float(now)
        bucket = self._hits.get(key, [])
        if not bucket:
            return 0.0
        return max(0.0, bucket[0] + self.window_s - moment)


# --------------------------------------------------------------------------- #
# Circuit breaker (CLOSED / OPEN / HALF_OPEN)
# --------------------------------------------------------------------------- #
class CircuitBreaker:
    """Three-state circuit breaker with an injectable monotonic clock."""

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: float = 30.0,
        clock=None,
    ) -> None:
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self._clock = clock or time.monotonic
        self.state = "CLOSED"
        self.failures = 0
        self.opened_at: Optional[float] = None

    def _maybe_half_open(self) -> None:
        if self.state == "OPEN" and self.opened_at is not None:
            if self._clock() - self.opened_at >= self.recovery_timeout:
                self.state = "HALF_OPEN"

    def allow(self) -> bool:
        """Return True when the protected call may proceed."""
        self._maybe_half_open()
        return self.state != "OPEN"

    def record_success(self) -> None:
        self.state = "CLOSED"
        self.failures = 0
        self.opened_at = None

    def record_failure(self) -> None:
        if self.state == "HALF_OPEN":
            self.state = "OPEN"
            self.failures = self.failure_threshold
            self.opened_at = self._clock()
            return
        self.failures += 1
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
            self.opened_at = self._clock()


# --------------------------------------------------------------------------- #
# Sanitisation / contact normalisation
# --------------------------------------------------------------------------- #
_E164_RE = re.compile(r"^\+[1-9]\d{7,14}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")


def sanitize_text(value: Any) -> str:
    """HTML-escape untrusted text (XSS neutralisation)."""
    return html.escape(str(value), quote=True)


def validate_e164(phone: str) -> bool:
    return bool(_E164_RE.match(phone or ""))


def normalize_phone(phone: str) -> str:
    """Strip masks/separators and return an E.164-ish ``+<digits>`` string."""
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return f"+{digits}" if digits else ""


def validate_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email or ""))


# --------------------------------------------------------------------------- #
# amoCRM REST v4 projection
# --------------------------------------------------------------------------- #
def to_camel_case(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(part.capitalize() for part in rest)


def lead_to_amocrm(lead: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"name": lead["name"], "price": int(lead["price"])}
    custom = lead.get("custom_fields_values") or []
    if custom:
        payload["custom_fields_values"] = custom
    return payload


def contact_to_amocrm(contact: Dict[str, Any]) -> Dict[str, Any]:
    custom: List[Dict[str, Any]] = []
    for phone in contact.get("phones") or []:
        custom.append(
            {"field_code": "PHONE", "values": [{"value": phone, "enum_code": "WORK"}]}
        )
    for email in contact.get("emails") or []:
        custom.append(
            {"field_code": "EMAIL", "values": [{"value": email, "enum_code": "WORK"}]}
        )
    payload: Dict[str, Any] = {"name": contact["name"]}
    if custom:
        payload["custom_fields_values"] = custom
    return payload


# --------------------------------------------------------------------------- #
# Validation error normalisation (BugMatrix projection)
# --------------------------------------------------------------------------- #
def normalize_validation_error(exc: Any) -> Dict[str, Any]:
    """Turn a Pydantic ``ValidationError`` into a normalised failure record."""
    entries: List[Dict[str, Any]] = []
    for err in exc.errors():
        location = err.get("loc", ())
        entries.append(
            {
                "field": ".".join(str(part) for part in location),
                "type": err.get("type", "value_error"),
                "message": err.get("msg", ""),
            }
        )
    return {"status": "invalid", "error_count": len(entries), "errors": entries}


def build_bug_matrix(samples: Sequence[Sequence[Any]]) -> Dict[str, Any]:
    """Aggregate ``(source, normalised_error)`` pairs into a BugMatrix doc."""
    return {
        "schema_version": 1,
        "failures": [{"source": source, **error} for source, error in samples],
    }


# --------------------------------------------------------------------------- #
# Queue scheduling / rotation (status machine)
# --------------------------------------------------------------------------- #
QUEUE_STATES = ("NEW", "PROCESSING", "PROCESSED")


class QueueStore:
    """Minimal lead queue backed by the same SQLite connection as the state DB."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS lead_queue ("
            "id INTEGER PRIMARY KEY, "
            "status TEXT NOT NULL DEFAULT 'NEW', "
            "payload TEXT, "
            "updated_at TEXT)"
        )

    def enqueue(self, ids: Iterable[int]) -> None:
        self.conn.execute("BEGIN")
        try:
            for item in ids:
                self.conn.execute(
                    "INSERT OR IGNORE INTO lead_queue (id, status) VALUES (?, 'NEW')",
                    (item,),
                )
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    def claim_batch(self, limit: int) -> List[int]:
        """Atomically mark up to ``limit`` NEW rows PROCESSING."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self.conn.execute(
                "SELECT id FROM lead_queue WHERE status='NEW' ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
            ids = [row[0] for row in rows]
            for item in ids:
                self.conn.execute(
                    "UPDATE lead_queue SET status='PROCESSING' WHERE id=?", (item,)
                )
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        return ids

    def mark_processed(self, ids: Iterable[int]) -> None:
        self.conn.execute("BEGIN")
        try:
            for item in ids:
                self.conn.execute(
                    "UPDATE lead_queue SET status='PROCESSED' WHERE id=?", (item,)
                )
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    def status_of(self, item: int) -> Optional[str]:
        row = self.conn.execute(
            "SELECT status FROM lead_queue WHERE id=?", (item,)
        ).fetchone()
        return row[0] if row else None


def ensure_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS gateway_logs ("
        "id INTEGER PRIMARY KEY, created_ts REAL, message TEXT)"
    )


def prune_logs(conn: sqlite3.Connection, now_epoch: float, max_age_days: int = 7) -> int:
    """Delete log rows older than ``max_age_days``; return rows removed."""
    ensure_log_table(conn)
    cutoff = float(now_epoch) - max_age_days * 86400
    cursor = conn.execute("DELETE FROM gateway_logs WHERE created_ts < ?", (cutoff,))
    return cursor.rowcount


def safe_open(path: str):
    """Open a SQLite file, returning ``(conn, error_name_or_None)``."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("SELECT COUNT(*) FROM sqlite_master")
    except sqlite3.DatabaseError as exc:
        return conn, type(exc).__name__
    return conn, None


# --------------------------------------------------------------------------- #
# n8n fallback webhook + deadline helper
# --------------------------------------------------------------------------- #
async def post_n8n_fallback(
    client: httpx.AsyncClient, url: str, payload: Dict[str, Any]
) -> Dict[str, Any]:
    """POST a packet to the n8n fallback webhook, never raising."""
    try:
        response = await client.post(url, json=payload)
    except httpx.HTTPError as exc:
        return {
            "status": "fallback-error",
            "reason": type(exc).__name__,
            "delivered": False,
        }
    delivered = 200 <= response.status_code < 300
    try:
        body = response.json()
    except ValueError:
        body = None
    return {
        "status": "fallback-ok" if delivered else "fallback-error",
        "status_code": response.status_code,
        "delivered": delivered,
        "body": body,
    }


async def run_with_deadline(awaitable, timeout_s: float):
    """Await ``awaitable`` with a hard deadline (maps to 504/отсечка)."""
    return await asyncio.wait_for(awaitable, timeout=timeout_s)


# --------------------------------------------------------------------------- #
# 1C Egress support
# --------------------------------------------------------------------------- #
_INN_RE = re.compile(r"^\d{10}(\d{2})?$")


def _sanitize_inn(inn: Any) -> str:
    if inn is None:
        return ""
    s = str(inn)
    # Letters mean reject - per test requirements
    if re.search(r"[A-Za-z]", s):
        return ""
    digits = re.sub(r"\D", "", s)
    return digits


def _validate_inn_format(inn: str) -> bool:
    if not inn or not _INN_RE.match(inn):
        return False
    return True


def lead_to_1c_order(lead_data: dict, default_inn: str = "7700000000") -> dict:
    """Project amoCRM lead data to 1C order contract."""
    if not isinstance(lead_data, dict):
        lead_data = {}

    name = lead_data.get("name") or ""
    price = lead_data.get("price", 0)

    custom_fields = lead_data.get("custom_fields_values") or []

    inn_raw = None
    counterparty_name = lead_data.get("counterparty_name") or name or "Counterparty"
    items = []

    for field in custom_fields:
        field_code = field.get("field_code") or field.get("code") or field.get("name")
        values = field.get("values") or []
        value_str = values[0].get("value") if values else ""
        if isinstance(field_code, str) and "ИНН" in field_code:
            inn_raw = value_str
        if isinstance(field_code, str) and "позиции" in field_code.lower():
            # try to parse items if structured
            pass

    # Look for explicit fields
    if inn_raw is None:
        inn_raw = lead_data.get("inn") or lead_data.get("counterparty_inn")

    inn_clean = _sanitize_inn(inn_raw) if inn_raw else ""
    if not _validate_inn_format(inn_clean):
        inn_clean = _sanitize_inn(default_inn)
        if not _validate_inn_format(inn_clean):
            inn_clean = "7700000000"

    total_amount = float(price) if price is not None else 0.0

    # Try to extract items from various possible structures
    items_data = lead_data.get("items") or lead_data.get("products") or []
    if items_data:
        for item in items_data:
            if isinstance(item, dict):
                sku = str(item.get("sku") or item.get("id") or "SKU")
                item_name = str(item.get("name") or "Item")
                qty = float(item.get("quantity") or item.get("qty") or 1)
                item_price = float(item.get("price") or total_amount / qty if qty > 0 else 0)
                items.append(
                    {
                        "sku": sku,
                        "name": item_name,
                        "quantity": qty,
                        "price": item_price,
                        "vat_rate": item.get("vat_rate", "20%"),
                    }
                )

    order_number = str(lead_data.get("order_number") or lead_data.get("id") or "ORD-001")
    date = str(lead_data.get("date") or lead_data.get("created_at") or "2024-01-01")

    order_data = {
        "order_number": order_number,
        "date": date,
        "counterparty_inn": inn_clean,
        "counterparty_name": str(counterparty_name),
        "items": items,
        "total_amount": total_amount,
        "status": lead_data.get("status") or "New",
    }

    # Validate via schema
    validated = OneCOrderCreateSchema.model_validate(order_data)
    return validated.model_dump()


__all__ = [
    "event_hash",
    "status_for_claim",
    "SlidingWindowRateLimiter",
    "CircuitBreaker",
    "sanitize_text",
    "validate_e164",
    "normalize_phone",
    "validate_email",
    "to_camel_case",
    "lead_to_amocrm",
    "contact_to_amocrm",
    "lead_to_1c_order",
    "normalize_validation_error",
    "build_bug_matrix",
    "QUEUE_STATES",
    "QueueStore",
    "ensure_log_table",
    "prune_logs",
    "safe_open",
    "post_n8n_fallback",
    "run_with_deadline",
]
