"""EgressAdapter — downstream port (Anti-Corruption Layer).

All traffic to the downstream CRM REST API (HubSpot by default, amoCRM /
Bitrix24 / Google Sheets via subclass) flows through :class:`EgressAdapter`:

* :meth:`EgressAdapter.build_request` is the **only** place where the URL, the
  authenticated headers (``Authorization: Bearer <token>`` from ``config.py`` /
  ``config.json``) and the JSON body are assembled. It is a plain, synchronous
  method returning ``(url, headers, body)`` and is designed to be overridden so
  any external system can be adapted without touching the transport logic.
* :meth:`EgressAdapter.send` consumes ``build_request()`` and adds proactive
  ``TokenBucketRateLimiter`` throttling (``rate_limit_rps`` / ``burst``),
  single-flight token rotation on 401 guarded by an adapter-level
  ``asyncio.Lock`` (no race conditions, no parallel invalidation),
  full-jitter backoff for 429 / 5xx, and funnels exhausted retries and
  timeouts into the DLQ without raising.

The module-level :func:`send` function is kept as a thin backward-compatible
wrapper around the class.
"""
from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

import httpx

from test_suite.config_loader import load_config
from .storage_adapter import write_dlq

Classifier = str

#: Proactive rate-limit defaults (Token Bucket), overridable via
#: ``rate_limit`` in ``test_suite/config.json`` / ``GatewaySettings``.
DEFAULT_RATE_LIMIT_RPS = 2.0
DEFAULT_RATE_LIMIT_BURST = 5


def classify(status_code: int) -> Classifier:
    """Classify a downstream HTTP status."""
    if 200 <= status_code < 300:
        return "ok"
    if status_code == 429:
        return "retry"
    if 500 <= status_code < 600:
        return "fail"
    return "error"


def compute_backoff_delay(
    attempt: int,
    retry_after: Optional[float],
    rng,
    base: float = 0.5,
    cap: float = 8.0,
) -> float:
    """Full-jitter backoff: uniform(0, min(cap, base * 2**attempt))."""
    ceiling = min(cap, base * (2 ** attempt))
    if retry_after is not None:
        ceiling = min(ceiling, float(retry_after))
    if ceiling <= 0:
        return 0.0
    return rng.uniform(0, ceiling)


def _parse_retry_after(response: httpx.Response) -> Optional[float]:
    value = response.headers.get("Retry-After")
    if not value:
        return None
    value = value.strip()
    try:
        return float(value)
    except ValueError:
        pass
    try:
        moment = parsedate_to_datetime(value)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return max(0.0, (moment - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return None


class TokenBucketRateLimiter:
    """Proactive async token-bucket rate limiter.

    ``rate_limit_rps`` tokens are refilled per second up to ``burst`` tokens
    (both defaults: 2.0 / 5 — bound to ``config.py`` and
    ``test_suite/config.json``). ``acquire()`` consumes one token and sleeps
    exactly long enough for the bucket to refill when it is empty, so callers
    are throttled *before* the request leaves the process.

    ``clock`` and ``sleep`` are injectable for deterministic tests.
    """

    def __init__(
        self,
        rate_limit_rps: float = DEFAULT_RATE_LIMIT_RPS,
        burst: int = DEFAULT_RATE_LIMIT_BURST,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
    ) -> None:
        if float(rate_limit_rps) <= 0:
            raise ValueError("rate_limit_rps must be > 0")
        if int(burst) < 1:
            raise ValueError("burst must be >= 1")
        self.rate_limit_rps = float(rate_limit_rps)
        self.burst = float(burst)
        self._tokens = self.burst
        self._clock = clock
        self._sleep = sleep or asyncio.sleep
        self._updated_at = clock()
        self._lock = asyncio.Lock()

    @property
    def tokens(self) -> float:
        """Current number of tokens in the bucket (informational)."""
        return self._tokens

    async def acquire(self, tokens: float = 1.0) -> float:
        """Consume ``tokens`` permits; sleep until the bucket refills.

        Returns the time (seconds) spent waiting for a permit.
        """
        waited = 0.0
        async with self._lock:
            while True:
                now = self._clock()
                elapsed = now - self._updated_at
                if elapsed > 0:
                    self._tokens = min(self.burst, self._tokens + elapsed * self.rate_limit_rps)
                    self._updated_at = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited
                deficit = tokens - self._tokens
                delay = deficit / self.rate_limit_rps
                await self._sleep(delay)
                waited += delay


class EgressAdapter:
    """Downstream HTTP port: ACL request building, auth rotation, throttling.

    Instances are cheap; the module-level :func:`send` helper builds one per
    call. The adapter owns:

    * an :class:`asyncio.Lock` serialising token rotation (single-flight
      refresh on 401 — concurrent requests never invalidate in parallel),
    * a :class:`TokenBucketRateLimiter` consulted **before** every
      ``client.post``.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        dlq_dir: str = "dlq",
        client: Optional[httpx.AsyncClient] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        token_refresher: Optional[Callable[[], Awaitable[Optional[str]]]] = None,
    ) -> None:
        self.config = config if config is not None else load_config()
        self.dlq_dir = dlq_dir
        self.client = client
        self.sleep = sleep or asyncio.sleep
        # Race-free token rotation: exactly one refresher runs at a time.
        self._token_lock = asyncio.Lock()
        self._token_refresher = token_refresher or self._reload_token_from_config
        self._token = (self.config.get("egress") or {}).get("token")
        self._token_version = 0
        rate_cfg = self.config.get("rate_limit") or {}
        self.rate_limiter = TokenBucketRateLimiter(
            rate_limit_rps=float(rate_cfg.get("rate_limit_rps", DEFAULT_RATE_LIMIT_RPS)),
            burst=int(rate_cfg.get("burst", DEFAULT_RATE_LIMIT_BURST)),
            sleep=self.sleep,
        )

    # ------------------------------------------------------------------ #
    # Anti-Corruption Layer — override `build_request` for new systems
    # ------------------------------------------------------------------ #
    @property
    def token(self) -> Optional[str]:
        """The bearer token currently attached to outbound requests."""
        return self._token

    @property
    def token_version(self) -> int:
        """Number of completed token rotations (diagnostics/tests)."""
        return self._token_version

    def build_request(self, event: Dict[str, Any]) -> Tuple[str, Dict[str, str], Any]:
        """Map an internal event to ``(url, headers, body)`` for the wire.

        The single place where URL, authorisation header and payload are
        formed — subclasses override it to adapt any external system without
        touching retry/throttling/DLQ logic in :meth:`send`.
        """
        url = str(self.config["egress"]["url"])
        event_id = event.get("event_id")
        body: Dict[str, Any] = {
            "event_id": event_id,
            "source": event.get("source", "n8n"),
            "payload": event.get("payload", {}),
        }
        headers: Dict[str, str] = {"Idempotency-Key": str(event_id)}
        token = self._token
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return url, headers, body

    # ------------------------------------------------------------------ #
    # Token rotation (single-flight under asyncio.Lock)
    # ------------------------------------------------------------------ #
    async def _reload_token_from_config(self) -> Optional[str]:
        """Default refresher: re-resolve the token from the runtime config."""
        try:
            cfg = load_config()
        except (OSError, ValueError):  # pragma: no cover - defensive
            return None
        return (cfg.get("egress") or {}).get("token")

    async def refresh_token(self, seen_version: Optional[int] = None) -> Optional[str]:
        """Rotate the downstream token exactly once per invalidation wave.

        The first coroutine to observe a 401 takes the adapter-level lock and
        performs the refresh; any concurrent 401 handler arriving with the
        same ``seen_version`` blocks on the lock, sees the version already
        bumped and reuses the new token instead of refreshing again.
        """
        async with self._token_lock:
            if seen_version is not None and self._token_version != seen_version:
                return self._token  # another coroutine already rotated it
            new_token = await self._token_refresher()
            if new_token:
                self._token = new_token
            self._token_version += 1
            return self._token

    # ------------------------------------------------------------------ #
    # Transport: retries + backoff + DLQ (never raises)
    # ------------------------------------------------------------------ #
    async def send(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Send an event downstream with throttling, retries and backoff.

        Every attempt goes through :meth:`build_request` and waits on the rate
        limiter first. Returns ``{"status": "ok" | "dlq", ...}``; timeouts and
        exhausted retries never escape as exceptions.
        """
        retry_cfg = self.config.get("retry", {})
        max_attempts = int(retry_cfg.get("max_attempts", 3))
        base = float(retry_cfg.get("base_delay_s", 0.1))
        cap = float(self.config.get("backoff", {}).get("max_delay_s", 5.0))
        timeout = float(retry_cfg.get("timeout_s", 5.0))

        event_id = event.get("event_id")
        last_reason = "unknown"
        owns_client = self.client is None
        client = self.client if self.client is not None else httpx.AsyncClient(timeout=timeout)
        rotated_after_401 = False

        try:
            for attempt in range(max_attempts):
                url, headers, body = self.build_request(event)
                seen_token_version = self._token_version
                try:
                    # Proactive (not reactive) rate limiting: wait for a
                    # token *before* the request is dispatched.
                    await self.rate_limiter.acquire()
                    response = await client.post(url, json=body, headers=headers)
                except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.TimeoutException) as exc:
                    last_reason = type(exc).__name__
                    break
                except httpx.TransportError as exc:
                    last_reason = type(exc).__name__
                    break

                if response.status_code == 401 and not rotated_after_401:
                    # Single-flight rotation, then retry with the new token.
                    rotated_after_401 = True
                    last_reason = f"HTTP {response.status_code}"
                    await self.refresh_token(seen_token_version)
                    continue

                kind = classify(response.status_code)
                if kind == "ok":
                    return {
                        "status": "ok",
                        "event_id": event_id,
                        "attempts": attempt + 1,
                        "status_code": response.status_code,
                    }
                if kind == "retry":
                    retry_after = _parse_retry_after(response)
                    last_reason = f"HTTP {response.status_code}"
                    delay = compute_backoff_delay(attempt, retry_after, random, base, cap)
                    await self.sleep(delay)
                    continue

                last_reason = f"HTTP {response.status_code}"
                if attempt + 1 >= max_attempts:
                    break
                delay = compute_backoff_delay(attempt, None, random, base, cap)
                await self.sleep(delay)

            path = write_dlq(event_id, event, last_reason, self.dlq_dir)
            return {
                "status": "dlq",
                "event_id": event_id,
                "reason": last_reason,
                "dlq_path": path,
            }
        finally:
            if owns_client:
                await client.aclose()


async def send(
    event: Dict[str, Any],
    conn,
    dlq_dir: str,
    config: Optional[Dict[str, Any]] = None,
    client: Optional[httpx.AsyncClient] = None,
    sleep: Optional[Callable[[float], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    """Functional entry point kept for backward compatibility.

    Thin wrapper over :class:`EgressAdapter` — the (unused) ``conn`` argument
    is retained so existing callers keep working. See
    :meth:`EgressAdapter.send` for semantics.
    """
    adapter = EgressAdapter(
        config=config,
        dlq_dir=dlq_dir,
        client=client,
        sleep=sleep,
    )
    return await adapter.send(event)


__all__ = [
    "Classifier",
    "DEFAULT_RATE_LIMIT_BURST",
    "DEFAULT_RATE_LIMIT_RPS",
    "EgressAdapter",
    "TokenBucketRateLimiter",
    "classify",
    "compute_backoff_delay",
    "send",
]
