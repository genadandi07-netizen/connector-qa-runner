"""Central configuration for connector-qa-runner.

All secrets and environment-specific endpoints are resolved here through a
Pydantic v2 ``BaseSettings`` model. Values are read from the process
environment and, when present, from a local ``.env`` file next to this module.
The defaults below are *safe test values* intended only for local/CI runs and
must be overridden for any real deployment (see ``.env.example``).

The module is deliberately dependency-free (it only imports
``pydantic-settings``/``pydantic`` and the standard library) so that it can be
imported from the core, the adapters and the storage layer without creating an
import cycle.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

try:  # pydantic v2 keeps BaseSettings in the optional pydantic-settings package
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:  # pragma: no cover - defensive fallback for minimal envs
    from pydantic import BaseSettings  # type: ignore[attr-defined,no-redef]
    from pydantic import ConfigDict  # type: ignore[attr-defined]

    SettingsConfigDict = ConfigDict  # type: ignore[assignment,misc]

_ENV_FILE = Path(__file__).resolve().parent / ".env"


class GatewaySettings(BaseSettings):
    """Typed, validated configuration for the isolated connector gateway.

    Every field has a safe non-production default and can be overridden through
    an environment variable of the same (upper-case) name or via ``.env``.
    """

    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # HMAC shared secret used by the inbound webhook (Signature: sha256=...).
    HMAC_SECRET: str = "connector-qa-runner-test-secret"
    # amoCRM / downstream CRM access.
    AMOCRM_BASE_URL: str = "https://api.hubapi.com"
    AMOCRM_TOKEN: str = "test-amocrm-token"
    # Proactive egress rate limiting (Token Bucket) consumed by
    # ``adapters.egress_adapter.TokenBucketRateLimiter``.
    RATE_LIMIT_RPS: float = 2.0
    RATE_LIMIT_BURST: int = 5
    # SQLite WAL state database.
    SQLITE_DB_PATH: str = "events.db"
    # n8n dispatcher webhook the gateway can notify.
    N8N_WEBHOOK_URL: str = "http://localhost:5678/webhook/connector-qa-runner"

    @property
    def egress_url(self) -> str:
        """Fully-qualified downstream endpoint used by the egress adapter."""
        base = self.AMOCRM_BASE_URL.rstrip("/")
        return f"{base}/crm/v3/objects/contacts"

    def as_runtime_config(self) -> Dict[str, Any]:
        """Project the settings onto the nested runtime-config shape.

        Keeps the historical ``config.json`` schema working while making the
        settings object the single source of truth for secrets/endpoints.
        """
        return {
            "hmac_secret": self.HMAC_SECRET,
            "retry": {"max_attempts": 3, "base_delay_s": 0.1, "timeout_s": 2.0},
            "backoff": {"strategy": "full_jitter", "max_delay_s": 5.0},
            "circuit_breaker": {"max_passes": 2},
            "replay": {"max_age_s": 300},
            "storage": {"db_path": self.SQLITE_DB_PATH, "busy_timeout_ms": 5000, "journal_mode": "WAL"},
            "egress": {"url": self.egress_url, "token": self.AMOCRM_TOKEN},
            "rate_limit": {"rate_limit_rps": self.RATE_LIMIT_RPS, "burst": self.RATE_LIMIT_BURST},
            "n8n": {"webhook_url": self.N8N_WEBHOOK_URL},
        }


@lru_cache(maxsize=1)
def get_settings() -> GatewaySettings:
    """Return the process-wide settings singleton."""
    return GatewaySettings()


__all__ = ["GatewaySettings", "get_settings"]
