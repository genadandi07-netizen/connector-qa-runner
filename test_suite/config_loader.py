"""Runtime configuration loader for the connector-qa-runner test suite.

``test_suite/config.json`` stores the *non-secret* tuning knobs (retry policy,
backoff strategy, circuit-breaker limit, replay window). Secrets and
environment-specific endpoints are **not** stored here: they are resolved from
:class:`config.GatewaySettings` (process environment / ``.env``) and injected on
top of the JSON document. Any ``${VAR}`` placeholder is expanded from the
environment as well.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional

try:  # the settings module lives one level up (code/config.py)
    from config import get_settings
except ImportError:  # pragma: no cover - allow standalone use of the loader
    get_settings = None  # type: ignore[assignment]

_DEFAULT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
_PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _expand(value: Any, fallback: Dict[str, Any]) -> Any:
    """Recursively expand ``${VAR}`` placeholders from the environment."""
    if isinstance(value, str):
        def _replace(match: "re.Match[str]") -> str:
            name = match.group(1)
            if name in os.environ:
                return os.environ[name]
            return str(fallback.get(name, match.group(0)))

        return _PLACEHOLDER_RE.sub(_replace, value)
    if isinstance(value, dict):
        return {key: _expand(item, fallback) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item, fallback) for item in value]
    return value


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """Load runtime configuration, overlaying secrets from settings.

    The JSON file provides the composable, non-secret defaults; the settings
    object (environment / ``.env``) is authoritative for the HMAC secret,
    downstream endpoint, SQLite path and n8n webhook URL.
    """
    target = path or _DEFAULT_PATH
    with open(target, "r", encoding="utf-8") as handle:
        cfg: Dict[str, Any] = json.load(handle)

    settings = get_settings() if get_settings is not None else None
    fallback = {
        "HMAC_SECRET": getattr(settings, "HMAC_SECRET", ""),
        "AMOCRM_BASE_URL": getattr(settings, "AMOCRM_BASE_URL", ""),
        "AMOCRM_TOKEN": getattr(settings, "AMOCRM_TOKEN", ""),
        "SQLITE_DB_PATH": getattr(settings, "SQLITE_DB_PATH", ""),
        "N8N_WEBHOOK_URL": getattr(settings, "N8N_WEBHOOK_URL", ""),
    }
    cfg = _expand(cfg, fallback)

    if settings is not None:
        cfg["hmac_secret"] = settings.HMAC_SECRET
        cfg.setdefault("egress", {})
        cfg["egress"]["url"] = settings.egress_url
        cfg["egress"]["token"] = settings.AMOCRM_TOKEN
        # Token-bucket tuning: the JSON file stays authoritative when it
        # defines the knobs; missing keys fall back to ``GatewaySettings``.
        rate_limit = cfg.setdefault("rate_limit", {})
        rate_limit.setdefault("rate_limit_rps", float(getattr(settings, "RATE_LIMIT_RPS", 2.0)))
        rate_limit.setdefault("burst", int(getattr(settings, "RATE_LIMIT_BURST", 5)))
        cfg.setdefault("storage", {})
        cfg["storage"]["db_path"] = settings.SQLITE_DB_PATH
        cfg.setdefault("n8n", {})
        cfg["n8n"]["webhook_url"] = settings.N8N_WEBHOOK_URL

    return cfg
