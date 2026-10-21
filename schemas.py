"""Strict Pydantic v2 contracts for the connector gateway.

This module is the **single source of truth** for every wire contract; the
adapters import their models from here instead of redefining duplicates:

* :class:`WebhookPayload` — the generic inbound webhook envelope (event id,
  source, epoch-millisecond timestamp and an opaque ``data`` mapping).
* :class:`InboundWebhookPayload` — the HMAC/replay-aware ingress contraction
  validated by ``adapters.source_adapter`` after signature verification
  (``event_id``, ``event_ts``, ``source``, ``payload``).
* :class:`AmoLeadCreateSchema` / :class:`AmoContactCreateSchema` — request
  contracts for the amoCRM REST API (leads and contacts), used by the egress
  adapter when targeting amoCRM.

All models are ``extra="forbid"`` so that unexpected fields are rejected
instead of silently ignored.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field


class WebhookPayload(BaseModel):
    """Strict inbound webhook envelope."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(..., min_length=1)
    source: str = Field(..., min_length=1)
    timestamp: int = Field(..., ge=0)
    data: Dict[str, Any] = Field(default_factory=dict)


class InboundWebhookPayload(BaseModel):
    """Ingress contract for an HMAC-authenticated inbound webhook event.

    Owned by this module (Anti-Corruption Layer): ``adapters.source_adapter``
    imports it rather than declaring a duplicate ``WebhookPayload`` class, so
    the ingress shape can never drift from the schemas catalogue.
    """

    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_ts: datetime
    source: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class AmoLeadCreateSchema(BaseModel):
    """Contract for ``POST /api/v4/leads`` (create a deal)."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1)
    price: int = Field(..., ge=0)
    custom_fields_values: List[Dict[str, Any]] = Field(default_factory=list)


class AmoContactCreateSchema(BaseModel):
    """Contract for ``POST /api/v4/contacts`` (create a contact)."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1)
    phones: List[str] = Field(default_factory=list)
    emails: List[str] = Field(default_factory=list)


class OneCItemSchema(BaseModel):
    """1C order item schema."""

    model_config = ConfigDict(extra="forbid")

    sku: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    quantity: float = Field(..., ge=0)
    price: float = Field(..., ge=0)
    vat_rate: str = Field(default="20%")


class OneCOrderCreateSchema(BaseModel):
    """1C order create schema."""

    model_config = ConfigDict(extra="forbid")

    order_number: str = Field(..., min_length=1)
    date: str = Field(..., min_length=1)
    counterparty_inn: str = Field(..., min_length=1)
    counterparty_name: str = Field(..., min_length=1)
    items: List[OneCItemSchema] = Field(default_factory=list)
    total_amount: float = Field(..., ge=0)
    status: str = Field(default="New")


class OneCCounterpartySchema(BaseModel):
    """1C counterparty schema."""

    model_config = ConfigDict(extra="forbid")

    inn: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    kpp: str | None = None
    phone: str | None = None
    email: str | None = None


__all__ = [
    "WebhookPayload",
    "InboundWebhookPayload",
    "AmoLeadCreateSchema",
    "AmoContactCreateSchema",
    "OneCItemSchema",
    "OneCOrderCreateSchema",
    "OneCCounterpartySchema",
]
