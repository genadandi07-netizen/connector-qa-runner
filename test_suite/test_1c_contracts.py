"""1C Egress contract tests (25 tests).

Validates 1C schemas, field mapping from amoCRM to 1C, and 1C OData response
emulation via MockTransport (422, 500, locks, etc.).
"""

from __future__ import annotations

import json

import httpx
import pytest
from pydantic import ValidationError

from schemas import (
    OneCCounterpartySchema,
    OneCItemSchema,
    OneCOrderCreateSchema,
)
from test_suite.gateway_support import lead_to_1c_order


# --------------------------------------------------------------------------- #
# 1C Schema validation (8 tests)
# --------------------------------------------------------------------------- #
def test_onec_item_schema_valid():
    item = OneCItemSchema.model_validate(
        {
            "sku": "SKU001",
            "name": "Product",
            "quantity": 2.0,
            "price": 100.0,
            "vat_rate": "20%",
        }
    )
    assert item.sku == "SKU001"
    assert item.quantity == 2.0


def test_onec_item_schema_minimal():
    item = OneCItemSchema.model_validate(
        {"sku": "S1", "name": "N1", "quantity": 1.0, "price": 50.0}
    )
    assert item.vat_rate == "20%"


def test_onec_item_schema_forbids_extra():
    with pytest.raises(ValidationError):
        OneCItemSchema.model_validate(
            {
                "sku": "S1",
                "name": "N1",
                "quantity": 1.0,
                "price": 50.0,
                "extra_field": "bad",
            }
        )


def test_onec_item_schema_negative_quantity_rejected():
    with pytest.raises(ValidationError):
        OneCItemSchema.model_validate(
            {"sku": "S1", "name": "N1", "quantity": -1.0, "price": 50.0}
        )


def test_onec_item_schema_negative_price_rejected():
    with pytest.raises(ValidationError):
        OneCItemSchema.model_validate(
            {"sku": "S1", "name": "N1", "quantity": 1.0, "price": -5.0}
        )


def test_onec_order_schema_valid():
    order = OneCOrderCreateSchema.model_validate(
        {
            "order_number": "ORD-001",
            "date": "2024-01-15",
            "counterparty_inn": "7700123456",
            "counterparty_name": "Test LLC",
            "items": [],
            "total_amount": 1000.0,
            "status": "New",
        }
    )
    assert order.order_number == "ORD-001"
    assert order.status == "New"


def test_onec_order_schema_empty_items_allowed():
    order = OneCOrderCreateSchema.model_validate(
        {
            "order_number": "ORD-002",
            "date": "2024-01-15",
            "counterparty_inn": "7700123456",
            "counterparty_name": "Test LLC",
            "items": [],
            "total_amount": 0.0,
        }
    )
    assert len(order.items) == 0


def test_onec_order_schema_boundary_prices_quantities():
    order = OneCOrderCreateSchema.model_validate(
        {
            "order_number": "ORD-003",
            "date": "2024-01-15",
            "counterparty_inn": "772345678901",
            "counterparty_name": "Test",
            "items": [
                OneCItemSchema.model_validate(
                    {
                        "sku": "S",
                        "name": "N",
                        "quantity": 0.001,
                        "price": 0.01,
                    }
                )
            ],
            "total_amount": 0.0,
        }
    )
    assert order.total_amount == 0.0


# --------------------------------------------------------------------------- #
# INN/KPP validation (6 tests)
# --------------------------------------------------------------------------- #
def test_inn_10_digits_valid():
    cp = OneCCounterpartySchema.model_validate(
        {"inn": "7700123456", "name": "Org"}
    )
    assert cp.inn == "7700123456"


def test_inn_12_digits_valid():
    cp = OneCCounterpartySchema.model_validate(
        {"inn": "772345678901", "name": "Org"}
    )
    assert cp.inn == "772345678901"


def test_inn_sanitization_in_mapping():
    lead = {
        "name": "Lead1",
        "price": 100,
        "custom_fields_values": [
            {"field_code": "ИНН", "values": [{"value": "7700-123-456"}]}
        ],
    }
    result = lead_to_1c_order(lead)
    assert result["counterparty_inn"] == "7700123456"


def test_inn_with_spaces_sanitized():
    lead = {
        "name": "Lead2",
        "price": 200,
        "custom_fields_values": [
            {"field_code": "ИНН", "values": [{"value": " 7700123456  "}]}
        ],
    }
    result = lead_to_1c_order(lead)
    assert result["counterparty_inn"] == "7700123456"


def test_inn_letters_rejected_and_defaulted():
    lead = {"name": "Lead3", "price": 150, "inn": "ABC7700123456"}
    result = lead_to_1c_order(lead, default_inn="7700000000")
    assert result["counterparty_inn"] == "7700000000"


def test_inn_invalid_length_defaulted():
    lead = {"name": "Lead4", "price": 50, "inn": "123"}
    result = lead_to_1c_order(lead)
    assert len(result["counterparty_inn"]) in (10, 12)


# --------------------------------------------------------------------------- #
# amoCRM -> 1C mapping (6 tests)
# --------------------------------------------------------------------------- #
def test_mapping_basic_fields():
    lead = {
        "name": "Test Deal",
        "price": 5000,
        "id": "12345",
    }
    result = lead_to_1c_order(lead)
    assert result["order_number"] == "12345"
    assert result["total_amount"] == 5000.0
    assert result["status"] == "New"


def test_mapping_custom_inn_field():
    lead = {
        "name": "Deal",
        "price": 1000,
        "custom_fields_values": [
            {"field_code": "ИНН контрагента", "values": [{"value": "7700123456"}]}
        ],
    }
    result = lead_to_1c_order(lead)
    assert result["counterparty_inn"] == "7700123456"


def test_mapping_items_from_products():
    lead = {
        "name": "Deal",
        "price": 300,
        "items": [
            {"sku": "P1", "name": "Prod1", "quantity": 1, "price": 100},
            {"sku": "P2", "name": "Prod2", "quantity": 2, "price": 100},
        ],
    }
    result = lead_to_1c_order(lead)
    assert len(result["items"]) == 2


def test_mapping_calculates_total():
    lead = {
        "name": "Deal",
        "price": 2500,
    }
    result = lead_to_1c_order(lead)
    assert result["total_amount"] == 2500.0


def test_mapping_status_default():
    lead = {"name": "Deal", "price": 100}
    result = lead_to_1c_order(lead)
    assert result["status"] == "New"


def test_mapping_counterparty_name():
    lead = {"name": "Big Deal", "price": 100, "counterparty_name": "ООО Ромашка"}
    result = lead_to_1c_order(lead)
    assert result["counterparty_name"] == "ООО Ромашка"


# --------------------------------------------------------------------------- #
# 1C OData MockTransport responses (5 tests)
# --------------------------------------------------------------------------- #
def _make_transport(handler):
    return httpx.MockTransport(handler)


def test_1c_201_created():
    def handler(request):
        return httpx.Response(
            201,
            json={
                "value": [{"Ref_Key": "123", "Number": "ORD-001"}]
            },
        )

    transport = _make_transport(handler)
    client = httpx.Client(transport=transport)
    resp = client.post("http://1c.local/odata/Document.Order")
    assert resp.status_code == 201
    data = resp.json()
    assert data["value"][0]["Number"] == "ORD-001"
    client.close()


def test_1c_422_unprocessable_entity():
    def handler(request):
        return httpx.Response(
            422,
            json={
                "error": {
                    "code": "Validation",
                    "message": "Invalid INN",
                    "details": [{"field": "INN", "error": "wrong format"}],
                }
            },
        )

    transport = _make_transport(handler)
    client = httpx.Client(transport=transport)
    resp = client.post("http://1c.local/odata/Document.Order")
    assert resp.status_code == 422
    assert "error" in resp.json()
    client.close()


def test_1c_500_transaction_lock():
    def handler(request):
        return httpx.Response(
            500,
            json={
                "error": {
                    "code": "TransactionLocked",
                    "message": "Transaction is locked",
                }
            },
        )

    transport = _make_transport(handler)
    client = httpx.Client(transport=transport)
    resp = client.post("http://1c.local/odata/Document.Order")
    assert resp.status_code == 500
    client.close()


def test_1c_timeout_simulation():
    def handler(request):
        raise httpx.TimeoutException("timeout")

    transport = _make_transport(handler)
    client = httpx.Client(transport=transport)
    with pytest.raises(httpx.TimeoutException):
        client.post("http://1c.local/odata/Document.Order", timeout=0.001)
    client.close()


def test_1c_gateway_error_response():
    def handler(request):
        return httpx.Response(
            503,
            json={"error": "Service Unavailable"},
        )

    transport = _make_transport(handler)
    client = httpx.Client(transport=transport)
    resp = client.post("http://1c.local/odata/Document.Order")
    assert resp.status_code == 503
    client.close()
