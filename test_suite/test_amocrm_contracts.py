"""Contract category — amoCRM / webhook schemas and sanitisation (35 tests).

Validates the strict Pydantic v2 contracts in :mod:`schemas` together with the
reference projection/sanitisation helpers: required fields, ``extra="forbid"``,
phone/email normalisation, XSS/SQL-injection safety, the amoCRM REST v4 payload
shape and validation-error normalisation into the BugMatrix format.
"""
from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from adapters.storage_adapter import init_storage
from schemas import (
    AmoContactCreateSchema,
    AmoLeadCreateSchema,
    WebhookPayload,
)
from test_suite.gateway_support import (
    build_bug_matrix,
    contact_to_amocrm,
    lead_to_amocrm,
    normalize_phone,
    normalize_validation_error,
    sanitize_text,
    to_camel_case,
    validate_e164,
    validate_email,
)


# --------------------------------------------------------------------------- #
# WebhookPayload (6)
# --------------------------------------------------------------------------- #
def test_webhook_payload_valid():
    model = WebhookPayload.model_validate(
        {"event_id": "e1", "source": "n8n", "timestamp": 1_700_000_000_000, "data": {"k": 1}}
    )
    assert model.event_id == "e1"
    assert model.timestamp == 1_700_000_000_000
    assert model.data == {"k": 1}


def test_webhook_payload_missing_required_field():
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate({"source": "n8n", "timestamp": 1})


def test_webhook_payload_wrong_type_rejected():
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate(
            {"event_id": "e1", "source": "n8n", "timestamp": "not-an-int"}
        )


def test_webhook_payload_forbids_extra_fields():
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate(
            {"event_id": "e1", "source": "n8n", "timestamp": 1, "extra": True}
        )


def test_webhook_payload_nested_data_is_preserved():
    model = WebhookPayload.model_validate(
        {
            "event_id": "e1",
            "source": "n8n",
            "timestamp": 1,
            "data": {"outer": {"inner": [1, 2, 3]}},
        }
    )
    assert model.data["outer"]["inner"] == [1, 2, 3]


def test_webhook_payload_data_defaults_to_empty_dict():
    model = WebhookPayload.model_validate(
        {"event_id": "e1", "source": "n8n", "timestamp": 0}
    )
    assert model.data == {}


# --------------------------------------------------------------------------- #
# AmoLeadCreateSchema (7)
# --------------------------------------------------------------------------- #
def test_lead_valid_contract():
    model = AmoLeadCreateSchema.model_validate({"name": "Deal", "price": 100})
    assert model.name == "Deal"
    assert model.price == 100
    assert model.custom_fields_values == []


def test_lead_empty_name_rejected():
    with pytest.raises(ValidationError):
        AmoLeadCreateSchema.model_validate({"name": "", "price": 1})


def test_lead_negative_price_rejected():
    with pytest.raises(ValidationError):
        AmoLeadCreateSchema.model_validate({"name": "Deal", "price": -1})


def test_lead_zero_price_allowed():
    assert AmoLeadCreateSchema.model_validate({"name": "Deal", "price": 0}).price == 0


def test_lead_custom_fields_values_accepted():
    custom = [{"field_id": 123, "values": [{"value": "x"}]}]
    model = AmoLeadCreateSchema.model_validate(
        {"name": "Deal", "price": 1, "custom_fields_values": custom}
    )
    assert model.custom_fields_values == custom


def test_lead_forbids_extra_fields():
    with pytest.raises(ValidationError):
        AmoLeadCreateSchema.model_validate(
            {"name": "Deal", "price": 1, "pipeline_id": 9}
        )


def test_lead_missing_price_rejected():
    with pytest.raises(ValidationError):
        AmoLeadCreateSchema.model_validate({"name": "Deal"})


# --------------------------------------------------------------------------- #
# AmoContactCreateSchema (6)
# --------------------------------------------------------------------------- #
def test_contact_valid_contract():
    model = AmoContactCreateSchema.model_validate(
        {"name": "John", "phones": ["+79991234567"], "emails": ["john@example.com"]}
    )
    assert model.phones == ["+79991234567"]
    assert model.emails == ["john@example.com"]


def test_contact_e164_phone_validated():
    assert validate_e164("+79991234567") is True
    assert validate_e164("79991234567") is False


def test_contact_phone_mask_is_normalized():
    assert normalize_phone("+7 (999) 123-45-67") == "+79991234567"


def test_contact_invalid_email_rejected():
    assert validate_email("not-an-email") is False
    assert validate_email("john@example.com") is True


def test_contact_multichannel_contact():
    model = AmoContactCreateSchema.model_validate(
        {
            "name": "John",
            "phones": ["+79991234567", "+79997654321"],
            "emails": ["a@b.com", "c@d.com"],
        }
    )
    assert len(model.phones) == 2
    assert len(model.emails) == 2


def test_contact_defaults_to_empty_channels():
    model = AmoContactCreateSchema.model_validate({"name": "John"})
    assert model.phones == []
    assert model.emails == []


# --------------------------------------------------------------------------- #
# Sanitisation (6)
# --------------------------------------------------------------------------- #
def test_sanitize_escapes_script_tags():
    assert sanitize_text("<script>alert(1)</script>") == (
        "&lt;script&gt;alert(1)&lt;/script&gt;"
    )


def test_sanitize_neutralizes_attribute_xss():
    result = sanitize_text('"><img src=x onerror=alert(1)>')
    assert "<" not in result
    assert ">" not in result
    assert "&quot;" in result


def test_sql_injection_input_is_inert_when_parameterized(tmp_path):
    conn = init_storage(str(tmp_path / "events.db"))
    try:
        malicious = "'; DROP TABLE events; --"
        conn.execute(
            "INSERT INTO events (event_id,status,first_processed,created_at) "
            "VALUES (?,?,1,'t')",
            (malicious, "s"),
        )
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
        ).fetchone()
    finally:
        conn.close()
    assert table is not None


def test_special_characters_are_escaped():
    assert sanitize_text("a & b < c > d \" e ' f") == (
        "a &amp; b &lt; c &gt; d &quot; e &#x27; f"
    )


def test_unicode_text_is_preserved():
    assert sanitize_text("Привет, мир — Ω") == "Привет, мир — Ω"


def test_emoji_text_is_preserved():
    text = "lead 🚀🔥 done"
    assert sanitize_text(text) == text


# --------------------------------------------------------------------------- #
# Serialization / deserialization (5)
# --------------------------------------------------------------------------- #
def test_lead_serializes_to_amocrm_payload():
    model = AmoLeadCreateSchema.model_validate(
        {
            "name": "Deal",
            "price": 5000,
            "custom_fields_values": [{"field_id": 1, "values": [{"value": "v"}]}],
        }
    )
    payload = lead_to_amocrm(model.model_dump())
    assert payload["name"] == "Deal"
    assert payload["price"] == 5000
    assert payload["custom_fields_values"][0]["field_id"] == 1


def test_contact_serializes_to_amocrm_payload():
    model = AmoContactCreateSchema.model_validate(
        {"name": "John", "phones": ["+79991234567"], "emails": ["john@example.com"]}
    )
    payload = contact_to_amocrm(model.model_dump())
    codes = {item["field_code"] for item in payload["custom_fields_values"]}
    assert codes == {"PHONE", "EMAIL"}


def test_amocrm_payload_roundtrips_through_json():
    model = AmoLeadCreateSchema.model_validate({"name": "Deal", "price": 1})
    raw = json.dumps(lead_to_amocrm(model.model_dump()))
    assert json.loads(raw)["name"] == "Deal"


def test_snake_case_to_camel_case_conversion():
    assert to_camel_case("custom_fields_values") == "customFieldsValues"
    assert to_camel_case("event_id") == "eventId"


def test_json_serialization_keeps_unicode():
    payload = {"name": "Сделка 🚀"}
    raw = json.dumps(payload, ensure_ascii=False)
    assert "Сделка" in raw
    assert json.loads(raw) == payload


# --------------------------------------------------------------------------- #
# Validation error handling (5)
# --------------------------------------------------------------------------- #
def test_invalid_lead_produces_normalized_bug():
    with pytest.raises(ValidationError) as excinfo:
        AmoLeadCreateSchema.model_validate({"name": "", "price": -5})
    bug = normalize_validation_error(excinfo.value)
    assert bug["status"] == "invalid"
    assert bug["error_count"] >= 2
    assert all("field" in entry and "message" in entry for entry in bug["errors"])


def test_invalid_contact_produces_field_level_bug():
    with pytest.raises(ValidationError) as excinfo:
        AmoContactCreateSchema.model_validate({"name": "John", "extra": 1})
    bug = normalize_validation_error(excinfo.value)
    assert any(entry["field"] == "extra" for entry in bug["errors"])


def test_bug_matrix_aggregates_multiple_failures():
    with pytest.raises(ValidationError) as lead_exc:
        AmoLeadCreateSchema.model_validate({"name": "", "price": -1})
    with pytest.raises(ValidationError) as contact_exc:
        AmoContactCreateSchema.model_validate({"name": ""})
    matrix = build_bug_matrix(
        [
            ("lead", normalize_validation_error(lead_exc.value)),
            ("contact", normalize_validation_error(contact_exc.value)),
        ]
    )
    assert len(matrix["failures"]) == 2
    assert matrix["failures"][0]["source"] == "lead"


def test_bug_matrix_is_json_serializable():
    with pytest.raises(ValidationError) as excinfo:
        AmoLeadCreateSchema.model_validate({"name": "", "price": -1})
    matrix = build_bug_matrix(
        [("lead", normalize_validation_error(excinfo.value))]
    )
    assert json.loads(json.dumps(matrix))["schema_version"] == 1


def test_valid_contract_yields_no_bug_entries():
    model = AmoLeadCreateSchema.model_validate({"name": "Deal", "price": 1})
    matrix = build_bug_matrix([])
    assert model.name == "Deal"
    assert matrix["failures"] == []
