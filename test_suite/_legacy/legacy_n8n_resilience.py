"""n8n headless category — HTTP node resilience (2 tests)."""
from __future__ import annotations

from pathlib import Path

import pytest

from test_suite import n8n_validator as validator

pytestmark = pytest.mark.n8n

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_valid_workflow_is_resilient():
    workflow = validator.load_workflow(FIXTURES / "valid_workflow.n8n.json")
    assert validator.has_resilience(workflow) is True


def test_missing_resilience_is_detected():
    workflow = validator.load_workflow(FIXTURES / "no_resilience_workflow.n8n.json")
    assert validator.has_resilience(workflow) is False