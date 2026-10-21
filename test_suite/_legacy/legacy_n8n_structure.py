"""n8n headless category — nodes/connections, orphans and DAG cycles (3 tests)."""
from __future__ import annotations

from pathlib import Path

import pytest

from test_suite import n8n_validator as validator

pytestmark = pytest.mark.n8n

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_workflow_has_nodes_and_connections():
    workflow = validator.load_workflow(FIXTURES / "valid_workflow.n8n.json")
    assert isinstance(workflow.get("nodes"), list) and workflow["nodes"]
    assert isinstance(workflow.get("connections"), dict) and workflow["connections"]


def test_orphan_nodes_are_detected():
    valid = validator.load_workflow(FIXTURES / "valid_workflow.n8n.json")
    assert validator.find_orphans(valid) == []
    broken = validator.load_workflow(FIXTURES / "orphan_workflow.n8n.json")
    assert validator.find_orphans(broken) != []


def test_dag_cycles_are_detected():
    valid = validator.load_workflow(FIXTURES / "valid_workflow.n8n.json")
    assert validator.is_acyclic(valid) is True
    cyclic = validator.load_workflow(FIXTURES / "cyclic_workflow.n8n.json")
    assert validator.is_acyclic(cyclic) is False