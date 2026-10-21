"""Headless validation helpers for ``*.n8n.json`` workflows."""
from __future__ import annotations

import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


def load_workflow(path: Any) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def node_names(workflow: Dict[str, Any]) -> Set[str]:
    return {
        node.get("name")
        for node in workflow.get("nodes", [])
        if isinstance(node, dict) and node.get("name")
    }


def connection_endpoints(workflow: Dict[str, Any]) -> Tuple[Set[str], Set[str]]:
    sources: Set[str] = set()
    targets: Set[str] = set()
    for source, outputs in (workflow.get("connections") or {}).items():
        sources.add(source)
        for branch in (outputs.get("main") or []):
            for connection in (branch or []):
                if isinstance(connection, dict) and connection.get("node"):
                    targets.add(connection["node"])
    return sources, targets


def find_orphans(workflow: Dict[str, Any]) -> List[str]:
    """Names present on only one side of nodes/connections."""
    names = node_names(workflow)
    sources, targets = connection_endpoints(workflow)
    referenced = sources | targets
    return sorted(names ^ referenced)


def _edges(workflow: Dict[str, Any]) -> List[Tuple[str, str]]:
    edges: List[Tuple[str, str]] = []
    for source, outputs in (workflow.get("connections") or {}).items():
        for branch in (outputs.get("main") or []):
            for connection in (branch or []):
                if isinstance(connection, dict) and connection.get("node"):
                    edges.append((source, connection["node"]))
    return edges


def is_acyclic(workflow: Dict[str, Any]) -> bool:
    names = node_names(workflow)
    adjacency: Dict[str, List[str]] = defaultdict(list)
    indegree: Dict[str, int] = {name: 0 for name in names}
    for source, target in _edges(workflow):
        adjacency[source].append(target)
        indegree.setdefault(source, 0)
        indegree[target] = indegree.get(target, 0) + 1
    queue = deque(sorted(name for name, degree in indegree.items() if degree == 0))
    visited = 0
    while queue:
        current = queue.popleft()
        visited += 1
        for neighbour in adjacency.get(current, []):
            indegree[neighbour] -= 1
            if indegree[neighbour] == 0:
                queue.append(neighbour)
    return bool(indegree) and visited == len(indegree)


def http_nodes(workflow: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        node
        for node in workflow.get("nodes", [])
        if isinstance(node, dict) and "http" in str(node.get("type", "")).lower()
    ]


def has_resilience(workflow: Dict[str, Any]) -> bool:
    """True when an HTTP node has an error branch or ``continueOnFail: true``."""
    connections = workflow.get("connections") or {}
    for node in http_nodes(workflow):
        parameters = node.get("parameters") or {}
        if node.get("continueOnFail") is True or parameters.get("continueOnFail") is True:
            return True
        if node.get("onError") in ("continueErrorOutput", "continueRegularOutput"):
            return True
        outputs = (connections.get(node.get("name")) or {}).get("main") or []
        if len(outputs) > 1 and any(outputs[1]):
            return True
    return False