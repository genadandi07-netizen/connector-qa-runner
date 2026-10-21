"""Execution graph for connector-qa-runner.

Models the four-component Ports-and-Adapters pipeline as a DAG. Pure standard
library so that it is importable in any environment (no FastAPI/httpx needed).
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class Node:
    id: str
    role: str
    module: str


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    label: str


@dataclass
class Graph:
    nodes: List[Node] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)

    def add_node(self, node: Node) -> None:
        if any(existing.id == node.id for existing in self.nodes):
            raise ValueError(f"duplicate node id: {node.id}")
        self.nodes.append(node)

    def add_edge(self, edge: Edge) -> None:
        ids = {node.id for node in self.nodes}
        if edge.source not in ids or edge.target not in ids:
            raise ValueError(f"edge references unknown node: {edge}")
        self.edges.append(edge)

    def node_ids(self) -> List[str]:
        return [node.id for node in self.nodes]

    def _indegree(self) -> Dict[str, int]:
        indegree = {node.id: 0 for node in self.nodes}
        for edge in self.edges:
            indegree[edge.target] += 1
        return indegree

    def topological_order(self) -> List[str]:
        indegree = self._indegree()
        adjacency: Dict[str, List[str]] = {node.id: [] for node in self.nodes}
        for edge in self.edges:
            adjacency[edge.source].append(edge.target)
        queue = deque(sorted(node for node, degree in indegree.items() if degree == 0))
        order: List[str] = []
        while queue:
            current = queue.popleft()
            order.append(current)
            for neighbour in adjacency[current]:
                indegree[neighbour] -= 1
                if indegree[neighbour] == 0:
                    queue.append(neighbour)
        if len(order) != len(self.nodes):
            raise ValueError("graph contains a cycle")
        return order

    def is_acyclic(self) -> bool:
        try:
            self.topological_order()
        except ValueError:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "nodes": [node.__dict__ for node in self.nodes],
            "edges": [edge.__dict__ for edge in self.edges],
            "acyclic": self.is_acyclic(),
        }


def build_graph() -> Graph:
    """Build the dependency graph with a strictly one-directional flow.

    Dependency inversion removed the former ``qa_runner_core <-> storage_adapter``
    cycle: the runner now depends on the abstract ``StoragePort`` interface while
    the concrete ``storage_adapter`` implements it. Neither side imports the
    other, so the graph is acyclic (Core/Runner -> Adapters -> Storage).
    """
    graph = Graph()
    graph.add_node(Node("dispatcher", "n8n Dispatcher", "n8n"))
    graph.add_node(Node("source_adapter", "Ingress", "adapters/source_adapter.py"))
    graph.add_node(Node("storage_adapter", "State/Dedup + DLQ", "adapters/storage_adapter.py"))
    graph.add_node(Node("storage_port", "StoragePort (DIP interface)", "test_suite/qa_runner_core.py"))
    graph.add_node(Node("qa_runner_core", "QA-Runner + QA-Fixer", "test_suite/qa_runner_core.py"))
    graph.add_node(Node("egress_adapter", "Egress", "adapters/egress_adapter.py"))
    graph.add_node(Node("reports", "test_report.json / bug_matrix.json", "reports/"))
    graph.add_edge(Edge("dispatcher", "source_adapter", "HTTP POST /webhook"))
    graph.add_edge(Edge("source_adapter", "storage_adapter", "ValidatedEvent"))
    graph.add_edge(Edge("storage_adapter", "storage_port", "implements"))
    graph.add_edge(Edge("qa_runner_core", "storage_port", "depends on abstraction (DIP)"))
    graph.add_edge(Edge("qa_runner_core", "egress_adapter", "EgressRequest"))
    graph.add_edge(Edge("egress_adapter", "reports", "EgressResult"))
    graph.add_edge(Edge("qa_runner_core", "reports", "test_report.json"))
    return graph


GRAPH = build_graph()


if __name__ == "__main__":
    import json

    print(json.dumps(GRAPH.to_dict(), ensure_ascii=False, indent=2))