"""qa_runner_core — pytest orchestration, AST guard and two-pass refinement.

Every test module is screened by ``ast_guard`` before execution. Completion is
signalled only by ``reports/test_report.json`` with zero failures plus a populated
``reports/bug_matrix.json``; the refinement loop is capped by the circuit breaker
(``circuit_breaker.max_passes`` == 2).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, runtime_checkable

from .ast_guard import AST_GUARD_BLOCKED, collect_blocked_modules, partition_modules
from .config_loader import load_config

CATEGORIES = ("network", "storage", "n8n", "contract")


@runtime_checkable
class StoragePort(Protocol):
    """Dependency-inversion seam for storage.

    ``qa_runner_core`` depends only on this abstraction, never on the concrete
    ``adapters.storage_adapter`` module. The adapter implements the port
    structurally, so the dependency direction stays
    ``Core/Runner -> Adapters -> Storage`` and ``storage_adapter`` never imports
    ``qa_runner_core``.
    """

    async def claim_event(self, event_id: str) -> Dict[str, Any]:
        ...


def discover_test_modules(test_dir: str) -> List[str]:
    """Recursively discover ``test_*.py`` modules below ``test_dir``."""
    return sorted(str(path) for path in Path(test_dir).rglob("test_*.py"))


class SystemPytestRunner:
    """Runs the allowed modules with the system pytest under ``--network=none``."""

    def run(self, allowed_modules: List[str]) -> Dict[str, Any]:
        import pytest

        stats: Dict[str, Any] = {
            "collected": 0,
            "passed": 0,
            "failed": 0,
            "categories": {category: 0 for category in CATEGORIES},
        }

        class _StatsPlugin:
            def pytest_collection_finish(self, session):
                stats["collected"] = len(session.items)
                for item in session.items:
                    for category in CATEGORIES:
                        if item.get_closest_marker(category):
                            stats["categories"][category] += 1

            def pytest_runtest_logreport(self, report):
                if report.when == "call":
                    if report.passed:
                        stats["passed"] += 1
                    elif report.failed:
                        stats["failed"] += 1
                elif report.when == "setup" and report.failed:
                    stats["failed"] += 1

        args = ["-q", "--network=none", *allowed_modules]
        pytest.main(args, plugins=[_StatsPlugin()])
        return stats


def run_tests(
    test_dir: str,
    reports_dir: Optional[str] = None,
    runner: Optional[Any] = None,
) -> Dict[str, Any]:
    """Screen modules with the AST guard, then run the allowed ones only."""
    modules = discover_test_modules(test_dir)
    allowed, blocked = partition_modules(modules)
    runner = runner or SystemPytestRunner()
    outcome = runner.run(allowed)
    return {
        "collected": outcome.get("collected", 0),
        "passed": outcome.get("passed", 0),
        "failed": outcome.get("failed", 0),
        "blocked": len(blocked),
        "blocked_modules": blocked,
        "categories": outcome.get("categories", {category: 0 for category in CATEGORIES}),
    }


def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def write_reports(
    result: Dict[str, Any],
    reports_dir: str,
    pass_no: int = 1,
    circuit_breaker: str = "closed",
) -> Dict[str, Any]:
    """Emit ``test_report.json`` and ``bug_matrix.json`` (only in reports/)."""
    reports = Path(reports_dir)
    reports.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()

    test_report = {
        "collected": result.get("collected", 0),
        "passed": result.get("passed", 0),
        "failed": result.get("failed", 0),
        "blocked": result.get("blocked", 0),
        "categories": result.get("categories", {category: 0 for category in CATEGORIES}),
        "finished_at": now,
    }
    bug_matrix = {
        "pass": pass_no,
        "failures": result.get("failures", []),
        "blocked_modules": result.get("blocked_modules", []),
        "circuit_breaker": {"state": circuit_breaker},
        "updated_at": now,
    }
    _atomic_write_json(reports / "test_report.json", test_report)
    _atomic_write_json(reports / "bug_matrix.json", bug_matrix)
    return {"test_report": test_report, "bug_matrix": bug_matrix}


def _read_bug_matrix(reports_dir: str) -> Dict[str, Any]:
    path = Path(reports_dir) / "bug_matrix.json"
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def refine_loop(
    test_dir: str,
    reports_dir: str,
    max_passes: Optional[int] = None,
    runner: Optional[Any] = None,
    fixer: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """Two-pass refinement with a circuit breaker capped at ``max_passes``."""
    cfg = load_config()
    if max_passes is None:
        max_passes = int(cfg["circuit_breaker"]["max_passes"])

    passes: List[Dict[str, Any]] = []
    state = "closed"

    for attempt in range(1, max_passes + 1):
        result = run_tests(test_dir, reports_dir, runner=runner)
        result["failures"] = result.get("failures", [])
        passes.append(result)
        write_reports(result, reports_dir, pass_no=attempt, circuit_breaker="closed")

        if result["failed"] == 0:
            state = "closed"
            break

        if attempt < max_passes:
            if fixer is not None:
                fixer(result)
        else:
            state = "open"
            write_reports(result, reports_dir, pass_no=attempt, circuit_breaker="open")

    return {
        "passes": len(passes),
        "circuit_breaker": state,
        "bug_matrix": _read_bug_matrix(reports_dir),
        "results": passes,
    }


def _main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point: ``python -m test_suite.qa_runner_core``.

    Runs the two-pass refinement loop over the repository's test suite and
    refreshes ``reports/test_report.json`` + ``reports/bug_matrix.json``.
    Exit code 0 means "no failures" (the completion signal of the runner).
    """
    import argparse

    package_root = Path(__file__).resolve().parent
    repo_root = package_root.parent

    parser = argparse.ArgumentParser(
        prog="test_suite.qa_runner_core",
        description="Run the connector-qa-runner suite and refresh the reports.",
    )
    parser.add_argument(
        "--test-dir",
        default=str(repo_root),
        help="directory scanned for test_*.py modules (default: repository root)",
    )
    parser.add_argument(
        "--reports-dir",
        default=str(repo_root / "reports"),
        help="directory for test_report.json / bug_matrix.json (default: <repo>/reports)",
    )
    args = parser.parse_args(argv)

    outcome = refine_loop(args.test_dir, args.reports_dir)
    last = outcome["results"][-1] if outcome["results"] else {}
    print(
        "collected={collected} passed={passed} failed={failed} blocked={blocked} "
        "passes={passes} circuit_breaker={cb}".format(
            collected=last.get("collected", 0),
            passed=last.get("passed", 0),
            failed=last.get("failed", 0),
            blocked=last.get("blocked", 0),
            passes=outcome.get("passes", 0),
            cb=outcome.get("circuit_breaker", "unknown"),
        )
    )
    return 0 if last.get("failed", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(_main())


__all__ = [
    "CATEGORIES",
    "AST_GUARD_BLOCKED",
    "StoragePort",
    "collect_blocked_modules",
    "partition_modules",
    "discover_test_modules",
    "SystemPytestRunner",
    "run_tests",
    "write_reports",
    "refine_loop",
]