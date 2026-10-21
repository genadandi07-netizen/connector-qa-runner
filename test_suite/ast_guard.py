"""Pre-execution AST guard.

Scans Python sources and reports calls to shell/process/raw-socket primitives
BEFORE the module is imported or executed. Alias-aware: ``import subprocess as
sp``, ``from subprocess import check_output`` and
``from socket import socket as _sock`` are all resolved to their canonical
dotted names.

Forbidden surface (acceptance criterion A-8):
    * os.system / os.popen
    * subprocess.* (any attribute)
    * socket.socket / socket.create_connection
"""
from __future__ import annotations

import ast
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

AST_GUARD_BLOCKED = "AST_GUARD_BLOCKED"

FORBIDDEN_CALLS = frozenset(
    {
        "os.system",
        "os.popen",
        "socket.socket",
        "socket.create_connection",
    }
)
FORBIDDEN_PREFIXES = ("subprocess.",)


def _dotted_name(node: ast.AST) -> str | None:
    """Return the dotted name for a Name/Attribute chain, else None."""
    parts: List[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    else:
        return None
    return ".".join(reversed(parts))


def _build_aliases(tree: ast.AST) -> Dict[str, str]:
    """Map local names to fully qualified targets introduced by imports."""
    aliases: Dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    aliases[alias.asname] = alias.name
                else:
                    root = alias.name.split(".")[0]
                    aliases[root] = root
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                if alias.name == "*":
                    continue
                full = f"{module}.{alias.name}" if module else alias.name
                local = alias.asname or alias.name
                aliases[local] = full
    return aliases


def _resolve(func: ast.AST, aliases: Dict[str, str]) -> str | None:
    """Resolve a call target to a canonical dotted name using the alias map."""
    dotted = _dotted_name(func)
    if dotted is None:
        return None
    parts = dotted.split(".")
    root = parts[0]
    if root in aliases:
        parts = aliases[root].split(".") + parts[1:]
    return ".".join(parts)


def is_forbidden(dotted_name: str | None) -> bool:
    """Return True if the canonical dotted call name is on the forbidden list."""
    if not dotted_name:
        return False
    if dotted_name in FORBIDDEN_CALLS:
        return True
    return dotted_name.startswith(FORBIDDEN_PREFIXES)


def _violations_from_node(tree: ast.AST, aliases: Dict[str, str]) -> List[str]:
    violations: List[str] = []
    for sub in ast.walk(tree):
        if isinstance(sub, ast.Call):
            resolved = _resolve(sub.func, aliases)
            if is_forbidden(resolved):
                violations.append(resolved)
    return violations


def check_source(source: str, filename: str = "<string>") -> List[str]:
    """Return the list of forbidden calls found in a source string."""
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError:
        return []
    aliases = _build_aliases(tree)
    return _violations_from_node(tree, aliases)


def check_module(path: str) -> List[str]:
    """Return the list of forbidden calls found in a module file."""
    with open(path, "r", encoding="utf-8") as handle:
        source = handle.read()
    return check_source(source, filename=str(path))


def is_blocked(path: str) -> bool:
    """True when the module contains at least one forbidden call."""
    return bool(check_module(path))


def iter_module_paths(target: str | Iterable[str]) -> List[str]:
    """Yield candidate test modules for a directory (recursive) or explicit list."""
    if isinstance(target, (list, tuple, set, frozenset)):
        return sorted(str(item) for item in target)
    base = Path(target)
    if base.is_dir():
        return sorted(str(path) for path in base.rglob("test_*.py"))
    return [str(base)]


def partition_modules(module_paths: Iterable[str]) -> Tuple[List[str], List[str]]:
    """Split modules into ``(allowed, blocked)`` by AST-guard verdict."""
    allowed: List[str] = []
    blocked: List[str] = []
    for path in module_paths:
        (blocked if is_blocked(path) else allowed).append(path)
    return allowed, blocked


def collect_blocked_modules(target: str | Iterable[str]) -> List[str]:
    """Recursively collect blocked modules from a directory or explicit list."""
    return [path for path in iter_module_paths(target) if is_blocked(path)]


def guard_message(path: str, violations: List[str] | None = None) -> str:
    """Human-readable blocking message for reports and pytest failures."""
    if violations:
        return f"{AST_GUARD_BLOCKED}: {path} -> {', '.join(violations)}"
    return f"{AST_GUARD_BLOCKED}: {path}"