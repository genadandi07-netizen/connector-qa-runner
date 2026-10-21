"""Network/gateway category — AST guard security gate (single test, 5 patterns)."""
from __future__ import annotations

import pytest

from test_suite import ast_guard, qa_runner_core

pytestmark = pytest.mark.network

FORBIDDEN_SNIPPETS = {
    "os_system": "import os\nos.system('echo hi')\n",
    "os_popen": "import os\nos.popen('ls')\n",
    "subprocess_run": "import subprocess\nsubprocess.run(['ls'])\n",
    "subprocess_aliased_import": "import subprocess as sp\nsp.check_output(['ls'])\n",
    "subprocess_from_import": "from subprocess import check_output\ncheck_output(['ls'])\n",
    "socket_socket": "import socket\nsocket.socket()\n",
    "socket_create_connection": "import socket\nsocket.create_connection(('example.com', 80))\n",
    "socket_from_import_alias": "from socket import socket as _sock\n_sock()\n",
}

CLEAN_SNIPPET = "import json\njson.loads('{}')\n"


class _FakeRunner:
    def run(self, allowed_modules):
        return {
            "collected": len(allowed_modules),
            "passed": len(allowed_modules),
            "failed": 0,
            "categories": {},
        }


def test_ast_guard_blocks_forbidden_and_partitions(tmp_path):
    written = {}
    for name, snippet in FORBIDDEN_SNIPPETS.items():
        path = tmp_path / f"test_case_{name}.py"
        path.write_text(snippet, encoding="utf-8")
        written[name] = str(path)
        assert ast_guard.check_module(str(path)), f"{name} must be blocked"
        assert ast_guard.is_blocked(str(path)) is True

    clean_path = tmp_path / "test_case_clean.py"
    clean_path.write_text(CLEAN_SNIPPET, encoding="utf-8")
    assert ast_guard.check_module(str(clean_path)) == []
    assert ast_guard.is_blocked(str(clean_path)) is False

    allowed, blocked = ast_guard.partition_modules([str(clean_path), *written.values()])
    assert len(allowed) == 1
    assert len(blocked) == len(FORBIDDEN_SNIPPETS)

    result = qa_runner_core.run_tests(str(tmp_path), runner=_FakeRunner())
    assert result["blocked"] == len(FORBIDDEN_SNIPPETS)
    assert result["collected"] == 1