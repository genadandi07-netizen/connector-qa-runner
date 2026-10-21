"""Shared pytest fixtures and markers for the connector-qa-runner test suite."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent


def pytest_addoption(parser):
    try:
        parser.addoption(
            "--network",
            action="store",
            default="none",
            help="network access policy for the test run (none)",
        )
    except ValueError:
        # Option already registered by another plugin.
        pass


def pytest_configure(config):
    for marker, description in (
        ("network", "network/gateway tests (respx in-process mocks)"),
        ("storage", "storage/idempotency/DLQ tests"),
        ("n8n", "n8n headless workflow validation tests"),
        ("contract", "Pydantic v2 contract tests"),
    ):
        config.addinivalue_line("markers", f"{marker}: {description}")

    # Prefer a tmpfs for the per-test temporary directories. SQLite WAL and DLQ
    # writes fsync aggressively; on this host an ext4-``/tmp`` fsync can take
    # hundreds of milliseconds, whereas ``/dev/shm`` is memory-backed. We only
    # relocate when the caller did not pass ``--basetemp`` and ``/dev/shm`` is
    # actually writable, so behaviour is unchanged on other platforms.
    if not getattr(config.option, "basetemp", None):
        import os

        shm = "/dev/shm"
        if os.path.isdir(shm) and os.access(shm, os.W_OK):
            config.option.basetemp = os.path.join(shm, f"pytest-cqr-{os.getpid()}")


@pytest.fixture(scope="session", autouse=True)
def _forbid_outbound_network():
    """Fail fast if a test attempts a real outbound connection.

    Only the name-resolution / connection primitives are patched; the asyncio
    self-pipe (``socket.socketpair``) and in-process transports keep working.
    """
    import socket
    from unittest import mock

    def _blocked(*_args, **_kwargs):
        raise RuntimeError("outbound network access is forbidden in this test suite")

    with mock.patch.object(socket, "getaddrinfo", _blocked), mock.patch.object(
        socket, "create_connection", _blocked
    ):
        yield


@pytest.fixture
def config():
    with open(HERE / "config.json", "r", encoding="utf-8") as handle:
        return json.load(handle)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "events.db")


@pytest.fixture
def dlq_dir(tmp_path):
    target = tmp_path / "dlq"
    target.mkdir(parents=True, exist_ok=True)
    return str(target)


@pytest.fixture
def webhook_app(tmp_db, config):
    from adapters.source_adapter import create_app
    from adapters.storage_adapter import claim_event, init_storage

    class _Storage:
        def __init__(self, db_path):
            self.conn = init_storage(db_path)
            self.claims = []

        async def claim_event(self, event_id):
            self.claims.append(event_id)
            return await claim_event(event_id, self.conn)

    storage = _Storage(tmp_db)
    app = create_app(storage, config=config)
    app.state.storage = storage
    try:
        yield app
    finally:
        storage.conn.close()


@pytest.fixture
def n8n_fixtures_dir():
    return HERE / "fixtures"