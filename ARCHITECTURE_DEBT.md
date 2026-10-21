# ARCHITECTURE & DEPENDENCY DEBT

Test baseline: `pytest test_suite -q --network=none` → **155 passed**
(155/155). Legacy baselines (23 / 111 / 136) are superseded — do not cite them.

## Core-Adapter Decoupling (Priority: High) — RESOLVED
- **Component**: `qa_runner_core` / `code/graph.py`
- **Observed State**: `acyclic=False` caused by back-edge `qa_runner_core -> storage_adapter`.
- **Required Refactoring**: Introduce dependency inversion interface (DIP) in `qa_runner_core` to consume storage abstractions without direct back-referencing concrete `storage_adapter`.
- **Current Status**: RESOLVED. `StoragePort` (DIP protocol) added to `qa_runner_core`; the concrete `storage_adapter` implements it and never imports the runner. The direct `qa_runner_core <-> storage_adapter` edges were replaced by `qa_runner_core -> storage_port` and `storage_adapter -> storage_port`, making `code/graph.py` acyclic (`acyclic=True`). Secrets moved from config to `code/config.py` (`GatewaySettings`) + `.env`; inbound/amoCRM contracts consolidated in `code/schemas.py` (Pydantic v2). Regression: 155/155 tests pass.

## Missing explicit ACL / `build_request` (Priority: Medium) — RESOLVED
- **Component**: `egress_adapter` / `code/adapters/egress_adapter.py`
- **Observed State**: the downstream request (URL, headers, body) was assembled inside the transport path, so adapting a new external system required touching retry, backoff and DLQ logic. There was no single, overridable mapping point.
- **Required Refactoring**: introduce a pure Anti-Corruption Layer method `build_request(self, event: Dict[str, Any]) -> Tuple[str, Dict[str, str], Any]` as the only place where the wire request is formed; transport, retries, backoff and DLQ must stay untouched.
- **Current Status**: RESOLVED. `EgressAdapter.build_request()` is implemented (`code/adapters/egress_adapter.py`, lines 197–215) and is the documented extension point: a subclass overrides only the mapping, while `send()` keeps calling it on every attempt (so a token rotated after a 401 is picked up automatically). The base implementation builds the URL from `config["egress"]["url"]`, the `Idempotency-Key` header and the `Authorization: Bearer <token>` header. Regression: `code/test_suite/test_egress_acl.py` (19 tests) pins the ACL contract, incl. `test_build_request_is_the_acl_seam_for_subclasses`.

## Duplicated `WebhookPayload` model (Priority: Medium) — RESOLVED
- **Component**: `source_adapter` / `code/adapters/source_adapter.py` ↔ `code/schemas.py`
- **Observed State**: `source_adapter` declared its own Pydantic `WebhookPayload` model, duplicating the catalogue in `schemas.py` — the ingress shape could silently drift from the contract registry.
- **Required Refactoring**: declare the ingress model exactly once in `schemas.py` and import it into the adapter; forbid adapter-local `BaseModel` declarations.
- **Current Status**: RESOLVED. `InboundWebhookPayload` is declared once in `code/schemas.py` and imported by `code/adapters/source_adapter.py`; the backward-compatible alias `WebhookPayload = InboundWebhookPayload` is kept, the duplicate class is gone. The generic envelope `schemas.WebhookPayload` remains a separate, strictly distinct contract (`extra="forbid"` on every model). Regression: `test_source_adapter_imports_webhook_payload_from_schemas`, `test_generic_and_ingress_webhook_payloads_are_distinct_contracts`, `test_inbound_webhook_payload_contract_is_strict`.

## No proactive rate limiting (Priority: Medium) — RESOLVED
- **Component**: `egress_adapter` / `code/adapters/egress_adapter.py`
- **Observed State**: only reactive handling of `429` (full-jitter backoff) existed; the connector could burst into the downstream API and trigger throttling limits of its own making.
- **Required Refactoring**: add a proactive token bucket that grants a permit *before* every outbound request, with the rate and burst size taken from configuration rather than hard-coded.
- **Current Status**: RESOLVED. `TokenBucketRateLimiter` (defaults `rate_limit_rps=2.0`, `burst=5`; bound to `rate_limit` in `code/test_suite/config.json` and to `GatewaySettings`) is constructed by `EgressAdapter.__init__` and `send()` awaits `rate_limiter.acquire()` before every `client.post`. `clock`/`sleep` are injectable, so throttling is verified deterministically without wall-clock delays. Regression: 7 tests under `-k "token_bucket or rate_limit"`, plus `test_send_waits_on_rate_limiter_before_every_post`.

## No containerised test run (Priority: Low) — RESOLVED
- **Component**: test contour / `code/Dockerfile`, `code/docker-compose.yml`
- **Observed State**: the suite could only be executed on the host; isolation relied solely on in-process transport mocks and the socket guards inside `conftest.py`.
- **Required Refactoring**: add a hermetic image plus a compose service with `network_mode: "none"` and `read_only: true`, so any accidental outbound connection fails at the runtime level (belt) on top of the in-process mocks (braces).
- **Current Status**: RESOLVED. `code/Dockerfile` (`CMD ["pytest", "test_suite", "-q", "--network=none"]`) and `code/docker-compose.yml` (service `test-runner`, `network_mode: "none"`, `read_only: true`, tmpfs for `/tmp`, `/dev/shm`, `/app/.pytest_cache`). Verification: `docker compose run --rm test-runner` → 155 passed; rebuild variant `docker compose up --build --abort-on-container-exit` → 155 passed.
