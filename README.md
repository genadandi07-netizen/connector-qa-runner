# Reference Production-Grade API Connector & ACL Adapter

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![Pydantic v2](https://img.shields.io/badge/pydantic-v2-0a7ea4)
![155 Tests Passing](https://img.shields.io/badge/tests-155%20passing-success)
![Docker Isolated](https://img.shields.io/badge/docker-isolated-2496ed)
![Zero-Network QA](https://img.shields.io/badge/QA-zero--network-5319e7)

> **Production-grade reference connector** for B2B API gateways (Tier 2.1–3).
> A three-port, dependency-inverted implementation of the
> *Single Agent + Ports-and-Adapters* pattern: **Ingress → State → Egress**.
> Strict one-way dependencies: `Core/Runner → Adapters → Storage`.

---

## Why this repository exists

Most "connector" codebases hard-wire one downstream API, one auth scheme and one
storage engine into the business logic. Changing the external API then means a
rewrite and a re-test of the whole system.

This repository is the opposite: **one override point per boundary**.
`build_request()` maps an internal event onto the wire, and every cross-cutting
concern — throttling, authentication, retries, idempotency, dead-lettering —
lives *outside* that mapping and is reused by every downstream system you plug in.

The cost of the abstraction is paid once; the benefit is that the next
integration is a subclass, not a migration.

---

## Key capabilities

| Capability | Implementation | Contract |
|---|---|---|
| **Anti-Corruption Layer** | `EgressAdapter.build_request(event) → (url, headers, body)` — the single place where the URL, `Authorization: Bearer` header and JSON body are assembled | Override one method to adapt any external API |
| **Token Bucket Rate Limiting** | `TokenBucketRateLimiter` — proactive throttling *before* the request leaves the process; **2.0 RPS / burst 5** by default | Injectable `clock` / `sleep` for deterministic tests |
| **Single-flight Token Refresh** | OAuth 2.0 / API token rotation on `401`, serialised by an adapter-level `asyncio.Lock()` — N concurrent 401s trigger **exactly one** refresh | No race conditions, no parallel token invalidation |
| **SQLite WAL Idempotency** | `BEGIN IMMEDIATE` + `busy_timeout=5000` + WAL journal mode → exactly one `first_processed` per `event_id` under 30–50 concurrent coroutines | `Idempotency-Key` header set on every outbound call |
| **Dead Letter Queue (DLQ)** | Atomic writes via `tmpfile → fsync → os.replace`; partial files can never survive | Replayable via `replay_dlq()` |
| **HMAC-SHA256 Ingress** | Constant-time signature verification (`hmac.compare_digest`) + 300 s replay window | Malformed payloads rejected with HTTP 422 *before* business logic |
| **Strict Wire Contracts** | Pydantic v2, `extra="forbid"`, single source of truth in `schemas.py` | Contract drift is impossible by construction |

---

## One-command validation

```bash
docker compose run --rm test-runner
```

The container has **no network at all** (`network_mode: "none"`), a read-only
root filesystem and `tmpfs` mounts for `/tmp`, `/dev/shm` and `.pytest_cache`.
Any accidental outbound connection fails at the runtime level, on top of the
in-process `httpx.MockTransport` / `ASGITransport` mocks and the socket guards
inside the suite.

```
155 passed in 6.94s
```

Local run without Docker:

```bash
pip install -r requirements.txt
pytest test_suite -q
```

---

## Test suite — 155 tests

| Module | Tests | Focus |
|---|---:|---|
| `test_suite/test_egress_acl.py` | 19 | `build_request()`, Token Bucket, single-flight token rotation |
| `test_suite/test_resilience_dlq.py` | 26 | Full-jitter backoff, 429/5xx handling, DLQ exhaustion |
| `test_suite/test_storage_wal.py` | 25 | WAL, `BEGIN IMMEDIATE`, idempotency, atomic DLQ writes |
| `test_suite/test_security_hmac.py` | 25 | HMAC-SHA256 verification, replay window, 422 contracts |
| `test_suite/test_amocrm_contracts.py` | 35 | amoCRM REST request/response contracts |
| `test_suite/test_1c_contracts.py` | 25 | 1C order/counterparty contracts |

Machine-readable results: `reports/test_report.json`, `reports/bug_matrix.json`.

---

## Repository layout

```
code/
├── adapters/
│   ├── source_adapter.py     # Ingress port — FastAPI webhook, HMAC-SHA256, replay window
│   ├── storage_adapter.py    # State port — SQLite WAL, dedup, atomic DLQ
│   └── egress_adapter.py     # Egress port — ACL, rate limiting, auth rotation, backoff
├── schemas.py                # Single source of truth for all wire contracts (Pydantic v2)
├── config.py                 # GatewaySettings (BaseSettings) — secrets & endpoints
├── config.json               # Connector manifest
├── test_suite/               # 155 tests + fixtures + runner core
├── reports/                  # test_report.json / bug_matrix.json
├── Dockerfile                # Hermetic python:3.11-slim test image
├── docker-compose.yml        # network_mode: none, read_only: true
├── MAINTENANCE.md            # ← the adaptation playbook
├── ADAPTER_EXTENSION_GUIDE.md
└── ARCHITECTURE_DEBT.md
```

---

## Adaptation in 15 minutes

The full runbook — where to plug the API in, where to put the secrets, how to
extend each adapter, how to run the verification commands — lives in
**[`MAINTENANCE.md`](./MAINTENANCE.md)**.

> **Адаптация под любые изменения внешних API за 15 минут.**
> Follow [`MAINTENANCE.md`](./MAINTENANCE.md) to bring the connector up locally
> and adapt it to any changed external API — secret placement, config layering,
> adapter extension and verification commands, in one linear path.
>
> Per-adapter recipes: [`ADAPTER_EXTENSION_GUIDE.md`](./ADAPTER_EXTENSION_GUIDE.md).

**Configuration in three files** — nothing is hardcoded:

| File | Holds | Read by |
|---|---|---|
| `config.py` | `GatewaySettings` — typed secrets and endpoints | `config_loader.load_config()` |
| `test_suite/config.json` | Non-secret tuning: retries, backoff, circuit breaker, replay window, rate limit | `config_loader.load_config()` |
| `.env` | Real secrets (never committed — see `.env.example`) | `GatewaySettings.model_config` |

`test_suite/config.json` stores only `${VAR}` placeholders; real values always
come from the environment or `.env`.

---

## Security posture

- **No secrets in the repository.** Only `.env.example` is tracked; `.env`,
  `*.db`, `dlq/`, `__pycache__/` and `*.log` are excluded by `.gitignore`.
- **Safe defaults only.** `config.py` ships with `test-secret` / `test-token`
  placeholders that must be overridden before any real deployment.
- **Secret isolation.** `schemas.py` and the adapters never embed credentials —
  they are resolved through `GatewaySettings` and injected at request-build time.
- **Hermetic QA.** The whole suite runs with the network switched off.

---

## License

Provided as a reference implementation. See `agent.md` for the connector manifest
and the full component inventory.
