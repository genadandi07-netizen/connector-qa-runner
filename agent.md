# connector-qa-runner

Модульный B2B **Test-Runner & QA-Fixer** для API-шлюзов Tier 2.1–3.
Принимает события из n8n Dispatcher, проверяет подпись и replay, хранит
идемпотентное состояние с DLQ в SQLite WAL и прогоняет фиксированный набор
из **155 тестов** против in-process заглушек HubSpot REST с двухпроходной
доработкой и circuit breaker.

- **Pattern:** Single Agent + Ports-and-Adapters (трёхадаптерная, Вариант A)
- **Complexity:** 7/10
- **Delivery kind:** n8n (`n8n_workflow`) · trigger keyword: `коннектор`
- **Pipeline:** Developer → n8n Dispatcher → Connector QA-Runner (Prompt Engineer пропускается)

## Компоненты

| id | тип | роль | модуль |
|---|---|---|---|
| `source_adapter` | module | Ingress: FastAPI-вебхук, HMAC-SHA256, replay 300 с, Pydantic v2 | `adapters/source_adapter.py` |
| `storage_adapter` | module | State/Dedup + DLQ: SQLite WAL, `BEGIN IMMEDIATE`, `busy_timeout=5000`, атомарный `os.replace` | `adapters/storage_adapter.py` |
| `qa_runner_core` | agent | Оркестрация pytest (`--network=none`), AST-фильтр, двухпроходный цикл, изолированный Docker-прогон | `test_suite/qa_runner_core.py` |
| `egress_adapter` | module | Egress: in-process моки HubSpot REST (`httpx.MockTransport`), ACL `build_request()`, Token Bucket (2.0 rps / burst 5), однократная ротация токена при 401, классификация 429/5xx, full-jitter backoff | `adapters/egress_adapter.py` |

## Поток данных

`n8n Dispatcher` → `source_adapter` (HMAC → replay → Pydantic 422) →
`storage_adapter` (дедуп `event_id`, ровно 1 `first_processed`; при исчерпании
ретраев → DLQ атомарным `os.replace`) → `qa_runner_core` (pytest 155 тестов,
`--network=none`, AST-фильтр; проход 1 → отчёт → проход 2 по `reports/bug_matrix.json`,
circuit breaker ≤ 2) → `egress_adapter` (in-process мок HubSpot, Token Bucket перед
каждым запросом, однократная ротация токена на 401, 429/5xx full-jitter) →
`reports/test_report.json`, `reports/bug_matrix.json`.

Обратные связи только от `qa_runner_core` к `storage_adapter` (запись DLQ и
артефактов отчётов). Циклов между компонентами нет.

## Возможности

- **Ingress-защита:** HMAC-SHA256 подпись (`hmac.compare_digest`), replay-окно
  300 с, строгий Pydantic v2-контракт `InboundWebhookPayload` → HTTP 422 до
  бизнес-логики.
- **Идемпотентность:** дедуп по `event_id` в SQLite WAL (`BEGIN IMMEDIATE`,
  `busy_timeout=5000`), ровно один `first_processed`; исчерпание ретраев →
  DLQ атомарным `os.replace`.
- **Встроенная защита от 429 (Token Bucket):** `TokenBucketRateLimiter`
  (`rate_limit_rps=2.0`, `burst=5`) выдаёт permit **до** каждого исходящего
  запроса, поэтому коннектор сам не провоцирует downstream-троттлинг; реактивно
  `429`/`5xx` обрабатываются full-jitter backoff с учётом `Retry-After`.
- **Однократная ротация токена при 401:** `EgressAdapter.refresh_token()` под
  `asyncio.Lock()` — single-flight: параллельные запросы не инвалидируют токен
  одновременно, повторный 401 уходит в DLQ, а не в бесконечный цикл попыток.
- **ACL-маппинг:** `build_request()` — единственное место сборки URL, заголовков
  и тела; подключение внешней системы = один подкласс без правок транспорта.
- **Hermetic-прогон:** AST-фильтр до исполнения, in-process моки и
  Docker-песочница без сети — 155 тестов без единого реального сетевого вызова.

## Навыки

- **SQLite Database Expert** (`storage_adapter`) — WAL, PRAGMA, миграции,
  диагностика блокировок. `security_status: safe`.

## Ограничения

- Запись только в `test_suite/`, `reports/`, `ADAPTER_EXTENSION_GUIDE.md`,
  `MAINTENANCE.md`, `agent.md` и корневом `config.json` (манифест агента);
  `adapters/`, `schemas.py`, `config.py` — только чтение.
- Запуск только системным pytest с `--network=none`.
- Тестовый контур исполняется в Docker-песочнице (`Dockerfile` +
  `docker-compose.yml`): `network_mode: "none"` — ни одного исходящего запроса
  на уровне рантайма; `read_only: true` — файловая система контейнера неизменяема;
  tmpfs для `/tmp`, `/dev/shm`, `/app/.pytest_cache`. Внутри —
  `CMD ["pytest", "test_suite", "-q", "--network=none"]`.
- Pre-execution AST-фильтр блокирует `os.system`, `subprocess.*`, `socket.socket`,
  `socket.create_connection` до исполнения тестового модуля.
- Тестовый набор — 155 тестов в 6 модулях (факт: amoCRM-контракты 35 /
  resilience-DLQ 26 / storage-WAL 25 / security-HMAC 25 / контракты 1С 25 /
  egress-ACL 19 = 155).
- Только in-process моки (`httpx.MockTransport` / `httpx.ASGITransport`); реальных
  сетевых вызовов нет. `respx` в сьюте не используется и в `requirements.txt` не значится.

## Расширение

Все три адаптера расширяются за 1 шаг — см. `ADAPTER_EXTENSION_GUIDE.md`:
`egress_adapter` — подклассом (переопределяется только `build_request()`),
`source_adapter` / `storage_adapter` — реализацией того же порта (у ingress это
`async claim_event(event_id)`, у хранилища — 5 операций `init/claim/read/write_dlq/replay`).
Обязательный smoke-тест контракта защищает от Incomplete CRUD.
Точка расширения egress — `EgressAdapter.build_request()`; троттлинг
(`TokenBucketRateLimiter`) и ротация токена под `asyncio.Lock()` наследуются
автоматически и не дублируются в подклассе.

## Тестовый контур

Фактический состав прогона (`pytest test_suite -q --network=none`):

| Модуль | Тестов | Что проверяет |
|---|---|---|
| `test_suite/test_amocrm_contracts.py` | 35 | строгие контракты amoCRM (`extra="forbid"`) |
| `test_suite/test_resilience_dlq.py` | 26 | ретраи, `Retry-After`, таймауты, атомарность DLQ, n8n fallback |
| `test_suite/test_storage_wal.py` | 25 | WAL/PRAGMA, конкурентные claim-ы, дедуп, crash-recovery |
| `test_suite/test_security_hmac.py` | 25 | HMAC-SHA256, replay-окно, коды 401/422 |
| `test_suite/test_1c_contracts.py` | 25 | строгие контракты 1С |
| `test_suite/test_egress_acl.py` | 19 | `build_request()`, Token Bucket, single-flight ротация на 401 |
| **Итого** | **155** | **155 passed** |

Маркеры категорий `network` / `storage` / `n8n` / `contract` зарегистрированы
в `test_suite/conftest.py` и используются оркестратором `qa_runner_core`
(`CATEGORIES`) для отчётов; фактическая группировка сьюта — по модулям выше.

## Примеры задач

- Прогнать сьют локально: `pytest test_suite -q --network=none` → `155 passed`.
- Прогнать сьют в изолированной Docker-песочнице: `docker compose run --rm test-runner`
  → `155 passed` (вариант с пересборкой — `docker compose up --build --abort-on-container-exit`).
- Прочитать результат из `reports/test_report.json` (0 отказов + заполненный
  `bug_matrix.json` — единственный сигнал завершения). Отчёты пишет оркестратор
  `qa_runner_core.refine_loop()`, а не сам `pytest`: прямой запуск тестов их не
  обновляет. Перегенерация одной командой — `python3 -m test_suite.qa_runner_core`
  (из `code/`); в репозитории лежит актуальный прогон: `collected: 155`,
  `passed: 155`, `failed: 0`, `blocked: 0`.
- Достроить второй проход по доказательствам из `reports/bug_matrix.json`.
