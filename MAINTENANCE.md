# MAINTENANCE — connector-qa-runner

Практическое руководство для разработчика, который впервые открыл репозиторий.
Цель — за **15 минут** поднять коннектор локально и изменить любой внешний API.

**Архитектура:** Single Agent + Ports-and-Adapters (трёхадаптерная, Вариант A).
Направление зависимостей строго одностороннее: `Core/Runner -> Adapters -> Storage`.

| Слой | Модуль | Роль |
|---|---|---|
| Ingress | `adapters/source_adapter.py` | FastAPI-вебхук, HMAC-SHA256, replay-окно 300 с, Pydantic v2 → HTTP 422 до бизнес-логики |
| State | `adapters/storage_adapter.py` | SQLite WAL, `BEGIN IMMEDIATE`, `busy_timeout=5000`, дедуп по `event_id`, DLQ атомарным `os.replace` |
| Egress | `adapters/egress_adapter.py` | ACL `build_request()`, `TokenBucketRateLimiter`, ротация токена под `asyncio.Lock()`, full-jitter backoff |
| Contracts | `schemas.py` | Единый источник истины для всех wire-контрактов (Pydantic v2, `extra="forbid"`) |
| Config | `config.py` + `test_suite/config.json` + `.env` | Секреты, endpoint-ы, ретраи, лимиты |
| Runner | `test_suite/qa_runner_core.py` | Оркестрация pytest, AST-guard, двухпроходный цикл, отчёты |

Все команды в документе выполняются из каталога `code/` (корень репозитория коннектора):
`/home/dandi/agent-factory/sessions/connector_qa_runner_build/code/`.

---

## 1. Где подключать API и секреты (0–3 мин)

### 1.1 Три файла конфигурации

| Файл | Что в нём лежит | Кто читает |
|---|---|---|
| `config.py` | `class GatewaySettings(BaseSettings)` — типизированные секреты и endpoint-ы; `get_settings()` — синглтон (`@lru_cache(maxsize=1)`) | `test_suite/config_loader.py:load_config()` |
| `test_suite/config.json` | Runtime-конфиг: ретраи, backoff, circuit breaker, replay-окно, `storage`, `rate_limit` | `test_suite/config_loader.py:load_config()` (путь `_DEFAULT_PATH`) |
| `.env` | Секреты и endpoint-ы в текстовом виде | `GatewaySettings.model_config` → `env_file=str(_ENV_FILE)`, где `_ENV_FILE = Path(__file__).resolve().parent / ".env"` → то есть **`code/.env`** |

`.env` **не** существует в репозитории — создайте его из шаблона:

```bash
cd /home/dandi/agent-factory/sessions/connector_qa_runner_build/code/
cp .env.example .env
```

Файл `.env` в `.dockerignore` — в образ он не попадает; в Docker-контуре используются
дефолты из `config.py`.

### 1.2 Переменные окружения: что за что отвечает

Все поля объявлены в `config.py`, класс `GatewaySettings` (строки 31–57):

| Переменная | Поле `GatewaySettings` | Дефолт в коде | Назначение | Где реально используется |
|---|---|---|---|---|
| `HMAC_SECRET` | `HMAC_SECRET: str` | `connector-qa-runner-test-secret` | Общий секрет подписи входящего вебхука (HMAC-SHA256). Заголовок `X-Signature`, префикс `sha256=` | `adapters/source_adapter.py:create_app()` → `verify_hmac_sha256(body, signature_header, secret)`; сравнение константное, через `hmac.compare_digest` |
| `AMOCRM_TOKEN` | `AMOCRM_TOKEN: str` | `test-amocrm-token` | Bearer-токен исходящего CRM-запроса | `adapters/egress_adapter.py:EgressAdapter.build_request()` → `headers["Authorization"] = f"Bearer {token}"` |
| `AMOCRM_BASE_URL` | `AMOCRM_BASE_URL: str` | `https://api.hubapi.com` | Базовый URL внешнего API | Свойство `GatewaySettings.egress_url` → `f"{base}/crm/v3/objects/contacts"` |
| `RATE_LIMIT_RPS` | `RATE_LIMIT_RPS: float` | `2.0` | Скорость пополнения token bucket (запросов/сек) | `adapters/egress_adapter.py:TokenBucketRateLimiter(rate_limit_rps=...)` |
| `RATE_LIMIT_BURST` | `RATE_LIMIT_BURST: int` | `5` | Ёмкость burst — сколько запросов проходит без ожидания | `adapters/egress_adapter.py:TokenBucketRateLimiter(burst=...)` |
| `SQLITE_DB_PATH` | `SQLITE_DB_PATH: str` | `events.db` | Путь к базе состояния/дедупа | `cfg["storage"]["db_path"]` → `adapters/storage_adapter.py:init_storage(db_path)` |
| `N8N_WEBHOOK_URL` | `N8N_WEBHOOK_URL: str` | `http://localhost:5678/webhook/connector-qa-runner` | Fallback-вебхук n8n | `test_suite/gateway_support.py:post_n8n_fallback()` |

### 1.3 Где прописывать базовые URL эндпоинтов

**Маршрут:** `.env` → `config.py:GatewaySettings` → `test_suite/config_loader.py:load_config()` → `cfg["egress"]["url"]` → `EgressAdapter.build_request()`.

Критично знать **приоритеты** (они реализованы буквально в `test_suite/config_loader.py`, строки 64–73):

```python
cfg["hmac_secret"] = settings.HMAC_SECRET       # перезаписывает JSON безусловно
cfg["egress"]["url"] = settings.egress_url      # перезаписывает JSON безусловно
cfg["egress"]["token"] = settings.AMOCRM_TOKEN  # перезаписывает JSON безусловно
cfg["storage"]["db_path"] = settings.SQLITE_DB_PATH
cfg["n8n"]["webhook_url"] = settings.N8N_WEBHOOK_URL

rate_limit = cfg.setdefault("rate_limit", {})              # ← setdefault!
rate_limit.setdefault("rate_limit_rps", settings.RATE_LIMIT_RPS)
rate_limit.setdefault("burst", settings.RATE_LIMIT_BURST)
```

1. **`egress.url` в `test_suite/config.json` перетирается.** Значение `"https://api.hubapi.com/crm/v3/objects/contacts"` из JSON — это запасной вариант (сработает только если `get_settings` недоступен). Реальный URL берётся из `AMOCRM_BASE_URL` через свойство `egress_url`.
2. **Свойство `egress_url` жёстко добавляет суффикс `/crm/v3/objects/contacts`.** Поэтому через `.env` вы получите только URL вида `{AMOCRM_BASE_URL}/crm/v3/objects/contacts`.
3. **`rate_limit`, наоборот, живёт в JSON.** `setdefault` означает: если ключ есть в `test_suite/config.json` — побеждает JSON, env игнорируется. Чтобы `RATE_LIMIT_RPS` из `.env` начал действовать, ключ `rate_limit.rate_limit_rps` должен быть **удалён** из `test_suite/config.json`.

**Gotcha (падение теста).** Тест `test_suite/test_egress_acl.py:test_rate_limiter_parameters_bind_to_config` утверждает:

```python
runtime = get_settings().as_runtime_config()
assert runtime["rate_limit"] == CFG["rate_limit"]
```

Значит `config.py`/`.env` и `test_suite/config.json` обязаны совпадать. Правильный рецепт смены лимита — менять **оба** места:

```bash
# .env
RATE_LIMIT_RPS=5.0
RATE_LIMIT_BURST=10
```
```json
// test_suite/config.json
"rate_limit": { "rate_limit_rps": 5.0, "burst": 10 }
```

Если поменять только одно из двух — `test_rate_limiter_parameters_bind_to_config` упадёт.

**Как направить трафик на нестандартный endpoint (amoCRM `POST /api/v4/leads`):**
суффикс `/crm/v3/objects/contacts` зашит в `egress_url`, поэтому одного `.env` мало —
переопределите `build_request()` в подклассе `EgressAdapter` (см. раздел 3, шаг Б).

### 1.4 Перезагрузка конфигурации

`get_settings()` обёрнут в `@lru_cache(maxsize=1)` → после правки `.env` процесс нужно
перезапустить, иначе он продолжит использовать старые значения.

Плейсхолдеры `${VAR}` в `test_suite/config.json` (`"hmac_secret": "${HMAC_SECRET}"`,
`"token": "${AMOCRM_TOKEN}"`) разворачиваются функцией `_expand()` из `os.environ`.
Финальные значения всё равно перезатираются полями `GatewaySettings` (см. 1.3).

Проверка, что конфигурация собралась правильно:

```bash
python3 -c "from test_suite.config_loader import load_config; import json; print(json.dumps(load_config(), indent=2, ensure_ascii=False))"
```

Ожидаемый фрагмент:

```json
{ "egress": { "url": "https://api.hubapi.com/crm/v3/objects/contacts", "token": "test-amocrm-token" },
  "rate_limit": { "rate_limit_rps": 2.0, "burst": 5 },
  "storage": { "busy_timeout_ms": 5000, "journal_mode": "WAL", "db_path": "events.db" } }
```

---

## 2. Где смотреть баги, логи и сбои (3–6 мин)

### 2.1 Как коннектор обрабатывает ошибки сети и HTTP-коды

Всё происходит в `adapters/egress_adapter.py:EgressAdapter.send()` (строки 248–320).
Метод **никогда не бросает исключений** — он всегда возвращает словарь.

**Ошибки сети — ретраев НЕТ, сразу в DLQ:**

| Исключение | Обработка | Итог |
|---|---|---|
| `httpx.ConnectTimeout`, `httpx.ReadTimeout`, `httpx.TimeoutException` | `except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.TimeoutException)` → `last_reason = type(exc).__name__` → `break` | `{"status": "dlq", "reason": "ConnectTimeout"}` |
| `httpx.TransportError` (напр. `ConnectError`, `ReadError`) | `except httpx.TransportError` → `last_reason = type(exc).__name__` → `break` | `{"status": "dlq", "reason": "ConnectError"}` |

Обратите внимание: `reason` — это **имя класса исключения**, а не HTTP-код.

**HTTP-коды — классификация функцией `classify(status_code)` (строки 43–51):**

| Диапазон | `classify()` | Поведение в `send()` |
|---|---|---|
| `200 ≤ code < 300` | `"ok"` | Немедленный успех: `{"status": "ok", "event_id": ..., "attempts": n, "status_code": code}` |
| `401` | (обрабатывается **до** `classify`) | Ротация токена: `rotated_after_401` → `await self.refresh_token(seen_token_version)` → `continue` (повтор с новым токеном). Ротация **не более одного раза** за вызов `send()` |
| `429` | `"retry"` | `_parse_retry_after()` (целое число секунд **или** HTTP-date) → `compute_backoff_delay(attempt, retry_after, random, base, cap)` → `await self.sleep(delay)` → следующая попытка |
| `500 ≤ code < 600` (`5xx`) | `"fail"` | Тот же full-jitter backoff, но без учёта `Retry-After` → следующая попытка |
| прочие 4xx (`400`, `403`, `404`, …) | `"error"` | Тот же backoff-путь (код в `send()` не различает `"fail"` и `"error"`) → следующая попытка |

**Бюджет попыток:** `retry.max_attempts` из `test_suite/config.json` = **3** попытки всего
(не 3 повтора после первой). `retry.base_delay_s` = `0.1`, `backoff.max_delay_s` = `5.0`,
`retry.timeout_s` = `2.0` (таймаут применяется только к клиенту, созданному самим адаптером;
при инъекции `client=` параметр игнорируется).

**Backoff — full jitter:** `compute_backoff_delay()` возвращает `rng.uniform(0, min(cap, base * 2**attempt))`,
при наличии `Retry-After` потолок дополнительно урезается до него.

**Исчерпание попыток → DLQ:**

```python
path = write_dlq(event_id, event, last_reason, self.dlq_dir)
return {"status": "dlq", "event_id": event_id, "reason": last_reason, "dlq_path": path}
```

Прочие точки наблюдения:

- `adapters/source_adapter.py:create_app()` — счётчик отказов аутентификации в
  `app.state.auth_failures` (HMAC-подделка и просроченный replay).
- `test_suite/qa_runner_core.py:write_reports()` — агрегированные отчёты (раздел 2.4).

### 2.2 Где физически искать аварийные события (DLQ)

**Каталог:** параметр `EgressAdapter.__init__(..., dlq_dir: str = "dlq")` → относительный
путь **`./dlq` от рабочего каталога процесса**. Каталог создаётся автоматически в
`write_dlq()` через `os.makedirs(dlq_dir, exist_ok=True)`.

**Файл:** `{dlq_dir}/{event_id}.json` — то есть `dlq/e-503.json`.

**Формат записи** (`adapters/storage_adapter.py:write_dlq()`, строки 105–136):

```json
{
  "event_id": "e-503",
  "reason": "HTTP 503",
  "payload": { "event_id": "e-503", "source": "n8n", "payload": { "k": 1 } },
  "created_at": "2026-10-21T12:00:00.123456+00:00"
}
```

- `payload` — **исходное событие целиком** (внутренний dict), а не преобразованное тело запроса.
- `reason` — `HTTP 503` для кодов, либо имя класса исключения (`ConnectError`, `ReadError`,
  `ConnectTimeout`, `ReadTimeout`) для сетевых сбоев.
- `created_at` — ISO-8601 в UTC.

**Атомарность:** запись идёт через временный файл `{event_id}.{pid}.{uuid4hex}.tmp` →
`flush()` → `os.fsync()` → `os.replace(tmp_path, final_path)`. Частичный JSON на диске
невозможен; в каталоге встречаются только `.json` и `.tmp`.

**Быстрый осмотр DLQ:**

```bash
ls -la dlq/
python3 -c "import json;d=json.load(open('dlq/<event_id>.json'));print(d['reason'], d['created_at'])"
```

Каталог `./dlq` создаётся **лениво** — `write_dlq()` вызывает
`os.makedirs(dlq_dir, exist_ok=True)` при первой неудачной отправке. Поэтому в
свежем checkout каталога нет: `ls` пишет «No such file or directory» — значит ни
одно событие ещё не ушло в DLQ (путь относительный, от CWD процесса; в репозитории
каталог `dlq/` не версионируется). Полный список отказов за сессию:

```bash
python3 - <<'PY'
import glob, json
for path in sorted(glob.glob("dlq/*.json")):
    rec = json.load(open(path))
    print(f"{rec['event_id']:<12} {rec['reason']:<20} {rec['created_at']}")
PY
```

**Возврат в обработку (replay):** `adapters/storage_adapter.py:replay_dlq(event_id, conn, dlq_dir)`
— удаляет строку из `events`, пере-claims событие (даёт `first_processed=True`),
удаляет `.json`-файл и возвращает `True`. Повторный вызов возвращает `False`.

```python
from adapters.storage_adapter import init_storage, replay_dlq
conn = init_storage("events.db")
print(replay_dlq("e-503", conn, "dlq"))   # True — событие возвращено в обработку
```

### 2.3 Как читать SQLite WAL хранилище дедупликации

**Файл базы:** `SQLITE_DB_PATH` (по умолчанию `events.db`) → `cfg["storage"]["db_path"]`.
Рядом появятся WAL-файлы `events.db-wal` и `events.db-shm`.

**Инициализация** — `adapters/storage_adapter.py:init_storage(db_path)` (строки 24–43):

```sql
PRAGMA journal_mode = WAL;      -- режим WAL
PRAGMA busy_timeout = 5000;     -- 5 с ожидания блокировки
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS events (
    event_id       TEXT PRIMARY KEY,
    status         TEXT,
    first_processed INTEGER DEFAULT 0,
    created_at     TEXT
);
```

**Семантика дедупа** — `_claim_event_sync()` (строки 46–77):
`BEGIN IMMEDIATE` (с повтором при `sqlite3.OperationalError` со словом `locked` до
5-секундного дедлайна, `time.sleep(0.002)`) → `INSERT OR IGNORE INTO events ...` →
`first_processed = (cursor.rowcount == 1)` → `COMMIT`.
Из N конкурентных корутин ровно одна получает `first_processed=True`.

**Читаем базу** (CLI `sqlite3` может быть не установлен — надёжнее Python):

```bash
python3 - <<'PY'
import sqlite3
conn = sqlite3.connect("events.db")
print("journal_mode:", conn.execute("PRAGMA journal_mode").fetchone()[0])
print("integrity   :", conn.execute("PRAGMA integrity_check").fetchone()[0])
for row in conn.execute(
    "SELECT event_id, status, first_processed, created_at FROM events "
    "ORDER BY created_at DESC LIMIT 50"
):
    print(row)
PY
```

**Если базы ещё нет** (свежий checkout, коннектор ни разу не запускался), этот скрипт
упадёт с `sqlite3.OperationalError: no such table: events`, а `PRAGMA journal_mode`
покажет `delete` — это не поломка WAL, а отсутствие самой базы. Создайте её
инициализатором хранилища (он сразу ставит WAL и создаёт таблицу):

```bash
python3 -c "from adapters.storage_adapter import init_storage; init_storage('events.db').close()"
```

после чего `PRAGMA journal_mode` вернёт `wal`, а таблица `events` будет пустой.

**Как читать результат:**

| Поле | Значение | Как трактовать |
|---|---|---|
| `first_processed = 1` | Событие обработано **впервые** — это каноническая доставка | Норма |
| `first_processed = 0` | Дубликат, отсечён дедупом по `event_id` | Норма, если `event_id` действительно повторяется |
| `status` | Литеральная строка `"first_processed"` для любой вставленной строки | Поле-заглушка, на дедуп не влияет |
| `created_at` | ISO-8601 UTC момента claim-а | Для сортировки/поиска инцидента |
| `PRAGMA journal_mode` | Должно быть `wal` | Иначе вы потеряли WAL-конфигурацию (в базе, созданной `init_storage()`, всегда `wal`) |
| `PRAGMA integrity_check` | Должно быть `ok` | `не ok` → база повреждена, требуется восстановление |

**Точечное чтение одного события:** `adapters/storage_adapter.py:get_event(event_id, conn)`
→ словарь `{"event_id", "status", "first_processed", "created_at"}` либо `None`.

```python
from adapters.storage_adapter import init_storage, get_event
conn = init_storage("events.db")
print(get_event("e-503", conn))
```

**Сжатие WAL** (после массовых вставок):

```bash
python3 -c "import sqlite3; c=sqlite3.connect('events.db'); print(c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())"
```

### 2.4 Отчёты прогона

| Файл | Пишет | Содержимое |
|---|---|---|
| `reports/test_report.json` | `test_suite/qa_runner_core.py:write_reports()` | `collected`, `passed`, `failed`, `blocked`, `categories` (network/storage/n8n/contract), `finished_at` |
| `reports/bug_matrix.json` | там же | `pass`, `failures[]`, `blocked_modules[]`, `circuit_breaker.state` |

**Важно:** эти файлы пишет **не** `pytest`, а оркестратор `qa_runner_core.refine_loop()`
(два прохода, circuit breaker ≤ 2 из `circuit_breaker.max_passes`). Прямой запуск
`pytest test_suite -q --network=none` отчёты **не обновляет**.

Перегенерировать отчёты одной командой (из `code/`):

```bash
cd /home/dandi/agent-factory/sessions/connector_qa_runner_build/code/
python3 -m test_suite.qa_runner_core
# → collected=155 passed=155 failed=0 blocked=0 passes=1 circuit_breaker=closed
```

Текущее состояние артефактов в репозитории (актуально):

| Файл | Значения |
|---|---|
| `reports/test_report.json` | `collected: 155`, `passed: 155`, `failed: 0`, `blocked: 0` |
| `reports/bug_matrix.json` | `pass: 1`, `failures: []`, `blocked_modules: []`, `circuit_breaker.state: closed` |

Поле `categories` остаётся нулевым: маркеры `network` / `storage` / `n8n` / `contract`
зарегистрированы в `test_suite/conftest.py`, но ни один тест их не несёт — фактическая
группировка сьюта — **по модулям** (таблица в §3, шаг В).

---

## 3. 15-минутный регламент адаптации к изменениям внешних API (6–12 мин)

**Сценарий:** внешний API изменил формат JSON — добавил или удалил поля.

Инвариант: **логика ядра, ретраи, backoff, троттлинг, ротация токена и DLQ не меняются.**
Меняются только две вещи: DTO и маппинг.

```
схема вендора → schemas.py (DTO) → EgressAdapter.build_request() (маппинг) → docker compose run --rm test-runner
```

### Шаг А. Редактирование DTO в `schemas.py` (≈3 мин)

`schemas.py` — **единственный источник истины** для всех wire-контрактов. Адаптеры
импортируют модели отсюда и не объявляют свои.

| Модель | Назначение | Кто им пользуется |
|---|---|---|
| `InboundWebhookPayload` | Ingress-контракт (`event_id`, `event_ts`, `source`, `payload`) | `adapters/source_adapter.py` (импортируется как `InboundWebhookPayload`, алиас `WebhookPayload = InboundWebhookPayload`) |
| `WebhookPayload` | Универсальный envelope (`event_id`, `source`, `timestamp`, `data`) | Тесты контрактов, `test_suite/test_amocrm_contracts.py` |
| `AmoLeadCreateSchema` | Запрос `POST /api/v4/leads` (сделка) | `EgressAdapter.build_request()` в подклассах amoCRM |
| `AmoContactCreateSchema` | Запрос `POST /api/v4/contacts` (контакт) | То же |
| `OneCItemSchema`, `OneCOrderCreateSchema`, `OneCCounterpartySchema` | Контракты 1С (заказ/позиция/контрагент) | `test_suite/gateway_support.py:lead_to_1c_order()` |

**Правила правки:**

1. **Все модели имеют `model_config = ConfigDict(extra="forbid")`.** Неизвестное поле → `ValidationError` → HTTP **422** из `adapters/source_adapter.py` (`raise HTTPException(status_code=422, detail=exc.errors())`) — **до** бизнес-логики и до записи в состояние.
   - Поле **добавлено** вендором и его нужно принять → добавьте его как явное типизированное поле модели.
   - Поле **добавлено** вендором, но принимать его не нужно → оставьте `extra="forbid"` (вендорское поле приведёт к 422; если это неприемлемо — смените `extra` на `"ignore"` **в этой модели**).
   - Поле **удалено** вендором → сделайте поле опциональным: `Optional[str] = None` или `Field(default=...)`, иначе 422 на каждом запросе.
2. **Не объявляйте Pydantic-модели в адаптерах.** Тест `test_suite/test_egress_acl.py:test_source_adapter_imports_webhook_payload_from_schemas` проверяет, что в `adapters/source_adapter.py` нет ни одной собственной `BaseModel` (`own_models == []`). Дубликат схемы — гарантированный провал теста.
3. Обязательно отражайте изменение в тестах контрактов: `test_suite/test_amocrm_contracts.py` (35 тестов) и `test_suite/test_1c_contracts.py` (25 тестов) содержат проверки `extra="forbid"` и обязательности полей.

**Пример (поле `price` стало опциональным):**

```python
class AmoLeadCreateSchema(BaseModel):
    """Contract for ``POST /api/v4/leads`` (create a deal)."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1)
    price: int = Field(..., ge=0)              # было: int = Field(..., ge=0)
    custom_fields_values: List[Dict[str, Any]] = Field(default_factory=list)
    external_id: str | None = None              # новое поле вендора
```

### Шаг Б. Корректировка маппинга в `adapters/egress_adapter.py` внутри `build_request()` (≈5 мин)

`EgressAdapter.build_request(self, event) -> Tuple[str, Dict[str, str], Any]` (строки 197–215) —
**единственное** место, где формируются URL, заголовки и тело запроса. Это и есть
ACL-ш seam для подключения любой внешней системы.

**Базовая реализация:**

```python
def build_request(self, event: Dict[str, Any]) -> Tuple[str, Dict[str, str], Any]:
    url = str(self.config["egress"]["url"])
    event_id = event.get("event_id")
    body: Dict[str, Any] = {
        "event_id": event_id,
        "source": event.get("source", "n8n"),
        "payload": event.get("payload", {}),
    }
    headers: Dict[str, str] = {"Idempotency-Key": str(event_id)}
    token = self._token
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return url, headers, body
```

**Вариант 1 (предпочтительный) — подкласс.** Не трогайте базовый класс, если изменение
касается только одного downstream:

```python
from adapters.egress_adapter import EgressAdapter
from schemas import AmoLeadCreateSchema


class AmoLeadEgress(EgressAdapter):
    """Egress для amoCRM REST v4: маппинг изменился, транспорт — нет."""

    def build_request(self, event):
        url = "https://example.invalid/api/v4/leads"
        lead = AmoLeadCreateSchema.model_validate(event["payload"])
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        return url, headers, lead.model_dump()
```

**Вариант 2** — правка `EgressAdapter.build_request()` на месте, если формат меняется
для всех downstream-систем.

**Правила шага Б:**

1. **Используйте `self.token` (свойство), а не `self.config["egress"]["token"]`.** После ротации на 401 `self._token` уже содержит новый токен — конфиг останется со старым.
2. **Сохраняйте заголовок `Idempotency-Key`** — это якорь идемпотентности на стороне downstream.
3. **Валидируйте через DTO из `schemas.py`** (`AmoLeadCreateSchema.model_validate(...)`), а не копируйте поля вручную: строгий контракт (`extra="forbid"`) отсечёт лишние поля ещё до выхода в сеть.
4. **НЕ трогайте** (логика ядра и ретраев не затрагивается):
   - `EgressAdapter.send()` — транспорт, ретраи, DLQ;
   - `classify()`, `compute_backoff_delay()`, `_parse_retry_after()` — классификация HTTP и backoff;
   - `TokenBucketRateLimiter` — троттлинг;
   - `refresh_token()` / `_token_lock` — ротация токена под `asyncio.Lock()`;
   - `adapters/storage_adapter.py` — состояние, дедуп, DLQ.
5. Пример изменения только маппинга (переименование поля `phone` → `phones`):

```python
def build_request(self, event):
    url = str(self.config["egress"]["url"])
    body = {
        "event_id": event.get("event_id"),
        "source": event.get("source", "n8n"),
        "contact": {"phones": event.get("payload", {}).get("phones", [])},
    }
    headers = {
        "Idempotency-Key": str(event.get("event_id")),
        "Authorization": f"Bearer {self.token}",
    }
    return url, headers, body
```

### Шаг В. Запуск автотестов в Docker одной командой (≈4 мин)

```bash
cd /home/dandi/agent-factory/sessions/connector_qa_runner_build/code/
docker compose run --rm test-runner
```

Ожидаемый вывод:

```
........................................................................ [ 46%]
........................................................................ [ 92%]
...........                                                              [100%]
155 passed
```

(точное время зависит от хоста — ориентир: единицы секунд локально, десятки секунд в Docker).

Контур изолирован: `network_mode: "none"`, `read_only: true`, tmpfs для `/tmp`, `/dev/shm`,
`/app/.pytest_cache` (см. `docker-compose.yml`). Внутри — `CMD ["pytest", "test_suite", "-q", "--network=none"]`
(`Dockerfile`).

**Быстрый цикл только по контракту (≈2 с локально):**

```bash
pytest test_suite/test_egress_acl.py test_suite/test_amocrm_contracts.py -q --network=none
```

**Что должно зелёным остаться после шага Б (не должно сломаться):**

| Файл | Тестов | Что защищает |
|---|---|---|
| `test_suite/test_egress_acl.py` | 19 | `build_request()`, `TokenBucketRateLimiter`, single-flight ротация на 401, отсутствие дублей схем в `source_adapter` |
| `test_suite/test_resilience_dlq.py` | 26 | Ретраи, backoff, таймауты, атомарность DLQ, n8n fallback |
| `test_suite/test_storage_wal.py` | 25 | WAL/PRAGMA, конкурентные claim-ы, дедуп, crash-recovery |
| `test_suite/test_security_hmac.py` | 25 | HMAC-SHA256, replay-окно, коды 401/422 |
| `test_suite/test_amocrm_contracts.py` | 35 | Строгие контракты amoCRM |
| `test_suite/test_1c_contracts.py` | 25 | Строгие контракты 1С |

**Ограничения, о которые спотыкается новый код:**

- `test_suite/ast_guard.py` блокирует **до импорта** тестового модуля вызовы
  `os.system`, `os.popen`, `socket.socket`, `socket.create_connection` и любой `subprocess.*`.
  В адаптерах их использовать нельзя.
- `test_suite/conftest.py` (фикстура `_forbid_outbound_network`) патчит `socket.getaddrinfo`
  и `socket.create_connection` на выброс `RuntimeError` — реальный сетевой вызов уронит прогон.
- AST-guard проверяет **тестовые** модули; тем не менее правило «никакого `subprocess`/`socket`
  в `adapters/`» обязательно и для продакшн-кода.

### Чек-лист всего регламента (6–12 мин)

```
[ ] 1. schemas.py:  принять/удалить поле в нужной модели (extra="forbid" в mind)
[ ] 2. schemas.py:  не дублировать модель в адаптере (правило AST-теста)
[ ] 3. adapters/egress_adapter.py:  изменить ТОЛЬКО build_request()
[ ] 4. adapters/egress_adapter.py:  использовать self.token, сохранить Idempotency-Key
[ ] 5. НЕ трогать: send(), classify(), compute_backoff_delay(), TokenBucketRateLimiter,
       refresh_token(), storage_adapter.py
[ ] 6. docker compose run --rm test-runner  →  155 passed
```

---

## 4. Команды запуска и верификации (12–15 мин)

### 4.1 Локальный запуск тестов

```bash
cd /home/dandi/agent-factory/sessions/connector_qa_runner_build/code/
pytest test_suite -q --network=none
```

Ожидаемо: `155 passed` (запуск из `code/` обязателен — модули импортируются как
top-level пакеты `config`, `schemas`, `adapters`, `test_suite`).

Если бинарник `pytest` не на PATH (частая ситуация в venv/контейнерах):

```bash
python3 -m pytest test_suite -q --network=none
```

**Что означает `--network=none`:**
- флаг регистрируется в `test_suite/conftest.py:pytest_addoption()` (значение по умолчанию — `none`);
- сессионная фикстура `_forbid_outbound_network` подменяет `socket.getaddrinfo` и
  `socket.create_connection` на выброс `RuntimeError("outbound network access is forbidden in this test suite")`;
- весь HTTP-трафик идёт через in-process `httpx.MockTransport` / `ASGITransport`.

Оптимизация `conftest.py`: `basetemp` автоматически переносится в `/dev/shm/pytest-cqr-<pid>`
(на ext4-`/tmp` `fsync` стоит сотни миллисекунд, а WAL/DLQ пишут его агрессивно).

### 4.2 Запуск в изолированном Docker-контейнере

```bash
cd /home/dandi/agent-factory/sessions/connector_qa_runner_build/code/
docker compose run --rm test-runner
```

Ожидаемо: `155 passed`.

Альтернативная форма (с пересборкой образа):

```bash
docker compose up --build --abort-on-container-exit
```

Гарантии изоляции (`docker-compose.yml`): `network_mode: "none"` — любой исходящий запрос
упадёт на уровне рантайма; `read_only: true` — файловая система контейнера неизменяема;
tmpfs для `/tmp`, `/dev/shm`, `/app/.pytest_cache`.

Переопределение команды внутри контейнера (для точечной проверки):

```bash
docker compose run --rm test-runner pytest test_suite/test_egress_acl.py -q --network=none -k "token_bucket or rate_limit"
# → 7 passed, 12 deselected
```

### 4.3 Проверка лимитов скорости (троттлинг)

**Способ 1 — юнит-тесты бакета (7 тестов, детерминированные):**

```bash
pytest test_suite/test_egress_acl.py -q --network=none -k "token_bucket or rate_limit"
# → 7 passed, 12 deselected
```

| Тест | Что проверяет |
|---|---|
| `test_rate_limiter_defaults_match_specification` | `DEFAULT_RATE_LIMIT_RPS == 2.0`, `DEFAULT_RATE_LIMIT_BURST == 5` |
| `test_rate_limiter_parameters_bind_to_config` | Лимиты берутся из `cfg["rate_limit"]` и совпадают с `get_settings().as_runtime_config()["rate_limit"]` |
| `test_token_bucket_allows_burst_without_delay` | Первые 5 `acquire()` возвращают `0.0` (burst без ожидания) |
| `test_token_bucket_delays_when_rps_exceeded` | Третий `acquire()` при `burst=2` действительно ждёт |
| `test_token_bucket_requested_delay_matches_configured_rps` | Задержка = `дефицит / rate_limit_rps` (при 100 rps → 10 мс, а не 500 мс) |
| `test_token_bucket_refills_deterministically_with_injected_clock` | Пополнение бакета на 1 токен при 2 rps = 0.5 с виртуального времени |
| `test_send_waits_on_rate_limiter_before_every_post` | `send()` берёт permit **перед каждым** `client.post` (3 попытки → 3 `acquire()`) |

**Способ 2 — проверка фактического времени ожидания (реальные часы, ~1.5 с):**

```bash
python3 - <<'PY'
import asyncio
from adapters.egress_adapter import TokenBucketRateLimiter

async def main():
    lim = TokenBucketRateLimiter(rate_limit_rps=2.0, burst=5)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    for _ in range(8):
        await lim.acquire()
    print(f"8 acquires took {loop.time() - t0:.2f}s (burst 5 + 3 x 0.5s)")

asyncio.run(main())
# → 8 acquires took 1.50s
```

Расчёт: первые 5 запросов проходят мгновенно (burst), оставшиеся 3 ждут по
`1 / 2.0 = 0.5 с` каждый → 1.5 с суммарно.

**Способ 3 — проверить, что конфиг действительно применился:**

```bash
python3 -c "from test_suite.config_loader import load_config; print(load_config()['rate_limit'])"
# → {'rate_limit_rps': 2.0, 'burst': 5}
```

**Способ 4 — реактивная обработка `429` (сервер отвечает троттлингом):**

```bash
pytest test_suite/test_resilience_dlq.py -q --network=none -k "retry_after or retry_preserves"
# → 2 passed, 24 deselected
```

- `test_retry_after_caps_backoff` — заголовок `Retry-After` урезает потолок backoff
  (100 случайных выборок в диапазоне `[0.0, 0.05]`).
- `test_retry_preserves_error_context` — после исчерпания попыток при 503 в ответе
  остаётся `reason == "HTTP 503"`, а событие уходит в DLQ.

Полный путь реактивного троттлинга: `HTTP 429` → `classify()` → `"retry"` →
`_parse_retry_after()` (число секунд **или** HTTP-date) → `compute_backoff_delay()` →
`await self.sleep(delay)` → следующая попытка. Если попыток (3) не хватило —
`write_dlq()` с `reason = "HTTP 429"`.

**Способ 5 — убедиться, что ротация токена на 401 не превращается в шторм:**

```bash
pytest test_suite/test_egress_acl.py -q --network=none -k "401 or refresh or concurrent"
# → test_401_rotates_token_once_and_retries_with_new_token
#   test_concurrent_token_refreshes_are_single_flight
#   test_refresh_without_invalidator_still_rotates_and_bumps_version
```

Восемь конкурентных обработчиков 401 вызывают `refresh_token(seen_version=0)` →
`asyncio.Lock()` в `EgressAdapter._token_lock` пропускает только одного →
`self._token_version` увеличивается ровно на 1 → все получают один и тот же новый токен.

### 4.4 Сводка команд

| Задача | Команда | Ожидание |
|---|---|---|
| Полный прогон локально | `pytest test_suite -q --network=none` | `155 passed` |
| Полный прогон локально (fallback) | `python3 -m pytest test_suite -q --network=none` | `155 passed` |
| Полный прогон в Docker | `docker compose run --rm test-runner` | `155 passed` |
| Docker с пересборкой | `docker compose up --build --abort-on-container-exit` | `155 passed` |
| Контракты ACL + amoCRM | `pytest test_suite/test_egress_acl.py test_suite/test_amocrm_contracts.py -q --network=none` | 54 passed |
| Лимит скорости | `pytest test_suite/test_egress_acl.py -q --network=none -k "token_bucket or rate_limit"` | 7 passed |
| Реактивный 429 | `pytest test_suite/test_resilience_dlq.py -q --network=none -k "retry_after or retry_preserves"` | 2 passed |
| Обновить отчёты прогонов | `python3 -m test_suite.qa_runner_core` | `collected=155 passed=155 failed=0 blocked=0` |
| DLQ: список отказов | `python3 -c "import glob,json; [print(json.load(open(p))['reason'], p) for p in sorted(glob.glob('dlq/*.json'))]"` | по строке `reason` на каждый файл |
| Эффективный конфиг | `python3 -c "from test_suite.config_loader import load_config; print(load_config())"` | словарь с `egress`, `rate_limit`, `storage` |
| Состояние дедупа | `python3 -c "from adapters.storage_adapter import init_storage,get_event; c=init_storage('events.db'); print(get_event('<event_id>', c))"` | dict или `None` |
| Интеграционная целостность | `python3 -c "import sqlite3;print(sqlite3.connect('events.db').execute('PRAGMA integrity_check').fetchone()[0])"` | `ok` |

---

## Приложение: карта файлов

| Путь | Роль | Ключевые символы |
|---|---|---|
| `config.py` | Секреты и endpoint-ы | `GatewaySettings`, `get_settings()`, `GatewaySettings.egress_url`, `GatewaySettings.as_runtime_config()` |
| `schemas.py` | Wire-контракты (Pydantic v2) | `WebhookPayload`, `InboundWebhookPayload`, `AmoLeadCreateSchema`, `AmoContactCreateSchema`, `OneCItemSchema`, `OneCOrderCreateSchema`, `OneCCounterpartySchema` |
| `adapters/source_adapter.py` | Ingress | `create_app()`, `verify_hmac_sha256()`, `check_replay_window()`, `SIGNATURE_HEADER = "X-Signature"` |
| `adapters/storage_adapter.py` | Состояние, дедуп, DLQ | `init_storage()`, `claim_event()`, `_claim_event_sync()`, `get_event()`, `write_dlq()`, `replay_dlq()` |
| `adapters/egress_adapter.py` | Egress, ACL, троттлинг | `EgressAdapter`, `EgressAdapter.build_request()`, `EgressAdapter.send()`, `EgressAdapter.refresh_token()`, `TokenBucketRateLimiter`, `classify()`, `compute_backoff_delay()`, `_parse_retry_after()`, `send()` (legacy-wrapper) |
| `test_suite/config.json` | Runtime-конфиг | `description`, `retry`, `backoff`, `circuit_breaker`, `replay`, `storage`, `egress`, `rate_limit` |
| `test_suite/config_loader.py` | Сборка конфига | `load_config()`, `_expand()` |
| `test_suite/conftest.py` | Фикстуры, `--network` | `pytest_addoption()`, `_forbid_outbound_network`, `dlq_dir`, `webhook_app` |
| `test_suite/qa_runner_core.py` | Оркестратор, отчёты | `StoragePort`, `run_tests()`, `write_reports()`, `refine_loop()`, `SystemPytestRunner` |
| `test_suite/ast_guard.py` | Статический запрет опасных вызовов | `FORBIDDEN_CALLS`, `FORBIDDEN_PREFIXES`, `partition_modules()`, `check_module()` |
| `test_suite/gateway_support.py` | Эталонные помощники | `CircuitBreaker`, `QueueStore`, `post_n8n_fallback()`, `lead_to_amocrm()`, `normalize_validation_error()`, `build_bug_matrix()` |
| `test_suite/ingress_harness.py` | ASGI-стенд для ingress | `AsgiIngress`, `MemoryStorage` |
| `Dockerfile` | Герметичный образ | `CMD ["pytest", "test_suite", "-q", "--network=none"]` |
| `docker-compose.yml` | Изолированный прогон | сервис `test-runner`, `network_mode: "none"`, `read_only: true` |
| `ADAPTER_EXTENSION_GUIDE.md` | Расширение адаптеров подклассами | §3 — `EgressAdapter` → amoCRM/Bitrix24/Sheets |
| `ARCHITECTURE_DEBT.md` | Статус долга | Core-Adapter Decoupling — RESOLVED |
| `config.json` (корень) | Манифест агента | `description`, `components`, `runtime_config` → `test_suite/config.json`, `write_scope` / `read_only_scope` |
| `graph.py` | DAG компонентов | `Node`, `Edge`, `Graph` — только stdlib, без FastAPI/httpx |
| `n8n_workflow.json` | Workflow n8n-диспетчера | проверяется хелперами `test_suite/n8n_validator.py` (в активном прогоне сьюта не используются — см. `test_suite/_legacy/`) |
| `test_suite/n8n_validator.py` | Headless-валидация `*.n8n.json` | `load_workflow()`, `node_names()`, `find_orphans()`, `connection_endpoints()`, `is_acyclic()`, `http_nodes()`, `has_resilience()` |
| `test_suite/_legacy/` | **Не коллекторуется pytest** | файлы называются `legacy_*.py` (не `test_*.py`) → в прогон (155 тестов) не входят; используйте как справочник, не как источник истины |
| `reports/test_report.json`, `reports/bug_matrix.json` | Артефакты прогонов | пишет `qa_runner_core.write_reports()` |
