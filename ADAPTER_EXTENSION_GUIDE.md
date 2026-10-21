# ADAPTER_EXTENSION_GUIDE — connector-qa-runner

Гайд описывает расширение каждого из трёх адаптеров
(`source_adapter`, `storage_adapter`, `egress_adapter`) за один шаг — подклассом
(`egress_adapter`) либо реализацией того же порта, что и у базовой реализации
(`source_adapter`, `storage_adapter`), а также обязательный **smoke-тест контракта**,
защищающий от антипаттерна *Incomplete CRUD*.

Реальный функциональный контракт добавления адаптеров:
`schemas.py` → `gateway_support.py` → `test_*.py` → `config.py`

Все параметры (ретраи, backoff, лимит circuit breaker, окно replay, лимиты
скорости) вынесены в `test_suite/config.json` — новый коннектор подключается без
правок кода. Секреты и endpoint-ы (`HMAC_SECRET`, `AMOCRM_BASE_URL`,
`AMOCRM_TOKEN`) в JSON **не** хранятся: там только плейсхолдеры `${VAR}`, а реальные
значения всегда приходят из `config.py:GatewaySettings` (окружение / `.env`) —
см. §4.

---

## 0. Smoke-тест контракта (обязателен для любого подкласса)

Любой минимальный подкласс адаптера обязан пройти базовый набор операций.
Для `storage_adapter` это `init → claim → read → write_dlq → replay`.
Скопируйте тест в свой набор и запустите первой проверкой:

```python
# test_suite/test_adapter_contract_smoke.py
import asyncio
from adapters.storage_adapter import (
    init_storage, claim_event, get_event, write_dlq, replay_dlq,
)


def test_storage_contract_smoke(tmp_path):
    db = str(tmp_path / "smoke.db")
    dlq = str(tmp_path / "dlq")
    conn = init_storage(db)

    # claim (insert + dedup)
    assert asyncio.run(claim_event("smoke", conn))["first_processed"] is True
    assert asyncio.run(claim_event("smoke", conn))["first_processed"] is False
    # read
    assert get_event("smoke", conn)["first_processed"] is True
    # write_dlq (atomic os.replace)
    write_dlq("smoke", {"a": 1}, "test", dlq)
    # replay (DLQ reversal)
    assert replay_dlq("smoke", conn, dlq) is True
    assert replay_dlq("smoke", conn, dlq) is False
    conn.close()
```

Если подкласс не проходит smoke-тест — он неполный (Incomplete CRUD) и не принимается.

---

## 1. SourceAdapter → Telegram Bot / Workers

**Что реально есть в коде.** `adapters/source_adapter.py` — это **модуль функций**,
а не класс: базового класса `SourceAdapter`, который можно унаследовать, в репозитории
нет (`from adapters.source_adapter import SourceAdapter` → `ImportError`).

| Символ | Сигнатура | Роль |
|---|---|---|
| `create_app()` | `create_app(storage, config: Optional[Dict[str, Any]] = None) -> FastAPI` | собирает FastAPI-приложение: HMAC → replay → Pydantic → `storage.claim_event` |
| `verify_hmac_sha256()` | `verify_hmac_sha256(body: bytes, signature_header: str, secret: bytes) -> bool` | проверка подписи, сравнение константное (`hmac.compare_digest`) |
| `check_replay_window()` | `check_replay_window(event_ts: datetime, now: datetime, max_age_s: int = 300) -> bool` | окно replay |
| `SIGNATURE_HEADER` | `"X-Signature"` (префикс `sha256=`) | имя заголовка подписи |

**Точка расширения — объект `storage`:** ingress вызывает у него ровно один метод
`await storage.claim_event(event_id) -> {"event_id": str, "first_processed": bool}`.
Это и есть порт (duck-typing): подойдёт любой объект с такой корутиной.

**Вариант 1 (рекомендуемый) — собственный FastAPI-приложение на тех же примитивах.**
Telegram передаёт `secret_token` вместо HMAC-заголовка, поэтому подпись не проверяем,
а контракт/replay/дедуп оставляем теми же:

```python
import json
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError

from adapters.source_adapter import check_replay_window
from schemas import InboundWebhookPayload  # единый источник истины для wire-контрактов


class TelegramIngress:
    """Ингресс Telegram: secret_token -> контракт -> replay -> storage."""

    def __init__(self, storage, config, secret_token: str, max_age_s: int = 300):
        self.storage = storage
        self.cfg = config
        self.secret_token = secret_token
        self.max_age_s = max_age_s

    async def receive(self, raw_body: bytes, headers: dict) -> dict:
        if headers.get("X-Telegram-Bot-Api-Secret-Token") != self.secret_token:
            raise HTTPException(status_code=401, detail="invalid telegram token")
        try:
            data = json.loads(raw_body.decode("utf-8"))
            event = InboundWebhookPayload.model_validate(data)
        except (ValueError, UnicodeDecodeError):
            raise HTTPException(status_code=422, detail="invalid json")
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors())
        if not check_replay_window(event.event_ts, datetime.now(timezone.utc), self.max_age_s):
            raise HTTPException(status_code=401, detail="replay window exceeded")
        claim = await self.storage.claim_event(event.event_id)
        return {
            "status": "accepted",
            "event_id": event.event_id,
            "first_processed": bool(claim.get("first_processed", False)),
        }


def create_telegram_app(storage, config, secret_token: str) -> FastAPI:
    app = FastAPI(title="connector-qa-runner-telegram-source")
    ingress = TelegramIngress(storage, config, secret_token, int(config["replay"]["max_age_s"]))

    @app.post("/webhook")
    async def webhook(request: Request):
        return await ingress.receive(await request.body(), request.headers)

    return app
```

**Вариант 2 — обёртка над `create_app()`.** Если транспорт менять не нужно, но нужен
свой источник подписи, переопределите только проверку: `create_app()` принимает
`storage`, а проверка подписи вынесена в отдельную функцию `verify_hmac_sha256()` —
её можно вызвать самостоятельно, не поднимая FastAPI.

Ingress-контракт живёт в `schemas.py` (`InboundWebhookPayload`).
`adapters/source_adapter.py` импортирует её оттуда и держит совместимый алиас
`WebhookPayload = InboundWebhookPayload` — собственных Pydantic-моделей в
адаптере нет, дубль схемы в адаптере ломает AST-тест
(`test_source_adapter_imports_webhook_payload_from_schemas`).

Рекомендуемый smoke: отдать событие в `storage.claim_event` и убедиться, что
`first_processed` для повторного `event_id` равен `False` — поведение дедупа не меняется.

## 2. StorageAdapter → ChromaDB

**Что реально есть в коде.** `adapters/storage_adapter.py` — тоже **модуль функций**:
класса `StorageAdapter`, который можно унаследовать, нет
(`from adapters.storage_adapter import StorageAdapter` → `ImportError`).
Контракт хранилища раскладывается на две части:

| Часть | Где живёт | Что требуется от реализации |
|---|---|---|
| Порт дедупа | `test_suite/qa_runner_core.py:StoragePort` (Protocol) | `async def claim_event(self, event_id: str) -> Dict[str, Any]` |
| CRUD-операции состояния | `adapters/storage_adapter.py` | `init_storage`, `claim_event`, `get_event`, `write_dlq`, `replay_dlq` |

Реализация для ChromaDB должна предоставлять **те же 5 операций** (те же имена и смысл),
чтобы подменять хранилище без правок ingress/egress:
```python
from typing import Any, Dict


class ChromaStorage:
    """Хранит состояние/дедуп в ChromaDB; контракт claim_event сохраняется."""

    def __init__(self, path: str = "./chroma"):
        self._client = None      # chromadb.PersistentClient(path=path)
        self._collection = None   # self._client.get_or_create_collection("events")

    async def claim_event(self, event_id: str) -> Dict[str, Any]:
        """Единственный метод, который вызывает ingress (`create_app`)."""
        # upsert с проверкой существования -> {"event_id": ..., "first_processed": bool}
        raise NotImplementedError
```

Обязательно прогнать smoke-тест контракта (раздел 0) с адаптером под Chroma.
Реализация на подклассе **не обязательна**: smoke-тест раздела 0 проверяет
5 функций модуля, поэтому для Chroma достаточно написать модуль с теми же именами
(`init_storage`, `claim_event`, `get_event`, `write_dlq`, `replay_dlq`) и подменить
импорт.

## 3. EgressAdapter → amoCRM / Bitrix24 / Google Sheets

Шаг 1: унаследуйтесь от `EgressAdapter` и переопределите `build_request`.

**Точка расширения уже реализована в базовом классе.** Это не абстрактная
идея и не TODO: `EgressAdapter.build_request(...)` в
`adapters/egress_adapter.py` (строки 197–215) — рабочая реализация Anti-Corruption
Layer, а подкласс переопределяет только маппинг запроса.

```python
def build_request(self, event: Dict[str, Any]) -> Tuple[str, Dict[str, str], Any]:
    """Map an internal event to ``(url, headers, body)`` for the wire."""

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

Контракт метода:

| Аспект | Значение |
|---|---|
| Сигнатура | `build_request(self, event: Dict[str, Any]) -> Tuple[str, Dict[str, str], Any]` |
| Возвращает | кортеж `(url, headers, body)`; `headers` — плоский `Dict[str, str]` |
| Синхронность | метод **синхронный** (`await` внутри запрещён) |
| Чистота | без сетевых вызовов и без побочных эффектов — только маппинг |
| Вызовы | `send()` вызывает `build_request()` на **каждой** попытке отправки |
| Дефолт | URL из `config["egress"]["url"]`, заголовок `Idempotency-Key`, `Authorization: Bearer <token>` |

Подкласс переопределяет только маппинг, опираясь на DTO из `schemas.py`:

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

Классификация `429`/`5xx`, full-jitter backoff, обработка таймаутов и запись в
DLQ наследуются без изменений — меняется только маппинг запроса (1 шаг).

### 3.1. Взаимодействие со встроенным `TokenBucketRateLimiter`

Базовый `EgressAdapter.__init__` сам создаёт `TokenBucketRateLimiter` из секции
`rate_limit` конфига и кладёт его в `self.rate_limiter`:

```python
rate_cfg = self.config.get("rate_limit") or {}
self.rate_limiter = TokenBucketRateLimiter(
    rate_limit_rps=float(rate_cfg.get("rate_limit_rps", DEFAULT_RATE_LIMIT_RPS)),  # 2.0
    burst=int(rate_cfg.get("burst", DEFAULT_RATE_LIMIT_BURST)),                      # 5
    sleep=self.sleep,
)
```

`send()` берёт permit **до** каждого `client.post` — троттлинг проактивный,
а не реактивный:

```python
await self.rate_limiter.acquire()
response = await client.post(url, json=body, headers=headers)
```

Что это значит для наследника:

- **Не создавайте свой rate limiter.** Бакет уже есть у адаптера; второй бакет
  лишь снизит пропускную способность и разъедется с конфигом.
- **Не обходите `send()`.** Собственный `httpx`-клиент в подклассе потеряет
  троттлинг, ретраи, классификацию статусов и запись в DLQ.
- **Лимиты настраиваются конфигом, а не кодом**: `rate_limit.rate_limit_rps` и
  `rate_limit.burst` в `test_suite/config.json` (дефолты — 2.0 rps / burst 5).
  Приоритет: **JSON побеждает `.env`** (`setdefault` в `test_suite/config_loader.py`) —
  чтобы начал действовать `RATE_LIMIT_RPS` из `.env`, ключ должен быть удалён из JSON.
- **Тесты детерминированы**: `clock` и `sleep` у `TokenBucketRateLimiter`
  инъецируются, поэтому проверка троттлинга не зависит от реального времени.
- Подкласс наследует то же поведение: permit берётся перед каждой попыткой,
  включая повтор после ротации токена на 401.

### 3.2. Безопасная ротация токена при наследовании

Ротация при 401 реализована в базовом классе и уже защищена от гонок:
`asyncio.Lock()` (`self._token_lock`) в сочетании с монотонным счётчиком версии
(`self._token_version`) делает обновление **single-flight** — параллельные
запросы не инвалидируют токен одновременно, а повторно используют уже
обновлённый:

```python
async def refresh_token(self, seen_version: Optional[int] = None) -> Optional[str]:
    async with self._token_lock:
        if seen_version is not None and self._token_version != seen_version:
            return self._token  # другой корутине уже ротировал токен
        new_token = await self._token_refresher()
        if new_token:
            self._token = new_token
        self._token_version += 1
        return self._token
```

Правила для наследника:

- **Читайте токен через свойство `self.token`**, а не через
  `self.config["egress"]["token"]`: в конфиге остаётся старый токен, а
  `self._token` уже обновлён после ротации.
- **Не создавайте свой `_token_lock`** — блокировка уровня адаптера уже даёт
  single-flight; собственный lock вернёт параллельную ротацию.
- **Не вызывайте `refresh_token()` из `build_request()`** — это чистая функция
  маппинга; побочный эффект нарушит чистоту ACL и детерминированность тестов.
- **Ротация за одну отправку — не более одного раза** (`rotated_after_401`):
  повторный 401 уходит в DLQ, а не в бесконечный цикл попыток.
- Если нужен собственный источник токена — передайте `token_refresher` в
  конструктор, а не переписывайте `send()`.

---

## 4. Конфигурация без правок кода (Composability)

| Ключ `test_suite/config.json` | Назначение | Приоритет (реализовано в `test_suite/config_loader.py`, строки 64–77) |
|---|---|---|
| `hmac_secret` | секрет проверки подписи ingress | **перезаписывается** `GatewaySettings.HMAC_SECRET` (env / `.env`) — значение `${HMAC_SECRET}` в JSON служит только плейсхолдером |
| `egress.url` | downstream endpoint (HubSpot / amoCRM / Bitrix24 / Sheets) | **перезаписывается** `GatewaySettings.egress_url` = `{AMOCRM_BASE_URL}/crm/v3/objects/contacts` |
| `egress.token` | Bearer-токен исходящего запроса | **перезаписывается** `GatewaySettings.AMOCRM_TOKEN` |
| `storage.db_path` | путь к SQLite-базе состояния | **перезаписывается** `GatewaySettings.SQLITE_DB_PATH` |
| `replay.max_age_s` | окно replay (по умолчанию 300 с) | только из JSON |
| `retry.max_attempts` / `retry.base_delay_s` | число попыток и база задержки Egress | только из JSON |
| `backoff.strategy` = `full_jitter` | стратегия задержки | только из JSON |
| `circuit_breaker.max_passes` = 2 | лимит проходов QA-Fixer | только из JSON |
| `storage.busy_timeout_ms` = 5000, `storage.journal_mode` = WAL | параметры SQLite | только из JSON |
| `rate_limit.rate_limit_rps` = 2.0 | скорость пополнения token bucket (Egress) | **JSON побеждает env** (`setdefault`) |
| `rate_limit.burst` = 5 | ёмкость burst — сколько запросов проходит без ожидания | **JSON побеждает env** (`setdefault`) |

Правило `setdefault`: пока ключ присутствует в `test_suite/config.json`, значение из
`.env` (`RATE_LIMIT_RPS` / `RATE_LIMIT_BURST`) игнорируется — и наоборот, для
`hmac_secret` / `egress.*` / `storage.db_path` JSON всегда проигрывает среде.
Подробности и «gotcha» — в `MAINTENANCE.md`, §1.3.

Терминология едина: `source_adapter`, `storage_adapter`, `qa_runner_core`,
`egress_adapter`, `DLQ`, `bug_matrix` — без синонимов.

---

## 5. Верификация расширения

После изменения подкласса (или `build_request`) прогон обязателен в двух
режимах: локально и в изолированном Docker-контуре.

**Локально — быстрый цикл:**

```bash
cd /home/dandi/agent-factory/sessions/connector_qa_runner_build/code/
pytest test_suite -q --network=none
```

Ожидаемо: `155 passed`. Если бинарник `pytest` не на PATH:

```bash
python3 -m pytest test_suite -q --network=none
```

**Docker-песочница — полный изолированный прогон:**

```bash
cd /home/dandi/agent-factory/sessions/connector_qa_runner_build/code/
docker compose run --rm test-runner
```

Ожидаемо: `155 passed`. Контур изолирован: `network_mode: "none"` (любой
исходящий запрос падает на уровне рантайма), `read_only: true` (файловая
система контейнера неизменяема), tmpfs для `/tmp`, `/dev/shm`,
`/app/.pytest_cache`. Внутри контейнера выполняется
`CMD ["pytest", "test_suite", "-q", "--network=none"]` (`Dockerfile`).

Альтернативная форма с пересборкой образа:

```bash
docker compose up --build --abort-on-container-exit
```

Точечная проверка только контракта ACL и троттлинга:

```bash
pytest test_suite/test_egress_acl.py test_suite/test_amocrm_contracts.py -q --network=none
# → 54 passed
```

```bash
docker compose run --rm test-runner pytest test_suite/test_egress_acl.py -q --network=none -k "token_bucket or rate_limit"
# → 7 passed, 12 deselected
```

**Чек-лист приёмки расширения:**

```
[ ] 1. Подкласс переопределяет только build_request()
[ ] 2. Токен читается через self.token, заголовок Idempotency-Key сохранён
[ ] 3. DTO валидируются моделью из schemas.py
[ ] 4. Свой TokenBucketRateLimiter / свой _token_lock / refresh_token() из
       build_request() не добавлялись
[ ] 5. pytest test_suite -q --network=none            → 155 passed
[ ] 6. docker compose run --rm test-runner             → 155 passed
```