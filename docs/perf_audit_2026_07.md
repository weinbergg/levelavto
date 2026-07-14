# Performance Audit — Public Catalog Serving Path

**Scope:** LevelAvto (`/opt/levelavto`) — read-only audit проведён 2026-07-14
**Стек:** FastAPI + Gunicorn/UvicornWorker, SQLAlchemy 2.x (sync), Postgres 16, Redis 7, Nginx 1.28.
**Хост:** Ubuntu 26.04 LTS, 4 vCPU / 16 GB RAM / SSD (Reg.ru Cloud, IP 80.78.246.107).
**Цель:** обосновать пакет патчей, дающих 30–40 % ускорения без изменения расчётной логики (`calculator.yml`, `customs.yml`, `calculator_runtime.py`).

Аудит основан на статическом анализе кода. Числовые бенчмарки должны быть сняты *до* и *после* каждого патча через `scripts/perf_full_audit.sh` на боевом VPS с наполненной БД. Это документ — план работы, а не пост-фактум отчёт.

Гарантии, которые сохраняются во всех патчах ниже:
- **Точность расчётов**: НЕ трогаем YAML/логику калькулятора; изменения только в слое хранения/выдачи/кеша.
- **Фильтры не пропадают**: изменения покрыты существующими тестами (`backend/tests/test_ui_filter_contracts.py`), плюс каждое сокращение facet-путей делается только при подтверждении паритета через тест.
- **Фотографии всегда грузятся**: `/thumb` фолбэки и негативный кеш 404 остаются нетронутыми; перекладывание в nginx делается через `alias` на тот же каталог, что использует FastAPI.

---

## 1. Count-эндпоинты (`/api/cars_count`, `/api/advanced_count`, `/api/filter_ctx_*`)

### 1.1 Роуты и цепочка вызовов

| Endpoint | Файл:строки | Сервис |
|----------|-------------|--------|
| `GET /api/cars_count` | `backend/app/routers/catalog.py:975–1291` | `count_cars()` или `list_cars(count_only=True)` |
| `GET /api/advanced_count` | `backend/app/routers/catalog.py:1294–1461` | всегда `list_cars(count_only=True)` |
| `GET /api/filter_ctx_base` | `backend/app/routers/catalog.py:1582–1734` | `facet_counts()` × N, `payload_values_bulk_filtered()` |
| `GET /api/filter_ctx_brand` | `backend/app/routers/catalog.py:1760–1815` | `models_for_brand_filtered()`, `build_model_groups()` |
| `GET /api/filter_ctx_model` | `backend/app/routers/catalog.py:1818–1886` | `SELECT DISTINCT generation` |

### 1.2 Ключевая находка: `_can_fast_count()` блокирует агрегаты при фильтре по модели

```1270:1315:backend/app/services/cars_service.py
        if model:
            return False
        if any([lines, source_key, q, generation, color, ... kr_type, ...]):
            return False
        return True
```

Даже при наличии агрегата `car_counts_model` (миграция `0024_counts_tables_split.py`) фильтр `brand+model` **всегда** идёт живым `count(*)` по `cars`. По прикидкам это 20–30 % каталогового трафика.

### 1.3 Прочие «дыры» fast-count

- `strict_local_photo_mode` (для EU) — отключает `_fast_count` (`cars_service.py:2633, 2688`).
- `color_group` в `filter_ctx_base` — всегда живой скан, а не агрегат (`cars_service.py:1520–1521`).
- `filter_ctx_*` **без dogpile-lock** — на холодном Redis N воркеров одновременно строят один и тот же ответ.

### 1.4 Оценка покрытия агрегатами (при холодном Redis)

| Фильтр | Fast-aggregate? | Доля трафика | Комментарий |
|--------|:---------------:|:------------:|-------------|
| Только region/country | ✅ `car_counts_core` | 10–20 % | Есть и «простой» Redis-ключ |
| + brand (без model) | ✅ `car_counts_brand` | 15–25 % | Простой ключ для hot brand |
| + model | ❌ | 20–30 % | **Живой count несмотря на существующий агрегат** |
| Любой facet | ❌ | 35–50 % | `advanced_count` = 100 % живой |
| `line` / `q` / `source` | ❌ | 5–15 % | Уходит в `list_cars` |

### 1.5 Патчи

| Приоритет | Патч | Ожидаемый выигрыш | Риск |
|:---------:|------|-------------------|------|
| **P0** | Разрешить `_can_fast_count` для `brand+model`; догнать `car_counts_model` в cron | 200 мс – 2 с на count | Низкий, если рефрешить агрегаты перед `BUMP_DATASET` |
| **P0** | Пред-агрегация `color_group` в отдельную таблицу либо в `car_counts_core` | 500 мс – 3 с cold `filter_ctx_base` | Низкий |
| **P1** | `redis_try_lock` на `filter_ctx_*` (аналогично `cars_count`) | Только tail-latency | Нулевой |
| **P1** | `advanced_count`: узкий SQL под count-only с partial-индексами | Высокий для советской «продвинутой формы» | Средний — покрыть контракт-тестами |

---

## 2. `GET /api/cars` (список)

### 2.1 Кеш и dogpile

- Простой ключ `build_cars_list_key` (region/country/brand/sort/page/size/photo) — TTL по умолчанию **21600 с**.
- Полный ключ `build_cars_list_full_key` — для остальных комбо.
- Dogpile-lock 25 с + wait 2.2 с (`catalog.py:745–751, 805–811`).
- **Даже на cache-hit** вызывается `sync_light_rows_from_db(refresh_prices=…)` (`catalog.py:826–835`) — лишний `SELECT ... WHERE id IN (...)` при включённом refresh.

### 2.2 SQL и сортировки

```2779:2819:backend/app/services/cars_service.py
            stmt = (
                select(Car.id, Car.brand, ..., Car.calc_breakdown_json, ...)
                .where(where_expr)
                .order_by(*(([thumb_rank] if use_thumb_rank else [])), *order_clause)
            )
```

| Сорт | Order-clause | Индекс |
|------|--------------|--------|
| `price_asc/desc` (полный путь) | `_public_display_price_rub_expr()` + JSON `LIKE '%__without_util_fee%'` | **Плохо** — не совпадает с `idx_cars_country_price_coalesce_avail` (0033) |
| `price_asc/desc` (light window) | `COALESCE(total_price_rub_cached, price_rub_cached)` | ✅ Совпадает с 0033 |
| `price_*` + `engine_type` | Тот же + `lower(trim(engine_type))` | ✅ `idx_cars_country_engine_price_coalesce_avail` (0036) |
| `listing_*` | `Car.listing_sort_ts` | ✅ 0023/0017 |
| `mileage_*` | `Car.mileage` | ✅ 0023 |
| `reg_*` | `Car.reg_sort_key` | ✅ 0023 |

Light-price-window (`cars_service.py:1771–1801`) читает 240–2000 строк и сортирует в Python. По страницам 1–5 это тратит IO даже при попадании в кеш.

### 2.3 N+1 — не найдено

- Списочный `list_cars` тянет `CarImage` одним `IN (...)` (`catalog.py:914–931`).
- Детальный `get_car` использует `selectinload(Car.images)` (`cars_service.py:3788`).
- `calc_breakdown_json` в проекции — не N+1, но раздувает строки.

### 2.4 Патчи

| Приоритет | Патч | Выигрыш | Риск |
|:---------:|------|---------|------|
| **P0** | Дефолтную сортировку `price_*` завести на coalesce-путь как в `light-price-window`; JSON-check делать только для дорасчёта финального порядка в Python-слое | 100 мс – 1 с на EU list | **Средний** — нужен полный набор снапшот-тестов сортировки, чтобы гарантировать «те же машины на первой странице» |
| **P1** | Убрать `calc_breakdown_json` из проекции light-списка | 20–100 мс на страницу | Низкий |
| **P1** | Отключить `sync_light_rows_from_db` при cache-hit по умолчанию | 20–80 мс cache-hit | Нулевой |
| **P2** | Расширить набор filters под «простой ключ» (photo, engine_type, kr_type) | Медиум на cold Redis | Низкий |

---

## 3. `ensure_calc_cache` (ленивый recalc)

**Файл:** `backend/app/services/cars_service.py:3378–3785`

### 3.1 Триггеры на пути запроса

| Триггер | По умолчанию |
|---------|:------------:|
| `_lazy_recalc_light_items` после списка | ❌ (только если `_should_catalog_inline_price_refresh()`) |
| `_refresh_price_sensitive_candidates` перед сортировкой по цене (до **120+** машин) | ❌ (тот же гейт) |
| `sync_light_rows_from_db(refresh_prices=True)` на cache-hit | Только `CATALOG_CACHE_HIT_REFRESH_PRICES=1` |
| Детальный `car detail` | Только `DETAIL_INLINE_CALC=1` |

Всё **синхронно**, с `db.commit()` внутри воркера — если случайно включить эти флаги, каждый холодный запрос по цене тащит батч рекалков.

### 3.2 Правильное решение

Оставить как сейчас (по умолчанию OFF). Массовый догон стейлов — ежедневный `recalc_calc_cache.py` cron. `__config_version` в `calc_breakdown_json` протекает от изменений в `calculator.yml` автоматически — не менять эту логику.

**Патч:** нулевой в этой области; вместо этого — таск ежесуточного пересчёта только «протухших» с алертом в Telegram при массовом «протуханий».

---

## 4. Redis

### 4.1 Ключи

| Паттерн | TTL |
|---------|-----|
| `cars_count:{params}:v{dataset}` | 1800 с |
| `cars_count:{r}:{c}:{b}:photo=:v` | 1200 с |
| `cars_list:{r}:{c}:{b}:sort:page:size:v` | 21600 с (env) |
| `cars_list_full:*` | 21600 с |
| `filter_ctx_base:*` | 86400 с |
| `filter_ctx_brand/model:*` | 86400 с |
| `filter_payload:*` | route-specific |
| `dataset_version` | persistent |
| `*:lock` | 20–25 с |

### 4.2 Проблемы

- `maxmemory 256mb, allkeys-lru` (`docker-compose.yml:73–84`) — большие `filter_ctx_base` (~24 ч TTL) вытесняют горячие `cars_list`.
- Dogpile-lock у `cars_count` и `cars_list` есть, у **`filter_ctx_*` нет** — стемпид на холодном Redis.
- Локальный `TTLCache` в `cars_service.py:1222` (counts, 120 c) — по воркеру. 4 воркера = 4 холодных in-process кеша.

### 4.3 Патчи

| Приоритет | Патч | Выигрыш | Риск |
|:---------:|------|---------|------|
| **P1** | Redis `maxmemory 1–2 GB`, отдельные логические `db` под `filter_*` и `list_*` | Устранение LRU-каннибализма | Нулевой |
| **P1** | Dogpile-lock на `filter_ctx_*` | Tail-latency под нагрузкой | Нулевой |
| **P2** | Убрать `TTLCache` для счётчиков; полагаться на Redis | Согласованность между воркерами | Низкий |

---

## 5. Prewarm

Скрипт `scripts/prewarm_public_site.sh` + `backend/app/scripts/prewarm_cache.py` по умолчанию греет только `EU+DE`, без разбивки по брендам. Реальный трафик — гораздо шире.

| Реальный трафик | Прогревается по умолчанию? |
|-----------------|:--------------------------:|
| `EU` без страны | Только с `PREWARM_INCLUDE_BROAD_*=1` |
| `EU+DE+brand` | Только с `PREWARM_INCLUDE_BRAND_COUNTS/LISTS=1` |
| `engine_type=diesel` | С `PREWARM_INCLUDE_ENGINE_LISTS=1` |
| `filter_ctx_brand` | С `PREWARM_INCLUDE_BRAND_CTX=1` |
| KR | С `PREWARM_INCLUDE_KR_PUBLIC=1` |
| `cars_list_full:*` | Никогда |
| SSR `/catalog` fallback | curl только на голый URL |

**Патч P1:** включить прогрев hot-brands + KR публичной ветки в дневном pipeline.

---

## 6. HTML / SSR

- `catalog.html` — SSR + client hydration (`static/js/app.js`).
- На каждом cold catalog SSR — **2 лишних вызова `facet_counts(region)` и `facet_counts(country)`** (`pages.py:1976–1988`), даже если `filter_ctx_base` уже в кеше.
- Synchronous fallback `filter_ctx_base` в шаблоне (`pages.py:2027–2036`) может блокировать TTFB на 2–6 с на холодном Redis.
- KR page 1 — client-only (`_catalog_should_defer_initial_items`), это ок при условии, что API KR прогрет.
- Search page (`pages.py:2451–2560`) собирает большой `filter_ctx`. HTML жирный.

### Патчи

| Приоритет | Патч | Выигрыш | Риск |
|:---------:|------|---------|------|
| **P1** | Читать `regions`/`countries` из `base_ctx_cached`, убрать дубли facet_counts на SSR | 20–50 мс SSR | Нулевой |
| **P1** | Slim-версия SSR: рендерить основной каркас, догружать facet payload через AJAX (стереть sync-fallback) | 500 мс – 5 с на cold | Средний — UX смещается «на клиента» |

---

## 7. Изображения / `/thumb`

- `/thumb` (`backend/app/routers/thumbs.py:238+`) — resize + WebP + on-disk кеш в `THUMB_CACHE_DIR` (`/app/thumb_cache`).
- `Cache-Control: public, max-age=604800, stale-while-revalidate=86400`; слабый ETag = basename. Хорошо.
- **Проходит через FastAPI-воркер**. При открытии страницы каталога это десятки-сотни запросов, каждый заходит в python.
- Nginx (`deploy/nginx.conf`, `deploy/nginx.levelavto.ru.conf`) НЕ обслуживает `/thumb_cache` напрямую.

### Патч

| Приоритет | Патч | Выигрыш | Риск |
|:---------:|------|---------|------|
| **P1** | Nginx: `location /thumb_cache/ { alias /var/lib/levelavto/thumb_cache/; expires 30d; add_header Cache-Control "public, immutable"; }` + переписать `Car.thumbnail_local_path` на этот путь. FastAPI обслуживает **только миссы** (`/thumb?u=...`) | 40–70 % CPU-снятие на страницах со списками | Низкий (тот же каталог, тот же файл) |
| **P2** | Пре-генерация webp thumbs в импорте, чтобы `/thumb` cold-miss ушёл вовсе | Ощутимый выигрыш UX на первом заходе | Медиум — надо синхронизировать при переносе VPS |

---

## 8. Gunicorn / Uvicorn / DB pool

```yaml
command: gunicorn backend.app.main:app -k uvicorn.workers.UvicornWorker \
  -w ${WEB_CONCURRENCY:-4} -t 60 --bind 0.0.0.0:8000
```

- **4 воркера × async loop**, но SQLAlchemy — синхронный. Любой блокирующий `db.execute` останавливает loop воркера. При параллельных `/thumb` + `/api/cars` это заметно.
- `--preload` не задан — на импортах памяти × 4.
- Volume `./:/app` — прод-читает исходники с хостовой FS.

### Патчи

| Приоритет | Патч | Выигрыш | Риск |
|:---------:|------|---------|------|
| **P0** | Заменить `UvicornWorker` на `gthread` (или `SyncWorker` + `threads=4`) для FastAPI (sync ORM) ИЛИ мигрировать ORM на async | Устраняет event-loop-блокировки при sync-ORM | Средний — прогнать интеграционные тесты |
| **P1** | Добавить `--preload` | Экономия ~200–400 МБ RAM | Низкий |
| **P1** | Уменьшить `WEB_CONCURRENCY=3` при поднятии Postgres до 4 GB shared_buffers | Освобождает RAM под БД | Нулевой |

---

## 9. Postgres — undertuning

| Параметр | Сейчас | Рекомендуется под 16 ГБ SSD | Δ |
|----------|:------:|:--------------------------:|:---:|
| `shared_buffers` | **512MB** | ~4 GB | **+3.5 GB** |
| `effective_cache_size` | (не задано → 128 MB) | ~12 GB | **~+12 GB** |
| `work_mem` | **16 MB** | 32–64 MB | +16..48 MB |
| `random_page_cost` | 4.0 | **1.1** | −2.9 |
| `effective_io_concurrency` | (0) | 200 | +200 |
| `maintenance_work_mem` | 64 MB (default) | 512 MB | +448 MB |
| `jit` | on | **off** для OLTP | — |
| `max_wal_size` | 4 GB | ok | — |
| `log_min_duration_statement` | 500 ms | ok | — |

Это **самый недорогой** патч с самым большим ожидаемым эффектом (30–60 % на холодных списках EU по ощущениям — надо подтвердить бенчмарками).

### Патч (`docker-compose.yml`)

```yaml
  db:
    ...
    command: >
      postgres
      -c max_wal_size=8GB
      -c checkpoint_timeout=15min
      -c checkpoint_completion_target=0.9
      -c shared_buffers=4GB
      -c effective_cache_size=12GB
      -c work_mem=32MB
      -c maintenance_work_mem=512MB
      -c random_page_cost=1.1
      -c effective_io_concurrency=200
      -c jit=off
      -c shared_preload_libraries=pg_stat_statements
      -c pg_stat_statements.track=all
      -c log_min_duration_statement=500
```

Риск: параметры валидные, но нужно убедиться, что контейнер получает нужный `shm_size` и что `sysctl vm.overcommit_memory=1` на хосте.

---

## 10. Миграции — избыточные индексы

По логам миграций у `cars` уже ~25 индексов. Кластеры перекрытий:

| Кластер | Индексы | Проблема |
|---------|---------|----------|
| Цена | 8+ индексов (0014, 0015, 0017×2, 0023, 0029, 0031, 0033×2, 0036) | Amplification на INSERT/UPDATE, планировщик путается |
| Listing sort | `idx_cars_available_listing_id` (0016), `idx_cars_country_listing_avail` (0023), `idx_cars_available_listing_sort_id` (0017) | 3 варианта |
| Reg sort | `idx_cars_available_reg_id` (0016), `idx_cars_country_reg_avail` (0023), `idx_cars_available_reg_sort_id` (0017) | 3 варианта |
| Brand/model | 0023 + 0028 создают один и тот же индекс `IF NOT EXISTS` | Безопасный дубль-миграция |
| Country price | 0023 (только `price_rub_cached`) + 0029 (`total_price_rub_cached, price_rub_cached, id`) | 0023 избыточен |

### Патч P2

1. Собрать метрики `pg_stat_user_indexes` за 7 дней после наполнения БД.
2. Дропнуть **неиспользуемые** (0 idx_scan) без риска — конкретный список подтверждается только с реальными данными.
3. Кандидаты «под нож» на первом взгляде: `idx_cars_price_sort` (0014), `idx_cars_price_total_sort` (0015), `idx_cars_country_price_avail`-`price_rub_cached` (0023), одна из трёх листинг-версий, одна из трёх reg-версий.

Риск: **средний**. Только после `pg_stat_user_indexes` и проверки `EXPLAIN ANALYZE` ключевых сортировок.

---

## Матрица приоритетов

| Приоритет | Патч | Оценка выигрыша | Риск для корректности |
|:---------:|------|-----------------|-----------------------|
| **P0** | Postgres tuning под 16 GB (shared_buffers, effective_cache_size, work_mem, random_page_cost, jit off) | **30–60 %** на холодных count/list | Нулевой |
| **P0** | `_can_fast_count` разрешить `brand+model` + гарантированный рефреш `car_counts_model` | **200 мс – 2 с** на count | Низкий (при рефреше агрегатов) |
| **P0** | Дефолтная сортировка `price_*` — на coalesce-путь (уйти от JSON-expr в order_by) | **100 мс – 1 с** на EU list | Средний — требует снапшот-тестов |
| **P0** | Заменить `UvicornWorker` на `gthread` для sync ORM | Устраняет loop-блокировки при параллельной нагрузке | Средний |
| **P1** | Redis 256 MB → 1–2 GB + dogpile-lock на `filter_ctx_*` | Tail-latency, устранение LRU-каннибализма | Нулевой |
| **P1** | Nginx serve `/thumb_cache` alias + `/static` | 40–70 % CPU-снятие на страницах со списками | Низкий |
| **P1** | Prewarm hot-brands + KR публичной ветки | Cold-start | Нулевой |
| **P1** | SSR catalog: убрать дубли facet_counts | 20–50 мс SSR | Нулевой |
| **P2** | Дропнуть избыточные индексы (после `pg_stat_user_indexes`) | Ingest throughput | Средний — верификация обязательна |
| **P2** | Пре-генерация webp thumbs в импорте | UX cold-catalog | Средний |
| **NEVER** | Включать `CATALOG_INLINE_PRICE_REFRESH` без жёстких капов | Мульти-секундные блоки | Ноль на калькуляторе, минус UX |

---

## План проверки эффекта (обязательно)

**Прогонять на VPS с наполненной БД**:

```bash
cd /opt/levelavto

# 1) baseline ДО патчей (после наполнения EU/KR):
COUNTRY=DE BRAND=BMW MODEL=X5 REQUESTS=40 WARMUP=5 TIMEOUT_SEC=20 \
  bash scripts/perf_full_audit.sh
latest=$(ls -1dt logs/perf_audit/* | head -n1)
cp "$latest/summary.json" /root/perf_before.json
cp "$latest/report.md"   /root/perf_before.md

# 2) после каждого патча:
bash scripts/perf_full_audit.sh
latest_after=$(ls -1dt logs/perf_audit/* | head -n1)
diff -u /root/perf_before.md "$latest_after/report.md" | less
```

Плюс отдельно замерять:

- `docker compose logs --since=1h web | grep -E 'list_slow|count_slow'` — фактические медленные запросы.
- `pg_stat_statements` top-20 по `mean_exec_time` и по `total_exec_time`.
- `docker stats` под нагрузкой — RAM/CPU по контейнерам.

---

## Гарантии по бюджету

- **Точность калькулятора**: ни один патч из P0–P2 не трогает `calculator.yml`, `customs.yml`, `calculator_runtime.py`. Все проверки калькулятора (`test_calculator_*`, snapshot-тесты рассчитанных цен) — часть pipeline перед мержом.
- **Фильтры**: контракты покрыты `backend/tests/test_ui_filter_contracts.py`. Каждый патч, меняющий facet-путь, проходит через них.
- **Фото**: `/thumb` FastAPI-endpoint остаётся как есть; nginx подключается через `alias` на тот же on-disk кеш.
- **Откатываемость**: каждый патч — отдельный commit; фича-флаги через env (`CATALOG_USE_FAST_COUNT`, `LAZY_RECALC_ENABLED`, `WEB_CONCURRENCY`) остаются.

*Audit performed read-only. No code, config, or data was modified.*
