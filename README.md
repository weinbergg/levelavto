# Автокаталог с парсерами (FastAPI + Postgres)

Полноценный проект автодилера: каталог авто с фильтрацией, рендер страниц (Jinja2), JSON API и парсеры источников (`mobile.de`, `encar`, `emavto_klg`). Готов к запуску на одном VPS через `docker-compose`.

## Стек
- Backend: Python 3.11, FastAPI, SQLAlchemy 2.0, Alembic
- База данных: PostgreSQL
- HTTP/Parsing: httpx, BeautifulSoup4
- Frontend: Jinja2-шаблоны, легкий JS (fetch + History API)
- Инфраструктура: docker-compose, (пример) nginx

## Структура
```
project-root/
  docker-compose.yml
  sites.txt
  backend/
    Dockerfile
    requirements.txt
    app/
      main.py, config.py, db.py
      models/ (Source, Car)
      schemas/
      routers/ (pages, catalog)
      services/ (CarsService)
      parsing/ (base, mobile_de, encar, emavto_klg, runner)
      templates/ (base, index, catalog, car_detail)
      static/ (css, js, img)
  migrations/ (alembic.ini, env.py, versions/0001_init.py)
  deploy/nginx.conf
```

## Быстрый старт (dev/prod)

1) Подготовьте `.env` по образцу:
```
WEB_PORT=8000
DB_HOST=db
DB_PORT=5432
DB_USER=autodealer
DB_PASSWORD=autodealer
DB_NAME=autodealer
DB_SYNC_ECHO=false
PARSER_USER_AGENT=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36
PARSER_REQUEST_TIMEOUT_SECONDS=20
PARSER_MIN_DELAY_SECONDS=1
PARSER_MAX_DELAY_SECONDS=3
PARSER_LOG_FILE=logs/parsing.log
APP_ENV=development
APP_SECRET=please-change-me
```

2) Запустите:
```
docker-compose up --build
```
После успешного запуска:
- Приложение: http://localhost:8000
- Автомиграции применяются автоматически в командной строке контейнера web перед запуском `uvicorn`.

Если нужно вручную:
```
docker-compose run --rm web alembic -c migrations/alembic.ini upgrade head
```

## Наполнение `sites.txt`
Файл `sites.txt` в корне репозитория определяет, какие парсеры запускать при `--all`. Пример:
```
mobile.de
encar
emavto_klg
```

## Парсеры
Парсеры располагаются в `backend/app/parsing`. Базовый класс — `BaseParser` (логирование, задержки, сетевые настройки). Для каждого источника реализован адаптер:
- `MobileDeParser` (`mobile.de`, страна DE)
- `EncarParser` (`encar`, страна KR)
- `EmAvtoKlgParser` (`emavto_klg`, страна RU)

Текущая реализация — устойчивая заготовка: базовые запросы и очень упрощенная выборка ссылок как демо (по тэгам `<a>`). В реальной эксплуатации потребуется уточнение селекторов/эндпоинтов, пагинации и нормализации полей (бренд, модель, цена, валюта и т.д.). В коде уже предусмотрена модель данных для полноты.

### Обновление и доступность
- Новые и изменившиеся объявления — upsert по уникальному ключу `(source_id, external_id)`.
- Отсутствующие после очередного запуска из конкретного источника помечаются `is_available=false`.

## CLI для парсинга
Запуск из корня репозитория:
```
python -m backend.app.parsing.runner --all
python -m backend.app.parsing.runner --source mobile.de
```

## API
- HTML:
  - `/` — главная (лендинг)
  - `/catalog` — каталог с фильтрами
  - `/car/{car_id}` — карточка
- JSON:
  - `GET /api/cars` — список с фильтрами:
    - `country`, `brand`, `price_min`, `price_max`, `year_min`, `year_max`, `mileage_max`, `page`, `page_size`
    - формат ответа:
      ```json
      {
        "items": [...],
        "total": 123,
        "page": 1,
        "page_size": 20
      }
      ```
  - `GET /api/cars/{id}` — детальная информация

## Админка и личный кабинет
- Маршруты: `/login`, `/register`, `/logout`, `/account`, `/admin`.
- Первый зарегистрированный пользователь получает флаг администратора.
- Админ может:
  - Редактировать ключевые тексты лендинга (заголовок, подзаголовок, примечание hero).
  - Назначать подборки авто (популярные/рекомендуемые на главной и в каталоге) по ID через форму.
  - Если подборка пустая, на витрине показываются последние авто с фото.
- Личный кабинет позволяет обновить имя и пароль.
- Не забудьте применить миграцию `0004_admin_auth_content`:
  ```
  docker-compose run --rm web alembic -c migrations/alembic.ini upgrade head
  ```

## Фильтры и пагинация
На странице `/catalog` форма обновляет список через `fetch`. Есть примитивный спиннер и кнопки навигации по страницам.

## nginx (пример)
См. `deploy/nginx.conf` — проксирование `domain.ru -> web:8000`. Для TLS добавьте конфигурацию `listen 443 ssl` и сертификаты/ключи (например, через certbot).

## Тесты
Рекомендуется добавить тестовые HTML/JSON-снимки страниц источников и написать юнит-тесты для парсеров (примеров в данном MVP нет).

## Ограничения и нюансы
- Источники могут менять разметку/эндпоинты — актуализируйте селекторы и маппинг полей.
- Уважайте правила источников: разумные задержки, корректный User-Agent (настраивается через `.env`), частота запросов.
- Для prod используйте очередь/пул задач, ретраи и лимит скорости.

## Cron на проде
Пример ежедневно в 03:00:
```
0 3 * * * cd /opt/autodealer && /usr/bin/python3 -m backend.app.parsing.runner --all >> /var/log/autodealer_parsing.log 2>&1
```

## Конфигурация парсеров (sites_config.yaml)
Файл `backend/app/parsing/sites_config.yaml` описывает каждый источник в декларативном виде:
- `base_search_url`, `query_params` (соответствие логических параметров профилей → query-параметрам сайта),
- `selectors` (CSS селекторы для HTML),
- `pagination` (параметры пагинации).
- `enabled` (true/false) — временно отключить источник, не меняя код.
Заполните значения на основе DevTools целевых сайтов. Это позволит менять парсинг без правки Python-кода.

## Профили поиска
Создавайте записи в таблице `search_profiles` (через SQL/psql или будущий админ-интерфейс) для задания брендов/цен/годов и пр. активных фильтров. Парсер загрузит активные профили автоматически.

## Запуски и отчеты
- Все запуски фиксируются в `parser_runs` и `parser_run_sources` (вставки/обновления/деактивации).
- Инкрементальные обновления: уникальный ключ `(source_id, external_id)` и столбец `hash` позволяют пропускать неизменившиеся объявления.
- Если один источник падает, общий запуск помечается как `partial`, падающий источник логируется и записывается в `parser_runs.error_message`, остальные продолжают выполняться.
- Чтобы отключить проблемный источник (например, Encar), выставьте `enabled: false` для него и выполните `--all` — будут обработаны только включённые.

## Диагностика качества парсинга
Для быстрой проверки заполненности полей и адекватности диапазонов значений есть CLI:
```
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source all
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source mobile_de
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source encar
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source emavto_klg
```
Отчёт выводит:
- итоги последнего прогона (total_seen/inserted/updated/deactivated),
- общее число автомобилей по источнику,
- заполненность каждого поля (сколько NOT NULL), min/max для year/price/mileage,
- частые значения для `brand`, `currency`, `country`.
По умолчанию берётся малый сэмпл: `pagination.max_pages=2` в `sites_config.yaml`. Увеличьте позже для продакшена.

### Импорт CSV mobile.de
```
docker-compose run --rm web \
  python -m backend.app.tools.mobilede_csv_import \
  --file /app/imports/mobilede_active_offers.csv \
  --trigger manual
```
Источник `mobile_de` должен быть `enabled: false` (мы не скрейпим сайт), но зарегистрирован в БД. После импорта прогоните диагностику:
```
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source mobile_de
```

### Сброс данных Encar
Если в БД накопился мусор по Encar, используйте:
```
docker-compose run --rm web python -m backend.app.tools.encar_reset        # мягко: is_available=false
docker-compose run --rm web python -m backend.app.tools.encar_reset --hard # жёстко: DELETE cars/images
```
Затем перезапустите парсер:
```
docker-compose run --rm web python -m backend.app.parsing.runner --source encar --trigger manual
docker-compose run --rm web python -m backend.app.tools.parsing_diagnostics --source encar
```




