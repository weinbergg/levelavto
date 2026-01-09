## Mobile.de ежедневный импорт

Файлы mobilede_active_offers.csv доступны после 09:00 UTC (≈12:00 МСК). Скрипт `scripts/fetch_mobilede_csv.sh`:
1. Берёт дату UTC, при 404 пробует вчера.
2. Скачивает в `backend/app/imports/mobilede/mobilede_active_offers_YYYY-MM-DD.csv` и ставит ссылку `backend/app/imports/mobilede_active_offers.csv`.
3. Запускает импорт без деактивации: `python -m backend.app.tools.mobilede_csv_import --skip-deactivate --file backend/app/imports/mobilede_active_offers.csv`.

### Переменные окружения
```
MOBILEDE_HOST=https://parsers1-valdez.auto-parser.ru
MOBILEDE_USER=admin
MOBILEDE_PASS=8C8CuVAgnBKARBKNchBr
WEB_SERVICE=web   # docker-compose service name
```

### Ручной запуск
```
bin/bash scripts/fetch_mobilede_csv.sh
```

### Cron (UTC)
Пример в `deploy/cron.mobilede`:
```
15 9 * * * cd /opt/levelavto && /bin/bash scripts/fetch_mobilede_csv.sh >> /var/log/mobilede_fetch.log 2>&1
```
Добавьте в crontab на хосте. Если используется systemd — аналогично через timer, вызывая тот же скрипт.
