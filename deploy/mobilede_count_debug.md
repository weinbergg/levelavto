# Mobile.de count audit (CSV vs DB)

Цель: понять, сколько объявлений реально в CSV и сколько попало в БД.

## 1) CSV аудит (без БД)

```bash
docker compose exec -T web \
  python -m backend.app.tools.mobilede_csv_audit \
  --file /app/backend/app/imports/mobilede_active_offers.csv
```

Проверка по списку брендов (если есть список от заказчика):

```bash
docker compose exec -T web \
  python -m backend.app.tools.mobilede_csv_audit \
  --file /app/backend/app/imports/mobilede_active_offers.csv \
  --brands "BMW,Mercedes-Benz,Audi,Volkswagen"
```

Что смотреть:
- `total_rows` — сколько строк реально в CSV.
- `distinct_external_id(inner_id)` — сколько уникальных объявлений.
- `missing_*` — доля строк с пустыми ключевыми полями.

## 2) DB аудит

```bash
docker compose exec -T web \
  python -m backend.app.tools.mobilede_db_audit \
  --source mobile_de
```

Проверка по списку брендов (если есть список от заказчика):

```bash
docker compose exec -T web \
  python -m backend.app.tools.mobilede_db_audit \
  --source mobile_de \
  --brands "BMW,Mercedes-Benz,Audi,Volkswagen"
```

Что смотреть:
- `cars_total` и `distinct_external_id` — сколько реально сохранено в БД.
- `cars_without_thumbnail` и `car_images_total` — качество по фото.

## Как интерпретировать разницу

- Если `total_rows` в CSV около 650k — это столько отдает провайдер (вопрос к провайдеру).
- Если `total_rows` ~1.2M, а в БД ~650k — значит на импорте есть фильтрация/дедуп/ошибки.
- Сравнивайте `distinct_external_id` CSV vs DB — это главный ориентир.
