# VPS Ops Checklist (2 CPU / 2-4 GB RAM / 80 GB disk)

## 1) Deploy and migrate

```bash
cd /opt/levelavto
git pull
docker compose build web
docker compose up -d --force-recreate web
docker compose exec -T web alembic -c migrations/alembic.ini upgrade head
```

## 1.1) One-command full pipeline

```bash
cd /opt/levelavto
chmod +x scripts/deploy_full_rebuild_check.sh
./scripts/deploy_full_rebuild_check.sh
```

### 1.2) One-command full pipeline (hot brands only, safe timeout)

```bash
cd /opt/levelavto
PREWARM_MAX_SEC=420 \
PREWARM_INCLUDE_BRAND_CTX=0 \
PREWARM_INCLUDE_MODEL_CTX=0 \
PREWARM_INCLUDE_BRAND_LISTS=1 \
PREWARM_EU_COUNTRY=DE \
HOT_CACHE_BRANDS="BMW,Audi,Mercedes-Benz,Porsche,Skoda,Toyota,Volkswagen,Volvo,Aston Martin,Bentley,Bugatti,BYD,Cadillac,Ferrari,GMC,Hummer,Hyundai,Jaguar,Jeep,Kia,Lamborghini,Land Rover,Lexus,Lincoln,Lynk&Co,Maybach,Mazda,McLaren,Mini,Rolls-Royce,Tesla,Zeekr" \
./scripts/deploy_full_rebuild_check.sh
```

## 2) Verify health and key endpoints

```bash
cd /opt/levelavto
curl -sS http://localhost:8000/health
curl -sS "http://localhost:8000/api/cars_count?region=EU&country=DE&brand=BMW" | jq
curl -sS "http://localhost:8000/api/advanced_count?region=EU&country=DE&brand=BMW&model=X5" | jq
curl -sS "http://localhost:8000/api/cars?region=EU&country=DE&brand=BMW&sort=price_asc&page=1&page_size=12" | jq '.total, .items[0].brand, .items[0].model, .items[0].price_note'
curl -sS "http://localhost:8000/api/filter_ctx_brand?region=EU&country=DE&brand=BMW" | jq '.models | length, .model_groups | length'
```

## 3) Cold/warm cache check for heavy scenarios

```bash
cd /opt/levelavto
for i in {1..5}; do
  curl -s -o /dev/null -w "cars_count_de_bmw: %{http_code} %{time_total}\n" \
    "http://localhost:8000/api/cars_count?region=EU&country=DE&brand=BMW"
done

for i in {1..5}; do
  curl -s -o /dev/null -w "advanced_count_de_bmw_x5: %{http_code} %{time_total}\n" \
    "http://localhost:8000/api/advanced_count?region=EU&country=DE&brand=BMW&model=X5"
done

for i in {1..5}; do
  curl -s -o /dev/null -w "cars_list_de_bmw: %{http_code} %{time_total}\n" \
    "http://localhost:8000/api/cars?region=EU&country=DE&brand=BMW&sort=price_asc&page=1&page_size=12"
done

docker compose logs --since=10m web | grep -E "ADVANCED_COUNT|CARS_COUNT|CARS_LIST|count_slow|list_slow"
```

## 4) Full performance audit

```bash
cd /opt/levelavto
COUNTRY=DE BRAND=BMW MODEL=X5 REQUESTS=20 WARMUP=3 TIMEOUT_SEC=15 scripts/perf_full_audit.sh
latest=$(ls -1dt logs/perf_audit/* | head -n1)
echo "$latest"
sed -n '1,220p' "$latest/report.md"
cat "$latest/summary.json"
```

## 5) Disk pressure diagnostics

```bash
cd /opt/levelavto
df -h /
docker system df
du -xh --max-depth=1 /opt/levelavto | sort -h | tail -n 25
find /opt/levelavto -type f -size +500M -print
```

## 6) Enable automatic cleanup

```bash
cd /opt/levelavto
chmod +x scripts/system_auto_cleanup.sh
(
  crontab -l 2>/dev/null
  echo "35 4 * * * cd /opt/levelavto && MIN_FREE_GB=12 TARGET_USAGE_PCT=85 CSV_DAYS=7 LOG_DAYS=14 THUMB_DAYS=10 scripts/system_auto_cleanup.sh >> /opt/levelavto/logs/system_auto_cleanup.log 2>&1"
) | crontab -
crontab -l
```

## 6.1) Optional docker cleanup twice per week

```bash
(
  crontab -l 2>/dev/null
  echo "20 5 * * 2,6 docker builder prune -af >/dev/null 2>&1"
) | crontab -
crontab -l
```

## 7) One-shot cleanup (safe)

```bash
cd /opt/levelavto
DRY_RUN=1 scripts/system_auto_cleanup.sh
DRY_RUN=0 scripts/system_auto_cleanup.sh
```

## 7.1) Cache maintenance

```bash
cd /opt/levelavto
chmod +x scripts/cache_maintenance.sh
scripts/cache_maintenance.sh
```

Soft purge (heavy cache keys only):
```bash
cd /opt/levelavto
PURGE_SOFT=1 scripts/cache_maintenance.sh
```

Hard purge + dataset version bump:
```bash
cd /opt/levelavto
PURGE_HARD=1 BUMP_DATASET=1 scripts/cache_maintenance.sh
```

## 8) Cache prewarm after deploy/restart

```bash
cd /opt/levelavto
docker compose exec -T web python -m backend.app.scripts.prewarm_cache
```

## 9) Recalculate prices only if needed (FX/customs changed)

```bash
cd /opt/levelavto
docker compose exec -T web python -m backend.app.scripts.update_fx_prices --batch 1000 --limit 0
docker compose exec -T web python -m backend.app.scripts.recalc_cached_prices
```

## 10) Optional: inspect indexes used by slow DE/BMW query

```bash
cd /opt/levelavto
docker compose exec -T db psql -U postgres -d autodealer -c "
EXPLAIN (ANALYZE, BUFFERS)
SELECT id
FROM cars
WHERE is_available = true
  AND country = 'DE'
  AND brand = 'BMW'
ORDER BY COALESCE(total_price_rub_cached, price_rub_cached) ASC NULLS LAST, id ASC
LIMIT 12;
"
```
