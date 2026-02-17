# TODO: Performance and UX Hardening (No Hardcode / No Quick Fixes)

## Completed in this iteration

- Added Redis lock/wait primitives to reduce cache stampede on hot endpoints.
- Added full-filter Redis cache path for `cars_count`.
- Added shared Redis count cache usage in `CarsService.list_cars` to persist totals across workers/restarts.
- Added functional DB indexes for price sorting by `COALESCE(total_price_rub_cached, price_rub_cached)`:
  - by `country`
  - by `source_id`
- Added model grouping API payload in `filter_ctx_brand` (`model_groups`).
- Added frontend rendering support for grouped models via `optgroup`.
- Added VPS auto-cleanup script to prevent disk saturation.
- Extended perf audit script with DE/BMW/DE+BMW+model benchmark cases and top-dirs snapshot.

## Next tasks (priority)

1. Measure cold latency after migration `0033_idx_price_coalesce` and confirm p95 for:
   - `/api/cars_count?region=EU&country=DE&brand=BMW`
   - `/api/cars?region=EU&country=DE&brand=BMW&sort=price_asc&page=1&page_size=12`
2. If p95 still high:
   - add covering indexes for `engine_type` + price sort (`country, engine_type, coalesce_price, id`).
   - add brand/model + price sort indexes for EU-heavy combinations.
3. Move advanced-search counter to dedicated count endpoint with full filter support (avoid `page_size=1` list call).
4. Add stale-while-revalidate for filter context payloads to reduce cold-start waits.
5. Prewarm cache after deploy:
   - DE all
   - DE BMW
   - DE BMW X5
   - KR all / KR popular brands
6. Add weekly DB maintenance task:
   - `ANALYZE cars`
   - `VACUUM (ANALYZE) car_counts_*`
7. Add alerting:
   - disk usage > 85%
   - repeated `count_slow/list_slow` in logs
   - Redis memory near maxmemory

