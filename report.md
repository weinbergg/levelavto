# Performance Audit

- Time: 2026-02-13T12:48:54+03:00
- Base URL: http://localhost:8000
- Requests per endpoint: 30
- Warmup requests: 5
- Timeout per request: 12s


## Host Snapshot


## uname

```
Linux cv5704357.novalocal 6.8.0-85-generic #85-Ubuntu SMP PREEMPT_DYNAMIC Thu Sep 18 15:26:59 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux
```

## uptime

```
 12:48:54 up 3 days, 20:03,  1 user,  load average: 0.16, 0.84, 1.57
```

## free_mb

```
               total        used        free      shared  buff/cache   available
Mem:            3915        1629         125         504        2932        2286
Swap:           4095         142        3953
```

## disk_root

```
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1        79G   76G   18M 100% /
```

## Docker Snapshot


## docker_compose_ps

```
NAME               IMAGE                COMMAND                  SERVICE   CREATED          STATUS                    PORTS
autodealer_db      postgres:16-alpine   "docker-entrypoint.s…"   db        2 weeks ago      Up 3 days (healthy)       5432/tcp
autodealer_redis   redis:7-alpine       "docker-entrypoint.s…"   redis     2 weeks ago      Up 3 days (healthy)       6379/tcp
autodealer_web     levelavto-web        "sh -c 'gunicorn bac…"   web       11 minutes ago   Up 10 minutes (healthy)   0.0.0.0:8000->8000/tcp, [::]:8000->8000/tcp
```

## docker_system_df

```
TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images          3         3         8.538GB   8.538GB (100%)
Containers      4         3         36.86kB   4.096kB (11%)
Local Volumes   2         2         56.83GB   0B (0%)
Build Cache     69        0         9.168GB   7.987GB
```

## Service Snapshot


## health

```
{"status":"ok"}```

## web_env_perf_flags

```
PRICE_ROUND_STEP_RUB=10000
REDIS_URL=redis://redis:6379/0
```

## Data Snapshot


## db_counts

```
cars_total=2554126
cars_available=2554126
cars_no_cached_price=1227
cars_no_cached_price_pct=0.05
```

## redis_memory

```
# Memory
used_memory:14060216
used_memory_human:13.41M
used_memory_rss:13090816
used_memory_rss_human:12.48M
used_memory_peak:39271296
used_memory_peak_human:37.45M
used_memory_peak_perc:35.80%
used_memory_overhead:1047688
used_memory_startup:948544
used_memory_dataset:13012528
used_memory_dataset_perc:99.24%
allocator_allocated:14598608
allocator_active:14966784
allocator_resident:24502272
allocator_muzzy:0
total_system_memory:4105506816
total_system_memory_human:3.82G
used_memory_lua:32768
used_memory_vm_eval:32768
used_memory_lua_human:32.00K
used_memory_scripts_eval:0
number_of_cached_scripts:0
number_of_functions:0
number_of_libraries:0
used_memory_vm_functions:33792
used_memory_vm_total:66560
used_memory_vm_total_human:65.00K
used_memory_functions:192
used_memory_scripts:192
used_memory_scripts_human:192B
maxmemory:268435456
maxmemory_human:256.00M
maxmemory_policy:allkeys-lru
allocator_frag_ratio:1.02
allocator_frag_bytes:292144
allocator_rss_ratio:1.64
allocator_rss_bytes:9535488
rss_overhead_ratio:0.53
rss_overhead_bytes:-11411456
```

## API Benchmarks

| endpoint | req | ok | err | avg, s | p50, s | p95, s | p99, s | max, s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| cars_count_eu | 30 | 30 | 0 | 0.003 | 0.003 | 0.005 | 0.007 | 0.007 |
| cars_list_eu | 30 | 30 | 0 | 0.006 | 0.005 | 0.010 | 0.010 | 0.010 |
| cars_list_search | 30 | 0 | 30 | 12.002 | 12.002 | 12.003 | 12.003 | 12.003 |
| filter_ctx_base_eu | 30 | 30 | 0 | 0.566 | 0.562 | 0.587 | 0.587 | 0.587 |
| filter_payload_eu | 30 | 30 | 0 | 0.003 | 0.002 | 0.003 | 0.028 | 0.028 |
| health | 30 | 30 | 0 | 0.002 | 0.002 | 0.002 | 0.004 | 0.004 |

## Recent Timing Logs


## web_logs_timing

```
autodealer_web  | CARS_LIST_CACHE hit=1 source=redis key=cars_list:EU:DE:all:price_asc:1:12:v0
autodealer_web  | CARS_LIST_CACHE hit=1 source=redis key=cars_list:EU:DE:all:price_asc:1:12:v0
autodealer_web  | CARS_LIST_CACHE hit=1 source=redis key=cars_list:EU:DE:all:price_asc:1:12:v0
autodealer_web  | CARS_LIST_CACHE hit=1 source=redis key=cars_list:EU:DE:all:price_asc:1:12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | list_slow total=13.092s sort=price_asc page=1 size=12 filters=('EU', 'DE', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | count_slow total=183.778s filters=('EU', 'DE', None, None, None, 'diesel', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | count_slow total=219.669s filters=('EU', 'DE', None, None, None, 'diesel', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | count_slow total=269.792s filters=('EU', 'DE', None, None, None, 'diesel', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | count_slow total=351.177s filters=('EU', 'DE', None, None, None, 'diesel', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | CARS_LIST_FULL_CACHE hit=0 source=fallback key=cars_list_full:(('country', 'DE'), ('q', 'diesel'), ('region', 'EU')):sort=price_asc:page=1:size=12:v0
autodealer_web  | [2026-02-13 09:58:01 +0000] [10] [ERROR] Exception in ASGI application
autodealer_web  |   + Exception Group Traceback (most recent call last):
autodealer_web  |   |   File "/usr/local/lib/python3.11/site-packages/starlette/_utils.py", line 77, in collapse_excgroups
autodealer_web  |   |     yield
autodealer_web  |   |   File "/usr/local/lib/python3.11/site-packages/starlette/middleware/base.py", line 186, in __call__
autodealer_web  |   |     async with anyio.create_task_group() as task_group:
autodealer_web  |   |   File "/usr/local/lib/python3.11/site-packages/anyio/_backends/_asyncio.py", line 783, in __aexit__
autodealer_web  |   |     raise BaseExceptionGroup(
autodealer_web  |   | ExceptionGroup: unhandled errors in a TaskGroup (1 sub-exception)
autodealer_web  |   +-+---------------- 1 ----------------
autodealer_web  |     | Traceback (most recent call last):
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/uvicorn/protocols/http/httptools_impl.py", line 401, in run_asgi
autodealer_web  |     |     result = await app(  # type: ignore[func-returns-value]
autodealer_web  |     |              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/uvicorn/middleware/proxy_headers.py", line 70, in __call__
autodealer_web  |     |     return await self.app(scope, receive, send)
autodealer_web  |     |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/fastapi/applications.py", line 1054, in __call__
autodealer_web  |     |     await super().__call__(scope, receive, send)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/applications.py", line 113, in __call__
autodealer_web  |     |     await self.middleware_stack(scope, receive, send)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/middleware/errors.py", line 187, in __call__
autodealer_web  |     |     raise exc
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/middleware/errors.py", line 165, in __call__
autodealer_web  |     |     await self.app(scope, receive, _send)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/middleware/base.py", line 185, in __call__
autodealer_web  |     |     with collapse_excgroups():
autodealer_web  |     |   File "/usr/local/lib/python3.11/contextlib.py", line 158, in __exit__
autodealer_web  |     |     self.gen.throw(typ, value, traceback)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/_utils.py", line 83, in collapse_excgroups
autodealer_web  |     |     raise exc
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/middleware/base.py", line 187, in __call__
autodealer_web  |     |     response = await self.dispatch_func(request, call_next)
autodealer_web  |     |                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
autodealer_web  |     |   File "/app/backend/app/main.py", line 39, in timing_middleware
autodealer_web  |     |     response = await call_next(request)
autodealer_web  |     |                ^^^^^^^^^^^^^^^^^^^^^^^^
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/middleware/base.py", line 163, in call_next
autodealer_web  |     |     raise app_exc
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/middleware/base.py", line 149, in coro
autodealer_web  |     |     await self.app(scope, receive_or_disconnect, send_no_error)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/middleware/sessions.py", line 85, in __call__
autodealer_web  |     |     await self.app(scope, receive, send_wrapper)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/middleware/exceptions.py", line 62, in __call__
autodealer_web  |     |     await wrap_app_handling_exceptions(self.app, conn)(scope, receive, send)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/_exception_handler.py", line 62, in wrapped_app
autodealer_web  |     |     raise exc
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/_exception_handler.py", line 51, in wrapped_app
autodealer_web  |     |     await app(scope, receive, sender)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/routing.py", line 715, in __call__
autodealer_web  |     |     await self.middleware_stack(scope, receive, send)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/routing.py", line 735, in app
autodealer_web  |     |     await route.handle(scope, receive, send)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/routing.py", line 288, in handle
autodealer_web  |     |     await self.app(scope, receive, send)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/routing.py", line 76, in app
autodealer_web  |     |     await wrap_app_handling_exceptions(app, request)(scope, receive, send)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/_exception_handler.py", line 62, in wrapped_app
autodealer_web  |     |     raise exc
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/_exception_handler.py", line 51, in wrapped_app
autodealer_web  |     |     await app(scope, receive, sender)
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/routing.py", line 73, in app
autodealer_web  |     |     response = await f(request)
autodealer_web  |     |                ^^^^^^^^^^^^^^^^
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/fastapi/routing.py", line 301, in app
autodealer_web  |     |     raw_response = await run_endpoint_function(
autodealer_web  |     |                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/fastapi/routing.py", line 214, in run_endpoint_function
autodealer_web  |     |     return await run_in_threadpool(dependant.call, **values)
autodealer_web  |     |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/starlette/concurrency.py", line 39, in run_in_threadpool
autodealer_web  |     |     return await anyio.to_thread.run_sync(func, *args)
autodealer_web  |     |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
autodealer_web  |     |   File "/usr/local/lib/python3.11/site-packages/anyio/to_thread.py", line 63, in run_sync
autodealer_web  |     |     return await get_async_backend().run_sync_in_worker_thread(
autodealer_web  |     |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
```
