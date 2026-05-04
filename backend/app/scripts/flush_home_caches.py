"""Drop Redis keys and bump dataset_version so the home page rebuilds caches.

Подборки на главной (``home_recommendation_block:*``, in-process TTLCache) могут
остаться старыми после ``git pull``, если версия датасета не менялась — этот
скрипт форсирует промах по Redis и смещает ``dataset_version``.

Пример::

    docker compose exec -T web python -m backend.app.scripts.flush_home_caches

При нескольких воркерах gunicorn после скрипта перезапустите web, чтобы
очистить RAM во всех процессах::

    docker compose restart web
"""

from __future__ import annotations


def main() -> None:
    from backend.app.utils.redis_cache import bump_dataset_version, redis_delete_by_pattern

    n = redis_delete_by_pattern("home_*")
    bump_dataset_version()
    print(f"redis_delete_by_pattern('home_*') -> ~{n} keys")
    print("dataset_version bumped — следующий запрос главной пересоберёт кэши")

    from backend.app.routers import pages as pages_router

    for name in (
        "_HOME_FILTER_CTX_CACHE",
        "_HOME_MEDIA_CACHE",
        "_HOME_RECOMMENDED_CACHE",
        "_HOME_RECOMMENDATION_BLOCK_CACHE",
        "_HOME_MORE_OFFERS_CACHE",
    ):
        cache = getattr(pages_router, name, None)
        if cache is not None:
            cache.clear()
    print("in-process TTLCache очищен в этом интерпретаторе (один процесс)")

    print()
    print("Если веб работает в нескольких воркерах: docker compose restart web")


if __name__ == "__main__":
    main()
