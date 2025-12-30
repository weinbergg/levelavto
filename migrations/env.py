from __future__ import annotations

from logging.config import fileConfig
from alembic import context
from sqlalchemy import engine_from_config, pool
import os
import time

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None


def build_database_url() -> str:
    user = os.getenv("DB_USER", "autodealer")
    password = os.getenv("DB_PASSWORD", "autodealer")
    host = os.getenv("DB_HOST", "db")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "autodealer")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"


def run_migrations_offline() -> None:
    url = build_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    cfg = config.get_section(config.config_ini_section)
    cfg["sqlalchemy.url"] = build_database_url()

    # Simple retry loop to wait for DB DNS/availability inside container
    last_exc = None
    for _ in range(15):
        try:
            connectable = engine_from_config(
                cfg,
                prefix="sqlalchemy.",
                poolclass=pool.NullPool,
            )
            with connectable.connect() as connection:
                context.configure(connection=connection, target_metadata=target_metadata)
                with context.begin_transaction():
                    context.run_migrations()
            return
        except Exception as exc:  # typically OperationalError while DB not ready
            last_exc = exc
            time.sleep(2)
    if last_exc:
        raise last_exc


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()


