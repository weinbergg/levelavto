from __future__ import annotations

import logging

from sqlalchemy import inspect, text

from .db import engine
from .models.phone_verification import PhoneVerificationChallenge

logger = logging.getLogger(__name__)


def ensure_runtime_schema() -> None:
    try:
        inspector = inspect(engine)
        if not inspector.has_table("phone_verification_challenges"):
            PhoneVerificationChallenge.__table__.create(bind=engine, checkfirst=True)
        if not inspector.has_table("users"):
            return
        user_columns = {col["name"] for col in inspector.get_columns("users")}
        with engine.begin() as conn:
            if "phone" not in user_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN phone VARCHAR(32)"))
            if "phone_verified_at" not in user_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN phone_verified_at TIMESTAMP"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_users_phone ON users (phone)"))
    except Exception:
        logger.exception("runtime_schema_bootstrap_failed")
