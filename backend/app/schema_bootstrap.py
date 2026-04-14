from __future__ import annotations

import logging

from sqlalchemy import inspect, text

from .db import engine
from .models.email_verification import EmailVerificationChallenge
from .models.phone_verification import PhoneVerificationChallenge
from .models.parser_run import ParserRun, ParserRunSource

logger = logging.getLogger(__name__)


def ensure_runtime_schema() -> None:
    try:
        inspector = inspect(engine)
        if not inspector.has_table("phone_verification_challenges"):
            PhoneVerificationChallenge.__table__.create(bind=engine, checkfirst=True)
        if not inspector.has_table("email_verification_challenges"):
            EmailVerificationChallenge.__table__.create(bind=engine, checkfirst=True)
        if not inspector.has_table("parser_runs"):
            ParserRun.__table__.create(bind=engine, checkfirst=True)
        if not inspector.has_table("parser_run_sources"):
            ParserRunSource.__table__.create(bind=engine, checkfirst=True)
        inspector = inspect(engine)
        if not inspector.has_table("users"):
            with engine.begin() as conn:
                if inspector.has_table("parser_runs"):
                    parser_run_columns = {
                        col["name"]: col for col in inspector.get_columns("parser_runs")
                    }
                    trigger_col = parser_run_columns.get("trigger")
                    trigger_len = getattr(trigger_col.get("type"), "length", None) if trigger_col else None
                    if trigger_len is not None and trigger_len < 64:
                        conn.execute(
                            text("ALTER TABLE parser_runs ALTER COLUMN trigger TYPE VARCHAR(64)")
                        )
            return
        user_columns = {col["name"] for col in inspector.get_columns("users")}
        parser_run_columns = (
            {col["name"]: col for col in inspector.get_columns("parser_runs")}
            if inspector.has_table("parser_runs")
            else {}
        )
        with engine.begin() as conn:
            if "phone" not in user_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN phone VARCHAR(32)"))
            if "email_verified_at" not in user_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN email_verified_at TIMESTAMP"))
            if "phone_verified_at" not in user_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN phone_verified_at TIMESTAMP"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_users_phone ON users (phone)"))
            trigger_col = parser_run_columns.get("trigger")
            trigger_len = getattr(trigger_col.get("type"), "length", None) if trigger_col else None
            if trigger_len is not None and trigger_len < 64:
                conn.execute(text("ALTER TABLE parser_runs ALTER COLUMN trigger TYPE VARCHAR(64)"))
    except Exception:
        logger.exception("runtime_schema_bootstrap_failed")
