from __future__ import annotations

import os
import hashlib
import hmac
from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import User


class AuthService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def _hash(self, password: str, salt: bytes | None = None, iterations: int = 390000) -> str:
        if salt is None:
            salt = os.urandom(16)
        derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return f"pbkdf2_sha256${iterations}${salt.hex()}${derived.hex()}"

    def verify_password(self, stored: str, password: str) -> bool:
        try:
            _, iter_str, salt_hex, hash_hex = stored.split("$", 3)
            iterations = int(iter_str)
            salt = bytes.fromhex(salt_hex)
        except Exception:
            return False
        candidate = self._hash(password, salt=salt, iterations=iterations)
        return hmac.compare_digest(candidate, stored)

    def create_user(self, email: str, password: str, full_name: Optional[str] = None, is_admin: bool = False) -> User:
        email_norm = email.strip().lower()
        existing = self.db.execute(select(User).where(User.email == email_norm)).scalar_one_or_none()
        if existing:
            raise ValueError("Пользователь с таким email уже существует")
        pwd_hash = self._hash(password)
        user = User(email=email_norm, full_name=full_name, password_hash=pwd_hash, is_admin=is_admin)
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def authenticate(self, email: str, password: str) -> Optional[User]:
        email_norm = email.strip().lower()
        user = self.db.execute(select(User).where(User.email == email_norm, User.is_active.is_(True))).scalar_one_or_none()
        if not user:
            return None
        if not self.verify_password(user.password_hash, password):
            return None
        user.last_login_at = datetime.utcnow()
        self.db.commit()
        return user


