from __future__ import annotations

import hashlib
import hmac
import json
import logging
import random
import secrets
import smtplib
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import EmailVerificationChallenge, User

logger = logging.getLogger(__name__)


class EmailVerificationError(ValueError):
    pass


@dataclass
class EmailDeliveryResult:
    provider: str
    message_id: str | None = None
    raw_payload: str | None = None


def normalize_email_address(raw: str) -> str:
    email = str(raw or "").strip().lower()
    if not email:
        raise EmailVerificationError("Введите email")
    if "@" not in email:
        raise EmailVerificationError("Некорректный email")
    local, _, domain = email.partition("@")
    if not local or not domain or "." not in domain or domain.startswith(".") or domain.endswith("."):
        raise EmailVerificationError("Некорректный email")
    return email


def mask_email_address(email: str) -> str:
    normalized = normalize_email_address(email)
    local, _, domain = normalized.partition("@")
    domain_name, dot, rest = domain.partition(".")
    local_mask = f"{local[:1]}***" if len(local) > 1 else "*"
    domain_mask = f"{domain_name[:1]}***" if len(domain_name) > 1 else "*"
    if dot:
        return f"{local_mask}@{domain_mask}{dot}{rest}"
    return f"{local_mask}@{domain_mask}"


class BaseEmailProvider:
    provider_name = "base"

    def send_verification_code(self, email: str, code: str, *, client_ip: str | None = None) -> EmailDeliveryResult:
        raise NotImplementedError


class LogEmailProvider(BaseEmailProvider):
    provider_name = "log"

    def send_verification_code(self, email: str, code: str, *, client_ip: str | None = None) -> EmailDeliveryResult:
        logger.warning("email_log_provider email=%s code=%s client_ip=%s", email, code, client_ip or "")
        return EmailDeliveryResult(provider=self.provider_name, message_id=f"log-{secrets.token_hex(6)}")


class SmtpEmailProvider(BaseEmailProvider):
    provider_name = "smtp"

    def send_verification_code(self, email: str, code: str, *, client_ip: str | None = None) -> EmailDeliveryResult:
        host = (settings.EMAIL_HOST or "").strip()
        user = (settings.EMAIL_HOST_USER or "").strip()
        password = settings.EMAIL_HOST_PASSWORD or ""
        if not host or not user or not password:
            raise EmailVerificationError(
                "Email не настроен: нужны EMAIL_HOST, EMAIL_HOST_USER и EMAIL_HOST_PASSWORD"
            )
        mail_from = (settings.EMAIL_FROM or user).strip()
        subject = "Код подтверждения Level Avto"
        body = (
            "Код подтверждения Level Avto\n\n"
            f"Ваш код: {code}\n"
            f"Код действует {max(1, int(settings.EMAIL_CODE_TTL_SEC / 60))} мин.\n\n"
            "Если вы не запрашивали этот код, просто проигнорируйте письмо."
        )
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = mail_from
        msg["To"] = email
        try:
            if settings.EMAIL_USE_SSL:
                with smtplib.SMTP_SSL(host, settings.EMAIL_PORT, timeout=15) as smtp:
                    smtp.login(user, password)
                    smtp.sendmail(mail_from, [email], msg.as_string())
            else:
                with smtplib.SMTP(host, settings.EMAIL_PORT, timeout=15) as smtp:
                    if settings.EMAIL_USE_TLS:
                        smtp.starttls()
                    smtp.login(user, password)
                    smtp.sendmail(mail_from, [email], msg.as_string())
        except Exception as exc:
            raise EmailVerificationError("Не удалось отправить email-код. Проверьте SMTP-настройки.") from exc
        payload = {
            "host": host,
            "port": settings.EMAIL_PORT,
            "mail_from": mail_from,
            "used_tls": settings.EMAIL_USE_TLS,
            "used_ssl": settings.EMAIL_USE_SSL,
        }
        return EmailDeliveryResult(
            provider=self.provider_name,
            message_id=f"smtp-{secrets.token_hex(6)}",
            raw_payload=json.dumps(payload, ensure_ascii=False),
        )


def build_email_provider() -> BaseEmailProvider:
    key = (settings.EMAIL_PROVIDER or "smtp").strip().lower()
    if key == "log":
        return LogEmailProvider()
    return SmtpEmailProvider()


class EmailVerificationService:
    def __init__(self, db: Session, provider: Optional[BaseEmailProvider] = None) -> None:
        self.db = db
        self.provider = provider or build_email_provider()

    def _hash_code(self, code: str) -> str:
        secret = f"{settings.APP_SECRET}:{code}".encode("utf-8")
        return hashlib.sha256(secret).hexdigest()

    def _code_matches(self, stored_hash: str, code: str) -> bool:
        candidate = self._hash_code(str(code or "").strip())
        return hmac.compare_digest(candidate, stored_hash)

    def _generate_code(self) -> str:
        length = max(4, min(8, settings.EMAIL_CODE_LENGTH))
        start = 10 ** (length - 1)
        end = (10**length) - 1
        return str(random.SystemRandom().randint(start, end))

    def _find_latest_pending(self, email: str, *, purpose: str = "register") -> EmailVerificationChallenge | None:
        now = datetime.utcnow()
        stmt = (
            select(EmailVerificationChallenge)
            .where(
                EmailVerificationChallenge.email == email,
                EmailVerificationChallenge.purpose == purpose,
                EmailVerificationChallenge.status == "pending",
                EmailVerificationChallenge.expires_at > now,
            )
            .order_by(EmailVerificationChallenge.created_at.desc())
            .limit(1)
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def create_register_challenge(self, raw_email: str, *, client_ip: str | None = None) -> EmailVerificationChallenge:
        email = normalize_email_address(raw_email)
        existing_user = self.db.execute(select(User).where(User.email == email)).scalar_one_or_none()
        if existing_user:
            raise EmailVerificationError("Этот email уже используется в другом аккаунте")
        now = datetime.utcnow()
        recent = self._find_latest_pending(email)
        if recent and (now - recent.created_at).total_seconds() < settings.EMAIL_RESEND_COOLDOWN_SEC:
            wait_for = settings.EMAIL_RESEND_COOLDOWN_SEC - int((now - recent.created_at).total_seconds())
            raise EmailVerificationError(f"Повторная отправка будет доступна через {max(wait_for, 1)} сек.")
        code = self._generate_code()
        challenge = EmailVerificationChallenge(
            email=email,
            purpose="register",
            session_token=secrets.token_urlsafe(24),
            code_hash=self._hash_code(code),
            status="pending",
            attempts=0,
            max_attempts=settings.EMAIL_MAX_ATTEMPTS,
            expires_at=now + timedelta(seconds=settings.EMAIL_CODE_TTL_SEC),
            ip_address=(client_ip or "")[:64] or None,
        )
        self.db.add(challenge)
        self.db.flush()
        try:
            delivery = self.provider.send_verification_code(email, code, client_ip=client_ip)
        except Exception:
            self.db.rollback()
            raise
        challenge.provider = delivery.provider
        challenge.provider_message_id = delivery.message_id
        challenge.provider_response = delivery.raw_payload
        self.db.commit()
        self.db.refresh(challenge)
        return challenge

    def verify_register_code(self, challenge_token: str, raw_email: str, raw_code: str) -> EmailVerificationChallenge:
        token = str(challenge_token or "").strip()
        code = str(raw_code or "").strip()
        if not token or not code:
            raise EmailVerificationError("Введите код из email")
        email = normalize_email_address(raw_email)
        challenge = self.db.execute(
            select(EmailVerificationChallenge).where(
                EmailVerificationChallenge.session_token == token,
                EmailVerificationChallenge.email == email,
                EmailVerificationChallenge.purpose == "register",
            )
        ).scalar_one_or_none()
        if not challenge:
            raise EmailVerificationError("Сессия подтверждения не найдена. Запросите код заново.")
        now = datetime.utcnow()
        if challenge.status == "consumed":
            raise EmailVerificationError("Этот код уже использован. Запросите новый.")
        if challenge.expires_at <= now:
            challenge.status = "expired"
            self.db.commit()
            raise EmailVerificationError("Код истёк. Запросите новый.")
        if challenge.status == "blocked" or challenge.attempts >= challenge.max_attempts:
            challenge.status = "blocked"
            self.db.commit()
            raise EmailVerificationError("Превышено количество попыток. Запросите новый код.")
        if not self._code_matches(challenge.code_hash, code):
            challenge.attempts += 1
            if challenge.attempts >= challenge.max_attempts:
                challenge.status = "blocked"
            self.db.commit()
            raise EmailVerificationError("Неверный код из email")
        challenge.status = "verified"
        challenge.verified_at = now
        self.db.commit()
        self.db.refresh(challenge)
        return challenge

    def get_verified_registration(self, raw_email: str, verification_token: str) -> EmailVerificationChallenge:
        email = normalize_email_address(raw_email)
        token = str(verification_token or "").strip()
        challenge = self.db.execute(
            select(EmailVerificationChallenge).where(
                EmailVerificationChallenge.session_token == token,
                EmailVerificationChallenge.email == email,
                EmailVerificationChallenge.purpose == "register",
                EmailVerificationChallenge.status == "verified",
            )
        ).scalar_one_or_none()
        if not challenge:
            raise EmailVerificationError("Email не подтверждён")
        if challenge.expires_at <= datetime.utcnow():
            challenge.status = "expired"
            self.db.commit()
            raise EmailVerificationError("Срок действия подтверждения истёк. Запросите новый код.")
        return challenge

    def mark_registration_consumed(self, challenge: EmailVerificationChallenge) -> None:
        challenge.status = "consumed"
        challenge.consumed_at = datetime.utcnow()
        self.db.commit()
