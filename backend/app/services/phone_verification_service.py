from __future__ import annotations

import hashlib
import hmac
import json
import logging
import random
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import PhoneVerificationChallenge, User

logger = logging.getLogger(__name__)


class PhoneVerificationError(ValueError):
    pass


@dataclass
class SmsDeliveryResult:
    provider: str
    message_id: str | None = None
    raw_payload: str | None = None


def normalize_phone_number(raw: str) -> str:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if not digits:
        raise PhoneVerificationError("Введите номер телефона")
    if len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 11 and digits.startswith("7"):
        pass
    elif digits.startswith("00") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) < 11:
        raise PhoneVerificationError("Некорректный номер телефона")
    return f"+{digits}"


def mask_phone_number(phone: str) -> str:
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if len(digits) < 4:
        return phone
    return f"+{digits[0]} *** *** {digits[-4:-2]} {digits[-2:]}"


class BaseSmsProvider:
    provider_name = "base"

    def send_verification_code(self, phone: str, code: str, *, client_ip: str | None = None) -> SmsDeliveryResult:
        raise NotImplementedError


class LogSmsProvider(BaseSmsProvider):
    provider_name = "log"

    def send_verification_code(self, phone: str, code: str, *, client_ip: str | None = None) -> SmsDeliveryResult:
        logger.warning("sms_log_provider phone=%s code=%s client_ip=%s", phone, code, client_ip or "")
        return SmsDeliveryResult(provider=self.provider_name, message_id=f"log-{secrets.token_hex(6)}")


class SmsRuProvider(BaseSmsProvider):
    provider_name = "sms_ru"
    api_url = "https://sms.ru/sms/send"

    def send_verification_code(self, phone: str, code: str, *, client_ip: str | None = None) -> SmsDeliveryResult:
        api_id = (settings.SMS_RU_API_ID or "").strip()
        if not api_id:
            raise PhoneVerificationError("SMS.ru не настроен: отсутствует SMS_RU_API_ID")
        message = f"Код подтверждения Level Avto: {code}. Никому его не сообщайте."
        payload = {
            "api_id": api_id,
            "to": phone.lstrip("+"),
            "msg": message,
            "json": 1,
            "ttl": max(1, int(settings.SMS_CODE_TTL_SEC / 60)),
        }
        sender = (settings.SMS_SENDER_NAME or "").strip()
        if sender:
            payload["from"] = sender
        if client_ip:
            payload["ip"] = client_ip
        try:
            response = requests.post(self.api_url, data=payload, timeout=20)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise PhoneVerificationError("Не удалось отправить SMS-код. Проверьте настройки провайдера.") from exc
        body = response.json()
        if int(body.get("status_code") or 0) != 100:
            raise PhoneVerificationError(body.get("status_text") or "SMS.ru отклонил запрос")
        sms_map = body.get("sms") or {}
        item = next(iter(sms_map.values()), {}) if isinstance(sms_map, dict) else {}
        item_status = int(item.get("status_code") or 0)
        if item_status != 100:
            raise PhoneVerificationError(item.get("status_text") or "SMS.ru не принял сообщение")
        return SmsDeliveryResult(
            provider=self.provider_name,
            message_id=str(item.get("sms_id") or ""),
            raw_payload=json.dumps(body, ensure_ascii=False),
        )


def build_sms_provider() -> BaseSmsProvider:
    key = (settings.SMS_PROVIDER or "log").strip().lower()
    if key == "sms_ru":
        return SmsRuProvider()
    return LogSmsProvider()


class PhoneVerificationService:
    def __init__(self, db: Session, provider: Optional[BaseSmsProvider] = None) -> None:
        self.db = db
        self.provider = provider or build_sms_provider()

    def _hash_code(self, code: str) -> str:
        secret = f"{settings.APP_SECRET}:{code}".encode("utf-8")
        return hashlib.sha256(secret).hexdigest()

    def _code_matches(self, stored_hash: str, code: str) -> bool:
        candidate = self._hash_code(str(code or "").strip())
        return hmac.compare_digest(candidate, stored_hash)

    def _generate_code(self) -> str:
        length = max(4, min(8, settings.SMS_CODE_LENGTH))
        start = 10 ** (length - 1)
        end = (10**length) - 1
        return str(random.SystemRandom().randint(start, end))

    def _find_latest_pending(self, phone: str, *, purpose: str = "register") -> PhoneVerificationChallenge | None:
        now = datetime.utcnow()
        stmt = (
            select(PhoneVerificationChallenge)
            .where(
                PhoneVerificationChallenge.phone == phone,
                PhoneVerificationChallenge.purpose == purpose,
                PhoneVerificationChallenge.status == "pending",
                PhoneVerificationChallenge.expires_at > now,
            )
            .order_by(PhoneVerificationChallenge.created_at.desc())
            .limit(1)
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def create_register_challenge(self, raw_phone: str, *, client_ip: str | None = None) -> PhoneVerificationChallenge:
        phone = normalize_phone_number(raw_phone)
        existing_user = self.db.execute(select(User).where(User.phone == phone)).scalar_one_or_none()
        if existing_user:
            raise PhoneVerificationError("Этот номер уже используется в другом аккаунте")
        now = datetime.utcnow()
        recent = self._find_latest_pending(phone)
        if recent and (now - recent.created_at).total_seconds() < settings.SMS_RESEND_COOLDOWN_SEC:
            wait_for = settings.SMS_RESEND_COOLDOWN_SEC - int((now - recent.created_at).total_seconds())
            raise PhoneVerificationError(f"Повторная отправка будет доступна через {max(wait_for, 1)} сек.")
        code = self._generate_code()
        challenge = PhoneVerificationChallenge(
            phone=phone,
            purpose="register",
            session_token=secrets.token_urlsafe(24),
            code_hash=self._hash_code(code),
            status="pending",
            attempts=0,
            max_attempts=settings.SMS_MAX_ATTEMPTS,
            expires_at=now + timedelta(seconds=settings.SMS_CODE_TTL_SEC),
            ip_address=(client_ip or "")[:64] or None,
        )
        self.db.add(challenge)
        self.db.flush()
        try:
            delivery = self.provider.send_verification_code(phone, code, client_ip=client_ip)
        except Exception:
            self.db.rollback()
            raise
        challenge.provider = delivery.provider
        challenge.provider_message_id = delivery.message_id
        challenge.provider_response = delivery.raw_payload
        self.db.commit()
        self.db.refresh(challenge)
        return challenge

    def verify_register_code(self, challenge_token: str, raw_phone: str, raw_code: str) -> PhoneVerificationChallenge:
        token = str(challenge_token or "").strip()
        code = str(raw_code or "").strip()
        if not token or not code:
            raise PhoneVerificationError("Введите код из SMS")
        phone = normalize_phone_number(raw_phone)
        challenge = self.db.execute(
            select(PhoneVerificationChallenge).where(
                PhoneVerificationChallenge.session_token == token,
                PhoneVerificationChallenge.phone == phone,
                PhoneVerificationChallenge.purpose == "register",
            )
        ).scalar_one_or_none()
        if not challenge:
            raise PhoneVerificationError("Сессия подтверждения не найдена. Запросите код заново.")
        now = datetime.utcnow()
        if challenge.status == "consumed":
            raise PhoneVerificationError("Этот код уже использован. Запросите новый.")
        if challenge.expires_at <= now:
            challenge.status = "expired"
            self.db.commit()
            raise PhoneVerificationError("Код истёк. Запросите новый.")
        if challenge.status == "blocked" or challenge.attempts >= challenge.max_attempts:
            challenge.status = "blocked"
            self.db.commit()
            raise PhoneVerificationError("Превышено количество попыток. Запросите новый код.")
        if not self._code_matches(challenge.code_hash, code):
            challenge.attempts += 1
            if challenge.attempts >= challenge.max_attempts:
                challenge.status = "blocked"
            self.db.commit()
            raise PhoneVerificationError("Неверный код из SMS")
        challenge.status = "verified"
        challenge.verified_at = now
        self.db.commit()
        self.db.refresh(challenge)
        return challenge

    def get_verified_registration(self, raw_phone: str, verification_token: str) -> PhoneVerificationChallenge:
        phone = normalize_phone_number(raw_phone)
        token = str(verification_token or "").strip()
        challenge = self.db.execute(
            select(PhoneVerificationChallenge).where(
                PhoneVerificationChallenge.session_token == token,
                PhoneVerificationChallenge.phone == phone,
                PhoneVerificationChallenge.purpose == "register",
                PhoneVerificationChallenge.status == "verified",
            )
        ).scalar_one_or_none()
        if not challenge:
            raise PhoneVerificationError("Номер телефона не подтверждён")
        if challenge.expires_at <= datetime.utcnow():
            challenge.status = "expired"
            self.db.commit()
            raise PhoneVerificationError("Срок действия подтверждения истёк. Запросите новый код.")
        return challenge

    def mark_registration_consumed(self, challenge: PhoneVerificationChallenge) -> None:
        challenge.status = "consumed"
        challenge.consumed_at = datetime.utcnow()
        self.db.commit()
