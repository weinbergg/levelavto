from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.app.models.source import Base
from backend.app.models.user import User
from backend.app.models.phone_verification import PhoneVerificationChallenge
from backend.app.services.phone_verification_service import (
    BaseSmsProvider,
    PhoneVerificationService,
    SmsDeliveryResult,
    mask_phone_number,
    normalize_phone_number,
)


class DummySmsProvider(BaseSmsProvider):
    provider_name = "dummy"

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    def send_verification_code(self, phone: str, code: str, *, client_ip: str | None = None) -> SmsDeliveryResult:
        self.sent.append((phone, code))
        return SmsDeliveryResult(provider=self.provider_name, message_id="dummy-1")


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[User.__table__, PhoneVerificationChallenge.__table__])
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)()


def test_phone_normalization_handles_ru_prefixes():
    assert normalize_phone_number("8 (999) 123-45-67") == "+79991234567"
    assert normalize_phone_number("9991234567") == "+79991234567"
    assert mask_phone_number("+79991234567").startswith("+7")


def test_phone_verification_challenge_lifecycle():
    db = _session()
    provider = DummySmsProvider()
    service = PhoneVerificationService(db, provider=provider)

    challenge = service.create_register_challenge("+7 (999) 123-45-67", client_ip="127.0.0.1")
    assert challenge.status == "pending"
    assert provider.sent and provider.sent[0][0] == "+79991234567"

    verified = service.verify_register_code(challenge.session_token, "+7 999 123-45-67", provider.sent[0][1])
    assert verified.status == "verified"

    fetched = service.get_verified_registration("+79991234567", challenge.session_token)
    assert fetched.id == verified.id

    service.mark_registration_consumed(fetched)
    assert fetched.status == "consumed"
