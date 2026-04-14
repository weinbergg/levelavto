from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.app.models.source import Base
from backend.app.models.email_verification import EmailVerificationChallenge
from backend.app.models.user import User
from backend.app.services.email_verification_service import (
    BaseEmailProvider,
    EmailDeliveryResult,
    EmailVerificationService,
    mask_email_address,
    normalize_email_address,
)


class DummyEmailProvider(BaseEmailProvider):
    provider_name = "dummy"

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    def send_verification_code(self, email: str, code: str, *, client_ip: str | None = None) -> EmailDeliveryResult:
        self.sent.append((email, code))
        return EmailDeliveryResult(provider=self.provider_name, message_id="dummy-1")


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[User.__table__, EmailVerificationChallenge.__table__])
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)()


def test_email_normalization_and_masking():
    assert normalize_email_address("  Test.User@Example.com  ") == "test.user@example.com"
    assert mask_email_address("test.user@example.com").startswith("t***@")


def test_email_verification_challenge_lifecycle():
    db = _session()
    provider = DummyEmailProvider()
    service = EmailVerificationService(db, provider=provider)

    challenge = service.create_register_challenge("Test.User@Example.com", client_ip="127.0.0.1")
    assert challenge.status == "pending"
    assert provider.sent and provider.sent[0][0] == "test.user@example.com"

    verified = service.verify_register_code(challenge.session_token, "test.user@example.com", provider.sent[0][1])
    assert verified.status == "verified"

    fetched = service.get_verified_registration("test.user@example.com", challenge.session_token)
    assert fetched.id == verified.id

    service.mark_registration_consumed(fetched)
    assert fetched.status == "consumed"
