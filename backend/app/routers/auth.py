import os
from typing import Any, Dict

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ..db import get_db
from ..services.auth_service import AuthService
from ..models import User
from ..services.email_verification_service import (
    EmailVerificationError,
    EmailVerificationService,
    mask_email_address,
    normalize_email_address,
)
from ..services.phone_verification_service import (
    PhoneVerificationError,
    PhoneVerificationService,
    mask_phone_number,
    normalize_phone_number,
)


router = APIRouter()


def _env_flag(name: str, default: str) -> bool:
    """Читаем bool-env: 1/true/yes/on -> True, 0/false/no/off/'' -> False."""
    raw = str(os.getenv(name, default) or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _email_verification_required() -> bool:
    """Требуется ли обязательное подтверждение email при регистрации.

    Управляется env-флагом EMAIL_VERIFICATION_REQUIRED. По умолчанию — False,
    т.к. SMTP-провайдер может быть не настроен (тогда register 500-ит).
    Включаем в 1, когда почтовый провайдер готов.
    """
    return _env_flag("EMAIL_VERIFICATION_REQUIRED", "0")


def _phone_verification_required() -> bool:
    """Требуется ли обязательное подтверждение телефона при регистрации.

    Управляется env-флагом PHONE_VERIFICATION_REQUIRED. По умолчанию — False
    (как и было до этого — телефон необязателен).
    """
    return _env_flag("PHONE_VERIFICATION_REQUIRED", "0")


def _register_context(request: Request, *, error: str | None = None, form_data: Dict[str, Any] | None = None):
    return {
        "request": request,
        "error": error,
        "success": False,
        "form_data": form_data or {},
        "email_verification_required": _email_verification_required(),
        "phone_verification_required": _phone_verification_required(),
    }


class SendPhoneCodePayload(BaseModel):
    phone: str


class VerifyPhoneCodePayload(BaseModel):
    phone: str
    challenge_token: str
    code: str


class SendEmailCodePayload(BaseModel):
    email: str


class VerifyEmailCodePayload(BaseModel):
    email: str
    challenge_token: str
    code: str


def _safe_next_path(value: str | None) -> str:
    """Only allow same-site relative paths to prevent open-redirect."""
    if not value:
        return "/"
    candidate = str(value).strip()
    if not candidate.startswith("/"):
        return "/"
    if candidate.startswith("//"):
        return "/"
    return candidate


@router.get("/login")
def login_page(request: Request, next: str | None = None):
    next_path = _safe_next_path(next)
    if request.session.get("user_id"):
        return RedirectResponse(url=next_path, status_code=302)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "auth/login.html",
        {"request": request, "error": None, "next_path": next_path},
    )


@router.post("/login")
def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    next: str = Form(default="/"),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    auth = AuthService(db)
    user = auth.authenticate(email, password)
    next_path = _safe_next_path(next)
    if not user:
        return templates.TemplateResponse(
            "auth/login.html",
            {"request": request, "error": "Неверная пара email/пароль", "next_path": next_path},
            status_code=400,
        )
    request.session["user_id"] = user.id
    return RedirectResponse(url=next_path, status_code=303)


@router.get("/register")
def register_page(request: Request):
    if request.session.get("user_id"):
        return RedirectResponse(url="/", status_code=302)
    templates = request.app.state.templates
    return templates.TemplateResponse("auth/register.html", _register_context(request))


@router.post("/api/auth/email/send-code")
def send_email_code(
    payload: SendEmailCodePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    if not _email_verification_required():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Подтверждение email временно отключено",
        )
    service = EmailVerificationService(db)
    try:
        email = normalize_email_address(payload.email)
        challenge = service.create_register_challenge(email, client_ip=request.client.host if request.client else None)
    except EmailVerificationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return {
        "ok": True,
        "challenge_token": challenge.session_token,
        "email_masked": mask_email_address(challenge.email),
        "expires_in_sec": max(int((challenge.expires_at - challenge.created_at).total_seconds()), 0),
    }


@router.post("/api/auth/email/verify-code")
def verify_email_code(
    payload: VerifyEmailCodePayload,
    db: Session = Depends(get_db),
):
    if not _email_verification_required():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Подтверждение email временно отключено",
        )
    service = EmailVerificationService(db)
    try:
        challenge = service.verify_register_code(payload.challenge_token, payload.email, payload.code)
    except EmailVerificationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return {
        "ok": True,
        "verification_token": challenge.session_token,
        "email_masked": mask_email_address(challenge.email),
    }


@router.post("/api/auth/phone/send-code")
def send_phone_code(
    payload: SendPhoneCodePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    if not _phone_verification_required():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Подтверждение телефона временно отключено",
        )
    service = PhoneVerificationService(db)
    try:
        phone = normalize_phone_number(payload.phone)
        challenge = service.create_register_challenge(phone, client_ip=request.client.host if request.client else None)
    except PhoneVerificationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return {
        "ok": True,
        "challenge_token": challenge.session_token,
        "phone_masked": mask_phone_number(challenge.phone),
        "expires_in_sec": max(int((challenge.expires_at - challenge.created_at).total_seconds()), 0),
    }


@router.post("/api/auth/phone/verify-code")
def verify_phone_code(
    payload: VerifyPhoneCodePayload,
    db: Session = Depends(get_db),
):
    if not _phone_verification_required():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Подтверждение телефона временно отключено",
        )
    service = PhoneVerificationService(db)
    try:
        challenge = service.verify_register_code(payload.challenge_token, payload.phone, payload.code)
    except PhoneVerificationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return {
        "ok": True,
        "verification_token": challenge.session_token,
        "phone_masked": mask_phone_number(challenge.phone),
    }


@router.post("/register")
def register(
    request: Request,
    email: str = Form(...),
    email_verification_token: str = Form(default=""),
    password: str = Form(...),
    full_name: str = Form(default=""),
    phone: str = Form(default=""),
    phone_verification_token: str = Form(default=""),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    auth = AuthService(db)
    email_verification = EmailVerificationService(db)
    verification = PhoneVerificationService(db)
    total_users = db.execute(select(func.count()).select_from(User)).scalar_one()
    is_admin = total_users == 0  # первый пользователь становится админом
    form_data = {
        "email": email,
        "email_verification_token": email_verification_token,
        "full_name": full_name,
        "phone": phone,
        "phone_verification_token": phone_verification_token,
    }
    email_required = _email_verification_required()
    phone_required = _phone_verification_required()
    try:
        email_norm = normalize_email_address(email)
        email_challenge = None
        email_verified_at = None
        if email_required:
            email_challenge = email_verification.get_verified_registration(email, email_verification_token)
            email_verified_at = email_challenge.verified_at
        phone_norm = normalize_phone_number(phone) if str(phone or "").strip() else None
        phone_challenge = None
        if phone_required:
            if not phone_norm:
                raise PhoneVerificationError("Введите номер телефона")
            phone_challenge = verification.get_verified_registration(phone_norm, phone_verification_token)
        elif phone_norm and str(phone_verification_token or "").strip():
            # Юзер сам решил подтвердить номер — оставляем как раньше.
            phone_challenge = verification.get_verified_registration(phone_norm, phone_verification_token)
        user = auth.create_user(
            email=email_norm,
            password=password,
            full_name=full_name or None,
            phone=phone_norm,
            email_verified_at=email_verified_at,
            phone_verified_at=phone_challenge.verified_at if phone_challenge else None,
            is_admin=is_admin,
        )
        if email_challenge is not None:
            email_verification.mark_registration_consumed(email_challenge)
        if phone_challenge:
            verification.mark_registration_consumed(phone_challenge)
    except (ValueError, EmailVerificationError, PhoneVerificationError) as exc:
        return templates.TemplateResponse(
            "auth/register.html",
            _register_context(request, error=str(exc), form_data=form_data),
            status_code=400,
        )
    request.session["user_id"] = user.id
    return RedirectResponse(url="/", status_code=303)


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=302)
