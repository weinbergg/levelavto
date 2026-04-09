from typing import Any, Dict

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ..db import get_db
from ..services.auth_service import AuthService
from ..models import User
from ..services.phone_verification_service import (
    PhoneVerificationError,
    PhoneVerificationService,
    mask_phone_number,
    normalize_phone_number,
)


router = APIRouter()


def _register_context(request: Request, *, error: str | None = None, form_data: Dict[str, Any] | None = None):
    return {
        "request": request,
        "error": error,
        "success": False,
        "form_data": form_data or {},
    }


class SendPhoneCodePayload(BaseModel):
    phone: str


class VerifyPhoneCodePayload(BaseModel):
    phone: str
    challenge_token: str
    code: str


@router.get("/login")
def login_page(request: Request):
    # если уже авторизован — сразу на главную
    if request.session.get("user_id"):
        return RedirectResponse(url="/", status_code=302)
    templates = request.app.state.templates
    return templates.TemplateResponse("auth/login.html", {"request": request, "error": None})


@router.post("/login")
def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    auth = AuthService(db)
    user = auth.authenticate(email, password)
    if not user:
        return templates.TemplateResponse(
            "auth/login.html",
            {"request": request, "error": "Неверная пара email/пароль"},
            status_code=400,
        )
    request.session["user_id"] = user.id
    return RedirectResponse(url="/", status_code=303)


@router.get("/register")
def register_page(request: Request):
    if request.session.get("user_id"):
        return RedirectResponse(url="/", status_code=302)
    templates = request.app.state.templates
    return templates.TemplateResponse("auth/register.html", _register_context(request))


@router.post("/api/auth/phone/send-code")
def send_phone_code(
    payload: SendPhoneCodePayload,
    request: Request,
    db: Session = Depends(get_db),
):
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
    password: str = Form(...),
    full_name: str = Form(default=""),
    phone: str = Form(...),
    phone_verification_token: str = Form(default=""),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    auth = AuthService(db)
    verification = PhoneVerificationService(db)
    total_users = db.execute(select(func.count()).select_from(User)).scalar_one()
    is_admin = total_users == 0  # первый пользователь становится админом
    form_data = {
        "email": email,
        "full_name": full_name,
        "phone": phone,
        "phone_verification_token": phone_verification_token,
    }
    try:
        challenge = verification.get_verified_registration(phone, phone_verification_token)
        phone_norm = normalize_phone_number(phone)
        user = auth.create_user(
            email=email,
            password=password,
            full_name=full_name or None,
            phone=phone_norm,
            phone_verified_at=challenge.verified_at,
            is_admin=is_admin,
        )
        verification.mark_registration_consumed(challenge)
    except (ValueError, PhoneVerificationError) as exc:
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
