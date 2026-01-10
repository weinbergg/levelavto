from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ..db import get_db
from ..services.auth_service import AuthService
from ..models import User


router = APIRouter()


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
    return templates.TemplateResponse("auth/register.html", {"request": request, "error": None, "success": False})


@router.post("/register")
def register(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    full_name: str = Form(None),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    auth = AuthService(db)
    total_users = db.execute(select(func.count()).select_from(User)).scalar_one()
    is_admin = total_users == 0  # первый пользователь становится админом
    try:
        user = auth.create_user(email=email, password=password, full_name=full_name, is_admin=is_admin)
    except ValueError as exc:
        return templates.TemplateResponse(
            "auth/register.html",
            {"request": request, "error": str(exc), "success": False},
            status_code=400,
        )
    request.session["user_id"] = user.id
    return RedirectResponse(url="/", status_code=303)


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=302)

