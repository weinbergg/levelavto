from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from ..auth import require_user
from ..db import get_db
from ..models import User
from ..services.auth_service import AuthService
from ..services.favorites_service import FavoritesService


router = APIRouter()


@router.get("/account")
def account_page(request: Request, user: User = Depends(require_user), db: Session = Depends(get_db)):
    templates = request.app.state.templates
    favs = FavoritesService(db).list_cars(user)
    return templates.TemplateResponse("account/index.html", {"request": request, "user": user, "status": None, "favorites": favs})


@router.post("/account/profile")
def update_profile(
    request: Request,
    full_name: str = Form(None),
    user: User = Depends(require_user),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    user.full_name = full_name
    db.add(user)
    db.commit()
    status = {"success": True, "message": "Профиль обновлён"}
    favs = FavoritesService(db).list_cars(user)
    return templates.TemplateResponse("account/index.html", {"request": request, "user": user, "status": status, "favorites": favs})


@router.post("/account/password")
def update_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    user: User = Depends(require_user),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    auth = AuthService(db)
    if not auth.verify_password(user.password_hash, current_password):
        status = {"success": False, "message": "Текущий пароль неверный"}
        return templates.TemplateResponse(
            "account/index.html",
            {"request": request, "user": user, "status": status},
            status_code=400,
        )
    user.password_hash = auth._hash(new_password)
    db.add(user)
    db.commit()
    status = {"success": True, "message": "Пароль обновлён"}
    favs = FavoritesService(db).list_cars(user)
    return templates.TemplateResponse("account/index.html", {"request": request, "user": user, "status": status, "favorites": favs})


