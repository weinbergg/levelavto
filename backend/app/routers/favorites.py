from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ..auth import require_user, get_current_user
from ..db import get_db
from ..models import User
from ..services.favorites_service import FavoritesService

router = APIRouter(prefix="/api/favorites", tags=["favorites"])


@router.get("")
def get_favorites(user: User | None = Depends(get_current_user), db: Session = Depends(get_db)):
    if not user:
        return {"ids": []}
    service = FavoritesService(db)
    return {"ids": service.list_ids(user)}


@router.post("/{car_id}")
def add_favorite(car_id: int, user: User = Depends(require_user), db: Session = Depends(get_db)):
    service = FavoritesService(db)
    service.add(user, car_id)
    return {"ok": True}


@router.delete("/{car_id}")
def remove_favorite(car_id: int, user: User = Depends(require_user), db: Session = Depends(get_db)):
    service = FavoritesService(db)
    service.remove(user, car_id)
    return {"ok": True}
