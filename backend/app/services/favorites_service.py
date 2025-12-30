from __future__ import annotations

from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from ..models import Favorite, Car, User


class FavoritesService:
    def __init__(self, db: Session):
        self.db = db

    def list_ids(self, user: User) -> list[int]:
        stmt = select(Favorite.car_id).where(Favorite.user_id == user.id)
        return [row[0] for row in self.db.scalars(stmt)]

    def add(self, user: User, car_id: int) -> None:
        exists = self.db.scalar(select(Favorite).where(Favorite.user_id == user.id, Favorite.car_id == car_id))
        if exists:
            return
        fav = Favorite(user_id=user.id, car_id=car_id)
        self.db.add(fav)
        self.db.commit()

    def remove(self, user: User, car_id: int) -> None:
        self.db.execute(delete(Favorite).where(Favorite.user_id == user.id, Favorite.car_id == car_id))
        self.db.commit()

    def list_cars(self, user: User) -> list[Car]:
        stmt = (
            select(Car)
            .join(Favorite, Favorite.car_id == Car.id)
            .where(Favorite.user_id == user.id)
            .order_by(Favorite.created_at.desc())
        )
        return list(self.db.scalars(stmt))

