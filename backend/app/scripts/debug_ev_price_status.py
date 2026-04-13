from __future__ import annotations

import argparse
import json
from datetime import datetime

from sqlalchemy import String, and_, case, cast, func, or_, select

from backend.app.db import SessionLocal
from backend.app.models import Car, Source


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit electric vehicle pricing and util-fee fallback coverage")
    ap.add_argument("--sample-limit", type=int, default=20)
    args = ap.parse_args()
    sample_limit = max(1, int(args.sample_limit))

    fuel_expr = func.lower(func.coalesce(Car.engine_type, ""))
    electric_like_expr = or_(
        fuel_expr.like("%electric%"),
        fuel_expr.like("% ev%"),
        fuel_expr.like("ev %"),
        fuel_expr == "ev",
    )
    power_missing_expr = and_(
        or_(Car.power_kw.is_(None), Car.power_kw <= 0),
        or_(Car.power_hp.is_(None), Car.power_hp <= 0),
    )
    without_util_marker_expr = cast(func.coalesce(Car.calc_breakdown_json, "[]"), String).like("%__without_util_fee%")

    with SessionLocal() as db:
        summary = {
            "electric_like_available": int(
                db.execute(select(func.count(Car.id)).where(Car.is_available.is_(True), electric_like_expr)).scalar() or 0
            ),
            "electric_like_with_total": int(
                db.execute(
                    select(func.count(Car.id)).where(
                        Car.is_available.is_(True),
                        electric_like_expr,
                        Car.total_price_rub_cached.is_not(None),
                    )
                ).scalar()
                or 0
            ),
            "electric_like_missing_total": int(
                db.execute(
                    select(func.count(Car.id)).where(
                        Car.is_available.is_(True),
                        electric_like_expr,
                        Car.total_price_rub_cached.is_(None),
                    )
                ).scalar()
                or 0
            ),
            "electric_like_without_util_marker": int(
                db.execute(
                    select(func.count(Car.id)).where(
                        Car.is_available.is_(True),
                        electric_like_expr,
                        without_util_marker_expr,
                    )
                ).scalar()
                or 0
            ),
            "electric_like_missing_power": int(
                db.execute(
                    select(func.count(Car.id)).where(
                        Car.is_available.is_(True),
                        electric_like_expr,
                        power_missing_expr,
                    )
                ).scalar()
                or 0
            ),
            "electric_like_engine_cc_positive": int(
                db.execute(
                    select(func.count(Car.id)).where(
                        Car.is_available.is_(True),
                        electric_like_expr,
                        Car.engine_cc.is_not(None),
                        Car.engine_cc > 0,
                    )
                ).scalar()
                or 0
            ),
        }

        by_country = [
            {
                "country": country,
                "available": int(total or 0),
            }
            for country, total in db.execute(
                select(func.coalesce(Car.country, "__NULL__"), func.count(Car.id))
                .where(Car.is_available.is_(True), electric_like_expr)
                .group_by(func.coalesce(Car.country, "__NULL__"))
                .order_by(func.count(Car.id).desc())
            ).all()
        ]

        by_source = [
            {
                "source": key,
                "available": int(total or 0),
                "missing_total": int(missing_total or 0),
                "without_util_marker": int(without_util or 0),
            }
            for key, total, missing_total, without_util in db.execute(
                select(
                    Source.key,
                    func.count(Car.id),
                    func.sum(case((Car.total_price_rub_cached.is_(None), 1), else_=0)),
                    func.sum(case((without_util_marker_expr, 1), else_=0)),
                )
                .join(Source, Source.id == Car.source_id)
                .where(Car.is_available.is_(True), electric_like_expr)
                .group_by(Source.key)
                .order_by(func.count(Car.id).desc(), Source.key.asc())
            ).all()
        ]

        samples = [
            {
                "id": int(row.id),
                "source": row.key,
                "brand": row.brand,
                "model": row.model,
                "country": row.country,
                "engine_type": row.engine_type,
                "engine_cc": row.engine_cc,
                "power_hp": float(row.power_hp) if row.power_hp is not None else None,
                "power_kw": float(row.power_kw) if row.power_kw is not None else None,
                "price_rub_cached": float(row.price_rub_cached) if row.price_rub_cached is not None else None,
                "total_price_rub_cached": float(row.total_price_rub_cached) if row.total_price_rub_cached is not None else None,
                "calc_updated_at": _dt(row.calc_updated_at),
                "source_url": row.source_url,
            }
            for row in db.execute(
                select(
                    Car.id,
                    Source.key,
                    Car.brand,
                    Car.model,
                    Car.country,
                    Car.engine_type,
                    Car.engine_cc,
                    Car.power_hp,
                    Car.power_kw,
                    Car.price_rub_cached,
                    Car.total_price_rub_cached,
                    Car.calc_updated_at,
                    Car.source_url,
                )
                .join(Source, Source.id == Car.source_id)
                .where(
                    Car.is_available.is_(True),
                    electric_like_expr,
                    or_(Car.total_price_rub_cached.is_(None), without_util_marker_expr, power_missing_expr),
                )
                .order_by(Car.calc_updated_at.asc().nullsfirst(), Car.id.asc())
                .limit(sample_limit)
            ).all()
        ]

    print(
        json.dumps(
            {
                "summary": summary,
                "by_country": by_country,
                "by_source": by_source,
                "samples": samples,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
