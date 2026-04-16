from __future__ import annotations

import argparse
import json

from sqlalchemy import String, and_, case, cast, func, or_, select

from backend.app.db import SessionLocal
from backend.app.models import Car, Source


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit KR price calculation coverage and missing util inputs")
    ap.add_argument("--sample-limit", type=int, default=30)
    args = ap.parse_args()

    with SessionLocal() as db:
        engine_cc_expr = func.coalesce(Car.engine_cc, Car.inferred_engine_cc)
        power_hp_expr = func.coalesce(Car.power_hp, Car.inferred_power_hp)
        power_kw_expr = func.coalesce(Car.power_kw, Car.inferred_power_kw)
        payload_expr = cast(func.coalesce(Car.calc_breakdown_json, "[]"), String)
        without_util_expr = payload_expr.like("%__without_util_fee%")
        kr_scope = and_(Car.is_available.is_(True), Car.country.like("KR%"))

        summary = db.execute(
            select(
                func.count().label("kr_available"),
                func.sum(case((Car.total_price_rub_cached.is_not(None), 1), else_=0)).label("with_total"),
                func.sum(case((Car.total_price_rub_cached.is_(None), 1), else_=0)).label("missing_total"),
                func.sum(case((without_util_expr, 1), else_=0)).label("without_util_marker"),
                func.sum(case((engine_cc_expr.is_(None), 1), else_=0)).label("missing_engine_cc"),
                func.sum(
                    case(
                        (and_(power_hp_expr.is_(None), power_kw_expr.is_(None)), 1),
                        else_=0,
                    )
                ).label("missing_power"),
                func.sum(
                    case(
                        (
                            and_(
                                engine_cc_expr.is_(None),
                                power_hp_expr.is_(None),
                                power_kw_expr.is_(None),
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("missing_cc_and_power"),
            ).where(kr_scope)
        ).mappings().one()

        by_market = [
            {
                "kr_market_type": row.kr_market_type or "__NULL__",
                "available": int(row.available or 0),
                "with_total": int(row.with_total or 0),
                "without_util_marker": int(row.without_util_marker or 0),
                "missing_engine_cc": int(row.missing_engine_cc or 0),
                "missing_power": int(row.missing_power or 0),
            }
            for row in db.execute(
                select(
                    Car.kr_market_type,
                    func.count().label("available"),
                    func.sum(case((Car.total_price_rub_cached.is_not(None), 1), else_=0)).label("with_total"),
                    func.sum(case((without_util_expr, 1), else_=0)).label("without_util_marker"),
                    func.sum(case((engine_cc_expr.is_(None), 1), else_=0)).label("missing_engine_cc"),
                    func.sum(
                        case(
                            (and_(power_hp_expr.is_(None), power_kw_expr.is_(None)), 1),
                            else_=0,
                        )
                    ).label("missing_power"),
                )
                .where(kr_scope)
                .group_by(Car.kr_market_type)
                .order_by(func.count().desc())
            )
        ]

        samples = [
            {
                "id": int(row.id),
                "source": row.source,
                "brand": row.brand,
                "model": row.model,
                "kr_market_type": row.kr_market_type,
                "price": float(row.price) if row.price is not None else None,
                "currency": row.currency,
                "price_rub_cached": float(row.price_rub_cached) if row.price_rub_cached is not None else None,
                "total_price_rub_cached": float(row.total_price_rub_cached) if row.total_price_rub_cached is not None else None,
                "engine_cc": int(row.engine_cc) if row.engine_cc is not None else None,
                "power_hp": float(row.power_hp) if row.power_hp is not None else None,
                "power_kw": float(row.power_kw) if row.power_kw is not None else None,
                "calc_updated_at": row.calc_updated_at.isoformat() if row.calc_updated_at else None,
                "source_url": row.source_url,
            }
            for row in db.execute(
                select(
                    Car.id,
                    Source.key.label("source"),
                    Car.brand,
                    Car.model,
                    Car.kr_market_type,
                    Car.price,
                    Car.currency,
                    Car.price_rub_cached,
                    Car.total_price_rub_cached,
                    engine_cc_expr.label("engine_cc"),
                    power_hp_expr.label("power_hp"),
                    power_kw_expr.label("power_kw"),
                    Car.calc_updated_at,
                    Car.source_url,
                )
                .join(Source, Source.id == Car.source_id)
                .where(
                    kr_scope,
                    or_(
                        Car.total_price_rub_cached.is_(None),
                        without_util_expr,
                    ),
                )
                .order_by(Car.id.desc())
                .limit(max(1, int(args.sample_limit)))
            )
        ]

    print(
        json.dumps(
            {
                "summary": {key: int(value or 0) for key, value in dict(summary).items()},
                "by_market_type": by_market,
                "samples": samples,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
