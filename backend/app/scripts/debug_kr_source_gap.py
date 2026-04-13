from __future__ import annotations

import argparse
import json
from datetime import datetime

from sqlalchemy import and_, case, func, or_, select

from backend.app.db import SessionLocal
from backend.app.models import Car, Source
from backend.app.services.cars_service import CarsService


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit KR catalog scope by source, market type, and availability")
    ap.add_argument("--sample-limit", type=int, default=20)
    args = ap.parse_args()

    with SessionLocal() as db:
        service = CarsService(db)
        kr_source_ids = set(service._source_ids_for_hints(service.KOREA_SOURCE_HINTS))
        base_expr = or_(Car.country.like("KR%"), Car.source_id.in_(kr_source_ids))
        available_sum_expr = func.sum(case((Car.is_available.is_(True), 1), else_=0))
        sample_limit = max(1, int(args.sample_limit))
        country_kr_non_kr_source_expr = and_(
            Car.country.like("KR%"),
            Car.is_available.is_(True),
            Car.source_id.not_in(kr_source_ids),
        ) if kr_source_ids else False
        source_hint_non_kr_country_expr = and_(
            Car.source_id.in_(kr_source_ids),
            Car.is_available.is_(True),
            or_(Car.country.is_(None), ~Car.country.like("KR%")),
        ) if kr_source_ids else False

        summary = {
            "kr_source_ids": sorted(int(source_id) for source_id in kr_source_ids),
            "matching_total": int(db.execute(select(func.count(Car.id)).where(base_expr)).scalar() or 0),
            "matching_available": int(
                db.execute(select(func.count(Car.id)).where(base_expr, Car.is_available.is_(True))).scalar() or 0
            ),
            "country_kr_available": int(
                db.execute(select(func.count(Car.id)).where(Car.country.like("KR%"), Car.is_available.is_(True))).scalar() or 0
            ),
            "source_hint_available": int(
                db.execute(select(func.count(Car.id)).where(Car.source_id.in_(kr_source_ids), Car.is_available.is_(True))).scalar() or 0
            ),
            "country_kr_but_non_kr_source_available": int(
                db.execute(select(func.count(Car.id)).where(country_kr_non_kr_source_expr)).scalar() or 0
            ),
            "source_hint_but_country_not_kr_available": int(
                db.execute(select(func.count(Car.id)).where(source_hint_non_kr_country_expr)).scalar() or 0
            ),
            "missing_market_type_available": int(
                db.execute(
                    select(func.count(Car.id)).where(
                        base_expr,
                        Car.is_available.is_(True),
                        Car.kr_market_type.is_(None),
                    )
                ).scalar()
                or 0
            ),
        }

        by_source = []
        rows = db.execute(
            select(
                Source.key,
                func.count(Car.id).label("total"),
                available_sum_expr.label("available_sum"),
            )
            .join(Source, Source.id == Car.source_id)
            .where(base_expr)
            .group_by(Source.key)
            .order_by(func.count(Car.id).desc(), Source.key.asc())
        ).all()
        for key, total, available_sum in rows:
            available = int(available_sum or 0)
            by_source.append(
                {
                    "source": key,
                    "total": int(total or 0),
                    "available": available,
                    "unavailable": int(total or 0) - available,
                }
            )

        by_source_market_type = []
        rows = db.execute(
            select(
                Source.key,
                func.coalesce(Car.kr_market_type, "__NULL__"),
                func.count(Car.id),
                available_sum_expr,
            )
            .join(Source, Source.id == Car.source_id)
            .where(base_expr)
            .group_by(Source.key, func.coalesce(Car.kr_market_type, "__NULL__"))
            .order_by(Source.key.asc(), func.count(Car.id).desc())
        ).all()
        for key, market_type, total, available_sum in rows:
            available = int(available_sum or 0)
            by_source_market_type.append(
                {
                    "source": key,
                    "kr_market_type": market_type,
                    "total": int(total or 0),
                    "available": available,
                    "unavailable": int(total or 0) - available,
                }
            )

        by_country = []
        rows = db.execute(
            select(
                func.coalesce(Car.country, "__NULL__"),
                func.count(Car.id),
                available_sum_expr,
            )
            .where(base_expr)
            .group_by(func.coalesce(Car.country, "__NULL__"))
            .order_by(func.count(Car.id).desc())
        ).all()
        for country, total, available_sum in rows:
            available = int(available_sum or 0)
            by_country.append(
                {
                    "country": country,
                    "total": int(total or 0),
                    "available": available,
                    "unavailable": int(total or 0) - available,
                }
            )

        recent_unavailable = [
            {
                "id": int(row.id),
                "source": row.key,
                "external_id": row.external_id,
                "country": row.country,
                "kr_market_type": row.kr_market_type,
                "updated_at": _dt(row.updated_at),
                "last_seen_at": _dt(row.last_seen_at),
                "source_url": row.source_url,
            }
            for row in db.execute(
                select(
                    Car.id,
                    Source.key,
                    Car.external_id,
                    Car.country,
                    Car.kr_market_type,
                    Car.updated_at,
                    Car.last_seen_at,
                    Car.source_url,
                )
                .join(Source, Source.id == Car.source_id)
                .where(base_expr, Car.is_available.is_(False))
                .order_by(Car.updated_at.desc().nullslast(), Car.id.desc())
                .limit(sample_limit)
            ).all()
        ]

        suspicious_source_scope = [
            {
                "id": int(row.id),
                "source": row.key,
                "external_id": row.external_id,
                "country": row.country,
                "kr_market_type": row.kr_market_type,
                "updated_at": _dt(row.updated_at),
                "source_url": row.source_url,
            }
            for row in db.execute(
                select(
                    Car.id,
                    Source.key,
                    Car.external_id,
                    Car.country,
                    Car.kr_market_type,
                    Car.updated_at,
                    Car.source_url,
                )
                .join(Source, Source.id == Car.source_id)
                .where(
                    Car.is_available.is_(True),
                    or_(
                        source_hint_non_kr_country_expr,
                        country_kr_non_kr_source_expr,
                    ),
                )
                .order_by(Car.updated_at.desc().nullslast(), Car.id.desc())
                .limit(sample_limit)
            ).all()
        ]

    print(
        json.dumps(
            {
                "summary": summary,
                "by_source": by_source,
                "by_source_market_type": by_source_market_type,
                "by_country": by_country,
                "recent_unavailable_samples": recent_unavailable,
                "suspicious_scope_samples": suspicious_source_scope,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
