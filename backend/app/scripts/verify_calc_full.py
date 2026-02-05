from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from sqlalchemy import text

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.calc_debug import build_calc_debug
from backend.app.services.calculator_runtime import _calc_age_months, is_bev
from backend.app.services.cars_service import CarsService
from backend.app.services.customs_config import calc_duty_eur, calc_util_fee_rub, get_customs_config
from backend.app.utils.telegram import send_telegram_message


def _parse_countries(val: str | None) -> list[str] | None:
    if not val:
        return None
    out = [v.strip().upper() for v in val.replace(" ", ",").split(",") if v.strip()]
    return out or None


def _country_filter_sql(countries: list[str] | None) -> tuple[str, Dict[str, Any]]:
    if not countries:
        return "", {}
    return " AND country = ANY(:countries)", {"countries": countries}


def _run_count(db, sql: str, params: Dict[str, Any]) -> int:
    return int(db.execute(text(sql), params).scalar() or 0)


def _select_ids(db, sql: str, params: Dict[str, Any], limit: int) -> list[int]:
    rows = db.execute(text(sql + " LIMIT :limit"), {**params, "limit": limit}).fetchall()
    return [int(r[0]) for r in rows]


def _extract_step(steps: list[dict], name: str) -> float | None:
    for s in steps:
        if s.get("name") == name:
            try:
                return float(s.get("value"))
            except Exception:
                return None
    return None


def _sample_groups(db, countries: list[str] | None, n: int) -> dict[str, list[int]]:
    where_country, params = _country_filter_sql(countries)

    samples: dict[str, list[int]] = {}
    # under_3
    sql_under3 = (
        "SELECT id FROM cars WHERE is_available IS true"
        + where_country
        + " AND (registration_year IS NOT NULL)"
        + " AND (EXTRACT(year FROM now()) - COALESCE(registration_year, year)) * 12"
        + " + (EXTRACT(month FROM now()) - COALESCE(registration_month, 1)) < 36"
    )
    samples["under_3"] = _select_ids(db, sql_under3, params, n)

    # 3_5
    sql_35 = (
        "SELECT id FROM cars WHERE is_available IS true"
        + where_country
        + " AND (registration_year IS NOT NULL)"
        + " AND (EXTRACT(year FROM now()) - COALESCE(registration_year, year)) * 12"
        + " + (EXTRACT(month FROM now()) - COALESCE(registration_month, 1)) BETWEEN 36 AND 60"
    )
    samples["3_5"] = _select_ids(db, sql_35, params, n)

    # BEV
    sql_bev = (
        "SELECT id FROM cars WHERE is_available IS true"
        + where_country
        + " AND COALESCE(engine_cc, 0) = 0"
        + " AND (COALESCE(power_kw, 0) > 0 OR COALESCE(power_hp, 0) > 0)"
    )
    samples["bev"] = _select_ids(db, sql_bev, params, n)

    # PHEV-like (engine_cc>0, engine_type electric)
    sql_phev = (
        "SELECT id FROM cars WHERE is_available IS true"
        + where_country
        + " AND COALESCE(engine_cc, 0) > 0"
        + " AND LOWER(COALESCE(engine_type, '')) LIKE '%electric%'"
    )
    samples["phev"] = _select_ids(db, sql_phev, params, n)

    return samples


def _calc_age_bucket(car: Car) -> str:
    reg_year = car.registration_year or car.year
    reg_month = car.registration_month or 1
    age_months = _calc_age_months(reg_year, reg_month) if reg_year else None
    if is_bev(car.engine_cc, car.power_kw, car.power_hp, car.engine_type):
        return "electric"
    if age_months is None:
        return "unknown"
    return "under_3" if age_months < 36 else "3_5"


def _verify_sample(db, ids: Iterable[int], eur_rate: float) -> list[dict[str, Any]]:
    cfg = get_customs_config()
    svc = CarsService(db)
    out: list[dict[str, Any]] = []
    for car in db.query(Car).filter(Car.id.in_(list(ids))).all():
        age_bucket = _calc_age_bucket(car)
        if age_bucket in ("under_3", "3_5") and not car.engine_cc:
            out.append(
                {
                    "id": car.id,
                    "country": car.country,
                    "engine_cc": car.engine_cc,
                    "power_hp": car.power_hp,
                    "power_kw": car.power_kw,
                    "engine_type": car.engine_type,
                    "age_bucket": age_bucket,
                    "error": "missing engine_cc for ICE scenario",
                }
            )
            continue
        util_expected = calc_util_fee_rub(
            engine_cc=car.engine_cc or 0,
            kw=float(car.power_kw) if car.power_kw is not None else None,
            hp=int(car.power_hp) if car.power_hp is not None else None,
            cfg=cfg,
            age_bucket=age_bucket,
        )
        duty_expected = 0.0
        if age_bucket == "3_5" and (car.engine_cc or 0) > 0:
            duty_expected = calc_duty_eur(car.engine_cc or 0, cfg) * eur_rate

        try:
            result = build_calc_debug(db, car.id, eur_rate=eur_rate)
            steps = result.get("steps") or []
            util_actual = _extract_step(steps, "Утилизационный сбор")
            duty_actual = _extract_step(steps, "Пошлина РФ")
            total_actual = _extract_step(steps, "Итого (RUB)")
        except Exception as exc:
            out.append(
                {
                    "id": car.id,
                    "country": car.country,
                    "engine_cc": car.engine_cc,
                    "power_hp": car.power_hp,
                    "power_kw": car.power_kw,
                    "engine_type": car.engine_type,
                    "age_bucket": age_bucket,
                    "error": str(exc),
                }
            )
            continue

        out.append(
            {
                "id": car.id,
                "country": car.country,
                "engine_cc": car.engine_cc,
                "power_hp": car.power_hp,
                "power_kw": car.power_kw,
                "engine_type": car.engine_type,
                "age_bucket": age_bucket,
                "util_expected": util_expected,
                "util_actual": util_actual,
                "duty_expected": duty_expected,
                "duty_actual": duty_actual,
                "total_rub": total_actual,
                "ok_util": util_actual == util_expected,
                "ok_duty": duty_actual == duty_expected,
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--countries", default=os.getenv("COUNTRIES"))
    parser.add_argument("--sample-per-group", type=int, default=10)
    parser.add_argument("--stride", type=int, default=0, help="sample every Nth id for util check (0=off)")
    parser.add_argument("--max-stride", type=int, default=0, help="limit stride sample size (0=off)")
    parser.add_argument("--out", default="/app/artifacts/verify_calc_full_report.json")
    parser.add_argument("--telegram", action="store_true")
    args = parser.parse_args()

    countries = _parse_countries(args.countries)

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "countries": countries or "ALL",
        "counts": {},
        "samples": {},
    }

    with SessionLocal() as db:
        svc = CarsService(db)
        fx = svc.get_fx_rates(allow_fetch=True) or {}
        eur_rate = float(fx.get("EUR") or 95.0)
        report["fx"] = fx
        report["customs_version"] = get_customs_config().version

        where_country, params = _country_filter_sql(countries)
        base = "FROM cars WHERE is_available IS true" + where_country

        report["counts"]["total_available"] = _run_count(db, "SELECT count(*) " + base, params)
        report["counts"]["missing_total"] = _run_count(
            db,
            "SELECT count(*) " + base + " AND total_price_rub_cached IS NULL",
            params,
        )
        report["counts"]["missing_breakdown"] = _run_count(
            db,
            "SELECT count(*) " + base + " AND calc_breakdown_json IS NULL",
            params,
        )
        report["counts"]["missing_util_step"] = _run_count(
            db,
            "SELECT count(*) "
            + base
            + " AND calc_breakdown_json IS NOT NULL"
            + " AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(calc_breakdown_json::jsonb) e"
            + " WHERE e->>'name'='Утилизационный сбор')",
            params,
        )
        report["counts"]["mismatch_total_vs_breakdown"] = _run_count(
            db,
            "SELECT count(*) "
            + base
            + " AND total_price_rub_cached IS NOT NULL"
            + " AND calc_breakdown_json IS NOT NULL"
            + " AND EXISTS ("
            + "   SELECT 1 FROM jsonb_array_elements(calc_breakdown_json::jsonb) e"
            + "   WHERE e->>'name'='Итого (RUB)'"
            + "     AND abs((e->>'value')::numeric - total_price_rub_cached) > 1"
            + " )",
            params,
        )

        if args.stride and args.stride > 0:
            stride_where = " AND (cars.id % :stride) = 0"
            stride_params = {**params, "stride": args.stride}
            limit_clause = ""
            if args.max_stride and args.max_stride > 0:
                limit_clause = " LIMIT :limit"
                stride_params["limit"] = args.max_stride
            sql_stride = (
                "SELECT cars.id FROM cars WHERE is_available IS true"
                + where_country
                + stride_where
                + " ORDER BY cars.id"
                + limit_clause
            )
            stride_ids = [int(r[0]) for r in db.execute(text(sql_stride), stride_params).fetchall()]
            report["counts"]["stride_sample"] = len(stride_ids)
            report["samples"]["stride_sample"] = _verify_sample(db, stride_ids, eur_rate)

        samples = _sample_groups(db, countries, args.sample_per_group)
        for group, ids in samples.items():
            report["samples"][group] = _verify_sample(db, ids, eur_rate)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)

    if args.telegram:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id:
            summary = (
                "verify_calc_full done\n"
                f"countries: {report['countries']}\n"
                f"fx EUR: {report['fx'].get('EUR')}\n"
                f"missing_total: {report['counts'].get('missing_total')}\n"
                f"missing_util_step: {report['counts'].get('missing_util_step')}\n"
                f"mismatch_total_vs_breakdown: {report['counts'].get('mismatch_total_vs_breakdown')}\n"
                f"report: {args.out}"
            )
            send_telegram_message(token, chat_id, summary)


if __name__ == "__main__":
    main()
