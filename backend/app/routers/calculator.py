from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from ..services.calculator import calculate_import_cost, get_eur_rate
from ..services.calculator_config_service import CalculatorConfigService
from ..services.calculator_extractor import CalculatorExtractor
from ..services.cars_service import CarsService
from ..services.calc_debug import build_calc_debug
from fastapi import UploadFile, File
import tempfile
import json
from pathlib import Path
from ..db import get_db
from ..models.car import Car
from ..utils.taxonomy import ru_body, ru_color, ru_fuel
from ..utils.breakdown_labels import label_for


class CalcRequest(BaseModel):
    scenario: Optional[str] = Field(None, pattern="^(under_3|3_5|electric)$")
    price_net_eur: Optional[float] = None
    car_id: Optional[int] = None
    source: Optional[str] = None
    eur_rate: Optional[float] = None
    usd_rate: Optional[float] = None
    engine_cc: Optional[int] = None
    power_hp: Optional[float] = None
    power_kw: Optional[float] = None
    first_registration_year: Optional[int] = None
    first_registration_month: Optional[int] = None
    is_electric: Optional[bool] = False
    other: Optional[Dict[str, Any]] = None


router = APIRouter(prefix="/api", tags=["calculator"])


@router.post("/calc")
def calc_endpoint(payload: CalcRequest, db: Session = Depends(get_db)):
    try:
        data = payload.dict()
        car_id = data.get("car_id")
        is_electric = bool(data.get("is_electric"))
        # price fallback from car if provided
        if car_id:
            car = db.query(Car).filter(Car.id == car_id).first()
            if not car:
                raise HTTPException(status_code=404, detail="car not found")
            if data.get("price_net_eur") is None and car.price and car.currency:
                fx = CarsService(db).get_fx_rates() or {}
                eur_rate = data.get("eur_rate") or fx.get("EUR") or get_eur_rate(data.get("eur_rate"))
                usd_rate = data.get("usd_rate") or fx.get("USD")
                cur = car.currency.lower()
                if cur == "eur":
                    data["price_net_eur"] = float(car.price)
                elif cur == "usd":
                    if not usd_rate or not eur_rate:
                        raise HTTPException(status_code=400, detail="USD price provided but no USD/EUR rate")
                    data["price_net_eur"] = float(car.price) * (usd_rate / eur_rate)
                elif cur == "rub":
                    if not eur_rate:
                        raise HTTPException(status_code=400, detail="RUB price provided but no EUR rate")
                    data["price_net_eur"] = float(car.price) / eur_rate
            if data.get("engine_cc") is None and car.engine_type != "electric":
                data["engine_cc"] = car.engine_type and None or None
            if data.get("power_hp") is None and car.engine_type == "electric":
                data["power_hp"] = None
            # registration fields
            if data.get("first_registration_year") is None and hasattr(car, "registration_year"):
                data["first_registration_year"] = getattr(car, "registration_year", None)
            if data.get("first_registration_month") is None and hasattr(car, "registration_month"):
                data["first_registration_month"] = getattr(car, "registration_month", None)
            if car.engine_type and "electric" in car.engine_type.lower():
                is_electric = True
        data["is_electric"] = is_electric

        cfg_svc = CalculatorConfigService(db)
        cfg = cfg_svc.ensure_default_from_yaml(Path("/app/backend/app/config/calculator.yml"))
        if not cfg:
            cfg = cfg_svc.ensure_default_from_path(Path("/app/Калькулятор Авто под заказ.xlsx"))
        if not cfg:
            raise HTTPException(status_code=400, detail="calculator config not found")
        from ..services.calculator_runtime import EstimateRequest, calculate

        # auto eur_rate from CB if not provided
        fx = CarsService(db).get_fx_rates() or {}
        if data.get("eur_rate") is None:
            data["eur_rate"] = fx.get("EUR") or cfg.payload.get("meta", {}).get("eur_rate_default") or 95.0
        if data.get("usd_rate") is None:
            data["usd_rate"] = fx.get("USD") or cfg.payload.get("meta", {}).get("usd_rate_default") or 85.0

        req_obj = EstimateRequest(
            scenario=data.get("scenario"),
            price_net_eur=data.get("price_net_eur") or 0,
            eur_rate=data.get("eur_rate"),
            engine_cc=data.get("engine_cc"),
            power_hp=data.get("power_hp"),
            power_kw=data.get("power_kw"),
            is_electric=data.get("is_electric", False),
            reg_year=data.get("first_registration_year"),
            reg_month=data.get("first_registration_month"),
        )
        result = calculate(cfg.payload, req_obj)
        # локализация и приведение к RUB для UI
        label_map = cfg.payload.get("label_map", {})
        eur_rate_used = result.get("euro_rate_used") or data.get("eur_rate")
        disp = []
        for item in result.get("breakdown", []):
            t = item.get("title")
            cur = (item.get("currency") or "RUB").upper()
            amt = float(item.get("amount") or 0)
            ru = label_map.get(t, label_for(t)) if t else ""
            rub = amt
            if cur == "EUR" and eur_rate_used:
                rub = amt * eur_rate_used
            disp.append({
                "title": ru,
                "amount_rub": round(rub, 2),
                "original_amount": round(amt, 2),
                "original_currency": cur,
            })
        result["display_breakdown"] = disp
        result["price_net_eur_used"] = req_obj.price_net_eur
        result["eur_rate_used"] = eur_rate_used
        result["usd_rate_used"] = data.get("usd_rate")
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/calc_debug")
def calc_debug_endpoint(
    car_id: int = Query(..., ge=1),
    eur_rate: Optional[float] = Query(default=None),
    usd_rate: Optional[float] = Query(default=None),
    scenario: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    try:
        return build_calc_debug(db, car_id=car_id, eur_rate=eur_rate, usd_rate=usd_rate, scenario=scenario)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/calculator/config/current")
def calc_config_current(db: Session = Depends(get_db)):
    svc = CalculatorConfigService(db)
    cfg = svc.latest()
    if not cfg:
        raise HTTPException(status_code=404, detail="config not found")
    return {"version": cfg.version, "comment": cfg.comment, "source": cfg.source, "payload": cfg.payload}


@router.post("/calculator/config/upload_xlsx")
def calc_config_upload(file: UploadFile = File(...), db: Session = Depends(get_db)):
    # TODO: restrict to admin (auth not wired here)
    if not file.filename.lower().endswith((".xlsx", ".xlsm", ".xls")):
        raise HTTPException(status_code=400, detail="only Excel files are allowed")
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = file.file.read()
            tmp.write(data)
            tmp_path = tmp.name
        extractor = CalculatorExtractor(Path(tmp_path))
        payload = extractor.extract()
        svc = CalculatorConfigService(db)
        old = svc.latest()
        diff = svc.diff_payloads(old.payload if old else None, payload)
        cfg = svc.create(payload=payload, source="upload_xlsx", comment=f"upload {file.filename}")
        return {"version": cfg.version, "payload": cfg.payload, "diff": diff}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"failed to parse excel: {e}")
