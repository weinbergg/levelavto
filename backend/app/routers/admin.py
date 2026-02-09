from fastapi import APIRouter, Depends, Form, Request, UploadFile, File
from fastapi.responses import RedirectResponse, Response
from sqlalchemy.orm import Session
from pathlib import Path
import tempfile

from ..auth import require_admin
from ..db import get_db
from ..services.admin_service import AdminService
from ..services.cars_service import CarsService
from ..services.calculator_config_service import CalculatorConfigService
from ..services.calculator_extractor import CalculatorExtractor
from ..services.customs_config import reset_customs_config_cache
from ..utils.recommended_config import load_config, save_config, DEFAULT_CONFIG
from ..utils.home_content import build_home_content, default_home_content, serialize_home_content
from ..utils.customs_template import (
    load_customs_dict,
    save_customs_dict,
    bump_customs_version,
    build_util_template,
    apply_util_template,
)
from ..models import User


router = APIRouter()

CONTACT_KEYS = {
    "contact_phone": "Телефон",
    "contact_email": "Email",
    "contact_address": "Адрес",
    "contact_tg": "Telegram",
    "contact_wa": "WhatsApp",
    "contact_ig": "Instagram",
    "lead_email": "Email для заявок",
}
LEGACY_HOME_KEYS = ("hero_title", "hero_subtitle", "hero_note")


@router.get("/admin")
def admin_dashboard(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    admin_svc = AdminService(db)
    cars_svc = CarsService(db)
    featured_recommended = admin_svc.list_featured("recommended")
    content = admin_svc.list_site_content(
        list(CONTACT_KEYS.keys()) + ["home_content", *LEGACY_HOME_KEYS]
    )
    home_content = build_home_content(content)
    recommended_cfg = load_config()
    calc_cfg = CalculatorConfigService(db).latest()
    total_cars = cars_svc.total_cars()
    customs_data = load_customs_dict()
    cc_tables = set()
    for key in ("util_tables_under3", "util_tables_3_5", "util_tables_electric", "util_tables"):
        tables = customs_data.get(key) or {}
        if isinstance(tables, dict):
            cc_tables.update(tables.keys())
    customs_cc_tables = sorted(cc_tables)
    return templates.TemplateResponse(
        "admin/dashboard.html",
        {
            "request": request,
            "user": user,
            "content": content,
            "home": home_content,
            "recommended_cfg": recommended_cfg,
            "featured_recommended": featured_recommended,
            "calc_cfg": calc_cfg,
            "total_cars": total_cars,
            "customs_cc_tables": customs_cc_tables,
        },
    )


@router.post("/admin/contacts")
def update_contacts(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
    lead_email: str = Form(""),
    contact_phone: str = Form(""),
    contact_email: str = Form(""),
    contact_address: str = Form(""),
    contact_tg: str = Form(""),
    contact_wa: str = Form(""),
    contact_ig: str = Form(""),
):
    admin_svc = AdminService(db)
    admin_svc.set_site_content(
        {
            "lead_email": lead_email,
            "contact_phone": contact_phone,
            "contact_email": contact_email,
            "contact_address": contact_address,
            "contact_tg": contact_tg,
            "contact_wa": contact_wa,
            "contact_ig": contact_ig,
        }
    )
    return RedirectResponse(url="/admin", status_code=302)


@router.post("/admin/recommended")
def update_recommended(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
    max_age_years: int = Form(DEFAULT_CONFIG["max_age_years"]),
    price_min: int = Form(DEFAULT_CONFIG["price_min"]),
    price_max: int = Form(DEFAULT_CONFIG["price_max"]),
    mileage_max: int = Form(DEFAULT_CONFIG["mileage_max"]),
):
    save_config(
        {
            "max_age_years": max_age_years,
            "price_min": price_min,
            "price_max": price_max,
            "mileage_max": mileage_max,
        }
    )
    return RedirectResponse(url="/admin", status_code=302)


@router.post("/admin/featured")
def update_featured(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
    placement: str = Form("recommended"),
    car_ids: str = Form(""),
):
    ids = [int(x) for x in car_ids.replace(",", " ").split() if x.strip().isdigit()]
    AdminService(db).set_featured(placement, ids)
    return RedirectResponse(url="/admin", status_code=302)


@router.get("/admin/featured/template")
def download_featured_template(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    items = AdminService(db).list_featured("recommended")
    lines = ["# featured_template v1", "# один ID на строку"]
    lines.extend([str(fc.car_id) for fc in items])
    content = "\n".join(lines) + "\n"
    return Response(
        content,
        media_type="text/plain",
        headers={"Content-Disposition": "attachment; filename=featured_template.txt"},
    )


@router.post("/admin/featured/upload")
async def upload_featured_template(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
):
    if not file.filename.lower().endswith((".txt", ".csv")):
        return RedirectResponse(url="/admin?error=badfile", status_code=302)
    raw = (await file.read()).decode("utf-8", errors="ignore")
    ids = [int(x) for x in raw.replace(",", " ").split() if x.strip().isdigit()]
    AdminService(db).set_featured("recommended", ids)
    return RedirectResponse(url="/admin?featured_updated=1", status_code=302)


@router.get("/admin/customs/template")
def download_customs_template(
    request: Request,
    user: User = Depends(require_admin),
):
    data = load_customs_dict()
    content = build_util_template(data)
    return Response(
        content,
        media_type="text/plain",
        headers={"Content-Disposition": "attachment; filename=util_template.txt"},
    )


@router.post("/admin/customs/upload")
async def upload_customs_template(
    request: Request,
    user: User = Depends(require_admin),
    file: UploadFile = File(...),
):
    if not file.filename.lower().endswith((".txt", ".csv")):
        return RedirectResponse(url="/admin?error=badfile", status_code=302)
    raw = (await file.read()).decode("utf-8", errors="ignore")
    data = load_customs_dict()
    stats = apply_util_template(data, raw)
    bump_customs_version(data)
    save_customs_dict(data)
    reset_customs_config_cache()
    return RedirectResponse(url=f"/admin?customs_updated=1&customs_updated_cnt={stats.updated}&customs_errors={stats.errors}", status_code=302)


@router.post("/admin/customs/edit")
def edit_customs_row(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
    age_bucket: str = Form("under_3"),
    cc_table: str = Form(""),
    power_type: str = Form("kw"),
    range_from: float = Form(...),
    range_to: float = Form(...),
    price_rub: float = Form(...),
):
    line = f"{age_bucket},{cc_table},{power_type},{range_from},{range_to},{price_rub}"
    data = load_customs_dict()
    stats = apply_util_template(data, line)
    bump_customs_version(data)
    save_customs_dict(data)
    reset_customs_config_cache()
    return RedirectResponse(
        url=f"/admin?customs_updated=1&customs_updated_cnt={stats.updated}&customs_errors={stats.errors}",
        status_code=302,
    )


@router.post("/admin/home_content")
async def update_home_content(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    form = await request.form()

    def get_value(name: str) -> str:
        raw = form.get(name)
        return raw.strip() if isinstance(raw, str) else ""

    def lines(name: str) -> list[str]:
        value = get_value(name)
        return [line.strip() for line in value.splitlines() if line.strip()]

    home = default_home_content()
    home["search"]["title"] = get_value("search_title")
    home["search"]["subtitle_suffix"] = get_value("search_subtitle_suffix")
    home["search"]["submit_label"] = get_value("search_submit_label")
    home["search"]["submit_suffix"] = get_value("search_submit_suffix")
    home["search"]["reset_label"] = get_value("search_reset_label")
    home["search"]["catalog_label"] = get_value("search_catalog_label")
    home["hero"]["stats_suffix"] = get_value("hero_stats_suffix")
    home["hero"]["title"] = get_value("hero_title")
    home["hero"]["subtitle"] = get_value("hero_subtitle")
    home["hero"]["note"] = get_value("hero_note")
    home["hero"]["benefits"] = lines("hero_benefits")
    home["hero"]["actions"]["primary_label"] = get_value("hero_primary_label")
    home["hero"]["actions"]["secondary_label"] = get_value("hero_secondary_label")
    home["hero"]["why_title"] = get_value("hero_why_title")
    home["hero"]["why_items"] = lines("hero_why_items")
    home["cases"]["title"] = get_value("cases_title")
    home["cases"]["subtitle"] = get_value("cases_subtitle")
    home["about"]["eyebrow"] = get_value("about_eyebrow")
    home["about"]["title"] = get_value("about_title")
    home["about"]["subtitle"] = get_value("about_subtitle")
    home["about"]["cards"][0]["title"] = get_value("about_card1_title")
    home["about"]["cards"][0]["items"] = lines("about_card1_items")
    home["about"]["cards"][1]["title"] = get_value("about_card2_title")
    home["about"]["cards"][1]["items"] = lines("about_card2_items")
    home["about"]["cards"][2]["title"] = get_value("about_card3_title")
    home["about"]["cards"][2]["items"] = lines("about_card3_items")
    home["about"]["cards"][3]["title"] = get_value("about_card4_title")
    home["about"]["cards"][3]["items"] = lines("about_card4_items")
    home["about"]["cards"][3]["paragraph"] = get_value("about_card4_paragraph")
    home["about"]["cards"][3]["emphasis"] = get_value("about_card4_emphasis")
    home["recommended"]["title"] = get_value("recommended_title")
    home["recommended"]["subtitle"] = get_value("recommended_subtitle")
    home["recommended"]["badge_label"] = get_value("recommended_badge_label")
    home["recommended"]["empty_note"] = get_value("recommended_empty_note")
    home["vehicle_types"]["title"] = get_value("vehicle_types_title")
    home["vehicle_types"]["subtitle"] = get_value("vehicle_types_subtitle")
    home["vehicle_types"]["empty_note"] = get_value("vehicle_types_empty_note")
    home["advantages"]["title"] = get_value("advantages_title")
    home["advantages"]["subtitle"] = get_value("advantages_subtitle")
    home["advantages"]["cards"][0]["title"] = get_value("advantages_card1_title")
    home["advantages"]["cards"][0]["text"] = get_value("advantages_card1_text")
    home["advantages"]["cards"][1]["title"] = get_value("advantages_card2_title")
    home["advantages"]["cards"][1]["text"] = get_value("advantages_card2_text")
    home["advantages"]["cards"][2]["title"] = get_value("advantages_card3_title")
    home["advantages"]["cards"][2]["text"] = get_value("advantages_card3_text")
    home["brands"]["title"] = get_value("brands_title")
    home["brands"]["subtitle"] = get_value("brands_subtitle")
    home["brands"]["empty_note"] = get_value("brands_empty_note")
    home["how_it_works"]["title"] = get_value("how_title")
    home["how_it_works"]["subtitle"] = get_value("how_subtitle")
    home["how_it_works"]["steps"][0]["title"] = get_value("how_step1_title")
    home["how_it_works"]["steps"][0]["text"] = get_value("how_step1_text")
    home["how_it_works"]["steps"][1]["title"] = get_value("how_step2_title")
    home["how_it_works"]["steps"][1]["text"] = get_value("how_step2_text")
    home["how_it_works"]["steps"][2]["title"] = get_value("how_step3_title")
    home["how_it_works"]["steps"][2]["text"] = get_value("how_step3_text")
    home["how_it_works"]["steps"][3]["title"] = get_value("how_step4_title")
    home["how_it_works"]["steps"][3]["text"] = get_value("how_step4_text")
    home["seo"]["title"] = get_value("seo_title")
    home["seo"]["paragraphs"][0] = get_value("seo_p1")
    home["seo"]["paragraphs"][1] = get_value("seo_p2")
    home["seo"]["paragraphs"][2] = get_value("seo_p3")
    home["lead"]["title"] = get_value("lead_title")
    home["lead"]["subtitle"] = get_value("lead_subtitle")
    home["lead"]["submit_label"] = get_value("lead_submit_label")
    home["lead"]["note"] = get_value("lead_note")
    home["lead"]["privacy_prefix"] = get_value("lead_privacy_prefix")
    home["lead"]["privacy_link_text"] = get_value("lead_privacy_link_text")
    home["contacts"]["title"] = get_value("contacts_title")
    home["contacts"]["subtitle_suffix"] = get_value("contacts_subtitle_suffix")
    home["contacts"]["callback_label"] = get_value("contacts_callback_label")
    home["contacts"]["hours_note"] = get_value("contacts_hours_note")
    home["contacts"]["call_button"] = get_value("contacts_call_button")
    home["contacts"]["email_button"] = get_value("contacts_email_button")

    admin_svc = AdminService(db)
    admin_svc.set_site_content({"home_content": serialize_home_content(home)})
    return RedirectResponse(url="/admin", status_code=302)


@router.post("/admin/calculator/upload")
async def upload_calculator_config(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
):
    if not file.filename.lower().endswith((".xlsx", ".xlsm", ".xls")):
        return RedirectResponse(url="/admin?error=badfile", status_code=302)
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(await file.read())
            tmp_path = Path(tmp.name)
        payload = CalculatorExtractor(tmp_path).extract()
        svc = CalculatorConfigService(db)
        svc.create(payload=payload, source="upload_xlsx", comment=f"upload {file.filename}")
    except Exception:
        return RedirectResponse(url="/admin?error=calc_upload", status_code=302)
    return RedirectResponse(url="/admin", status_code=302)
