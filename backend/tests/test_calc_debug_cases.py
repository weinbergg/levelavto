import os

import pytest

from backend.app.db import SessionLocal
from backend.app.services.calc_debug import build_calc_debug


def _db_available() -> bool:
    try:
        db = SessionLocal()
        db.execute("select 1")
        db.close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _db_available(), reason="DB not available")
def test_calc_car_285235_invariants():
    if os.environ.get("CALC_DEBUG_DB") != "1":
        pytest.skip("set CALC_DEBUG_DB=1 to run DB-backed calc tests")
    db = SessionLocal()
    try:
        data = build_calc_debug(db, car_id=285235, eur_rate=91.81)
    finally:
        db.close()
    total = data["result"]["total_rub"]
    steps = {s["name"]: s for s in data["steps"]}
    assert total is not None and total > 0
    assert steps.get("eur_to_rub", {}).get("value", 0) <= total
    util = next((s for s in data["steps"] if s["name"] == "Утилизационный сбор"), None)
    assert util is None or util.get("value", 0) >= 0


@pytest.mark.skipif(not _db_available(), reason="DB not available")
def test_calc_car_383527_invariants():
    if os.environ.get("CALC_DEBUG_DB") != "1":
        pytest.skip("set CALC_DEBUG_DB=1 to run DB-backed calc tests")
    db = SessionLocal()
    try:
        data = build_calc_debug(db, car_id=383527, eur_rate=91.81)
    finally:
        db.close()
    total = data["result"]["total_rub"]
    steps = {s["name"]: s for s in data["steps"]}
    assert total is not None and total > 0
    assert steps.get("eur_to_rub", {}).get("value", 0) <= total
    util = next((s for s in data["steps"] if s["name"] == "Утилизационный сбор"), None)
    assert util is None or util.get("value", 0) >= 0
