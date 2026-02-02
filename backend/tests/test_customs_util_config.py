from pathlib import Path
import pytest

from backend.app.services.customs_config import load_customs_config, calc_util_fee_rub, calc_duty_eur

CFG_PATH = Path(__file__).resolve().parents[2] / "backend" / "app" / "config" / "customs.yml"


def _cfg():
    return load_customs_config(CFG_PATH)


def test_util_2995_hp():
    cfg = _cfg()
    util = calc_util_fee_rub(engine_cc=2995, kw=0, hp=286, cfg=cfg)
    assert util == 2620800


def test_util_1995_hp():
    cfg = _cfg()
    util = calc_util_fee_rub(engine_cc=1995, kw=0, hp=286, cfg=cfg)
    assert util == 1291200


def test_util_3200_kw():
    cfg = _cfg()
    util = calc_util_fee_rub(engine_cc=3200, kw=170.0, hp=None, cfg=cfg)
    assert util == 2743200


def test_util_boundary_hp_to():
    cfg = _cfg()
    util = calc_util_fee_rub(engine_cc=1995, kw=0, hp=190, cfg=cfg)
    assert util == 900000


def test_duty_calc():
    cfg = _cfg()
    duty = calc_duty_eur(engine_cc=1500, cfg=cfg)
    assert duty == pytest.approx(2550.0, abs=0.1)


def test_duty_out_of_range_uses_max_bucket():
    cfg = _cfg()
    duty = calc_duty_eur(engine_cc=10000, cfg=cfg)
    assert duty == pytest.approx(36000.0, abs=0.1)


def test_util_out_of_range_uses_max_bucket():
    cfg = _cfg()
    util = calc_util_fee_rub(engine_cc=12000, kw=None, hp=200, cfg=cfg)
    assert util == 3403200
