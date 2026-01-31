from backend.app.services.customs_config import get_customs_config, calc_util_fee_rub


def test_util_under3_bucket_hp():
    cfg = get_customs_config()
    util = calc_util_fee_rub(engine_cc=2995, kw=0, hp=286, cfg=cfg)
    assert util == 2620800


def test_util_1995_hp_bucket():
    cfg = get_customs_config()
    util = calc_util_fee_rub(engine_cc=1995, kw=0, hp=190, cfg=cfg)
    assert util == 900000


def test_util_kw_bucket():
    cfg = get_customs_config()
    util = calc_util_fee_rub(engine_cc=3200, kw=170.0, hp=None, cfg=cfg)
    assert util == 2743200
