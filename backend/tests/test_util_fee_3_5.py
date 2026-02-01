from backend.app.services.customs_config import get_customs_config, calc_util_fee_rub


def test_util_fee_3_5_hp_bucket_281_310():
    cfg = get_customs_config()
    util = calc_util_fee_rub(engine_cc=2900, kw=None, hp=286, cfg=cfg, age_bucket="3_5")
    assert util == 3770400


def test_util_fee_3_5_hp_boundary():
    cfg = get_customs_config()
    util = calc_util_fee_rub(engine_cc=2900, kw=None, hp=281, cfg=cfg, age_bucket="3_5")
    assert util == 3770400
