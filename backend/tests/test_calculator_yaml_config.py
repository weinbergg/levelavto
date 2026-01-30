from pathlib import Path
import copy
import pytest
import yaml

from backend.app.services.calculator_config_loader import (
    load_yaml,
    CalculatorConfigYaml,
    load_runtime_payload,
)


CFG_PATH = Path(__file__).resolve().parents[2] / "backend" / "app" / "config" / "calculator.yml"


def _find_range(rows, val):
    for r in rows:
        if r.cc_from <= val <= r.cc_to:
            return r
    return None


def test_yaml_config_loads():
    cfg = load_yaml(CFG_PATH)
    assert cfg.version
    assert cfg.scenarios.under_3_ice.label
    assert cfg.scenarios.age_3_5_ice.label
    assert cfg.scenarios.electric.label


def test_cc_coverage_for_2995():
    cfg = load_yaml(CFG_PATH)
    cc = 2995
    duty = _find_range(cfg.scenarios.under_3_ice.duty_eur_per_cc_table, cc)
    util = _find_range(cfg.scenarios.under_3_ice.util_rub_by_engine_cc, cc)
    assert duty is not None
    assert util is not None


def test_overlapping_ranges_raise():
    data = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))
    bad = copy.deepcopy(data)
    # overlap util ranges
    bad["scenarios"]["under_3_ice"]["util_rub_by_engine_cc"] = [
        {"cc_from": 100, "cc_to": 2000, "util_rub": 1},
        {"cc_from": 1500, "cc_to": 2500, "util_rub": 2},
    ]
    with pytest.raises(ValueError):
        CalculatorConfigYaml.model_validate(bad)
