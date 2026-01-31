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


def test_yaml_config_loads():
    cfg = load_yaml(CFG_PATH)
    assert cfg.version
    assert cfg.scenarios.under_3.label
    assert cfg.scenarios.age_3_5.label
    assert cfg.scenarios.electric.label


def test_config_has_required_fields():
    cfg = load_yaml(CFG_PATH)
    assert cfg.scenarios.under_3.bank_transfer_eu.percent >= 0
    assert cfg.scenarios.age_3_5.purchase_netto.percent >= 0
    assert cfg.scenarios.electric.import_duty_percent >= 0


def test_overlapping_ranges_raise():
    data = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))
    bad = copy.deepcopy(data)
    # remove required key to trigger validation
    del bad["scenarios"]["under_3"]["bank_transfer_eu"]
    with pytest.raises(Exception):
        CalculatorConfigYaml.model_validate(bad)
