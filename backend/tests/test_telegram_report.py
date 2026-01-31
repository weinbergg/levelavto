from backend.app.utils.telegram import format_daily_report


def test_format_daily_report():
    payload = {
        "dataset_version": "123",
        "eur_rate": 91.08,
        "usd_rate": 76.2,
        "import_stats": {"seen": 10, "inserted": 3, "updated": 5, "deactivated": 1},
        "totals": {"active_total": 999},
        "by_source": {"mobilede": 900, "korea": 99},
        "elapsed_sec": 42,
    }
    msg = format_daily_report(payload)
    assert "dataset_version: 123" in msg
    assert "rates: EUR=91.08 USD=76.2" in msg
    assert "import: seen=10 inserted=3 updated=5 deactivated=1" in msg
    assert "active_total: 999" in msg
    assert "by_source: mobilede=900, korea=99" in msg
    assert "elapsed_sec: 42" in msg
