from backend.app.utils.telegram import format_daily_report, resolve_telegram_chat_id


def test_format_daily_report():
    payload = {
        "dataset_version": "123",
        "eur_rate": 91.08,
        "usd_rate": 76.2,
        "import_stats": {
            "seen": 10,
            "inserted": 3,
            "updated": 5,
            "deactivated": 1,
            "deactivation_allowed": True,
            "deactivate_mode": "auto",
            "deactivate_previous_seen": 12,
            "deactivate_reason": "ratio=0.9500>=0.9300",
        },
        "totals": {"active_total": 999},
        "by_source": {"mobilede": 900, "korea": 99},
        "elapsed_sec": 42,
    }
    msg = format_daily_report(payload)
    assert "dataset_version: 123" in msg
    assert "rates: EUR=91.08 USD=76.2" in msg
    assert "import: seen=10 inserted=3 updated=5 deactivated=1" in msg
    assert "deactivation: mode=auto allowed=yes prev_seen=12 reason=ratio=0.9500>=0.9300" in msg
    assert "active_total: 999" in msg
    assert "by_source: mobilede=900, korea=99" in msg
    assert "elapsed_sec: 42" in msg


def test_resolve_telegram_chat_id_prefers_admin_then_chat_then_allowed(monkeypatch):
    monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "admin")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")
    monkeypatch.setenv("TELEGRAM_ALLOWED_IDS", "111,222")
    assert resolve_telegram_chat_id() == "admin"

    monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID")
    assert resolve_telegram_chat_id() == "chat"

    monkeypatch.delenv("TELEGRAM_CHAT_ID")
    assert resolve_telegram_chat_id() == "111"
