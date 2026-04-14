from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_runtime_bootstrap_widens_parser_run_trigger_and_adds_email_verification_support():
    bootstrap = _read("app/schema_bootstrap.py")
    user_model = _read("app/models/user.py")
    email_model = _read("app/models/email_verification.py")
    model = _read("app/models/parser_run.py")
    runner = _read("app/tools/emavto_chunk_runner.py")
    assert 'mapped_column(String(64), nullable=False)' in model
    assert 'ALTER TABLE parser_runs ALTER COLUMN trigger TYPE VARCHAR(64)' in bootstrap
    assert "EmailVerificationChallenge.__table__.create" in bootstrap
    assert "ALTER TABLE users ADD COLUMN email_verified_at TIMESTAMP" in bootstrap
    assert "email_verified_at" in user_model
    assert 'class EmailVerificationChallenge(Base):' in email_model
    assert "ParserRun.__table__.create" in bootstrap
    assert "ParserRunSource.__table__.create" in bootstrap
    assert "ensure_runtime_schema()" in runner
