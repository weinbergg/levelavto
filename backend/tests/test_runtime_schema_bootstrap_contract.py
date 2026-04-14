from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_runtime_bootstrap_widens_parser_run_trigger_and_runner_calls_it():
    bootstrap = _read("app/schema_bootstrap.py")
    model = _read("app/models/parser_run.py")
    runner = _read("app/tools/emavto_chunk_runner.py")
    assert 'mapped_column(String(64), nullable=False)' in model
    assert 'ALTER TABLE parser_runs ALTER COLUMN trigger TYPE VARCHAR(64)' in bootstrap
    assert "ParserRun.__table__.create" in bootstrap
    assert "ParserRunSource.__table__.create" in bootstrap
    assert "ensure_runtime_schema()" in runner
