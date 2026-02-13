from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_perf_full_audit_script_exists_and_is_valid_bash():
    script = ROOT.parent / "scripts" / "perf_full_audit.sh"
    assert script.exists(), "scripts/perf_full_audit.sh must exist"
    completed = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr

