from __future__ import annotations

import os
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent
BASE_ENV = {
    "WEATHER_HRRR_CONDA_ENV": "base",
    "WEATHER_ENABLE_HRRR": "1",
    "WEATHER_EXECUTE_TOP": "0",
    "WEATHER_AUTO_TRADE_ENABLED": "1",
    "WEATHER_MAX_ORDERS_PER_EVENT": "1",
    "PAPERBOT_BANKROLL_USD": "18",
    "PAPERBOT_MIN_STAKE_USD": "2",
    "PAPERBOT_MAX_STAKE_USD": "2",
    "POLYMARKET_REQUEST_TIMEOUT_SECONDS": "6",
}


@dataclass
class SmokeCheck:
    label: str
    command: list[str]
    timeout_seconds: int


def _run_check(check: SmokeCheck) -> tuple[bool, str]:
    env = os.environ.copy()
    env.update(BASE_ENV)
    result = subprocess.run(
        check.command,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=check.timeout_seconds,
        check=False,
    )
    output = (result.stdout or "").strip()
    error = (result.stderr or "").strip()
    text = output or error or "(sem output)"
    if check.label == "latency_probe" and result.returncode == 0:
        try:
            payload = json.loads(output or "{}")
            measurements = payload.get("measurements") if isinstance(payload, dict) else []
            required = {"account_snapshot", "public_positions", "public_activity"}
            statuses = {
                str(item.get("label") or ""): str(item.get("ok") or "").strip().lower()
                for item in (measurements or [])
                if isinstance(item, dict)
            }
            missing_or_failed = sorted(label for label in required if statuses.get(label) != "true")
            if missing_or_failed:
                return False, f"latency_probe failed required checks: {', '.join(missing_or_failed)}"
        except Exception as exc:
            return False, f"latency_probe invalid output: {exc}"
    return result.returncode == 0, text


def main() -> int:
    python = sys.executable
    checks = [
        SmokeCheck("dashboard_py_compile", [python, "-m", "py_compile", "dashboard.py"], 60),
        SmokeCheck("scan_once", [python, "run_weather_models.py", "--run-source", "smoke_test", "--top", "10", "--show-blocked", "10", "--execute-top", "0"], 240),
        SmokeCheck("auto_trader_dry_run", [python, "run_auto_trade.py", "--iterations", "1", "--interval-seconds", "10"], 240),
        SmokeCheck("reconcile", [python, "run_reconcile_positions.py"], 120),
        SmokeCheck("latency_probe", [python, "run_latency_probe.py"], 180),
    ]

    failures = 0
    for check in checks:
        ok, text = _run_check(check)
        status = "OK" if ok else "FAIL"
        print(f"[{status}] {check.label}")
        print(text.splitlines()[0] if text else "")
        if not ok:
            failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
