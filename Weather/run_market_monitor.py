from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.env import load_app_env

load_app_env(ROOT)

def _run_scan(*, top: int, min_edge: float, min_consensus: float) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(ROOT / "run_weather_models.py"),
        "--top",
        str(top),
        "--min-edge",
        str(min_edge),
        "--min-consensus",
        str(min_consensus),
    ]
    return subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run weather market scans continuously.")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(os.getenv("WEATHER_MONITOR_INTERVAL_SECONDS", "60")),
        help="Seconds between scans.",
    )
    parser.add_argument("--top", type=int, default=int(os.getenv("WEATHER_MONITOR_TOP", "5")))
    parser.add_argument("--min-edge", type=float, default=float(os.getenv("WEATHER_MIN_EDGE", "10.0")))
    parser.add_argument("--min-consensus", type=float, default=float(os.getenv("WEATHER_MIN_CONSENSUS", "0.35")))
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="How many loops to run. Use 0 for infinite monitor mode.",
    )
    args = parser.parse_args(argv)

    interval_seconds = max(5, int(args.interval_seconds))
    iteration = 0
    print(f"Monitor iniciado em {ROOT}")
    print(f"Intervalo: {interval_seconds}s | top={args.top} | min_edge={args.min_edge} | min_consensus={args.min_consensus}")

    while True:
        iteration += 1
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{started_at}] Scan #{iteration}...")
        try:
            completed = _run_scan(top=args.top, min_edge=args.min_edge, min_consensus=args.min_consensus)
        except Exception as exc:
            print(f"Falha ao executar scan: {exc}")
        else:
            output = (completed.stdout or completed.stderr or "").strip()
            if completed.returncode == 0:
                print(output or "Scan concluido com sucesso.")
            else:
                print(f"Scan falhou com codigo {completed.returncode}")
                if output:
                    print(output)

        if args.iterations > 0 and iteration >= args.iterations:
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
