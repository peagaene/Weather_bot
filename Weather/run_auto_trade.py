from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

load_dotenv(ROOT / ".env", override=False)

from paperbot.trading_state import FileLock


def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _load_replay_gate() -> tuple[bool, str | None]:
    gate_path = _resolve_path(os.getenv("WEATHER_REPLAY_GATE_PATH", "export/replay/replay_gate.json"))
    if not gate_path.exists():
        return False, f"replay gate not found: {gate_path}"
    try:
        import json

        payload = json.loads(gate_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, f"invalid replay gate: {exc}"
    if not isinstance(payload, dict):
        return False, "invalid replay gate payload"
    if payload.get("approved") is not True:
        return False, "replay gate exists but is not approved"
    return True, None


def _run_weather_models(*, top: int, min_edge: float, min_consensus: float, execute_top: int, live: bool) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(ROOT / "run_weather_models.py"),
        "--top",
        str(top),
        "--min-edge",
        str(min_edge),
        "--min-consensus",
        str(min_consensus),
        "--execute-top",
        str(execute_top),
    ]
    if live:
        command.append("--live")
    return subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=360,
        check=False,
    )


def _preflight_or_raise(*, live: bool, execute_top: int) -> None:
    bankroll = float(os.getenv("PAPERBOT_BANKROLL_USD", "0") or 0)
    min_stake = float(os.getenv("PAPERBOT_MIN_STAKE_USD", "0") or 0)
    max_stake = float(os.getenv("PAPERBOT_MAX_STAKE_USD", "0") or 0)
    daily_limit = int(os.getenv("WEATHER_DAILY_LIVE_LIMIT", "0") or 0)
    bucket_live_limit = int(os.getenv("WEATHER_BUCKET_LIVE_LIMIT", "0") or 0)
    max_orders_per_event = int(os.getenv("WEATHER_MAX_ORDERS_PER_EVENT", "0") or 0)
    max_share_size = float(os.getenv("WEATHER_MAX_SHARE_SIZE", "0") or 0)
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    auto_live_enabled = _env_bool("WEATHER_AUTO_TRADE_ENABLED", default=False)
    allow_unapproved_replay = _env_bool("WEATHER_ALLOW_UNAPPROVED_REPLAY_FOR_MICRO_LIVE", default=False)

    if execute_top != 1:
        raise RuntimeError("safe auto-trade requires WEATHER_EXECUTE_TOP/--execute-top = 1")
    if max_orders_per_event != 1:
        raise RuntimeError("safe auto-trade requires WEATHER_MAX_ORDERS_PER_EVENT = 1")
    if bankroll <= 0:
        raise RuntimeError("PAPERBOT_BANKROLL_USD must be configured")
    if min_stake <= 0:
        raise RuntimeError("PAPERBOT_MIN_STAKE_USD must be > 0")
    if max_stake <= 0:
        raise RuntimeError("PAPERBOT_MAX_STAKE_USD must be > 0 for safe auto-trade")
    if max_stake > 2:
        raise RuntimeError("safe auto-trade blocks PAPERBOT_MAX_STAKE_USD > 2")
    if daily_limit <= 0:
        raise RuntimeError("WEATHER_DAILY_LIVE_LIMIT must be > 0")
    if bucket_live_limit <= 0:
        raise RuntimeError("WEATHER_BUCKET_LIVE_LIMIT must be > 0")
    if bucket_live_limit > 2:
        raise RuntimeError("safe auto-trade blocks WEATHER_BUCKET_LIVE_LIMIT > 2")
    if max_share_size <= 0:
        raise RuntimeError("WEATHER_MAX_SHARE_SIZE must be > 0")
    if live:
        if not auto_live_enabled:
            raise RuntimeError("WEATHER_AUTO_TRADE_ENABLED is not enabled")
        if not private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY is not configured")
        replay_ok, replay_error = _load_replay_gate()
        if not replay_ok:
            if not allow_unapproved_replay:
                raise RuntimeError(replay_error or "replay gate is not approved")
            if max_stake > 2 or daily_limit > 3 or bucket_live_limit > 2:
                raise RuntimeError("unsafe micro-live override rejected: limits exceed validation caps")
            print(
                "AVISO: replay gate nao aprovado; seguindo em modo de validacao micro-live "
                "com limites simbolicos."
            )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Safe automatic live trader for weather opportunities.")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(os.getenv("WEATHER_AUTO_TRADE_INTERVAL_SECONDS", os.getenv("WEATHER_MONITOR_INTERVAL_SECONDS", "60"))),
        help="Seconds between automated cycles.",
    )
    parser.add_argument("--top", type=int, default=int(os.getenv("WEATHER_MONITOR_TOP", "5")))
    parser.add_argument("--min-edge", type=float, default=float(os.getenv("WEATHER_MIN_EDGE", "10.0")))
    parser.add_argument("--min-consensus", type=float, default=float(os.getenv("WEATHER_MIN_CONSENSUS", "0.35")))
    parser.add_argument("--execute-top", type=int, default=int(os.getenv("WEATHER_EXECUTE_TOP", "1")))
    parser.add_argument("--iterations", type=int, default=0, help="0 means infinite loop.")
    parser.add_argument("--live", action="store_true", help="Actually send live orders. Requires WEATHER_AUTO_TRADE_ENABLED=1.")
    parser.add_argument(
        "--lock-file",
        default=os.getenv("WEATHER_AUTO_TRADER_LOCK_FILE", "export/state/auto_trader.lock"),
        help="Single-instance lock for the auto-trader process.",
    )
    args = parser.parse_args(argv)

    interval_seconds = max(10, int(args.interval_seconds))
    lock_path = _resolve_path(args.lock_file)
    _preflight_or_raise(live=args.live, execute_top=args.execute_top)

    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"Auto trader iniciado em {ROOT}")
    print(
        f"Modo: {mode} | intervalo={interval_seconds}s | top={args.top} | "
        f"min_edge={args.min_edge} | min_consensus={args.min_consensus} | execute_top={args.execute_top}"
    )

    iteration = 0
    with FileLock(lock_path, timeout_seconds=1.0, poll_seconds=0.1, stale_seconds=300.0):
        while True:
            iteration += 1
            started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{started_at}] Auto cycle #{iteration}...")
            try:
                completed = _run_weather_models(
                    top=args.top,
                    min_edge=args.min_edge,
                    min_consensus=args.min_consensus,
                    execute_top=args.execute_top,
                    live=args.live,
                )
            except Exception as exc:
                print(f"Falha ao executar ciclo: {exc}")
            else:
                output = (completed.stdout or completed.stderr or "").strip()
                if completed.returncode == 0:
                    print(output or "Ciclo concluido com sucesso.")
                else:
                    print(f"Ciclo falhou com codigo {completed.returncode}")
                    if output:
                        print(output)

            if args.iterations > 0 and iteration >= args.iterations:
                break
            time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
