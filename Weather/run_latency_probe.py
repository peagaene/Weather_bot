from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.env import load_app_env
from paperbot.live_trader import _build_client, get_account_snapshot
from paperbot.wallet_chain import resolve_public_wallet_address
from paperbot.polymarket_account import fetch_account_activity, fetch_open_positions

load_app_env(ROOT)

def _measure(label: str, fn, *, attempts: int = 3) -> dict[str, float | str]:
    samples: list[float] = []
    last_error: str | None = None
    for _ in range(max(1, attempts)):
        started = time.perf_counter()
        try:
            fn()
        except Exception as exc:
            last_error = str(exc)
            continue
        samples.append((time.perf_counter() - started) * 1000.0)
    if not samples:
        return {"label": label, "ok": "false", "error": last_error or "unknown_error"}
    return {
        "label": label,
        "ok": "true",
        "min_ms": round(min(samples), 2),
        "avg_ms": round(statistics.mean(samples), 2),
        "max_ms": round(max(samples), 2),
    }


def main() -> None:
    address = resolve_public_wallet_address()
    rows: list[dict[str, float | str]] = []
    rows.append(_measure("account_snapshot", get_account_snapshot))
    if address:
        rows.append(_measure("public_positions", lambda: fetch_open_positions(address)))
        rows.append(_measure("public_activity", lambda: fetch_account_activity(address)))

    client, error = _build_client()
    if client is not None:
        rows.append(_measure("client_get_sampling_simplified_markets", lambda: client.get_sampling_simplified_markets()))
    elif error:
        rows.append({"label": "client_build", "ok": "false", "error": error})

    print(json.dumps({"measurements": rows}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
