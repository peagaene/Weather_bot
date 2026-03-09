from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.env import load_app_env
from paperbot.reconciliation import sync_open_positions
from paperbot.storage import WeatherBotStorage

load_app_env(ROOT)

def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


def main() -> None:
    db_path = _resolve_path(os.getenv("WEATHER_DB_PATH", "export/db/weather_bot.db"))
    storage = WeatherBotStorage(db_path)
    summary = sync_open_positions(storage)
    print(
        f"Mercados checados: {summary['checked_markets']} | "
        f"Posicoes atualizadas: {summary['updated_positions']} | "
        f"Erros: {len(summary['errors'])}"
    )
    for error in summary["errors"]:
        print(f"- {error['market_slug']}: {error['error']}")


if __name__ == "__main__":
    main()
