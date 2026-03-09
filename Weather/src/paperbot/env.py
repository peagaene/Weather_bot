from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


def load_app_env(root: Path) -> None:
    if os.getenv("WEATHER_SKIP_DOTENV", "").strip().lower() in {"1", "true", "yes", "on"}:
        return
    env_path_raw = os.getenv("WEATHER_ENV_PATH", "").strip()
    env_path = Path(env_path_raw) if env_path_raw else (root / ".env")
    load_dotenv(env_path, override=False)
