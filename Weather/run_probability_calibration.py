from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.env import load_app_env
from paperbot.probability_calibration import build_probability_calibration

load_app_env(ROOT)

def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_resolved_predictions(db_path: Path) -> list[dict[str, Any]]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT generated_at, city_key, date_str, model_prob, settled_price_cents
            FROM scan_predictions
            WHERE settled_price_cents IS NOT NULL
              AND model_prob IS NOT NULL
            ORDER BY generated_at ASC, id ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ajusta a calibracao historica das probabilidades do bot.")
    parser.add_argument("--db-path", default="export/db/weather_bot.db")
    parser.add_argument("--output-json", default="export/calibration/weather_probability_calibration.json")
    parser.add_argument("--bin-size", type=int, default=10)
    parser.add_argument("--prior-sample-weight", type=float, default=5.0)
    parser.add_argument("--min-group-samples", type=int, default=5)
    args = parser.parse_args(argv)

    db_path = _resolve_path(args.db_path)
    output_path = _resolve_path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _fetch_resolved_predictions(db_path)
    payload = build_probability_calibration(
        rows,
        bin_size=max(5, min(25, args.bin_size)),
        prior_sample_weight=max(0.0, args.prior_sample_weight),
        min_group_samples=max(1, args.min_group_samples),
    )
    payload["db_path"] = str(db_path)
    payload["resolved_predictions"] = len(rows)

    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": True,
                "output_json": str(output_path),
                "resolved_predictions": len(rows),
                "total_samples": payload.get("total_samples", 0),
                "global_samples": ((payload.get("global") or {}).get("sample_count") or 0),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
