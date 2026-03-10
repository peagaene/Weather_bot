from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.degendoppler import CITY_CONFIGS
from paperbot.env import load_app_env
from paperbot.polymarket_weather import fetch_market_scan
from paperbot.storage import WeatherBotStorage

load_app_env(ROOT)

CITY_BY_KEY = {city.key: city for city in CITY_CONFIGS}


def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


def _serialize_market_scan(scan, *, captured_at: str, source: str) -> list[dict]:
    rows: list[dict] = []
    for bucket in scan.buckets:
        rows.append(
            {
                "captured_at": captured_at,
                "source": source,
                "city_key": scan.city_key,
                "date_str": scan.date_str,
                "event_slug": scan.event_slug,
                "event_title": scan.event_title,
                "market_slug": bucket.market_slug,
                "market_id": bucket.market_id,
                "bucket": bucket.label,
                "token_id_yes": bucket.token_id_yes,
                "token_id_no": bucket.token_id_no,
                "yes_price_cents": bucket.yes_price_cents,
                "no_price_cents": bucket.no_price_cents,
                "yes_best_ask_cents": bucket.yes_best_ask_cents,
                "no_best_ask_cents": bucket.no_best_ask_cents,
                "yes_best_bid_cents": bucket.yes_best_bid_cents,
                "no_best_bid_cents": bucket.no_best_bid_cents,
                "last_trade_price": bucket.last_trade_price,
                "order_min_size": bucket.order_min_size,
                "raw_json": {
                    "question": bucket.question,
                    "min_value": bucket.min_value,
                    "max_value": bucket.max_value,
                    "probability": bucket.probability,
                },
            }
        )
    return rows


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Backfill minimo de snapshots de mercado para analytics e replay.")
    parser.add_argument("--db-path", default="export/db/weather_bot.db")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--source", default="gamma_clob_backfill")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    storage = WeatherBotStorage(_resolve_path(args.db_path))
    targets = storage.list_recent_market_targets(limit=max(1, args.limit), lookback_days=max(1, args.lookback_days))
    captured_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    seen_events: set[str] = set()
    for target in targets:
        city_key = str(target.get("city_key") or "").strip().upper()
        date_str = str(target.get("date_str") or "").strip()
        event_slug = str(target.get("event_slug") or "").strip()
        if not city_key or not date_str or not event_slug or event_slug in seen_events:
            continue
        city = CITY_BY_KEY.get(city_key)
        if city is None:
            continue
        try:
            target_date = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        scan = fetch_market_scan(city, target_date)
        if scan is None:
            continue
        rows.extend(_serialize_market_scan(scan, captured_at=captured_at, source=str(args.source)))
        seen_events.add(event_slug)

    inserted = storage.record_market_history_snapshots(rows)
    payload = {
        "captured_at": captured_at,
        "targets_considered": len(targets),
        "events_snapshotted": len(seen_events),
        "snapshots_inserted": inserted,
    }
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(
            f"market history backfill: targets={payload['targets_considered']} "
            f"events={payload['events_snapshotted']} snapshots={payload['snapshots_inserted']}"
        )


if __name__ == "__main__":
    main()
