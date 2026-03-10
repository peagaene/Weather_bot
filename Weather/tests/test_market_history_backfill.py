from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.polymarket_weather import MarketScan
from paperbot.storage import WeatherBotStorage
from paperbot.degendoppler import MarketBucket
from run_market_history_backfill import _serialize_market_scan


class MarketHistoryBackfillTests(unittest.TestCase):
    def test_storage_records_and_lists_market_history_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "weather_bot.db"
            storage = WeatherBotStorage(db_path)
            inserted = storage.record_market_history_snapshots(
                [
                    {
                        "captured_at": "2026-03-10T00:00:00+00:00",
                        "source": "gamma_clob_backfill",
                        "city_key": "SEA",
                        "date_str": "2026-03-10",
                        "event_slug": "event-1",
                        "event_title": "Event 1",
                        "market_slug": "market-1",
                        "market_id": "m1",
                        "bucket": "46-47°F",
                        "token_id_yes": "y1",
                        "token_id_no": "n1",
                        "yes_price_cents": 34.0,
                        "no_price_cents": 66.0,
                        "yes_best_ask_cents": 35.0,
                        "no_best_ask_cents": 67.0,
                        "yes_best_bid_cents": 33.0,
                        "no_best_bid_cents": 65.0,
                        "last_trade_price": 0.34,
                        "order_min_size": 5.0,
                        "raw_json": {"question": "test"},
                    }
                ]
            )
            self.assertEqual(inserted, 1)
            rows = storage.list_market_history_snapshots(market_slug="market-1", limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["event_slug"], "event-1")
            self.assertEqual(rows[0]["raw_json"]["question"], "test")

    def test_serialize_market_scan_returns_one_row_per_bucket(self) -> None:
        scan = MarketScan(
            city_key="SEA",
            date_str="2026-03-10",
            event_slug="event-1",
            event_title="Event 1",
            buckets=[
                MarketBucket(
                    label="46-47°F",
                    min_value=46,
                    max_value=47,
                    probability=0.34,
                    yes_price_cents=34.0,
                    no_price_cents=66.0,
                    question="Will it be 46-47?",
                    market_slug="market-1",
                    market_id="m1",
                    token_id_yes="y1",
                    token_id_no="n1",
                    best_ask=0.34,
                    last_trade_price=0.35,
                    order_min_size=5.0,
                    yes_best_ask_cents=35.0,
                    no_best_ask_cents=67.0,
                    yes_best_bid_cents=33.0,
                    no_best_bid_cents=65.0,
                    yes_last_trade_cents=None,
                    no_last_trade_cents=None,
                )
            ],
        )
        rows = _serialize_market_scan(scan, captured_at="2026-03-10T00:00:00+00:00", source="gamma")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market_slug"], "market-1")
        self.assertEqual(rows[0]["bucket"], "46-47°F")


if __name__ == "__main__":
    unittest.main()
