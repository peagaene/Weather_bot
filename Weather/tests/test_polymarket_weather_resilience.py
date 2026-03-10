from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.degendoppler import CITY_CONFIGS
from paperbot.polymarket_weather import fetch_market_scan


class PolymarketWeatherResilienceTests(unittest.TestCase):
    def test_fetch_market_scan_returns_none_when_gamma_request_fails(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")
        with patch("paperbot.polymarket_weather._request_json", side_effect=TimeoutError("timeout")):
            scan = fetch_market_scan(city, datetime(2026, 3, 10, tzinfo=timezone.utc))
        self.assertIsNone(scan)


if __name__ == "__main__":
    unittest.main()
