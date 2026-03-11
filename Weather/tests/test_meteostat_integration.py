from __future__ import annotations

import unittest
from pathlib import Path
import sys
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import run_weather_models
from paperbot.degendoppler import CITY_CONFIGS
from paperbot.weather_models import fetch_meteostat_observed_daily_highs


class MeteostatIntegrationTests(unittest.TestCase):
    def test_fetch_meteostat_observed_daily_highs_returns_empty_without_runtime(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "PAR")
        with patch("paperbot.weather_models.importlib.import_module", side_effect=ModuleNotFoundError("meteostat")):
            station_id, rows = fetch_meteostat_observed_daily_highs(city, lookback_days=3)
        self.assertIsNone(station_id)
        self.assertEqual(rows, {})

    def test_build_station_observation_rows_uses_meteostat_for_celsius_city(self) -> None:
        paris = next(item for item in CITY_CONFIGS if item.key == "PAR")
        with patch(
            "run_weather_models.fetch_meteostat_observed_daily_highs",
            return_value=("07156", {"2026-03-10": 54.5}),
        ):
            rows = run_weather_models._build_station_observation_rows(captured_at="2026-03-11T00:00:00+00:00")
        paris_rows = [row for row in rows if row["city_key"] == paris.key]
        self.assertEqual(len(paris_rows), 1)
        self.assertEqual(paris_rows[0]["station_id"], "07156")
        self.assertEqual(paris_rows[0]["source"], "meteostat_daily_observation")
        self.assertEqual(paris_rows[0]["observed_high_f"], 54.5)


if __name__ == "__main__":
    unittest.main()
