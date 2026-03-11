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
from paperbot.weather_models import (
    _fetch_accuweather_daily,
    _fetch_brightsky_daily,
    _fetch_meteoblue_daily,
    _fetch_meteosource_daily,
    _fetch_met_norway_daily,
    _fetch_pirateweather_daily,
    _fetch_tomorrow_daily,
    _fetch_weatherstack_daily,
    _fetch_weatherbit_daily,
    fetch_noaa_isd_observed_daily_highs,
)


class ProviderExpansionTests(unittest.TestCase):
    def test_fetch_noaa_isd_observed_daily_highs_parses_daily_max(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "PAR")
        history_csv = (
            "USAF,WBAN,STATION NAME,CTRY,STATE,CALL,LAT,LON,ELEV(M),BEGIN,END\n"
            "07156,99999,PARIS TEST,FR,,LFPG,48.85,2.35,100,20200101,20261231\n"
        )
        access_csv = (
            "DATE,TMP\n"
            "2026-03-10T06:00:00+00:00,+0100,1\n"
            "2026-03-10T12:00:00+00:00,+0150,1\n"
            "2026-03-11T03:00:00+00:00,+0090,1\n"
        )

        def fake_request_text(url: str, **_: object) -> str:
            if "isd-history.csv" in url:
                return history_csv
            if url.endswith("/2026/07156-99999.csv"):
                return access_csv
            raise AssertionError(url)

        with patch("paperbot.weather_models._request_text", side_effect=fake_request_text):
            station_id, by_date = fetch_noaa_isd_observed_daily_highs(city, lookback_days=3)
        self.assertEqual(station_id, "07156-99999")
        self.assertIn("2026-03-10", by_date)
        self.assertGreater(by_date["2026-03-10"], by_date.get("2026-03-11", 0.0))

    def test_build_station_observation_rows_falls_back_to_noaa_isd(self) -> None:
        paris = next(item for item in CITY_CONFIGS if item.key == "PAR")
        with patch(
            "run_weather_models.fetch_meteostat_observed_daily_highs",
            return_value=(None, {}),
        ), patch(
            "run_weather_models.fetch_noaa_isd_observed_daily_highs",
            return_value=("07156-99999", {"2026-03-10": 54.5}),
        ):
            rows = run_weather_models._build_station_observation_rows(captured_at="2026-03-11T00:00:00+00:00")
        paris_rows = [row for row in rows if row["city_key"] == paris.key]
        self.assertEqual(len(paris_rows), 1)
        self.assertEqual(paris_rows[0]["station_id"], "07156-99999")
        self.assertEqual(paris_rows[0]["source"], "noaa_isd_observation")
        self.assertEqual(paris_rows[0]["observed_high_f"], 54.5)

    def test_fetch_weatherbit_daily_parses_daily_rows(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")
        payload = {
            "data": [
                {"datetime": "2026-03-11", "max_temp": 58.0, "min_temp": 44.0},
                {"datetime": "2026-03-12", "max_temp": 60.0, "min_temp": 46.0},
            ]
        }
        with patch("paperbot.weather_models.WEATHERBIT_API_KEY", "test-key"), patch(
            "paperbot.weather_models._request_json",
            return_value=payload,
        ):
            rows = _fetch_weatherbit_daily(city)
        self.assertEqual([item.model_name for item in rows], ["weatherbit", "weatherbit"])
        self.assertEqual(rows[0].date, "2026-03-11")
        self.assertEqual(rows[0].high, 58.0)
        self.assertEqual(rows[0].source, "weatherbit.io")

    def test_fetch_meteosource_daily_parses_daily_rows(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "PAR")
        payload = {
            "daily": {
                "data": [
                    {
                        "day": "2026-03-11",
                        "all_day": {"temperature_max": 57.2, "temperature_min": 44.1},
                    },
                    {
                        "day": "2026-03-12",
                        "all_day": {"temperature_max": 60.0, "temperature_min": 45.0},
                    },
                ]
            }
        }
        with patch("paperbot.weather_models.WEATHER_ENABLE_METEOSOURCE", True), patch("paperbot.weather_models.METEOSOURCE_API_KEY", "test-key"), patch(
            "paperbot.weather_models._request_json",
            return_value=payload,
        ):
            rows = _fetch_meteosource_daily(city)
        self.assertEqual([item.model_name for item in rows], ["meteosource", "meteosource"])
        self.assertEqual(rows[0].date, "2026-03-11")
        self.assertEqual(rows[0].high, 57.2)
        self.assertEqual(rows[0].source, "meteosource.com")

    def test_fetch_met_norway_daily_aggregates_hourly_rows(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")
        payload = {
            "properties": {
                "timeseries": [
                    {
                        "time": "2026-03-11T18:00:00Z",
                        "data": {"instant": {"details": {"air_temperature": 5.0}}},
                    },
                    {
                        "time": "2026-03-11T20:00:00Z",
                        "data": {"instant": {"details": {"air_temperature": 9.0}}},
                    },
                    {
                        "time": "2026-03-11T22:00:00Z",
                        "data": {"instant": {"details": {"air_temperature": 7.0}}},
                    },
                ]
            }
        }
        with patch("paperbot.weather_models.WEATHER_ENABLE_MET_NORWAY", True), patch("paperbot.weather_models._request_json", return_value=payload):
            rows = _fetch_met_norway_daily(city)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].model_name, "met_norway")
        self.assertEqual(rows[0].date, "2026-03-11")
        self.assertAlmostEqual(rows[0].high, 48.2, places=1)
        self.assertAlmostEqual(rows[0].low, 41.0, places=1)
        self.assertEqual(rows[0].source, "api.met.no")

    def test_fetch_brightsky_daily_aggregates_hourly_rows_for_munich(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "MUC")
        payload = {
            "weather": [
                {"timestamp": "2026-03-11T06:00:00+01:00", "temperature": 5.0},
                {"timestamp": "2026-03-11T13:00:00+01:00", "temperature": 11.0},
                {"timestamp": "2026-03-11T20:00:00+01:00", "temperature": 7.0},
            ]
        }
        with patch("paperbot.weather_models.WEATHER_ENABLE_BRIGHTSKY", True), patch(
            "paperbot.weather_models._request_json",
            return_value=payload,
        ):
            rows = _fetch_brightsky_daily(city)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].model_name, "brightsky")
        self.assertEqual(rows[0].date, "2026-03-11")
        self.assertAlmostEqual(rows[0].high, 51.8, places=1)
        self.assertAlmostEqual(rows[0].low, 41.0, places=1)
        self.assertEqual(rows[0].source, "brightsky.dev")

    def test_fetch_pirateweather_daily_parses_daily_rows(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "LON")
        payload = {
            "daily": {
                "data": [
                    {"time": 1773273600, "temperatureHigh": 57.0, "temperatureLow": 44.0},
                    {"time": 1773360000, "temperatureHigh": 60.0, "temperatureLow": 46.0},
                ]
            }
        }
        with patch("paperbot.weather_models.WEATHER_ENABLE_PIRATEWEATHER", True), patch(
            "paperbot.weather_models.PIRATEWEATHER_API_KEY",
            "test-key",
        ), patch(
            "paperbot.weather_models._request_json",
            return_value=payload,
        ):
            rows = _fetch_pirateweather_daily(city)
        self.assertEqual([item.model_name for item in rows], ["pirateweather", "pirateweather"])
        self.assertEqual(rows[0].high, 57.0)
        self.assertEqual(rows[0].low, 44.0)
        self.assertEqual(rows[0].source, "pirateweather.net")

    def test_fetch_meteoblue_daily_parses_daily_rows(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "PAR")
        payload = {
            "data_day": {
                "time": ["2026-03-11", "2026-03-12"],
                "temperature_max": [57.0, 60.0],
                "temperature_min": [44.0, 46.0],
            }
        }
        with patch("paperbot.weather_models.METEOBLUE_API_KEY", "test-key"), patch(
            "paperbot.weather_models._request_json",
            return_value=payload,
        ):
            rows = _fetch_meteoblue_daily(city)
        self.assertEqual([item.model_name for item in rows], ["meteoblue", "meteoblue"])
        self.assertEqual(rows[0].date, "2026-03-11")
        self.assertEqual(rows[0].high, 57.0)
        self.assertEqual(rows[0].source, "meteoblue.com")

    def test_fetch_weatherstack_daily_parses_daily_rows(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "LON")
        payload = {
            "forecast": {
                "2026-03-11": {"maxtemp": 54.0, "mintemp": 42.0},
                "2026-03-12": {"maxtemp": 56.0, "mintemp": 43.0},
            }
        }
        with patch("paperbot.weather_models.WEATHERSTACK_API_KEY", "test-key"), patch(
            "paperbot.weather_models._request_json",
            return_value=payload,
        ):
            rows = _fetch_weatherstack_daily(city)
        self.assertEqual([item.model_name for item in rows], ["weatherstack", "weatherstack"])
        self.assertEqual(rows[0].date, "2026-03-11")
        self.assertEqual(rows[0].high, 54.0)
        self.assertEqual(rows[0].source, "weatherstack.com")

    def test_fetch_accuweather_daily_parses_daily_rows(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "NYC")

        def fake_request(url: str, **_: object) -> object:
            if "locations/v1/cities/geoposition/search" in url:
                return {"Key": "349727"}
            if "forecasts/v1/daily/5day/349727" in url:
                return {
                    "DailyForecasts": [
                        {
                            "Date": "2026-03-11T07:00:00-04:00",
                            "Temperature": {
                                "Maximum": {"Value": 59.0},
                                "Minimum": {"Value": 43.0},
                            },
                        }
                    ]
                }
            raise AssertionError(url)

        with patch("paperbot.weather_models.ACCUWEATHER_API_KEY", "test-key"), patch(
            "paperbot.weather_models._request_json",
            side_effect=fake_request,
        ):
            rows = _fetch_accuweather_daily(city)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].model_name, "accuweather")
        self.assertEqual(rows[0].date, "2026-03-11")
        self.assertEqual(rows[0].high, 59.0)
        self.assertEqual(rows[0].source, "developer.accuweather.com")

    def test_fetch_tomorrow_daily_requests_imperial_units(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")
        requested = {}

        def fake_request(url: str, **_: object) -> object:
            requested["url"] = url
            return {
                "timelines": {
                    "daily": [
                        {
                            "time": "2026-03-11T00:00:00Z",
                            "values": {"temperatureMax": 52.0, "temperatureMin": 42.0},
                        }
                    ]
                }
            }

        with patch("paperbot.weather_models.TOMORROW_API_KEY", "test-key"), patch(
            "paperbot.weather_models._request_json",
            side_effect=fake_request,
        ):
            rows = _fetch_tomorrow_daily(city)
        self.assertEqual(len(rows), 1)
        self.assertIn("units=imperial", requested["url"])


if __name__ == "__main__":
    unittest.main()
