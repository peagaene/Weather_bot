from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.degendoppler import CITY_CONFIGS
from paperbot.weather_models import (
    _hrrr_daily_cache_key,
    _cache_key_for_name,
    _load_source_weight_profile,
    _load_truth_weight_profile,
    ModelForecast,
    _fetch_nws_daily,
    _hrrr_step_schedule,
    _hrrr_target_local_dates,
    _run_hrrr_subset_parser,
    build_ensemble_for_date,
)


class WeatherModelsEnsembleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._source_weight_patcher = patch("paperbot.weather_models._resolve_source_weight_multipliers", return_value={})
        self._truth_weight_patcher = patch("paperbot.weather_models._resolve_truth_weight_multipliers", return_value={})
        self._source_weight_patcher.start()
        self._truth_weight_patcher.start()

    def tearDown(self) -> None:
        self._source_weight_patcher.stop()
        self._truth_weight_patcher.stop()

    def test_robust_blend_stays_close_when_models_agree(self) -> None:
        city = CITY_CONFIGS[0]
        forecasts = {
            "best_match": [ModelForecast("best_match", "2026-03-08", 71.0)],
            "ecmwf": [ModelForecast("ecmwf", "2026-03-08", 72.0)],
            "gfs": [ModelForecast("gfs", "2026-03-08", 71.5)],
            "icon": [ModelForecast("icon", "2026-03-08", 72.5)],
            "nws": [ModelForecast("nws", "2026-03-08", 72.0)],
        }

        ensemble = build_ensemble_for_date(city, forecasts, "2026-03-08")

        self.assertIsNotNone(ensemble)
        assert ensemble is not None
        self.assertGreaterEqual(ensemble.blended_high, 71.3)
        self.assertLessEqual(ensemble.blended_high, 72.3)
        self.assertLessEqual(ensemble.sigma, 2.0)

    def test_robust_blend_downweights_single_outlier(self) -> None:
        city = CITY_CONFIGS[0]
        forecasts = {
            "best_match": [ModelForecast("best_match", "2026-03-08", 71.0)],
            "ecmwf": [ModelForecast("ecmwf", "2026-03-08", 72.0)],
            "gfs": [ModelForecast("gfs", "2026-03-08", 71.5)],
            "icon": [ModelForecast("icon", "2026-03-08", 72.5)],
            "nws": [ModelForecast("nws", "2026-03-08", 72.0)],
            "openweather": [ModelForecast("openweather", "2026-03-08", 86.0)],
        }

        ensemble = build_ensemble_for_date(city, forecasts, "2026-03-08")

        self.assertIsNotNone(ensemble)
        assert ensemble is not None
        self.assertLess(ensemble.blended_high, 75.0)
        self.assertGreaterEqual(ensemble.sigma, 1.5)
        self.assertLess(ensemble.consensus_score, 0.9)

    def test_city_horizon_calibration_applies_bias_and_weight_multiplier(self) -> None:
        city = CITY_CONFIGS[0]
        forecasts = {
            "best_match": [ModelForecast("best_match", "2026-03-08", 72.0)],
            "gfs": [ModelForecast("gfs", "2026-03-08", 76.0)],
            "ecmwf": [ModelForecast("ecmwf", "2026-03-08", 73.0)],
        }

        with patch(
            "paperbot.weather_models._resolve_calibration",
            return_value=({"gfs": -3.0, "best_match": 0.5}, {"ecmwf": 1.5, "gfs": 0.5}),
        ):
            ensemble = build_ensemble_for_date(city, forecasts, "2026-03-08", horizon_days=1)

        self.assertIsNotNone(ensemble)
        assert ensemble is not None
        self.assertEqual(ensemble.predictions["gfs"], 73.0)
        self.assertEqual(ensemble.predictions["best_match"], 72.5)
        self.assertGreaterEqual(ensemble.blended_high, 72.5)
        self.assertLessEqual(ensemble.blended_high, 73.5)

    def test_source_weight_profile_is_composed_with_existing_weights(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "NYC")
        forecasts = {
            "gfs": [ModelForecast("gfs", "2026-03-08", 73.0)],
            "nws": [ModelForecast("nws", "2026-03-08", 72.5)],
            "best_match": [ModelForecast("best_match", "2026-03-08", 72.8)],
        }
        self._source_weight_patcher.stop()
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "source_weight_profile.json"
            profile_path.write_text(
                """
                {
                  "global": {"model_weight_multiplier": {"gfs": 0.8}},
                  "cities": {"NYC": {"model_weight_multiplier": {"gfs": 1.1}}},
                  "regimes": {"coastal": {"model_weight_multiplier": {"gfs": 1.2}}}
                }
                """.strip(),
                encoding="utf-8",
            )
            _load_source_weight_profile.cache_clear()
            try:
                with patch.dict("os.environ", {"WEATHER_SOURCE_WEIGHT_PROFILE_PATH": str(profile_path)}, clear=False):
                    ensemble = build_ensemble_for_date(city, forecasts, "2026-03-08")
            finally:
                _load_source_weight_profile.cache_clear()
                self._source_weight_patcher.start()

        self.assertIsNotNone(ensemble)
        assert ensemble is not None
        self.assertAlmostEqual(ensemble.effective_weights["gfs"], 1.056, places=3)

    def test_truth_weight_profile_is_composed_with_existing_weights(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")
        forecasts = {
            "nws": [ModelForecast("nws", "2026-03-08", 72.5)],
            "gfs": [ModelForecast("gfs", "2026-03-08", 73.0)],
        }
        self._truth_weight_patcher.stop()
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "truth_weight_profile.json"
            profile_path.write_text(
                """
                {
                  "global": {"model_weight_multiplier": {"gfs": 0.9}},
                  "cities": {"SEA": {"model_weight_multiplier": {"gfs": 0.95}, "day_labels": {"today": {"model_weight_multiplier": {"gfs": 0.92}}}}},
                  "regimes": {"coastal": {"model_weight_multiplier": {"gfs": 0.98}}}
                }
                """.strip(),
                encoding="utf-8",
            )
            _load_truth_weight_profile.cache_clear()
            try:
                with patch.dict("os.environ", {"WEATHER_TRUTH_WEIGHT_PROFILE_PATH": str(profile_path)}, clear=False):
                    ensemble = build_ensemble_for_date(city, forecasts, "2026-03-08", horizon_days=0)
            finally:
                _load_truth_weight_profile.cache_clear()
                self._truth_weight_patcher.start()

        self.assertIsNotNone(ensemble)
        assert ensemble is not None
        self.assertAlmostEqual(ensemble.effective_weights["gfs"], 1.0 * 0.9 * 0.95 * 0.92 * 0.98 * 0.85, places=3)

    def test_short_horizon_weights_favor_near_term_sources(self) -> None:
        city = CITY_CONFIGS[0]
        forecasts = {
            "nws": [ModelForecast("nws", "2026-03-08", 78.0)],
            "best_match": [ModelForecast("best_match", "2026-03-08", 77.0)],
            "ecmwf": [ModelForecast("ecmwf", "2026-03-08", 70.0)],
            "gfs": [ModelForecast("gfs", "2026-03-08", 69.0)],
        }

        today = build_ensemble_for_date(city, forecasts, "2026-03-08", horizon_days=0)
        later = build_ensemble_for_date(city, forecasts, "2026-03-08", horizon_days=2)

        self.assertIsNotNone(today)
        self.assertIsNotNone(later)
        assert today is not None
        assert later is not None
        self.assertGreater(today.blended_high, later.blended_high)
        self.assertGreater(today.effective_weights["nws"], later.effective_weights["nws"])
        self.assertLess(today.effective_weights["gfs"], later.effective_weights["gfs"])

    def test_coverage_issue_type_and_failure_details_are_exposed(self) -> None:
        city = CITY_CONFIGS[0]
        forecasts = {
            "best_match": [ModelForecast("best_match", "2026-03-08", 71.0)],
        }

        ensemble = build_ensemble_for_date(
            city,
            forecasts,
            "2026-03-08",
            provider_failures=["gfs", "ecmwf"],
            provider_failure_details={"gfs": "HTTP 502", "ecmwf": "timeout"},
        )

        self.assertIsNotNone(ensemble)
        assert ensemble is not None
        self.assertEqual(ensemble.coverage_issue_type, "mixed")
        self.assertEqual(ensemble.valid_model_count, 1)
        self.assertGreaterEqual(ensemble.required_model_count, 5)
        self.assertEqual(ensemble.provider_failure_details, {"gfs": "HTTP 502", "ecmwf": "timeout"})

    def test_rate_limited_failures_get_specific_coverage_type(self) -> None:
        city = CITY_CONFIGS[0]
        forecasts = {
            "best_match": [ModelForecast("best_match", "2026-03-08", 71.0)],
        }

        ensemble = build_ensemble_for_date(
            city,
            forecasts,
            "2026-03-08",
            provider_failures=["gfs", "ecmwf"],
            provider_failure_details={"gfs": "HTTP 429 fetching ...", "ecmwf": "HTTP 429 fetching ..."},
        )

        self.assertIsNotNone(ensemble)
        assert ensemble is not None
        self.assertEqual(ensemble.coverage_issue_type, "mixed_rate_limited")

    def test_hrrr_step_schedule_uses_coarser_steps_after_near_term_window(self) -> None:
        steps = _hrrr_step_schedule(36)
        self.assertIn(0, steps)
        self.assertIn(24, steps)
        self.assertIn(30, steps)
        self.assertIn(36, steps)
        self.assertNotIn(27, steps)
        self.assertNotIn(33, steps)

    def test_hrrr_target_dates_focus_on_short_horizon(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")
        dates = _hrrr_target_local_dates(
            city,
            reference=datetime(2026, 3, 9, 6, 0, tzinfo=timezone.utc),
            target_days=2,
        )
        self.assertEqual(dates, ["2026-03-08", "2026-03-09"])

    def test_hrrr_daily_cache_key_includes_target_dates(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")
        key_a = _hrrr_daily_cache_key(city, "20260309", 12, ["2026-03-08", "2026-03-09"])
        key_b = _hrrr_daily_cache_key(city, "20260309", 12, ["2026-03-09", "2026-03-10"])
        self.assertNotEqual(key_a, key_b)

    def test_fetch_nws_daily_uses_stale_city_cache_on_failure(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")
        cached_payload = [
            {
                "model_name": "nws",
                "date": "2026-03-09",
                "high": 61.0,
                "low": None,
                "source": "weather.gov",
            }
        ]
        with patch("paperbot.weather_models._request_json_with_retry", side_effect=RuntimeError("timeout")):
            with patch(
                "paperbot.weather_models._load_cached_response",
                side_effect=lambda key, max_age_seconds: cached_payload if key == _cache_key_for_name(f"nws_daily/{city.key}") else None,
            ):
                rows = _fetch_nws_daily(city)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].model_name, "nws")
        self.assertEqual(rows[0].date, "2026-03-09")
        self.assertEqual(rows[0].high, 61.0)

    def test_fetch_nws_daily_uses_station_observation_plus_hourly_for_today(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")

        def _fake_request(url: str, provider_name: str, attempts: int):
            if url.endswith(f"/points/{city.lat},{city.lon}"):
                return {
                    "properties": {
                        "forecast": "https://api.weather.gov/gridpoints/SEW/forecast",
                        "forecastHourly": "https://api.weather.gov/gridpoints/SEW/forecast/hourly",
                    }
                }
            if url.endswith("/forecast"):
                return {
                    "properties": {
                        "periods": [
                            {
                                "isDaytime": True,
                                "startTime": "2026-03-10T06:00:00-07:00",
                                "temperature": 46,
                            }
                        ]
                    }
                }
            if url.endswith("/forecast/hourly"):
                return {
                    "properties": {
                        "periods": [
                            {
                                "startTime": "2026-03-10T14:00:00-07:00",
                                "temperature": 48,
                                "temperatureUnit": "F",
                            }
                        ]
                    }
                }
            if "/stations/" in url and "/observations" in url:
                return {
                    "features": [
                        {
                            "properties": {
                                "timestamp": "2026-03-10T18:00:00+00:00",
                                "temperature": {
                                    "value": 10.0,
                                    "unitCode": "wmoUnit:degC",
                                },
                            }
                        }
                    ]
                }
            raise AssertionError(url)

        with patch("paperbot.weather_models._request_json_with_retry", side_effect=_fake_request):
            with patch("paperbot.weather_models.datetime") as mock_datetime:
                mock_datetime.now.return_value = datetime(2026, 3, 10, 20, 0, tzinfo=timezone.utc)
                mock_datetime.fromisoformat.side_effect = datetime.fromisoformat
                rows = _fetch_nws_daily(city)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].date, "2026-03-10")
        self.assertEqual(rows[0].high, 50.0)

    def test_hrrr_subset_parser_prefers_inprocess_runtime(self) -> None:
        city = next(item for item in CITY_CONFIGS if item.key == "SEA")
        with patch("paperbot.weather_models._hrrr_inprocess_runtime_available", return_value=True):
            with patch(
                "paperbot.weather_models._run_hrrr_subset_parser_inprocess",
                return_value=("2026-03-09T12:00:00+00:00", 58.0),
            ) as inprocess:
                result = _run_hrrr_subset_parser(Path("C:/tmp/fake.grib2"), city)
        self.assertEqual(result, ("2026-03-09T12:00:00+00:00", 58.0))
        inprocess.assert_called_once()


if __name__ == "__main__":
    unittest.main()
