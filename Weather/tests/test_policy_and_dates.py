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
from paperbot.policy import _load_policy_profile, apply_trade_policy, parse_bucket_bounds
from paperbot.polymarket_weather import _local_target_dates


class PolicyAndDatesTests(unittest.TestCase):
    def test_parse_bucket_bounds_standard_degree_labels(self) -> None:
        self.assertEqual(parse_bucket_bounds("84-85°F"), (84.0, 85.0))
        self.assertEqual(parse_bucket_bounds("46°F or higher"), (46.0, None))
        self.assertEqual(parse_bucket_bounds("45°F or below"), (None, 45.0))

    def test_local_target_dates_use_city_timezone(self) -> None:
        reference = datetime(2026, 3, 9, 1, 30, tzinfo=timezone.utc)
        sea = next(city for city in CITY_CONFIGS if city.key == "SEA")
        mia = next(city for city in CITY_CONFIGS if city.key == "MIA")

        sea_dates = _local_target_dates(sea, reference, 2)
        mia_dates = _local_target_dates(mia, reference, 2)

        self.assertEqual(sea_dates[0].date().isoformat(), "2026-03-08")
        self.assertEqual(mia_dates[0].date().isoformat(), "2026-03-08")
        self.assertEqual(sea_dates[1].date().isoformat(), "2026-03-09")
        self.assertEqual(mia_dates[1].date().isoformat(), "2026-03-09")

    def test_policy_allows_actionable_b_tier_signal_after_relaxation(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "bucket": "84-85Â°F",
                "consensus_score": 0.8,
                "spread": 1.2,
                "sigma": 1.8,
                "ensemble_prediction": 84.2,
                "confidence_tier": "safe",
                "signal_tier": "B",
                "edge": 24.0,
                "min_agreeing_model_edge": 14.0,
                "price_cents": 28.0,
                "coverage_ok": True,
                "degraded_reason": None,
                "executable_quality_score": 0.9,
                "data_quality_score": 0.9,
            },
        )()
        decision = apply_trade_policy(opportunity)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, "allowed")

    def test_policy_allows_fallback_coverage_for_strong_4_of_4_consensus(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "bucket": "80-81Ã‚Â°F",
                "consensus_score": 0.74,
                "spread": 1.1,
                "sigma": 1.9,
                "ensemble_prediction": 80.5,
                "confidence_tier": "safe",
                "signal_tier": "C",
                "edge": 28.0,
                "min_agreeing_model_edge": 18.0,
                "price_cents": 32.0,
                "coverage_ok": False,
                "coverage_issue_type": "provider_failure",
                "valid_model_count": 4,
                "required_model_count": 4,
                "agreement_models": 4,
                "total_models": 4,
                "provider_failures": ["best_match", "ecmwf", "gfs", "icon", "gem", "jma", "ecmwf_ens", "gfs_ens", "icon_ens"],
                "degraded_reason": "provider_failures:best_match,ecmwf,gfs,icon,gem,jma,ecmwf_ens,gfs_ens,icon_ens",
                "executable_quality_score": 0.82,
                "data_quality_score": 0.35,
            },
        )()
        decision = apply_trade_policy(opportunity)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, "allowed")

    def test_policy_allows_isolated_nws_failure_when_other_core_support_is_strong(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "bucket": "80-81Ãƒâ€šÃ‚Â°F",
                "consensus_score": 0.71,
                "spread": 1.2,
                "sigma": 1.8,
                "ensemble_prediction": 80.6,
                "confidence_tier": "safe",
                "signal_tier": "B",
                "edge": 22.0,
                "min_agreeing_model_edge": 12.0,
                "price_cents": 31.0,
                "coverage_ok": False,
                "coverage_score": 0.64,
                "coverage_issue_type": "provider_failure",
                "valid_model_count": 5,
                "required_model_count": 5,
                "agreement_models": 5,
                "total_models": 5,
                "provider_failures": ["nws"],
                "degraded_reason": "provider_failures:nws",
                "executable_quality_score": 0.75,
                "data_quality_score": 0.74,
            },
        )()
        decision = apply_trade_policy(opportunity)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, "allowed")

    def test_policy_blocks_historically_bad_city_even_if_signal_is_strong(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "MIA",
                "day_label": "tomorrow",
                "bucket": "72-73Â°F",
                "consensus_score": 0.82,
                "spread": 1.0,
                "sigma": 1.4,
                "ensemble_prediction": 72.4,
                "confidence_tier": "safe",
                "signal_tier": "A",
                "edge": 28.0,
                "min_agreeing_model_edge": 15.0,
                "price_cents": 29.0,
                "coverage_ok": True,
                "coverage_score": 0.82,
                "degraded_reason": None,
                "executable_quality_score": 0.82,
                "data_quality_score": 0.83,
            },
        )()
        decision = apply_trade_policy(opportunity)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "city_blocked_historical_underperformance")

    def test_policy_requires_higher_confidence_for_today(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "SEA",
                "day_label": "today",
                "bucket": "46Â°F or higher",
                "consensus_score": 0.62,
                "spread": 1.1,
                "sigma": 1.7,
                "ensemble_prediction": 46.5,
                "confidence_tier": "near-safe",
                "signal_tier": "B",
                "edge": 21.0,
                "min_agreeing_model_edge": 13.0,
                "price_cents": 31.0,
                "coverage_ok": True,
                "coverage_score": 0.8,
                "degraded_reason": None,
                "executable_quality_score": 0.78,
                "data_quality_score": 0.8,
            },
        )()
        decision = apply_trade_policy(opportunity)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "today_requires_higher_confidence")

    def test_policy_applies_extra_thresholds_for_caution_bucket(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "SEA",
                "day_label": "tomorrow",
                "bucket": "84-85Â°F",
                "consensus_score": 0.72,
                "spread": 1.0,
                "sigma": 1.1,
                "ensemble_prediction": 83.4,
                "confidence_tier": "safe",
                "signal_tier": "A",
                "edge": 12.0,
                "min_agreeing_model_edge": 10.0,
                "price_cents": 26.0,
                "coverage_ok": True,
                "coverage_score": 0.83,
                "degraded_reason": None,
                "executable_quality_score": 0.81,
                "data_quality_score": 0.84,
            },
        )()
        with patch.dict("os.environ", {"WEATHER_POLICY_CAUTION_BUCKETS": "84-85F"}):
            decision = apply_trade_policy(opportunity)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "historical_segment_edge_too_low")

    def test_policy_uses_generated_profile_json(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "CHI",
                "day_label": "tomorrow",
                "bucket": "72-73Â°F",
                "consensus_score": 0.82,
                "spread": 1.0,
                "sigma": 1.1,
                "ensemble_prediction": 72.5,
                "confidence_tier": "safe",
                "signal_tier": "A",
                "edge": 24.0,
                "min_agreeing_model_edge": 14.0,
                "price_cents": 26.0,
                "coverage_ok": True,
                "coverage_score": 0.83,
                "degraded_reason": None,
                "executable_quality_score": 0.81,
                "data_quality_score": 0.84,
            },
        )()
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "policy_profile.json"
            profile_path.write_text('{"blocked_city_keys":["CHI"]}', encoding="utf-8")
            _load_policy_profile.cache_clear()
            try:
                with patch.dict("os.environ", {"WEATHER_POLICY_PROFILE_PATH": str(profile_path)}, clear=False):
                    decision = apply_trade_policy(opportunity)
            finally:
                _load_policy_profile.cache_clear()
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "city_blocked_historical_underperformance")


if __name__ == "__main__":
    unittest.main()
