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
from paperbot.policy import _load_policy_profile, apply_trade_policy, effective_price_bounds, parse_bucket_bounds
from paperbot.polymarket_weather import _local_target_dates


class PolicyAndDatesTests(unittest.TestCase):
    def test_parse_bucket_bounds_standard_degree_labels(self) -> None:
        self.assertEqual(parse_bucket_bounds("84-85°F"), (84.0, 85.0))
        self.assertEqual(parse_bucket_bounds("46°F or higher"), (46.0, None))
        self.assertEqual(parse_bucket_bounds("45°F or below"), (None, 45.0))

    def test_parse_bucket_bounds_supports_celsius_labels(self) -> None:
        self.assertEqual(parse_bucket_bounds("12-13°C"), (12.0, 13.0))
        self.assertEqual(parse_bucket_bounds("15°C or higher"), (15.0, None))
        self.assertEqual(parse_bucket_bounds("9°C or below"), (None, 9.0))

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

    def test_policy_blocks_observation_only_city_for_live(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "PAR",
                "day_label": "tomorrow",
                "bucket": "12-13°C",
                "consensus_score": 0.82,
                "spread": 1.0,
                "sigma": 1.4,
                "ensemble_prediction": 12.7,
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
        self.assertEqual(decision.reason, "city_observation_only")

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

    def test_policy_allows_tomorrow_b_tier_risky_label_when_consensus_is_exceptional(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "SEA",
                "day_label": "tomorrow",
                "bucket": "52-53°F",
                "consensus_score": 0.84,
                "spread": 2.4,
                "sigma": 3.8,
                "ensemble_prediction": 52.2,
                "confidence_tier": "safe",
                "signal_tier": "B",
                "edge": 27.0,
                "min_agreeing_model_edge": 14.0,
                "price_cents": 34.0,
                "coverage_ok": True,
                "coverage_score": 0.83,
                "agreement_models": 10,
                "total_models": 12,
                "agreement_pct": 83.33,
                "degraded_reason": None,
                "executable_quality_score": 0.84,
                "data_quality_score": 0.86,
            },
        )()
        decision = apply_trade_policy(opportunity)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, "allowed")
        self.assertEqual(decision.risk_label, "Risky")

    def test_policy_keeps_c_tier_blocked_even_with_tomorrow_risky_override_shape(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "SEA",
                "day_label": "tomorrow",
                "bucket": "52-53°F",
                "consensus_score": 0.84,
                "spread": 2.4,
                "sigma": 3.8,
                "ensemble_prediction": 52.2,
                "confidence_tier": "safe",
                "signal_tier": "C",
                "edge": 27.0,
                "min_agreeing_model_edge": 14.0,
                "price_cents": 34.0,
                "coverage_ok": True,
                "coverage_score": 0.83,
                "agreement_models": 10,
                "total_models": 12,
                "agreement_pct": 83.33,
                "degraded_reason": None,
                "executable_quality_score": 0.84,
                "data_quality_score": 0.86,
            },
        )()
        decision = apply_trade_policy(opportunity)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "signal_tier_not_actionable")

    def test_policy_allows_strong_city_tomorrow_risky_override_for_sea(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "SEA",
                "day_label": "tomorrow",
                "bucket": "52-53°F",
                "consensus_score": 0.79,
                "spread": 1.45,
                "sigma": 3.8,
                "ensemble_prediction": 50.3,
                "confidence_tier": "safe",
                "signal_tier": "B",
                "edge": 32.2,
                "min_agreeing_model_edge": 19.9,
                "price_cents": 59.0,
                "coverage_ok": True,
                "coverage_score": 0.94,
                "agreement_models": 10,
                "total_models": 12,
                "agreement_pct": 83.33,
                "degraded_reason": None,
                "executable_quality_score": 0.75,
                "data_quality_score": 1.0,
            },
        )()
        with patch.dict(
            "os.environ",
            {
                "WEATHER_POLICY_TOMORROW_RISKY_OVERRIDE_ENABLED": "0",
                "WEATHER_POLICY_TOMORROW_PRICE_OVERRIDE_ENABLED": "1",
                "WEATHER_POLICY_TOMORROW_MAX_PRICE_CENTS": "60",
            },
            clear=False,
        ):
            decision = apply_trade_policy(opportunity)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, "allowed")
        self.assertIn(decision.risk_label, {"Moderate", "Risky"})

    def test_policy_allows_sea_tomorrow_no_override_with_price_up_to_61(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "SEA",
                "day_label": "tomorrow",
                "side": "NO",
                "bucket": "52-53Â°F",
                "consensus_score": 0.78,
                "spread": 3.1,
                "sigma": 3.9,
                "ensemble_prediction": 50.8,
                "confidence_tier": "safe",
                "signal_tier": "B",
                "edge": 30.2,
                "model_prob": 91.7,
                "min_agreeing_model_edge": 19.0,
                "price_cents": 61.0,
                "coverage_ok": True,
                "coverage_score": 0.84,
                "agreement_models": 10,
                "total_models": 12,
                "agreement_pct": 83.33,
                "degraded_reason": None,
                "executable_quality_score": 0.82,
                "data_quality_score": 0.86,
            },
        )()
        with patch.dict(
            "os.environ",
            {
                "WEATHER_POLICY_TOMORROW_RISKY_OVERRIDE_ENABLED": "0",
                "WEATHER_POLICY_TOMORROW_PRICE_OVERRIDE_ENABLED": "0",
                "WEATHER_POLICY_SEA_TOMORROW_NO_OVERRIDE_ENABLED": "1",
                "WEATHER_POLICY_SEA_TOMORROW_NO_MAX_PRICE_CENTS": "61",
                "WEATHER_POLICY_MAX_SPREAD": "4",
            },
            clear=False,
        ):
            decision = apply_trade_policy(opportunity)
            min_price, max_price = effective_price_bounds(
                opportunity,
                min_price_cents=10.0,
                max_price_cents=55.0,
            )
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, "allowed")
        self.assertEqual(min_price, 10.0)
        self.assertEqual(max_price, 61.0)

    def test_policy_does_not_apply_strong_city_override_to_non_whitelisted_city(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "city_key": "DAL",
                "day_label": "tomorrow",
                "bucket": "74-75°F",
                "consensus_score": 0.79,
                "spread": 1.45,
                "sigma": 3.8,
                "ensemble_prediction": 74.3,
                "confidence_tier": "safe",
                "signal_tier": "B",
                "edge": 32.2,
                "min_agreeing_model_edge": 19.9,
                "price_cents": 59.0,
                "coverage_ok": True,
                "coverage_score": 0.94,
                "agreement_models": 10,
                "total_models": 12,
                "agreement_pct": 83.33,
                "degraded_reason": None,
                "executable_quality_score": 0.75,
                "data_quality_score": 1.0,
            },
        )()
        with patch.dict(
            "os.environ",
            {"WEATHER_POLICY_TOMORROW_RISKY_OVERRIDE_ENABLED": "0"},
            clear=False,
        ):
            decision = apply_trade_policy(opportunity)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "risk_label_risky")

    def test_effective_price_bounds_allows_higher_tomorrow_cap_for_safe_b_tier(self) -> None:
        opportunity = type(
            "Opportunity",
            (),
            {
                "day_label": "tomorrow",
                "confidence_tier": "near-safe",
                "signal_tier": "B",
            },
        )()
        with patch.dict("os.environ", {"WEATHER_POLICY_TOMORROW_MAX_PRICE_CENTS": "60"}, clear=False):
            min_price, max_price = effective_price_bounds(
                opportunity,
                min_price_cents=10.0,
                max_price_cents=55.0,
            )
        self.assertEqual(min_price, 10.0)
        self.assertEqual(max_price, 60.0)

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
