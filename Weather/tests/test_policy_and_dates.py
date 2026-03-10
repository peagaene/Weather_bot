from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.degendoppler import CITY_CONFIGS
from paperbot.policy import apply_trade_policy, parse_bucket_bounds
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


if __name__ == "__main__":
    unittest.main()
