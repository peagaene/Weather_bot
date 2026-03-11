from __future__ import annotations

import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.polymarket_weather import _agreeing_model_names, _ensemble_bucket_probability, _signal_tier
from paperbot.weather_models import EnsembleForecast


class WeatherProbabilityTests(unittest.TestCase):
    def test_bucket_probability_uses_probabilistic_members_not_only_gaussian(self) -> None:
        ensemble = EnsembleForecast(
            city_key="NYC",
            city_name="New York City",
            date="2026-03-08",
            predictions={"best_match": 72.0, "ecmwf": 72.0, "gfs": 72.0},
            blended_high=72.0,
            min_high=72.0,
            max_high=72.0,
            spread=0.0,
            sigma=5.0,
            consensus_score=0.95,
            probabilistic_family_highs={
                "gfs_ens": [72.0] * 20,
                "ecmwf_ens": [72.0] * 20,
            },
            probabilistic_spread=0.0,
            probabilistic_member_count=40,
            valid_model_count=3,
            coverage_ok=True,
            degraded_reason=None,
            provider_failures=[],
        )

        narrow_bucket_prob = _ensemble_bucket_probability(ensemble, 71.5, 72.5)
        far_bucket_prob = _ensemble_bucket_probability(ensemble, 75.5, 76.5)

        self.assertGreater(narrow_bucket_prob, 0.8)
        self.assertLess(far_bucket_prob, 0.1)

    def test_signal_tier_requires_robust_worst_case_and_execution(self) -> None:
        signal_tier, signal_decision, adversarial_score = _signal_tier(
            model_prob=78.0,
            mean_agreeing_model_edge=24.0,
            min_agreeing_model_edge=18.0,
            agreement_pct=87.0,
            executable_quality_score=0.82,
            data_quality_score=0.8,
            consensus_score=0.72,
        )
        self.assertEqual(signal_tier, "A")
        self.assertEqual(signal_decision, "auto")
        self.assertGreater(adversarial_score, 60.0)

        weak_tier, weak_decision, _ = _signal_tier(
            model_prob=78.0,
            mean_agreeing_model_edge=24.0,
            min_agreeing_model_edge=3.0,
            agreement_pct=87.0,
            executable_quality_score=0.82,
            data_quality_score=0.8,
            consensus_score=0.72,
        )
        self.assertEqual(weak_tier, "C")
        self.assertEqual(weak_decision, "watch")

    def test_agreeing_model_names_tracks_consensus_side(self) -> None:
        names = _agreeing_model_names(
            {
                "ecmwf": 70.0,
                "gfs": 71.0,
                "icon": 68.0,
                "nws": 74.0,
            },
            low=69.5,
            high=71.5,
            side="YES",
        )
        self.assertEqual(names, ["ecmwf", "gfs"])

    def test_bucket_probability_supports_celsius_market_bounds(self) -> None:
        ensemble = EnsembleForecast(
            city_key="PAR",
            city_name="Paris",
            date="2026-03-08",
            predictions={"best_match": 53.6, "ecmwf": 54.1, "gfs": 52.8},
            blended_high=53.8,
            min_high=52.8,
            max_high=54.1,
            spread=0.7,
            sigma=1.5,
            consensus_score=0.9,
            valid_model_count=3,
            coverage_ok=True,
            degraded_reason=None,
            provider_failures=[],
        )

        in_bucket_prob = _ensemble_bucket_probability(ensemble, 11.5, 12.5, "C")
        far_bucket_prob = _ensemble_bucket_probability(ensemble, 17.5, 18.5, "C")

        self.assertGreater(in_bucket_prob, 0.5)
        self.assertLess(far_bucket_prob, 0.05)


if __name__ == "__main__":
    unittest.main()
