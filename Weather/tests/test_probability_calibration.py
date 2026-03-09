from __future__ import annotations

import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.probability_calibration import apply_probability_calibration, build_probability_calibration


class ProbabilityCalibrationTests(unittest.TestCase):
    def test_build_probability_calibration_creates_city_and_horizon_groups(self) -> None:
        rows = [
            {
                "generated_at": "2026-03-08T10:00:00+00:00",
                "city_key": "SEA",
                "date_str": "2026-03-09",
                "model_prob": 80.0,
                "settled_price_cents": 100.0,
            },
            {
                "generated_at": "2026-03-08T11:00:00+00:00",
                "city_key": "SEA",
                "date_str": "2026-03-09",
                "model_prob": 82.0,
                "settled_price_cents": 100.0,
            },
            {
                "generated_at": "2026-03-08T12:00:00+00:00",
                "city_key": "SEA",
                "date_str": "2026-03-09",
                "model_prob": 78.0,
                "settled_price_cents": 0.0,
            },
            {
                "generated_at": "2026-03-08T12:00:00+00:00",
                "city_key": "MIA",
                "date_str": "2026-03-09",
                "model_prob": 30.0,
                "settled_price_cents": 0.0,
            },
            {
                "generated_at": "2026-03-08T13:00:00+00:00",
                "city_key": "SEA",
                "date_str": "2026-03-09",
                "model_prob": 81.0,
                "settled_price_cents": 100.0,
            },
        ]
        payload = build_probability_calibration(rows, min_group_samples=3)
        self.assertEqual(payload["total_samples"], 5)
        self.assertTrue(payload["global"]["usable"])
        self.assertTrue(payload["cities"]["SEA"]["usable"])
        self.assertIn("1", payload["cities"]["SEA"]["horizon_days"])

    def test_apply_probability_calibration_moves_probability_toward_observed_rate(self) -> None:
        payload = {
            "global": {
                "usable": True,
                "bins": [
                    {"low": 70, "high": 80, "sample_count": 10, "calibrated_mean": 0.6},
                    {"low": 80, "high": 90, "sample_count": 10, "calibrated_mean": 0.7},
                ],
            },
            "cities": {
                "SEA": {
                    "usable": True,
                    "bins": [
                        {"low": 80, "high": 90, "sample_count": 12, "calibrated_mean": 0.65},
                    ],
                    "horizon_days": {
                        "1": {
                            "usable": True,
                            "bins": [
                                {"low": 80, "high": 90, "sample_count": 12, "calibrated_mean": 0.55},
                            ],
                        }
                    },
                }
            },
        }
        result = apply_probability_calibration(0.84, city_key="SEA", horizon_days=1, calibration_payload=payload)
        self.assertEqual(result.source, "city:SEA:h1")
        self.assertLess(result.calibrated_probability, 0.84)
        self.assertGreater(result.bin_count, 0)


if __name__ == "__main__":
    unittest.main()
