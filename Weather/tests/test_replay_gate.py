from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import run_replay_backtest


class ReplayGateTests(unittest.TestCase):
    def test_point_in_time_validation_blocks_on_leakage_and_non_executable_prices(self) -> None:
        rows = [
            {
                "generated_at": "2026-03-09T01:00:00+00:00",
                "date_str": "2026-03-08",
                "event_slug": "evt-1",
                "market_slug": "mkt-1",
                "side": "YES",
                "model_prob": 80.0,
                "settled_price_cents": 100.0,
                "price_source": "gamma_outcome_price",
            }
        ]
        result = run_replay_backtest._build_point_in_time_validation(
            rows,
            min_trades=1,
            max_brier_score=0.25,
            max_non_executable_ratio=0.2,
        )
        self.assertTrue(result["present"])
        self.assertFalse(result["passed"])
        self.assertIn("lookahead_or_date_leakage_detected", result["reason"])

    def test_point_in_time_validation_passes_for_clean_first_seen_rows(self) -> None:
        rows = [
            {
                "generated_at": "2026-03-08T10:00:00+00:00",
                "date_str": "2026-03-09",
                "event_slug": "evt-1",
                "market_slug": "mkt-1",
                "side": "YES",
                "model_prob": 90.0,
                "settled_price_cents": 100.0,
                "price_source": "clob_best_ask",
            },
            {
                "generated_at": "2026-03-08T11:00:00+00:00",
                "date_str": "2026-03-09",
                "event_slug": "evt-2",
                "market_slug": "mkt-2",
                "side": "NO",
                "model_prob": 10.0,
                "settled_price_cents": 0.0,
                "price_source": "clob_best_ask",
            },
        ]
        result = run_replay_backtest._build_point_in_time_validation(
            rows,
            min_trades=2,
            max_brier_score=0.25,
            max_non_executable_ratio=0.2,
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["sample_count"], 2)


if __name__ == "__main__":
    unittest.main()
