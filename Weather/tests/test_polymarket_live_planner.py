from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.polymarket_live import build_order_plan


class PolymarketLivePlannerTests(unittest.TestCase):
    def test_build_order_plan_raises_stake_to_meet_order_min_size(self) -> None:
        opportunity = SimpleNamespace(
            event_slug="sea-event",
            market_slug="sea-market",
            side="NO",
            token_id="token-1",
            model_prob=70.0,
            price_cents=61.0,
            market_prob=61.0,
            edge=9.0,
            ev_percent=5.0,
            confidence_tier="near-safe",
            order_min_size=5.0,
            price_source="clob_best_ask",
        )
        with patch("paperbot.polymarket_live._fetch_tick_size_cents", return_value=0.1):
            plan = build_order_plan(
                opportunity,
                bankroll_usd=18.0,
                kelly_fraction=0.25,
                max_price_cents=62.0,
                min_stake_usd=1.0,
                max_stake_usd=5.0,
            )
        self.assertTrue(plan.valid)
        self.assertAlmostEqual(plan.stake_usd, 3.05, places=2)
        self.assertAlmostEqual(plan.share_size, 5.0, places=4)

    def test_build_order_plan_marks_cap_below_order_min_size_when_max_stake_is_too_low(self) -> None:
        opportunity = SimpleNamespace(
            event_slug="sea-event",
            market_slug="sea-market",
            side="NO",
            token_id="token-1",
            model_prob=70.0,
            price_cents=61.0,
            market_prob=61.0,
            edge=9.0,
            ev_percent=5.0,
            confidence_tier="near-safe",
            order_min_size=5.0,
            price_source="clob_best_ask",
        )
        with patch("paperbot.polymarket_live._fetch_tick_size_cents", return_value=0.1):
            plan = build_order_plan(
                opportunity,
                bankroll_usd=1000.0,
                kelly_fraction=0.25,
                max_price_cents=62.0,
                min_stake_usd=1.0,
                max_stake_usd=2.0,
            )
        self.assertFalse(plan.valid)
        self.assertEqual(plan.invalid_reason, "stake_cap_below_order_min_size")
        self.assertAlmostEqual(plan.stake_usd, 2.0, places=2)


if __name__ == "__main__":
    unittest.main()
