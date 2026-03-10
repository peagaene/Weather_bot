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

from paperbot.selection import explain_blocked_opportunities, filter_opportunities


class SelectionDiagnosticsTests(unittest.TestCase):
    def test_explain_blocked_opportunities_includes_reason_and_coverage_context(self) -> None:
        opportunity = SimpleNamespace(
            city_key="MIA",
            date_str="2026-03-08",
            bucket="84-85F",
            side="NO",
            edge=12.5,
            model_prob=66.0,
            price_cents=30.0,
            agreement_models=4,
            total_models=5,
            agreement_pct=80.0,
            agreement_summary="4/5",
            agreeing_model_names=["nws", "weatherapi", "openweather", "visualcrossing"],
            confidence_tier="safe",
            risk_label="Safe",
            signal_tier="A",
            adversarial_score=74.2,
            min_agreeing_model_edge=10.8,
            coverage_issue_type="provider_failure",
            valid_model_count=2,
            required_model_count=5,
            provider_failures=["gfs", "ecmwf"],
            provider_failure_details={"gfs": "HTTP 502", "ecmwf": "timeout"},
            degraded_reason="insufficient_model_coverage:2;provider_failures:ecmwf,gfs",
            market_slug="mia-market",
            event_slug="mia-event",
            token_id="token-1",
            spread=1.0,
            as_dict=lambda: {"polymarket_url": "https://polymarket.com/event/mia-event"},
        )
        plan = SimpleNamespace(valid=False, invalid_reason="share_size_below_order_min_size", share_size=1.2)
        policy = SimpleNamespace(allowed=True, reason="allowed", risk_label="Safe", risk_score=0.1)

        with patch("paperbot.selection.apply_trade_policy", return_value=policy):
            blocked = explain_blocked_opportunities(
                [opportunity],
                min_price_cents=10.0,
                max_price_cents=65.0,
                max_spread=4.0,
                max_share_size=400.0,
                require_token=True,
                max_orders_per_event=1,
                plans_by_slug={"mia-event|mia-market|NO": plan},
                limit=10,
            )

        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["reason"], "plan:share_size_below_order_min_size")
        self.assertEqual(blocked[0]["plan_invalid_reason"], "share_size_below_order_min_size")
        self.assertEqual(blocked[0]["polymarket_url"], "https://polymarket.com/event/mia-event")
        self.assertEqual(blocked[0]["coverage_issue_type"], "provider_failure")
        self.assertEqual(blocked[0]["valid_model_count"], 2)
        self.assertEqual(blocked[0]["required_model_count"], 5)
        self.assertEqual(blocked[0]["provider_failures"], ["gfs", "ecmwf"])
        self.assertEqual(blocked[0]["agreement_summary"], "4/5")
        self.assertEqual(blocked[0]["signal_tier"], "A")
        self.assertEqual(blocked[0]["agreeing_model_names"], ["nws", "weatherapi", "openweather", "visualcrossing"])

    def test_filter_opportunities_selects_best_viable_candidate_within_event(self) -> None:
        invalid = SimpleNamespace(
            event_slug="mia-event",
            market_slug="mia-market-a",
            side="NO",
            token_id="token-a",
            price_cents=30.0,
            spread=1.0,
            weighted_score=90.0,
            edge=14.0,
        )
        viable = SimpleNamespace(
            event_slug="mia-event",
            market_slug="mia-market-b",
            side="NO",
            token_id="token-b",
            price_cents=29.0,
            spread=1.0,
            weighted_score=80.0,
            edge=12.0,
        )
        invalid_plan = SimpleNamespace(valid=False, invalid_reason="share_size_below_order_min_size", share_size=1.2)
        viable_plan = SimpleNamespace(valid=True, invalid_reason=None, share_size=2.5)
        policy = SimpleNamespace(allowed=True, reason="allowed", risk_label="Safe", risk_score=0.1)

        with patch("paperbot.selection.apply_trade_policy", return_value=policy):
            selected = filter_opportunities(
                [invalid, viable],
                min_price_cents=10.0,
                max_price_cents=65.0,
                max_spread=4.0,
                max_share_size=2.6,
                require_token=True,
                max_orders_per_event=1,
                plans_by_slug={
                    "mia-event|mia-market-a|NO": invalid_plan,
                    "mia-event|mia-market-b|NO": viable_plan,
                },
            )

        self.assertEqual(len(selected), 1)
        self.assertIs(selected[0], viable)

    def test_filter_opportunities_applies_tomorrow_price_override(self) -> None:
        opportunity = SimpleNamespace(
            event_slug="sea-event",
            market_slug="sea-market",
            side="NO",
            token_id="token-a",
            price_cents=59.0,
            spread=1.0,
            weighted_score=90.0,
            edge=20.0,
            day_label="tomorrow",
            confidence_tier="near-safe",
            signal_tier="B",
        )
        policy = SimpleNamespace(allowed=True, reason="allowed", risk_label="Safe", risk_score=0.1)

        with patch("paperbot.selection.apply_trade_policy", return_value=policy):
            with patch.dict("os.environ", {"WEATHER_POLICY_TOMORROW_MAX_PRICE_CENTS": "60"}, clear=False):
                selected = filter_opportunities(
                    [opportunity],
                    min_price_cents=10.0,
                    max_price_cents=55.0,
                    max_spread=4.0,
                    max_share_size=None,
                    require_token=True,
                    max_orders_per_event=1,
                    plans_by_slug=None,
                )

        self.assertEqual(len(selected), 1)
        self.assertIs(selected[0], opportunity)


if __name__ == "__main__":
    unittest.main()
