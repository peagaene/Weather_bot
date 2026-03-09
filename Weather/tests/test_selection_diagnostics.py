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

from paperbot.selection import explain_blocked_opportunities


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


if __name__ == "__main__":
    unittest.main()
