from __future__ import annotations

import pandas as pd
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.dashboard_metrics import (
    build_live_snapshot_curve,
    compute_open_position_totals,
    normalize_open_positions,
)
from paperbot.weather_dashboard_ui import (
    _build_recent_trades_frame,
    _forecast_accuracy_frame,
    _policy_recommendations_frame,
    _recent_trade_status_class,
    _snapshot_net_worth_metrics,
)


def test_normalize_open_positions_prefers_value_minus_cost() -> None:
    frame = pd.DataFrame(
        [
            {
                "title": "Market A",
                "initialValue": 2.0,
                "currentValue": 0.5,
                "cashPnl": -99.0,
            }
        ]
    )

    normalized = normalize_open_positions(frame)

    assert normalized.loc[0, "dashboard_cost_usd"] == 2.0
    assert normalized.loc[0, "dashboard_value_usd"] == 0.5
    assert normalized.loc[0, "dashboard_open_pnl_usd"] == -1.5
    assert round(float(normalized.loc[0, "dashboard_open_pnl_pct"]), 2) == -75.0


def test_compute_open_position_totals_uses_normalized_columns() -> None:
    frame = normalize_open_positions(
        pd.DataFrame(
            [
                {"initialValue": 2.0, "currentValue": 0.5},
                {"initialValue": 3.0, "currentValue": 2.0},
            ]
        )
    )

    portfolio_value, open_pnl = compute_open_position_totals(frame)

    assert portfolio_value == 2.5
    assert open_pnl == -2.5


def test_build_live_snapshot_curve_prefers_net_worth_delta() -> None:
    curve = build_live_snapshot_curve(
        [
            {"captured_at": "2026-03-09T01:00:00+00:00", "total_net_worth_usd": 10.0, "total_open_pnl_usd": -5.0},
            {"captured_at": "2026-03-09T01:01:00+00:00", "total_net_worth_usd": 11.5, "total_open_pnl_usd": -4.0},
        ]
    )

    assert list(curve["pnl_curve_usd"]) == [0.0, 1.5]
    assert list(curve["pnl_curve_label"]) == ["PnL da curva", "PnL da curva"]


def test_build_live_snapshot_curve_smooths_transient_spike() -> None:
    curve = build_live_snapshot_curve(
        [
            {"captured_at": "2026-03-09T01:00:00+00:00", "total_net_worth_usd": 100.0},
            {"captured_at": "2026-03-09T01:01:00+00:00", "total_net_worth_usd": 10.0},
            {"captured_at": "2026-03-09T01:02:00+00:00", "total_net_worth_usd": 101.0},
        ]
    )

    assert round(float(curve.iloc[1]["total_net_worth_usd"]), 2) == 100.5
    assert round(float(curve.iloc[1]["pnl_curve_usd"]), 2) == 0.5


def test_snapshot_net_worth_metrics_uses_full_wallet_curve() -> None:
    curve = build_live_snapshot_curve(
        [
            {"captured_at": "2026-03-09T00:00:00+00:00", "total_net_worth_usd": 100.0, "total_open_pnl_usd": 4.0},
            {"captured_at": "2026-03-09T12:00:00+00:00", "total_net_worth_usd": 106.0, "total_open_pnl_usd": 1.0},
        ]
    )

    starting_capital, total_pnl, _daily_pnl = _snapshot_net_worth_metrics(curve)

    assert starting_capital == 100.0
    assert total_pnl == 6.0


def test_build_recent_trades_frame_uses_only_finalized_trades_sorted_by_latest_first() -> None:
    live_positions = pd.DataFrame(
        [
            {"market_slug": "open-market", "side": "NO", "opened_at": "2026-03-09T10:00:00+00:00", "status": "open"},
        ]
    )
    effective_closed_positions = pd.DataFrame(
        [
            {"market_slug": "older-closed-market", "side": "YES", "resolved_at": "2026-03-09T11:00:00+00:00", "status": "resolved"},
            {"market_slug": "latest-closed-market", "side": "NO", "resolved_at": "2026-03-09T12:00:00+00:00", "status": "resolved"},
        ]
    )

    recent = _build_recent_trades_frame(
        {
            "live_positions": live_positions,
            "effective_closed_positions": effective_closed_positions,
        }
    )

    assert list(recent["market_slug"]) == ["latest-closed-market", "older-closed-market"]


def test_recent_trade_status_class_keeps_open_separate_from_filled() -> None:
    assert _recent_trade_status_class("OPEN") == "pill-open"
    assert _recent_trade_status_class("FILLED") == "pill-filled"
    assert _recent_trade_status_class("RESOLVED") == "pill-closed"


def test_forecast_accuracy_frame_sorts_by_lowest_mae() -> None:
    frame = _forecast_accuracy_frame(
        [
            {"source_name": "gfs", "mae": 2.5, "rmse": 3.0, "bias": -1.0, "sample_count": 20},
            {"source_name": "nws", "mae": 1.2, "rmse": 1.5, "bias": 0.2, "sample_count": 10},
        ]
    )

    assert list(frame["source_name"]) == ["nws", "gfs"]


def test_policy_recommendations_frame_prioritizes_blocks_before_prefers() -> None:
    frame = _policy_recommendations_frame(
        [
            {"segment": "SEA/tomorrow", "recommendation": "prefer", "sample_count": 40},
            {"segment": "MIA/today", "recommendation": "block", "sample_count": 100},
        ]
    )

    assert list(frame["segment"]) == ["MIA/today", "SEA/tomorrow"]
