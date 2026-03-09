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
