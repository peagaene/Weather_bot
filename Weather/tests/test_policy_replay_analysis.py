from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_policy_replay_analysis import _build_segment_recommendations


def test_build_segment_recommendations_classifies_keep_expand_observe_block() -> None:
    rows = [
        *[
            {
                "city_key": "SEA",
                "day_label": "tomorrow",
                "confidence_tier": "safe",
                "signal_tier": "B",
                "resolved": True,
                "won": True,
                "lost": False,
                "edge": 18.0,
                "pnl_usd": 2.2,
                "roi_percent": 180.0,
            }
            for _ in range(30)
        ],
        *[
            {
                "city_key": "CHI",
                "day_label": "tomorrow",
                "confidence_tier": "near-safe",
                "signal_tier": "B",
                "resolved": True,
                "won": i < 25,
                "lost": i >= 25,
                "edge": 14.0,
                "pnl_usd": 1.4 if i < 25 else -0.4,
                "roi_percent": 95.0 if i < 25 else -20.0,
            }
            for i in range(30)
        ],
        *[
            {
                "city_key": "ATL",
                "day_label": "today",
                "confidence_tier": "near-safe",
                "signal_tier": "C",
                "resolved": True,
                "won": i < 8,
                "lost": i >= 8,
                "edge": 8.0,
                "pnl_usd": 0.6 if i < 8 else -0.1,
                "roi_percent": 35.0 if i < 8 else -5.0,
            }
            for i in range(10)
        ],
        *[
            {
                "city_key": "MIA",
                "day_label": "today",
                "confidence_tier": "safe",
                "signal_tier": "B",
                "resolved": True,
                "won": i < 4,
                "lost": i >= 4,
                "edge": 11.0,
                "pnl_usd": -0.8,
                "roi_percent": -40.0,
            }
            for i in range(10)
        ],
    ]
    segments = _build_segment_recommendations(
        rows,
        keys=("city_key", "day_label"),
        min_samples=10,
        target_win_rate=0.90,
    )
    recommendations = {item["segment"]: item["recommendation"] for item in segments}
    assert recommendations["SEA / tomorrow"] == "keep"
    assert recommendations["CHI / tomorrow"] == "expand"
    assert recommendations["ATL / today"] == "observe"
    assert recommendations["MIA / today"] == "block"
