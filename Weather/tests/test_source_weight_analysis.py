from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import run_weather_models
from run_source_weight_analysis import generate_source_weight_analysis


def _create_scan_prediction_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE scan_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                generated_at TEXT,
                city_key TEXT,
                day_label TEXT,
                date_str TEXT,
                bucket TEXT,
                side TEXT,
                edge REAL,
                settled_price_cents REAL,
                resolved_at TEXT,
                model_predictions_json TEXT
            )
            """
        )
        rows = [
            ("2026-03-08T12:00:00+00:00", "SEA", "today", "2026-03-08", "70-71°F", "YES", 22.0, 100.0, "2026-03-09T00:00:00+00:00", '{"nws": 70.5, "gfs": 66.0}'),
            ("2026-03-08T12:05:00+00:00", "SEA", "today", "2026-03-08", "70-71°F", "YES", 24.0, 100.0, "2026-03-09T00:00:00+00:00", '{"nws": 71.0, "gfs": 68.0}'),
            ("2026-03-08T12:10:00+00:00", "NYC", "tomorrow", "2026-03-09", "66°F or higher", "NO", 20.0, 100.0, "2026-03-10T00:00:00+00:00", '{"nws": 62.0, "gfs": 67.5}'),
            ("2026-03-08T12:15:00+00:00", "NYC", "tomorrow", "2026-03-09", "66°F or higher", "NO", 19.0, 100.0, "2026-03-10T00:00:00+00:00", '{"nws": 63.0, "gfs": 66.2}'),
            ("2026-03-08T12:20:00+00:00", "ATL", "tomorrow", "2026-03-09", "78°F or higher", "NO", 18.0, 0.0, "2026-03-10T00:00:00+00:00", '{"nws": 79.0, "gfs": 80.0}'),
        ]
        conn.executemany(
            """
            INSERT INTO scan_predictions (
                generated_at, city_key, day_label, date_str, bucket, side, edge, settled_price_cents, resolved_at, model_predictions_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


class SourceWeightAnalysisTests(unittest.TestCase):
    def test_generator_builds_global_city_and_regime_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "weather_bot.db"
            output_path = Path(temp_dir) / "analysis.json"
            profile_path = Path(temp_dir) / "source_weight_profile.json"
            _create_scan_prediction_db(db_path)

            payload = generate_source_weight_analysis(
                db_path=db_path,
                min_samples=2,
                top_segments=10,
                output_json=output_path,
                profile_json=profile_path,
            )

            self.assertEqual(payload["resolved_predictions"], 5)
            global_weights = payload["source_weight_profile"]["global"]["model_weight_multiplier"]
            self.assertGreater(global_weights["nws"], global_weights["gfs"])
            self.assertIn("SEA", payload["source_weight_profile"]["cities"])
            self.assertIn("coastal", payload["source_weight_profile"]["regimes"])
            self.assertNotIn("ATL", payload["source_weight_profile"]["cities"])
            self.assertEqual(
                payload["source_weight_profile"]["metadata"]["city_regimes"]["SEA"],
                ["coastal", "marine", "urban"],
            )
            self.assertTrue(output_path.exists())
            self.assertTrue(profile_path.exists())

    def test_refresh_triggers_for_missing_or_stale_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "weather_bot.db"
            _create_scan_prediction_db(db_path)
            profile_path = Path(temp_dir) / "source_weight_profile.json"
            analysis_path = Path(temp_dir) / "source_weight_analysis.json"
            calls: list[dict[str, object]] = []

            def _fake_generate_source_weight_analysis(**kwargs):
                calls.append(kwargs)
                profile_path.write_text("{}", encoding="utf-8")
                return {}

            with patch.dict(
                "os.environ",
                {
                    "WEATHER_DB_PATH": str(db_path),
                    "WEATHER_SOURCE_WEIGHT_PROFILE_PATH": str(profile_path),
                    "WEATHER_SOURCE_WEIGHT_ANALYSIS_PATH": str(analysis_path),
                    "WEATHER_SOURCE_WEIGHT_AUTO_REFRESH": "1",
                    "WEATHER_SOURCE_WEIGHT_PROFILE_MAX_AGE_HOURS": "24",
                    "WEATHER_SOURCE_WEIGHT_MIN_SAMPLES": "2",
                    "WEATHER_SOURCE_WEIGHT_TOP_SEGMENTS": "5",
                },
                clear=False,
            ):
                with patch.object(run_weather_models, "generate_source_weight_analysis", side_effect=_fake_generate_source_weight_analysis):
                    run_weather_models._ensure_source_weight_profile_fresh()
                    self.assertEqual(len(calls), 1)
                    run_weather_models._ensure_source_weight_profile_fresh()
                    self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
