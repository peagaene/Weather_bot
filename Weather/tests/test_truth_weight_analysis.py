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
from run_truth_weight_analysis import generate_truth_weight_analysis


def _create_truth_tables_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE forecast_source_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at TEXT,
                city_key TEXT,
                day_label TEXT,
                date_str TEXT,
                source_name TEXT,
                forecast_temp_f REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE station_observation_daily_highs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at TEXT,
                city_key TEXT,
                local_date TEXT,
                observed_high_f REAL
            )
            """
        )
        forecast_rows = [
            ("2026-03-10T00:00:00+00:00", "SEA", "today", "2026-03-10", "nws", 51.0),
            ("2026-03-10T00:00:00+00:00", "SEA", "today", "2026-03-10", "gfs", 55.0),
            ("2026-03-10T00:30:00+00:00", "SEA", "today", "2026-03-10", "nws", 50.8),
            ("2026-03-10T00:30:00+00:00", "SEA", "today", "2026-03-10", "gfs", 54.8),
            ("2026-03-10T01:00:00+00:00", "SEA", "tomorrow", "2026-03-11", "nws", 53.0),
            ("2026-03-10T01:00:00+00:00", "SEA", "tomorrow", "2026-03-11", "gfs", 57.0),
            ("2026-03-10T02:00:00+00:00", "NYC", "tomorrow", "2026-03-11", "nws", 64.0),
            ("2026-03-10T02:00:00+00:00", "NYC", "tomorrow", "2026-03-11", "gfs", 69.0),
        ]
        observation_rows = [
            ("2026-03-10T03:00:00+00:00", "SEA", "2026-03-10", 50.5),
            ("2026-03-10T03:00:00+00:00", "SEA", "2026-03-11", 52.5),
            ("2026-03-10T03:00:00+00:00", "NYC", "2026-03-11", 64.5),
        ]
        conn.executemany(
            """
            INSERT INTO forecast_source_snapshots (
                captured_at, city_key, day_label, date_str, source_name, forecast_temp_f
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            forecast_rows,
        )
        conn.executemany(
            """
            INSERT INTO station_observation_daily_highs (
                captured_at, city_key, local_date, observed_high_f
            ) VALUES (?, ?, ?, ?)
            """,
            observation_rows,
        )
        conn.commit()
    finally:
        conn.close()


class TruthWeightAnalysisTests(unittest.TestCase):
    def test_generator_builds_truth_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "weather_bot.db"
            output_path = Path(temp_dir) / "truth_analysis.json"
            profile_path = Path(temp_dir) / "truth_weight_profile.json"
            _create_truth_tables_db(db_path)

            payload = generate_truth_weight_analysis(
                db_path=db_path,
                min_samples=2,
                top_segments=10,
                output_json=output_path,
                profile_json=profile_path,
            )

            self.assertEqual(payload["truth_matches"], 8)
            global_weights = payload["truth_weight_profile"]["global"]["model_weight_multiplier"]
            self.assertGreater(global_weights["nws"], global_weights["gfs"])
            self.assertIn("SEA", payload["truth_weight_profile"]["cities"])
            self.assertIn("coastal", payload["truth_weight_profile"]["regimes"])
            self.assertIn("today", payload["truth_weight_profile"]["cities"]["SEA"]["day_labels"])
            self.assertTrue(output_path.exists())
            self.assertTrue(profile_path.exists())

    def test_refresh_triggers_for_missing_or_stale_truth_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "weather_bot.db"
            _create_truth_tables_db(db_path)
            profile_path = Path(temp_dir) / "truth_weight_profile.json"
            analysis_path = Path(temp_dir) / "truth_weight_analysis.json"
            calls: list[dict[str, object]] = []

            def _fake_generate_truth_weight_analysis(**kwargs):
                calls.append(kwargs)
                profile_path.write_text("{}", encoding="utf-8")
                return {}

            with patch.dict(
                "os.environ",
                {
                    "WEATHER_DB_PATH": str(db_path),
                    "WEATHER_TRUTH_WEIGHT_PROFILE_PATH": str(profile_path),
                    "WEATHER_TRUTH_WEIGHT_ANALYSIS_PATH": str(analysis_path),
                    "WEATHER_TRUTH_WEIGHT_AUTO_REFRESH": "1",
                    "WEATHER_TRUTH_WEIGHT_PROFILE_MAX_AGE_HOURS": "24",
                    "WEATHER_TRUTH_WEIGHT_MIN_SAMPLES": "2",
                    "WEATHER_TRUTH_WEIGHT_TOP_SEGMENTS": "5",
                },
                clear=False,
            ):
                with patch.object(run_weather_models, "generate_truth_weight_analysis", side_effect=_fake_generate_truth_weight_analysis):
                    run_weather_models._ensure_truth_weight_profile_fresh()
                    self.assertEqual(len(calls), 1)
                    run_weather_models._ensure_truth_weight_profile_fresh()
                    self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
