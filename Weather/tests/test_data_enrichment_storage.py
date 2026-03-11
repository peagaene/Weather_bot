from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.storage import WeatherBotStorage
from run_weather_models import _build_forecast_source_snapshot_rows


class DataEnrichmentStorageTests(unittest.TestCase):
    def test_storage_records_forecast_source_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = WeatherBotStorage(Path(temp_dir) / "weather_bot.db")
            inserted = storage.record_forecast_source_snapshots(
                [
                    {
                        "run_id": "run-1",
                        "captured_at": "2026-03-10T00:00:00+00:00",
                        "city_key": "SEA",
                        "city_name": "Seattle",
                        "day_label": "tomorrow",
                        "date_str": "2026-03-11",
                        "event_slug": "event-1",
                        "market_slug": "market-1",
                        "market_id": "m1",
                        "bucket": "52-53°F",
                        "side": "NO",
                        "source_name": "nws",
                        "forecast_temp_f": 50.2,
                        "effective_weight": 1.17,
                        "agreement_models": 5,
                        "total_models": 6,
                        "agreement_pct": 83.33,
                        "aligns_with_trade_side": True,
                        "source_in_bucket": False,
                        "source_delta_f": 1.8,
                        "raw_context": {"signal_tier": "B"},
                    }
                ]
            )
            self.assertEqual(inserted, 1)
            rows = storage.list_forecast_source_snapshots(run_id="run-1", limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["source_name"], "nws")
            self.assertTrue(rows[0]["aligns_with_trade_side"])
            self.assertEqual(rows[0]["raw_context"]["signal_tier"], "B")

    def test_storage_records_station_observation_daily_highs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = WeatherBotStorage(Path(temp_dir) / "weather_bot.db")
            inserted = storage.record_station_observation_daily_highs(
                [
                    {
                        "captured_at": "2026-03-10T00:00:00+00:00",
                        "city_key": "SEA",
                        "city_name": "Seattle",
                        "station_id": "KSEA",
                        "local_date": "2026-03-10",
                        "observed_high_f": 51.4,
                        "source": "nws_station_observation",
                        "raw_context": {"regime_tags": ["coastal", "marine"]},
                    }
                ]
            )
            self.assertEqual(inserted, 1)
            rows = storage.list_station_observation_daily_highs(city_key="SEA", limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["station_id"], "KSEA")
            self.assertEqual(rows[0]["raw_context"]["regime_tags"], ["coastal", "marine"])

    def test_build_forecast_source_snapshot_rows_explodes_models(self) -> None:
        rows = _build_forecast_source_snapshot_rows(
            run_id="run-1",
            captured_at="2026-03-10T00:00:00+00:00",
            raw_predictions=[
                {
                    "city_key": "SEA",
                    "city_name": "Seattle",
                    "day_label": "tomorrow",
                    "date_str": "2026-03-11",
                    "event_slug": "event-1",
                    "market_slug": "market-1",
                    "market_id": "m1",
                    "bucket": "52-53°F",
                    "side": "NO",
                    "model_predictions": {"nws": 50.0, "hrrr": 52.4},
                    "effective_weights": {"nws": 1.2, "hrrr": 1.1},
                    "agreement_models": 5,
                    "total_models": 6,
                    "agreement_pct": 83.33,
                    "signal_tier": "B",
                    "confidence_tier": "safe",
                    "policy_allowed": False,
                    "policy_reason": "risk_label_risky",
                    "price_source": "clob_best_ask",
                    "reference_price_cents": 59.0,
                    "coverage_score": 0.92,
                }
            ],
        )
        self.assertEqual(len(rows), 2)
        by_source = {item["source_name"]: item for item in rows}
        self.assertTrue(by_source["nws"]["aligns_with_trade_side"])
        self.assertFalse(by_source["hrrr"]["aligns_with_trade_side"])
        self.assertEqual(by_source["nws"]["effective_weight"], 1.2)

    def test_storage_computes_forecast_accuracy_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = WeatherBotStorage(Path(temp_dir) / "weather_bot.db")
            storage.record_forecast_source_snapshots(
                [
                    {
                        "run_id": "run-1",
                        "captured_at": "2026-03-10T00:00:00+00:00",
                        "city_key": "SEA",
                        "city_name": "Seattle",
                        "day_label": "today",
                        "date_str": "2026-03-10",
                        "event_slug": "event-1",
                        "market_slug": "market-1",
                        "market_id": "m1",
                        "bucket": "52-53°F",
                        "side": "NO",
                        "source_name": "nws",
                        "forecast_temp_f": 51.0,
                        "raw_context": {},
                    },
                    {
                        "run_id": "run-1",
                        "captured_at": "2026-03-10T00:05:00+00:00",
                        "city_key": "SEA",
                        "city_name": "Seattle",
                        "day_label": "today",
                        "date_str": "2026-03-10",
                        "event_slug": "event-1",
                        "market_slug": "market-1",
                        "market_id": "m1",
                        "bucket": "52-53°F",
                        "side": "NO",
                        "source_name": "gfs",
                        "forecast_temp_f": 54.0,
                        "raw_context": {},
                    },
                ]
            )
            storage.record_station_observation_daily_highs(
                [
                    {
                        "captured_at": "2026-03-10T01:00:00+00:00",
                        "city_key": "SEA",
                        "city_name": "Seattle",
                        "station_id": "KSEA",
                        "local_date": "2026-03-10",
                        "observed_high_f": 51.5,
                        "raw_context": {},
                    }
                ]
            )
            summary = storage.forecast_accuracy_summary(min_samples=1, limit=10)
            self.assertEqual(len(summary), 2)
            self.assertEqual(summary[0]["source_name"], "nws")

    def test_storage_computes_policy_recommendations_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = WeatherBotStorage(Path(temp_dir) / "weather_bot.db")
            storage.persist_run(
                run_id="run-1",
                generated_at="2026-03-10T00:00:00+00:00",
                raw_count=2,
                count_selected=0,
                filters={},
                raw_predictions=[
                    {
                        "city_key": "MIA",
                        "city_name": "Miami",
                        "day_label": "today",
                        "date_str": "2026-03-10",
                        "event_slug": "e1",
                        "event_title": "e1",
                        "bucket": "84-85°F",
                        "side": "NO",
                        "edge": 10.0,
                        "ev_percent": 1.0,
                        "price_cents": 55.0,
                        "model_prob": 60.0,
                        "market_prob": 45.0,
                        "ensemble_prediction": 84.0,
                        "weighted_score": 10.0,
                        "consensus_score": 0.5,
                        "spread": 1.0,
                        "sigma": 1.0,
                        "market_slug": "m1",
                        "market_id": "m1",
                        "polymarket_url": "u",
                        "model_predictions": {"nws": 84.0},
                    },
                    {
                        "city_key": "SEA",
                        "city_name": "Seattle",
                        "day_label": "tomorrow",
                        "date_str": "2026-03-11",
                        "event_slug": "e2",
                        "event_title": "e2",
                        "bucket": "52-53°F",
                        "side": "NO",
                        "edge": 20.0,
                        "ev_percent": 3.0,
                        "price_cents": 50.0,
                        "model_prob": 70.0,
                        "market_prob": 45.0,
                        "ensemble_prediction": 51.0,
                        "weighted_score": 20.0,
                        "consensus_score": 0.8,
                        "spread": 1.0,
                        "sigma": 1.0,
                        "market_slug": "m2",
                        "market_id": "m2",
                        "polymarket_url": "u",
                        "model_predictions": {"nws": 51.0},
                    },
                ],
                opportunities=[],
                order_plans=[],
                executions=[],
            )
            with storage._connect() as conn:
                conn.execute("UPDATE scan_predictions SET settled_price_cents = 0, pnl_usd = -1, resolved_at = '2026-03-11T00:00:00+00:00' WHERE city_key = 'MIA'")
                conn.execute("UPDATE scan_predictions SET settled_price_cents = 100, pnl_usd = 1, resolved_at = '2026-03-11T00:00:00+00:00' WHERE city_key = 'SEA'")
            summary = storage.policy_recommendations_summary(min_samples=1, limit=10)
            by_segment = {item["segment"]: item for item in summary}
            self.assertEqual(by_segment["MIA/today"]["recommendation"], "block")
            self.assertEqual(by_segment["SEA/tomorrow"]["recommendation"], "prefer")

    def test_persist_run_stores_extended_policy_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = WeatherBotStorage(Path(temp_dir) / "weather_bot.db")
            storage.persist_run(
                run_id="run-extended",
                generated_at="2026-03-10T00:00:00+00:00",
                raw_count=1,
                count_selected=1,
                filters={},
                raw_predictions=[
                    {
                        "city_key": "SEA",
                        "city_name": "Seattle",
                        "day_label": "tomorrow",
                        "date_str": "2026-03-11",
                        "event_slug": "e1",
                        "event_title": "e1",
                        "bucket": "52-53Â°F",
                        "side": "NO",
                        "edge": 20.0,
                        "ev_percent": 3.0,
                        "price_cents": 50.0,
                        "model_prob": 70.0,
                        "market_prob": 45.0,
                        "ensemble_prediction": 51.0,
                        "weighted_score": 20.0,
                        "consensus_score": 0.8,
                        "spread": 1.0,
                        "sigma": 1.0,
                        "market_slug": "m1",
                        "market_id": "m1",
                        "polymarket_url": "u",
                        "model_predictions": {"nws": 51.0},
                        "agreement_summary": "5/6",
                        "coverage_score": 0.9,
                        "coverage_issue_type": None,
                        "signal_tier": "B",
                        "signal_decision": "review",
                        "mean_agreeing_model_edge": 18.0,
                        "min_agreeing_model_edge": 14.0,
                        "agreeing_model_count": 5,
                        "executable_quality_score": 0.82,
                        "data_quality_score": 0.84,
                        "valid_model_count": 6,
                        "required_model_count": 5,
                        "provider_failures": ["nws"],
                        "provider_failure_details": {"nws": "timeout"},
                        "effective_weights": {"nws": 1.1},
                        "adversarial_score": 67.5,
                        "execution_priority_score": 61.2,
                    }
                ],
                opportunities=[],
                order_plans=[],
                executions=[],
            )
            with storage._connect() as conn:
                row = conn.execute(
                    """
                    SELECT signal_tier, signal_decision, min_agreeing_model_edge, executable_quality_score,
                           data_quality_score, coverage_score, provider_failures_json, effective_weights_json
                    FROM scan_predictions
                    WHERE run_id = 'run-extended'
                    """
                ).fetchone()
            self.assertEqual(row["signal_tier"], "B")
            self.assertEqual(row["signal_decision"], "review")
            self.assertEqual(row["min_agreeing_model_edge"], 14.0)
            self.assertEqual(row["executable_quality_score"], 0.82)
            self.assertEqual(row["data_quality_score"], 0.84)
            self.assertEqual(row["coverage_score"], 0.9)
            self.assertEqual(row["provider_failures_json"], '["nws"]')
            self.assertEqual(row["effective_weights_json"], '{"nws": 1.1}')


if __name__ == "__main__":
    unittest.main()
