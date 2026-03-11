from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paperbot.live_trader import _extract_allowance_usd, execute_order_plan
import run_weather_models
import run_auto_trade


class LiveGuardsTests(unittest.TestCase):
    def test_extract_allowance_usd_from_nested_payload(self) -> None:
        payload = {
            "default": "0",
            "exchange": {
                "usdc": "2500000",
            },
        }
        self.assertEqual(_extract_allowance_usd(payload, decimals=6), 2.5)

    def test_execute_order_plan_blocks_when_allowance_is_insufficient(self) -> None:
        plan = type(
            "Plan",
            (),
            {
                "market_slug": "example-market",
                "side": "NO",
                "limit_price_cents": 39.0,
                "share_size": 5.0,
                "valid": True,
                "invalid_reason": None,
                "event_slug": "example-event",
                "token_id": "token-1",
                "stake_usd": 1.95,
            },
        )()
        with (
            patch("paperbot.live_trader._build_client", return_value=(object(), None)),
            patch(
                "paperbot.live_trader.get_account_snapshot",
                return_value={
                    "ok": True,
                    "collateral_balance_usd": 10.0,
                    "collateral_allowance_usd": 1.0,
                    "allowances": {"exchange": "1000000"},
                },
            ),
        ):
            result = execute_order_plan(plan, live=True)
        self.assertFalse(result.success)
        self.assertEqual(result.error, "insufficient_collateral_allowance")

    def test_run_weather_models_rejects_live_without_history(self) -> None:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as ctx:
                run_weather_models.main(["--live", "--no-history"])
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--live requires persistent history/storage", stderr.getvalue())

    def test_run_weather_models_aborts_live_when_presync_fails(self) -> None:
        stdout = io.StringIO()
        with (
            contextlib.redirect_stdout(stdout),
            patch("run_weather_models.WeatherBotStorage") as storage_cls,
            patch("run_weather_models.sync_live_exchange_state", return_value={"ok": False, "error": "api_down"}),
        ):
            storage_cls.return_value.init_run.return_value = None
            run_weather_models.main(["--live", "--top", "1"])
        self.assertIn("live pre-sync failed: api_down", stdout.getvalue())

    def test_run_weather_models_warns_when_postsync_fails(self) -> None:
        stdout = io.StringIO()
        with tempfile.TemporaryDirectory() as temp_dir:
            scan_lock = str(Path(temp_dir) / "scan.lock")
            state_json = str(Path(temp_dir) / "state.json")
            db_path = str(Path(temp_dir) / "weather.db")
            with (
                contextlib.redirect_stdout(stdout),
                patch("run_weather_models.scan_weather_model_opportunities", return_value=[]),
                patch("run_weather_models.WeatherBotStorage") as storage_cls,
                patch("run_weather_models.sync_live_exchange_state", side_effect=[{"ok": True}, {"ok": False, "error": "postsync_down"}]),
            ):
                storage = storage_cls.return_value
                storage.init_run.return_value = None
                storage.persist_run.return_value = None
                run_weather_models.main(
                    [
                        "--live",
                        "--top",
                        "1",
                        "--scan-lock-file",
                        scan_lock,
                        "--state-json",
                        state_json,
                        "--db-path",
                        db_path,
                    ]
                )
        self.assertIn("AVISO: post-sync live falhou: postsync_down", stdout.getvalue())

    def test_auto_trade_allows_micro_live_override_when_replay_gate_not_approved(self) -> None:
        with (
            patch.dict(
                "os.environ",
                {
                    "PAPERBOT_BANKROLL_USD": "18",
                    "PAPERBOT_MIN_STAKE_USD": "2",
                    "PAPERBOT_MAX_STAKE_USD": "2",
                    "WEATHER_DAILY_LIVE_LIMIT": "3",
                    "WEATHER_BUCKET_LIVE_LIMIT": "2",
                    "WEATHER_MAX_ORDERS_PER_EVENT": "1",
                    "WEATHER_MAX_SHARE_SIZE": "20",
                    "POLYMARKET_PRIVATE_KEY": "0xabc",
                    "WEATHER_AUTO_TRADE_ENABLED": "1",
                    "WEATHER_ALLOW_UNAPPROVED_REPLAY_FOR_MICRO_LIVE": "1",
                },
                clear=False,
            ),
            patch("run_auto_trade._load_replay_gate", return_value=(False, "replay gate exists but is not approved")),
            patch(
                "run_auto_trade.get_account_snapshot",
                return_value={"ok": True, "collateral_balance_usd": 10.0, "collateral_allowance_usd": 10.0},
            ),
            patch(
                "run_auto_trade.fetch_public_wallet_snapshot",
                return_value={"ok": True, "liquid_cash_usd": 10.0},
            ),
        ):
            import run_auto_trade

            run_auto_trade._preflight_or_raise(live=True, execute_top=1)

    def test_auto_trade_dry_run_allows_max_stake_above_live_cap(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PAPERBOT_BANKROLL_USD": "18",
                "PAPERBOT_MIN_STAKE_USD": "1",
                "PAPERBOT_MAX_STAKE_USD": "5",
                "WEATHER_MAX_ORDERS_PER_EVENT": "1",
            },
            clear=False,
        ):
            run_auto_trade._preflight_or_raise(live=False, execute_top=0)

    def test_auto_trade_detects_rate_limited_snapshot(self) -> None:
        payload = {
            "blocked_opportunities": [
                {
                    "coverage_issue_type": "mixed_rate_limited",
                    "provider_failure_details": {"gfs": "HTTP 429 fetching ..."},
                }
            ]
        }
        self.assertTrue(run_auto_trade._is_rate_limited_snapshot(payload))
        self.assertEqual(
            run_auto_trade._next_sleep_seconds(base_interval_seconds=60, consecutive_rate_limited_cycles=2),
            180,
        )


if __name__ == "__main__":
    unittest.main()
