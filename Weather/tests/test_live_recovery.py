from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
from types import ModuleType
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.live_trader import (
    _generate_submission_nonce,
    _load_submission_identity,
    _match_open_order_candidate,
    _submission_identity_matches,
    execute_order_plan,
    sync_live_exchange_state,
)
from paperbot.storage import WeatherBotStorage


def _sample_opportunity() -> dict:
    return {
        "city_key": "SEA",
        "date_str": "2026-03-08",
        "polymarket_url": "https://polymarket.com/event/example",
        "order_min_size": 5.0,
        "event_slug": "example-event",
        "market_slug": "example-market",
        "side": "NO",
        "token_id": "token-1",
    }


def _sample_plan() -> dict:
    return {
        "event_slug": "example-event",
        "market_slug": "example-market",
        "side": "NO",
        "token_id": "token-1",
        "limit_price_cents": 39.0,
        "share_size": 5.0,
        "stake_usd": 1.95,
        "tick_size_cents": 1.0,
        "order_min_size": 5.0,
        "polymarket_url": "https://polymarket.com/event/example",
    }


def _sample_execution(**overrides: object) -> dict:
    base = {
        "mode": "live",
        "success": True,
        "accepted": True,
        "market_slug": "example-market",
        "event_slug": "example-event",
        "side": "NO",
        "token_id": "token-1",
        "client_order_id": "client-1",
        "nonce": 111,
        "submission_fingerprint": "token-1|BUY|39.0000|5.0000",
        "exchange_order_id": "exchange-1",
        "order_status": "resting",
        "filled_shares": 0.0,
        "avg_fill_price_cents": None,
        "price_cents": 39.0,
        "share_size": 5.0,
        "error": None,
        "response": {
            "submission": {
                "maker": "maker-1",
                "signer": "maker-1",
                "token_id": "token-1",
                "maker_amount": "1950000",
                "taker_amount": "5000000",
                "side": "BUY",
                "nonce": "111",
                "salt": "777",
            }
        },
        "fills": [],
    }
    base.update(overrides)
    return base


class _FakeClient:
    def __init__(self, orders: list[dict] | None = None, order_pages: list[list[dict]] | None = None) -> None:
        self._orders = orders or []
        self._order_pages = list(order_pages or [])
        self.cancelled: list[str] = []
        self.post_error: Exception | None = None
        self.created_order: object | None = None

    def get_orders(self, _params=None) -> list[dict]:
        if self._order_pages:
            return list(self._order_pages.pop(0))
        return list(self._orders)

    def cancel_orders(self, order_ids: list[str]) -> None:
        self.cancelled.extend(order_ids)

    def create_order(self, order_args):
        if self.created_order is not None:
            return self.created_order
        class _SignedOrder:
            def dict(self_nonlocal):
                return {
                    "maker": "maker-1",
                    "signer": "maker-1",
                    "tokenId": order_args.token_id,
                    "makerAmount": "1950000",
                    "takerAmount": "5000000",
                    "side": order_args.side,
                    "nonce": str(order_args.nonce),
                    "salt": "777",
                }

        return _SignedOrder()

    def post_order(self, *_args, **_kwargs):
        if self.post_error is not None:
            raise self.post_error
        return {"orderID": "exchange-1"}

    def get_address(self) -> str:
        return "maker-1"


class LiveRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "weather_bot.db"
        self.storage = WeatherBotStorage(self.db_path)
        self.storage.init_run(run_id="run-1", generated_at="2026-03-08T12:00:00+00:00", filters={})

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_append_live_execution_without_fills_does_not_create_position(self) -> None:
        self.storage.append_live_execution(
            run_id="run-1",
            generated_at="2026-03-08T12:00:00+00:00",
            rank=1,
            opportunity=_sample_opportunity(),
            plan=_sample_plan(),
            execution=_sample_execution(),
        )

        live_orders = self.storage.list_live_orders(statuses=("resting",))
        self.assertEqual(len(live_orders), 1)
        self.assertEqual(live_orders[0]["client_order_id"], "client-1")
        self.assertEqual(self.storage.list_open_positions(), [])

    def test_restart_rebuilds_position_from_persisted_fills(self) -> None:
        execution = _sample_execution(
            order_status="filled",
            filled_shares=5.0,
            avg_fill_price_cents=39.0,
            fills=[{"id": "fill-1", "share_size": 5.0, "price_cents": 39.0, "timestamp": "2026-03-08T12:00:01+00:00"}],
        )
        self.storage.append_live_execution(
            run_id="run-1",
            generated_at="2026-03-08T12:00:00+00:00",
            rank=1,
            opportunity=_sample_opportunity(),
            plan=_sample_plan(),
            execution=execution,
        )

        restarted = WeatherBotStorage(self.db_path)
        positions = restarted.list_open_positions()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["client_order_id"], "client-1")
        self.assertAlmostEqual(float(positions[0]["share_size"]), 5.0)
        self.assertAlmostEqual(float(positions[0]["entry_price_cents"]), 39.0)

    def test_sync_live_order_state_with_partial_fill_updates_position(self) -> None:
        self.storage.append_live_execution(
            run_id="run-1",
            generated_at="2026-03-08T12:00:00+00:00",
            rank=1,
            opportunity=_sample_opportunity(),
            plan=_sample_plan(),
            execution=_sample_execution(exchange_order_id=None, order_status="submission_unconfirmed", accepted=False),
        )

        self.storage.sync_live_order_state(
            client_order_id="client-1",
            exchange_order_id="exchange-1",
            status="partial_fill",
            accepted=True,
            filled_shares=2.5,
            avg_fill_price_cents=38.8,
            response={},
            fills=[{"id": "fill-1", "share_size": 2.5, "price_cents": 38.8, "timestamp": "2026-03-08T12:00:02+00:00"}],
            synced_at="2026-03-08T12:00:03+00:00",
        )

        positions = self.storage.list_open_positions()
        self.assertEqual(len(positions), 1)
        self.assertAlmostEqual(float(positions[0]["share_size"]), 2.5)
        self.assertAlmostEqual(float(positions[0]["entry_price_cents"]), 38.8)

    def test_sync_live_exchange_state_revisits_submission_unconfirmed(self) -> None:
        self.storage.append_live_execution(
            run_id="run-1",
            generated_at="2026-03-08T12:00:00+00:00",
            rank=1,
            opportunity=_sample_opportunity(),
            plan=_sample_plan(),
            execution=_sample_execution(exchange_order_id=None, order_status="submission_unconfirmed", accepted=False),
        )

        with (
            patch("paperbot.live_trader._build_client", return_value=(object(), None)),
            patch(
                "paperbot.live_trader._reconcile_submission",
                return_value=("resting", "exchange-1", 0.0, None, {"id": "exchange-1"}, []),
            ),
        ):
            result = sync_live_exchange_state(self.storage)

        self.assertTrue(result["ok"])
        refreshed = self.storage.list_live_orders(statuses=("resting",))
        self.assertEqual(len(refreshed), 1)
        self.assertEqual(refreshed[0]["exchange_order_id"], "exchange-1")

    def test_match_open_order_candidate_prefers_submission_identity_nonce(self) -> None:
        client = _FakeClient(
            orders=[
                {
                    "id": "wrong-order",
                    "side": "BUY",
                    "price": "0.39",
                    "size": "5",
                    "nonce": "999",
                    "tokenId": "token-1",
                },
                {
                    "id": "right-order",
                    "side": "BUY",
                    "price": "0.39",
                    "size": "5",
                    "nonce": "111",
                    "tokenId": "token-1",
                    "makerAmount": "1950000",
                    "takerAmount": "5000000",
                    "maker": "maker-1",
                    "signer": "maker-1",
                },
            ]
        )

        match = _match_open_order_candidate(
            client,
            token_id="token-1",
            order_side="BUY",
            price_cents=39.0,
            share_size=5.0,
            submission_identity={
                "maker": "maker-1",
                "signer": "maker-1",
                "token_id": "token-1",
                "maker_amount": "1950000",
                "taker_amount": "5000000",
                "side": "BUY",
                "nonce": "111",
                "salt": "777",
            },
        )

        self.assertIsNotNone(match)
        self.assertEqual(match["id"], "right-order")

    def test_match_open_order_candidate_does_not_fallback_when_identity_exists(self) -> None:
        client = _FakeClient(
            orders=[
                {
                    "id": "price-size-match-only",
                    "side": "BUY",
                    "price": "0.39",
                    "size": "5",
                    "nonce": "999",
                    "tokenId": "token-1",
                    "makerAmount": "1950000",
                    "takerAmount": "5000000",
                    "maker": "maker-other",
                    "signer": "maker-other",
                }
            ]
        )

        match = _match_open_order_candidate(
            client,
            token_id="token-1",
            order_side="BUY",
            price_cents=39.0,
            share_size=5.0,
            submission_identity={
                "maker": "maker-1",
                "signer": "maker-1",
                "token_id": "token-1",
                "maker_amount": "1950000",
                "taker_amount": "5000000",
                "side": "BUY",
                "nonce": "111",
                "salt": "777",
            },
        )

        self.assertIsNone(match)

    def test_load_submission_identity_round_trip_from_string_payload(self) -> None:
        payload = {
            "submission": {
                "maker": "maker-1",
                "signer": "maker-1",
                "token_id": "token-1",
                "maker_amount": "1950000",
                "taker_amount": "5000000",
                "side": "BUY",
                "nonce": "111",
                "salt": "777",
            }
        }
        restored = _load_submission_identity(str(payload).replace("'", '"'))
        self.assertEqual(restored["nonce"], "111")
        self.assertEqual(restored["salt"], "777")

    def test_submission_identity_nonce_alone_is_not_enough(self) -> None:
        identity = {
            "maker": "maker-1",
            "signer": "maker-1",
            "token_id": "token-1",
            "maker_amount": "1950000",
            "taker_amount": "5000000",
            "side": "BUY",
            "nonce": "111",
            "salt": "777",
        }
        candidate = {
            "maker": "maker-1",
            "signer": "maker-1",
            "tokenId": "token-OTHER",
            "makerAmount": "1950000",
            "takerAmount": "5000000",
            "side": "BUY",
            "nonce": "111",
            "salt": "777",
        }
        self.assertFalse(_submission_identity_matches(candidate, identity))

    def test_generate_submission_nonce_is_unique(self) -> None:
        nonce_a = _generate_submission_nonce()
        nonce_b = _generate_submission_nonce()
        self.assertNotEqual(nonce_a, nonce_b)

    def test_execute_order_plan_blocks_replace_when_cancel_not_confirmed(self) -> None:
        fake_client = _FakeClient(
            order_pages=[
                [{"id": "old-1", "side": "BUY", "price": "0.31", "size": "5"}],
                [{"id": "old-1", "side": "BUY", "price": "0.31", "size": "5"}],
                [{"id": "old-1", "side": "BUY", "price": "0.31", "size": "5"}],
                [{"id": "old-1", "side": "BUY", "price": "0.31", "size": "5"}],
                [{"id": "old-1", "side": "BUY", "price": "0.31", "size": "5"}],
            ]
        )
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
            patch("paperbot.live_trader._build_client", return_value=(fake_client, None)),
            patch(
                "paperbot.live_trader.get_account_snapshot",
                return_value={
                    "ok": True,
                    "collateral_balance_usd": 50.0,
                    "collateral_allowance_usd": 50.0,
                    "allowances": {"exchange": "50000000"},
                },
            ),
            patch("paperbot.live_trader.time.sleep", return_value=None),
        ):
            result = execute_order_plan(plan, live=True, replace_open_orders=True, replace_price_threshold_cents=0.5)
        self.assertFalse(result.success)
        self.assertEqual(result.error, "cancel_replace_not_confirmed")
        self.assertEqual(fake_client.cancelled, ["old-1"])

    def test_execute_order_plan_recovers_after_post_order_timeout_when_order_is_found(self) -> None:
        fake_client = _FakeClient()
        fake_client.post_error = RuntimeError("timeout")
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

        class _FakeOrderArgs:
            def __init__(self, token_id, price, size, side, nonce):
                self.token_id = token_id
                self.price = price
                self.size = size
                self.side = side
                self.nonce = nonce

        fake_clob_types = ModuleType("py_clob_client.clob_types")
        fake_clob_types.OrderArgs = _FakeOrderArgs
        fake_clob_types.OrderType = type("OrderType", (), {"GTC": "GTC"})
        fake_package = ModuleType("py_clob_client")
        fake_package.clob_types = fake_clob_types

        with (
            patch("paperbot.live_trader._build_client", return_value=(fake_client, None)),
            patch(
                "paperbot.live_trader.get_account_snapshot",
                return_value={
                    "ok": True,
                    "collateral_balance_usd": 50.0,
                    "collateral_allowance_usd": 50.0,
                    "allowances": {"exchange": "50000000"},
                },
            ),
            patch(
                "paperbot.live_trader._reconcile_submission",
                return_value=("resting", "exchange-1", 0.0, None, {"id": "exchange-1"}, []),
            ),
            patch.dict(sys.modules, {"py_clob_client": fake_package, "py_clob_client.clob_types": fake_clob_types}),
        ):
            result = execute_order_plan(plan, live=True)

        self.assertTrue(result.success)
        self.assertTrue(result.accepted)
        self.assertEqual(result.order_status, "resting")
        self.assertEqual(result.exchange_order_id, "exchange-1")
        self.assertIn("recovery", result.response)

    def test_sync_live_exchange_state_pages_pending_orders(self) -> None:
        for index in range(260):
            client_order_id = f"client-{index}"
            self.storage.append_live_execution(
                run_id="run-1",
                generated_at="2026-03-08T12:00:00+00:00",
                rank=index + 1,
                opportunity={**_sample_opportunity(), "market_slug": f"market-{index}", "token_id": f"token-{index}"},
                plan={**_sample_plan(), "market_slug": f"market-{index}", "token_id": f"token-{index}"},
                execution=_sample_execution(
                    client_order_id=client_order_id,
                    exchange_order_id=None,
                    order_status="submission_unconfirmed",
                    accepted=False,
                    market_slug=f"market-{index}",
                    token_id=f"token-{index}",
                ),
            )

        with (
            patch("paperbot.live_trader._build_client", return_value=(object(), None)),
            patch(
                "paperbot.live_trader._reconcile_submission",
                return_value=("submission_unconfirmed", None, 0.0, None, {}, []),
            ) as reconcile_mock,
        ):
            result = sync_live_exchange_state(self.storage)
        self.assertTrue(result["ok"])
        self.assertEqual(result["checked_orders"], 260)
        self.assertEqual(reconcile_mock.call_count, 260)


if __name__ == "__main__":
    unittest.main()
