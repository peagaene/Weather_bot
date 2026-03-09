from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paperbot.trading_state import FileLock, TradingStateStore
import run_weather_models


class _FakeStorage:
    def __init__(self, total: int) -> None:
        self.total = total

    def list_live_orders(self, *, statuses=None, limit=500, offset=0):
        if statuses != ("submission_unconfirmed",):
            return []
        end = min(offset + limit, self.total)
        if offset >= self.total:
            return []
        return [{"client_order_id": f"client-{index}"} for index in range(offset, end)]


class LockAndGuardTests(unittest.TestCase):
    def test_count_ambiguous_live_orders_pages_all_results(self) -> None:
        storage = _FakeStorage(total=620)
        self.assertEqual(run_weather_models._count_ambiguous_live_orders(storage), 620)

    def test_file_lock_does_not_clear_live_pid_lock_even_if_old(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "live.lock"
            old_ts = "2000-01-01T00:00:00+00:00"
            lock_path.write_text(f"pid={os.getpid()} ts={old_ts}", encoding="utf-8")
            lock = FileLock(lock_path, timeout_seconds=0.1, poll_seconds=0.05, stale_seconds=0.01)
            cleared = lock._clear_stale_lock_if_needed()
            self.assertFalse(cleared)
            self.assertTrue(lock_path.exists())

    def test_file_lock_does_not_clear_live_pid_lock_with_invalid_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "live.lock"
            lock_path.write_text(f"pid={os.getpid()} ts=invalid", encoding="utf-8")
            lock = FileLock(lock_path, timeout_seconds=0.1, poll_seconds=0.05, stale_seconds=0.01)
            cleared = lock._clear_stale_lock_if_needed()
            self.assertFalse(cleared)
            self.assertTrue(lock_path.exists())

    def test_file_lock_write_failure_aborts_acquisition(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "live.lock"
            lock = FileLock(lock_path, timeout_seconds=0.1, poll_seconds=0.05, stale_seconds=0.01)
            with patch.object(FileLock, "_write_payload", side_effect=RuntimeError("write failed")):
                with self.assertRaises(RuntimeError):
                    lock.__enter__()
            self.assertFalse(lock_path.exists())

    def test_bucket_live_limit_blocks_third_entry_same_trade(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            store = TradingStateStore(state_path)
            for _ in range(2):
                decision = store.can_execute(
                    city_key="miami",
                    event_slug="event-1",
                    bucket_key="event-1|market-1|NO",
                    daily_live_limit=3,
                    bucket_live_limit=2,
                    city_cooldown_minutes=0,
                    event_cooldown_minutes=0,
                    bucket_cooldown_minutes=0,
                )
                self.assertTrue(decision.ok)
                store.record_live_execution(
                    city_key="miami",
                    event_slug="event-1",
                    bucket_key="event-1|market-1|NO",
                )
            blocked = store.can_execute(
                city_key="miami",
                event_slug="event-1",
                bucket_key="event-1|market-1|NO",
                daily_live_limit=3,
                bucket_live_limit=2,
                city_cooldown_minutes=0,
                event_cooldown_minutes=0,
                bucket_cooldown_minutes=0,
            )
            self.assertFalse(blocked.ok)
            self.assertEqual(blocked.reason, "bucket_live_limit_reached")

if __name__ == "__main__":
    unittest.main()
