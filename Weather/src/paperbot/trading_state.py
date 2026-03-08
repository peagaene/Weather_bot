from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


@dataclass
class PolicyDecision:
    ok: bool
    reason: str = ""


class FileLock:
    def __init__(self, path: Path, *, timeout_seconds: float = 30.0, poll_seconds: float = 0.2) -> None:
        self.path = path
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = poll_seconds
        self._fd: int | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.time() + self.timeout_seconds
        while True:
            try:
                self._fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                payload = f"pid={os.getpid()} ts={_utcnow().isoformat()}".encode("utf-8")
                os.write(self._fd, payload)
                return self
            except FileExistsError:
                if time.time() >= deadline:
                    raise TimeoutError(f"timeout acquiring lock {self.path}")
                time.sleep(self.poll_seconds)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        try:
            self.path.unlink(missing_ok=True)
        except Exception:
            pass


class TradingStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data = self._load()

    def _empty_state(self) -> dict[str, Any]:
        return {
            "day": _utcnow().date().isoformat(),
            "daily_live_orders": 0,
            "last_city_trade": {},
            "last_event_trade": {},
            "last_bucket_trade": {},
        }

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._empty_state()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return self._empty_state()
            for key, default in self._empty_state().items():
                payload.setdefault(key, default)
            return payload
        except Exception:
            return self._empty_state()

    def _roll_day_if_needed(self) -> None:
        today = _utcnow().date().isoformat()
        if self.data.get("day") != today:
            self.data["day"] = today
            self.data["daily_live_orders"] = 0

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    def can_execute(
        self,
        *,
        city_key: str,
        event_slug: str,
        bucket_key: str,
        daily_live_limit: int,
        city_cooldown_minutes: int,
        event_cooldown_minutes: int,
        bucket_cooldown_minutes: int,
    ) -> PolicyDecision:
        self._roll_day_if_needed()
        if daily_live_limit > 0 and int(self.data.get("daily_live_orders", 0)) >= daily_live_limit:
            return PolicyDecision(False, "daily_live_limit_reached")

        now = _utcnow()
        checks = [
            ("city", city_key, city_cooldown_minutes, self.data.get("last_city_trade", {})),
            ("event", event_slug, event_cooldown_minutes, self.data.get("last_event_trade", {})),
            ("bucket", bucket_key, bucket_cooldown_minutes, self.data.get("last_bucket_trade", {})),
        ]
        for prefix, key, cooldown_minutes, mapping in checks:
            if cooldown_minutes <= 0:
                continue
            last_ts = _parse_ts(mapping.get(key))
            if last_ts is None:
                continue
            if now - last_ts < timedelta(minutes=cooldown_minutes):
                return PolicyDecision(False, f"{prefix}_cooldown_active")
        return PolicyDecision(True, "")

    def record_live_execution(self, *, city_key: str, event_slug: str, bucket_key: str) -> None:
        self._roll_day_if_needed()
        self.data["daily_live_orders"] = int(self.data.get("daily_live_orders", 0)) + 1
        now = _utcnow().isoformat()
        self.data.setdefault("last_city_trade", {})[city_key] = now
        self.data.setdefault("last_event_trade", {})[event_slug] = now
        self.data.setdefault("last_bucket_trade", {})[bucket_key] = now
        self.save()
