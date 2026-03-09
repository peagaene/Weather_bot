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
    def __init__(
        self,
        path: Path,
        *,
        timeout_seconds: float = 30.0,
        poll_seconds: float = 0.2,
        stale_seconds: float | None = None,
    ) -> None:
        self.path = path
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = poll_seconds
        self.stale_seconds = stale_seconds if stale_seconds is not None else max(timeout_seconds * 4.0, 120.0)
        self._fd: int | None = None

    def _lock_payload(self) -> dict[str, Any]:
        try:
            raw = self.path.read_text(encoding="utf-8")
        except Exception:
            return {}
        payload: dict[str, Any] = {}
        for part in raw.split():
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            payload[key] = value
        return payload

    def _pid_is_alive(self, pid_value: Any) -> bool:
        try:
            pid = int(pid_value)
        except (TypeError, ValueError):
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            return False
        return True

    def _clear_stale_lock_if_needed(self) -> bool:
        payload = self._lock_payload()
        ts = _parse_ts(str(payload.get("ts") or ""))
        pid_alive = self._pid_is_alive(payload.get("pid"))
        if not pid_alive:
            try:
                self.path.unlink(missing_ok=True)
                return True
            except Exception:
                return False
        return False

    def _write_payload(self) -> None:
        payload = f"pid={os.getpid()} ts={_utcnow().isoformat()}".encode("utf-8")
        try:
            os.lseek(self._fd, 0, os.SEEK_SET)
            os.ftruncate(self._fd, 0)
            os.write(self._fd, payload)
            os.fsync(self._fd)
        except Exception as exc:
            raise RuntimeError(f"failed to write lock payload for {self.path}") from exc

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.time() + self.timeout_seconds
        while True:
            try:
                self._fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                try:
                    self._write_payload()
                    return self
                except Exception:
                    if self._fd is not None:
                        os.close(self._fd)
                        self._fd = None
                    try:
                        self.path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    raise
            except FileExistsError:
                self._clear_stale_lock_if_needed()
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
            "daily_bucket_live_orders": {},
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
            self.data["daily_bucket_live_orders"] = {}

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
        bucket_live_limit: int,
        city_cooldown_minutes: int,
        event_cooldown_minutes: int,
        bucket_cooldown_minutes: int,
    ) -> PolicyDecision:
        self._roll_day_if_needed()
        if daily_live_limit > 0 and int(self.data.get("daily_live_orders", 0)) >= daily_live_limit:
            return PolicyDecision(False, "daily_live_limit_reached")
        bucket_counts = self.data.get("daily_bucket_live_orders", {})
        bucket_total = int(bucket_counts.get(bucket_key, 0))
        if bucket_live_limit > 0 and bucket_total >= bucket_live_limit:
            return PolicyDecision(False, "bucket_live_limit_reached")

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
        bucket_counts = self.data.setdefault("daily_bucket_live_orders", {})
        bucket_counts[bucket_key] = int(bucket_counts.get(bucket_key, 0)) + 1
        now = _utcnow().isoformat()
        self.data.setdefault("last_city_trade", {})[city_key] = now
        self.data.setdefault("last_event_trade", {})[event_slug] = now
        self.data.setdefault("last_bucket_trade", {})[bucket_key] = now
        self.save()
