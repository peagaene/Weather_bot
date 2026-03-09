from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any


POLYMARKET_DATA_API_BASE_URL = "https://data-api.polymarket.com"


def _request_json(url: str, timeout: float = 20.0) -> Any:
    request = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "weather-bot/1.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_open_positions(user: str) -> list[dict[str, Any]]:
    url = f"{POLYMARKET_DATA_API_BASE_URL}/positions?user={urllib.parse.quote(user, safe='')}"
    data = _request_json(url)
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def fetch_account_activity(user: str) -> list[dict[str, Any]]:
    url = f"{POLYMARKET_DATA_API_BASE_URL}/activity?user={urllib.parse.quote(user, safe='')}"
    data = _request_json(url)
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def fetch_account_value(user: str) -> float | None:
    url = f"{POLYMARKET_DATA_API_BASE_URL}/value?user={urllib.parse.quote(user, safe='')}"
    data = _request_json(url)
    if not isinstance(data, list) or not data:
        return None
    first = data[0]
    if not isinstance(first, dict):
        return None
    try:
        return float(first.get("value"))
    except (TypeError, ValueError):
        return None
