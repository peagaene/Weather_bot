from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


POLYMARKET_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"


@dataclass
class MarketResolution:
    event_slug: str
    market_slug: str
    market_closed: bool
    event_closed: bool
    settled_price_cents_yes: float | None
    settled_price_cents_no: float | None
    resolution_source: str | None
    resolved_by: str | None

    def settled_price_for_side(self, side: str) -> float | None:
        normalized = str(side).upper()
        if normalized == "YES":
            return self.settled_price_cents_yes
        if normalized == "NO":
            return self.settled_price_cents_no
        return None


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


def _parse_outcome_prices(raw: Any) -> list[float]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []

    prices: list[float] = []
    for item in raw:
        try:
            numeric = float(item)
        except (TypeError, ValueError):
            continue
        prices.append(numeric * 100.0 if numeric <= 1.0 else numeric)
    return prices


def fetch_market_resolution(event_slug: str, market_slug: str) -> MarketResolution | None:
    url = f"{POLYMARKET_GAMMA_BASE_URL}/events?slug={urllib.parse.quote(event_slug, safe='')}"
    data = _request_json(url)
    if not isinstance(data, list) or not data:
        return None

    event = data[0]
    markets = event.get("markets") or []
    if not isinstance(markets, list):
        return None

    market = next((item for item in markets if str(item.get("slug") or "") == market_slug), None)
    if not isinstance(market, dict):
        return None

    outcome_prices = _parse_outcome_prices(market.get("outcomePrices"))
    return MarketResolution(
        event_slug=event_slug,
        market_slug=market_slug,
        market_closed=bool(market.get("closed")),
        event_closed=bool(event.get("closed")),
        settled_price_cents_yes=outcome_prices[0] if len(outcome_prices) >= 1 else None,
        settled_price_cents_no=outcome_prices[1] if len(outcome_prices) >= 2 else None,
        resolution_source=str(market.get("resolutionSource") or event.get("resolutionSource") or "") or None,
        resolved_by=str(market.get("resolvedBy") or "") or None,
    )
