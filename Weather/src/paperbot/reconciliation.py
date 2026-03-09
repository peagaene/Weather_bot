from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .settlement import fetch_market_resolution
from .storage import WeatherBotStorage


def sync_open_positions(storage: WeatherBotStorage) -> dict[str, Any]:
    open_positions = storage.list_open_positions()
    updated = 0
    checked = 0
    errors: list[dict[str, str]] = []
    seen_markets: set[tuple[str, str, str]] = set()

    for position in open_positions:
        key = (
            str(position["event_slug"]),
            str(position["market_slug"]),
            str(position["side"]),
        )
        if key in seen_markets:
            continue
        seen_markets.add(key)
        checked += 1
        try:
            resolution = fetch_market_resolution(position["event_slug"], position["market_slug"])
            if resolution is None or not resolution.market_closed:
                continue
            settled_price_cents = resolution.settled_price_for_side(position["side"])
            if settled_price_cents is None:
                continue
            updated += storage.sync_position_resolution(
                event_slug=position["event_slug"],
                market_slug=position["market_slug"],
                side=position["side"],
                settled_price_cents=settled_price_cents,
                resolution_source=resolution.resolution_source,
                resolved_by=resolution.resolved_by,
                resolved_at=datetime.now(timezone.utc).isoformat(),
                notes="resolved via gamma-api outcomePrices",
            )
        except Exception as exc:
            errors.append(
                {
                    "event_slug": str(position["event_slug"]),
                    "market_slug": str(position["market_slug"]),
                    "error": str(exc),
                }
            )

    return {
        "checked_markets": checked,
        "updated_positions": updated,
        "errors": errors,
    }


def sync_prediction_resolutions(storage: WeatherBotStorage) -> dict[str, Any]:
    pending_predictions = storage.list_unresolved_prediction_markets()
    updated = 0
    checked = 0
    errors: list[dict[str, str]] = []
    seen_markets: set[tuple[str, str, str]] = set()

    for prediction in pending_predictions:
        key = (
            str(prediction["event_slug"]),
            str(prediction["market_slug"]),
            str(prediction["side"]),
        )
        if key in seen_markets:
            continue
        seen_markets.add(key)
        checked += 1
        try:
            resolution = fetch_market_resolution(prediction["event_slug"], prediction["market_slug"])
            if resolution is None or not resolution.market_closed:
                continue
            settled_price_cents = resolution.settled_price_for_side(prediction["side"])
            if settled_price_cents is None:
                continue
            updated += storage.sync_prediction_resolution(
                event_slug=prediction["event_slug"],
                market_slug=prediction["market_slug"],
                side=prediction["side"],
                settled_price_cents=settled_price_cents,
                resolution_source=resolution.resolution_source,
                resolved_by=resolution.resolved_by,
                resolved_at=datetime.now(timezone.utc).isoformat(),
            )
        except Exception as exc:
            errors.append(
                {
                    "event_slug": str(prediction["event_slug"]),
                    "market_slug": str(prediction["market_slug"]),
                    "side": str(prediction["side"]),
                    "error": str(exc),
                }
            )

    return {
        "checked_markets": checked,
        "updated_predictions": updated,
        "errors": errors,
    }
