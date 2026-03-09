from __future__ import annotations

import json
import math
import os
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any

MARKET_FEE = 0.02
POLYMARKET_CLOB_BASE_URL = os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")


@dataclass
class OrderPlan:
    event_slug: str
    market_slug: str
    side: str
    token_id: str | None
    limit_price_cents: float
    model_prob: float
    market_prob: float
    edge: float
    ev_percent: float
    confidence_tier: str
    bankroll_usd: float
    stake_fraction: float
    stake_usd: float
    share_size: float
    tick_size_cents: float | None
    order_min_size: float | None
    market_active: bool
    valid: bool
    invalid_reason: str | None
    polymarket_url: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _fee_adjusted_price(price_cents: float) -> float:
    if price_cents <= 0 or price_cents >= 100:
        return price_cents
    return price_cents / (1 - MARKET_FEE * (1 - price_cents / 100.0))


def kelly_size(model_prob_percent: float, market_price_cents: float, fraction: float = 0.25) -> float:
    if model_prob_percent <= 0 or model_prob_percent >= 100:
        return 0.0
    if market_price_cents <= 0 or market_price_cents >= 100:
        return 0.0
    effective_price_cents = _fee_adjusted_price(market_price_cents)
    if effective_price_cents <= 0 or effective_price_cents >= 100:
        return 0.0
    p = model_prob_percent / 100.0
    q = 1.0 - p
    b = (100.0 / effective_price_cents) - 1.0
    if b <= 0:
        return 0.0
    full_kelly = ((b * p) - q) / b
    return max(0.0, min(full_kelly * fraction, 0.25))


def confidence_tier(edge: float) -> str:
    if edge >= 30:
        return "strong"
    if edge >= 15:
        return "watch"
    if edge > 0:
        return "small"
    return "avoid"


def _request_json(url: str) -> Any:
    request = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "weather-bot/1.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=20.0) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_tick_size_cents(token_id: str | None) -> float | None:
    if not token_id:
        return None
    encoded = urllib.parse.quote(token_id, safe="")
    try:
        payload = _request_json(f"{POLYMARKET_CLOB_BASE_URL}/tick-size?token_id={encoded}")
        value = payload.get("minimum_tick_size") if isinstance(payload, dict) else None
        tick = float(value) if value is not None else None
        return tick * 100.0 if tick is not None and tick <= 1.0 else tick
    except Exception:
        return None


def _round_to_tick(value_cents: float, tick_size_cents: float | None) -> float:
    if tick_size_cents is None or tick_size_cents <= 0:
        return round(value_cents, 4)
    steps = math.floor(value_cents / tick_size_cents)
    return round(max(tick_size_cents, steps * tick_size_cents), 4)


def build_order_plan(
    opportunity: Any,
    *,
    bankroll_usd: float,
    kelly_fraction: float = 0.25,
    max_price_cents: float | None = None,
    min_stake_usd: float = 5.0,
    max_stake_usd: float | None = None,
) -> OrderPlan:
    stake_fraction = kelly_size(opportunity.model_prob, opportunity.price_cents, fraction=kelly_fraction)
    stake_usd = max(min_stake_usd, bankroll_usd * stake_fraction) if stake_fraction > 0 else 0.0
    if max_stake_usd is not None and max_stake_usd > 0:
        stake_usd = min(stake_usd, max_stake_usd)
    limit_price = opportunity.price_cents if max_price_cents is None else min(opportunity.price_cents, max_price_cents)
    limit_price = max(0.1, min(99.9, limit_price))
    tick_size_cents = _fetch_tick_size_cents(getattr(opportunity, "token_id", None))
    limit_price = _round_to_tick(limit_price, tick_size_cents)
    share_size = (stake_usd / (limit_price / 100.0)) if stake_usd > 0 and limit_price > 0 else 0.0
    order_min_size = getattr(opportunity, "order_min_size", None)
    valid = True
    invalid_reason = None
    if getattr(opportunity, "token_id", None) is None:
        valid = False
        invalid_reason = "missing_token_id"
    elif getattr(opportunity, "price_source", "") == "gamma_outcome_price":
        valid = False
        invalid_reason = "degraded_clob_price"
    elif tick_size_cents is None:
        valid = False
        invalid_reason = "missing_tick_size"
    elif order_min_size is not None and share_size < float(order_min_size):
        valid = False
        invalid_reason = "share_size_below_order_min_size"
    return OrderPlan(
        event_slug=opportunity.event_slug,
        market_slug=opportunity.market_slug,
        side=opportunity.side,
        token_id=opportunity.token_id,
        limit_price_cents=round(limit_price, 4),
        model_prob=opportunity.model_prob,
        market_prob=opportunity.market_prob,
        edge=opportunity.edge,
        ev_percent=opportunity.ev_percent,
        confidence_tier=getattr(opportunity, "confidence_tier", confidence_tier(opportunity.edge)),
        bankroll_usd=round(bankroll_usd, 2),
        stake_fraction=round(stake_fraction, 6),
        stake_usd=round(stake_usd, 2),
        share_size=round(share_size, 4),
        tick_size_cents=(round(tick_size_cents, 4) if tick_size_cents is not None else None),
        order_min_size=(round(float(order_min_size), 4) if order_min_size is not None else None),
        market_active=True,
        valid=valid,
        invalid_reason=invalid_reason,
        polymarket_url=f"https://polymarket.com/event/{opportunity.event_slug}",
    )


def summarize_plan(plan: OrderPlan) -> str:
    if not plan.token_id:
        token_text = "token_id ausente"
    else:
        token_text = plan.token_id
    return (
        f"{plan.side} {plan.market_slug} @ {plan.limit_price_cents:.2f}c "
        f"stake=${plan.stake_usd:.2f} shares={plan.share_size:.2f} "
        f"tier={plan.confidence_tier} token={token_text}"
        + (f" invalid={plan.invalid_reason}" if not plan.valid and plan.invalid_reason else "")
    )
