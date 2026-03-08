from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


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
    polymarket_url: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def kelly_size(model_prob_percent: float, market_price_cents: float, fraction: float = 0.25) -> float:
    if model_prob_percent <= 0 or model_prob_percent >= 100:
        return 0.0
    if market_price_cents <= 0 or market_price_cents >= 100:
        return 0.0
    p = model_prob_percent / 100.0
    q = 1.0 - p
    b = (100.0 / market_price_cents) - 1.0
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


def build_order_plan(
    opportunity: Any,
    *,
    bankroll_usd: float,
    kelly_fraction: float = 0.25,
    max_price_cents: float | None = None,
    min_stake_usd: float = 5.0,
) -> OrderPlan:
    stake_fraction = kelly_size(opportunity.model_prob, opportunity.price_cents, fraction=kelly_fraction)
    stake_usd = max(min_stake_usd, bankroll_usd * stake_fraction) if stake_fraction > 0 else 0.0
    limit_price = opportunity.price_cents if max_price_cents is None else min(opportunity.price_cents, max_price_cents)
    limit_price = max(0.1, min(99.9, limit_price))
    share_size = (stake_usd / (limit_price / 100.0)) if stake_usd > 0 and limit_price > 0 else 0.0
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
        confidence_tier=confidence_tier(opportunity.edge),
        bankroll_usd=round(bankroll_usd, 2),
        stake_fraction=round(stake_fraction, 6),
        stake_usd=round(stake_usd, 2),
        share_size=round(share_size, 4),
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
    )
