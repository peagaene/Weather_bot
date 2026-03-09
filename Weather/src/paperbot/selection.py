from __future__ import annotations

from collections import Counter
from typing import Any

from .policy import apply_trade_policy


def _evaluate_opportunity(
    opportunity: object,
    *,
    min_price_cents: float | None,
    max_price_cents: float | None,
    max_spread: float | None,
    max_share_size: float | None,
    require_token: bool,
    max_orders_per_event: int,
    plans_by_slug: dict[str, object] | None,
    per_event: dict[str, int],
) -> tuple[bool, str | None]:
    policy = apply_trade_policy(opportunity)
    setattr(opportunity, "risk_label", policy.risk_label)
    setattr(opportunity, "risk_score", policy.risk_score)
    setattr(opportunity, "policy_allowed", policy.allowed)
    setattr(opportunity, "policy_reason", policy.reason)
    if not policy.allowed:
        return False, f"policy:{policy.reason}"
    if require_token and not getattr(opportunity, "token_id", None):
        return False, "missing_token"
    price = float(getattr(opportunity, "price_cents", 0.0))
    if min_price_cents is not None and price < min_price_cents:
        return False, "price_below_min"
    if max_price_cents is not None and price > max_price_cents:
        return False, "price_above_max"
    if max_spread is not None and float(getattr(opportunity, "spread", 0.0)) > max_spread:
        return False, "spread_above_max"
    event_slug = getattr(opportunity, "event_slug", "")
    if max_orders_per_event > 0 and per_event.get(event_slug, 0) >= max_orders_per_event:
        return False, "max_orders_per_event"
    if max_share_size is not None and plans_by_slug is not None:
        key = f"{event_slug}|{getattr(opportunity, 'market_slug', '')}|{getattr(opportunity, 'side', '')}"
        plan = plans_by_slug.get(key)
        if plan is not None and not bool(getattr(plan, "valid", True)):
            reason = str(getattr(plan, "invalid_reason", "invalid_plan"))
            setattr(opportunity, "policy_allowed", False)
            setattr(opportunity, "policy_reason", reason)
            return False, f"plan:{reason}"
        if plan is not None and float(getattr(plan, "share_size", 0.0)) > max_share_size:
            setattr(opportunity, "policy_allowed", False)
            setattr(opportunity, "policy_reason", "share_size_above_max")
            return False, "share_size_above_max"
    per_event[event_slug] = per_event.get(event_slug, 0) + 1
    return True, None


def filter_opportunities(
    opportunities: list,
    *,
    min_price_cents: float | None,
    max_price_cents: float | None,
    max_spread: float | None,
    max_share_size: float | None,
    require_token: bool,
    max_orders_per_event: int,
    plans_by_slug: dict[str, object] | None = None,
) -> list:
    filtered = []
    per_event: dict[str, int] = {}
    for opportunity in opportunities:
        allowed, _ = _evaluate_opportunity(
            opportunity,
            min_price_cents=min_price_cents,
            max_price_cents=max_price_cents,
            max_spread=max_spread,
            max_share_size=max_share_size,
            require_token=require_token,
            max_orders_per_event=max_orders_per_event,
            plans_by_slug=plans_by_slug,
            per_event=per_event,
        )
        if allowed:
            filtered.append(opportunity)
    return filtered


def summarize_filter_rejections(
    opportunities: list,
    *,
    min_price_cents: float | None,
    max_price_cents: float | None,
    max_spread: float | None,
    max_share_size: float | None,
    require_token: bool,
    max_orders_per_event: int,
    plans_by_slug: dict[str, object] | None = None,
) -> dict[str, int]:
    reasons: Counter[str] = Counter()
    per_event: dict[str, int] = {}

    for opportunity in opportunities:
        allowed, reason = _evaluate_opportunity(
            opportunity,
            min_price_cents=min_price_cents,
            max_price_cents=max_price_cents,
            max_spread=max_spread,
            max_share_size=max_share_size,
            require_token=require_token,
            max_orders_per_event=max_orders_per_event,
            plans_by_slug=plans_by_slug,
            per_event=per_event,
        )
        if not allowed and reason:
            reasons[reason] += 1

    return dict(sorted(reasons.items(), key=lambda item: (-item[1], item[0])))


def explain_blocked_opportunities(
    opportunities: list,
    *,
    min_price_cents: float | None,
    max_price_cents: float | None,
    max_spread: float | None,
    max_share_size: float | None,
    require_token: bool,
    max_orders_per_event: int,
    plans_by_slug: dict[str, object] | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    blocked: list[dict[str, Any]] = []
    per_event: dict[str, int] = {}

    for opportunity in opportunities:
        allowed, reason = _evaluate_opportunity(
            opportunity,
            min_price_cents=min_price_cents,
            max_price_cents=max_price_cents,
            max_spread=max_spread,
            max_share_size=max_share_size,
            require_token=require_token,
            max_orders_per_event=max_orders_per_event,
            plans_by_slug=plans_by_slug,
            per_event=per_event,
        )
        if allowed or not reason:
            continue
        event_slug = getattr(opportunity, "event_slug", "")
        key = f"{event_slug}|{getattr(opportunity, 'market_slug', '')}|{getattr(opportunity, 'side', '')}"
        plan = plans_by_slug.get(key) if plans_by_slug is not None else None
        blocked.append(
            {
                "city_key": getattr(opportunity, "city_key", ""),
                "date_str": getattr(opportunity, "date_str", ""),
                "bucket": getattr(opportunity, "bucket", ""),
                "side": getattr(opportunity, "side", ""),
                "edge": float(getattr(opportunity, "edge", 0.0)),
                "model_prob": float(getattr(opportunity, "model_prob", 0.0)),
                "price_cents": float(getattr(opportunity, "price_cents", 0.0)),
                "confidence_tier": getattr(opportunity, "confidence_tier", ""),
                "risk_label": getattr(opportunity, "risk_label", ""),
                "reason": reason,
                "market_slug": getattr(opportunity, "market_slug", ""),
                "event_slug": event_slug,
                "polymarket_url": getattr(opportunity, "as_dict", lambda: {})().get("polymarket_url", ""),
                "plan_valid": None if plan is None else bool(getattr(plan, "valid", True)),
                "plan_invalid_reason": None if plan is None else getattr(plan, "invalid_reason", None),
                "plan_share_size": None if plan is None else float(getattr(plan, "share_size", 0.0)),
            }
        )
        if limit > 0 and len(blocked) >= limit:
            break

    return blocked
