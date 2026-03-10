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
    enforce_plan_validity: bool = False,
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
            setattr(opportunity, "plan_valid", False)
            setattr(opportunity, "plan_invalid_reason", reason)
            if enforce_plan_validity:
                setattr(opportunity, "policy_allowed", False)
                setattr(opportunity, "policy_reason", reason)
                return False, f"plan:{reason}"
        if plan is not None and float(getattr(plan, "share_size", 0.0)) > max_share_size:
            setattr(opportunity, "policy_allowed", False)
            setattr(opportunity, "policy_reason", "share_size_above_max")
            return False, "share_size_above_max"
    per_event[event_slug] = per_event.get(event_slug, 0) + 1
    return True, None


def _best_opportunity_per_event(opportunities: list) -> list:
    grouped: dict[str, object] = {}
    for opportunity in opportunities:
        event_slug = str(getattr(opportunity, "event_slug", "") or "")
        current = grouped.get(event_slug)
        if current is None:
            grouped[event_slug] = opportunity
            continue
        current_score = float(getattr(current, "weighted_score", 0.0) or 0.0)
        candidate_score = float(getattr(opportunity, "weighted_score", 0.0) or 0.0)
        if candidate_score > current_score:
            grouped[event_slug] = opportunity
            continue
        if candidate_score == current_score:
            current_edge = float(getattr(current, "edge", 0.0) or 0.0)
            candidate_edge = float(getattr(opportunity, "edge", 0.0) or 0.0)
            if candidate_edge > current_edge:
                grouped[event_slug] = opportunity
    selected = list(grouped.values())
    selected.sort(key=lambda item: float(getattr(item, "weighted_score", 0.0) or 0.0), reverse=True)
    return selected


def _rank_opportunity(opportunity: object) -> tuple[float, float]:
    return (
        float(getattr(opportunity, "weighted_score", 0.0) or 0.0),
        float(getattr(opportunity, "edge", 0.0) or 0.0),
    )


def _grouped_opportunities_by_event(opportunities: list) -> list[tuple[str, list[object]]]:
    grouped: dict[str, list[object]] = {}
    for opportunity in opportunities:
        event_slug = str(getattr(opportunity, "event_slug", "") or "")
        grouped.setdefault(event_slug, []).append(opportunity)
    items: list[tuple[str, list[object]]] = []
    for event_slug, candidates in grouped.items():
        ordered = sorted(candidates, key=_rank_opportunity, reverse=True)
        items.append((event_slug, ordered))
    items.sort(key=lambda item: _rank_opportunity(item[1][0]) if item[1] else (0.0, 0.0), reverse=True)
    return items


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
    for _event_slug, candidates in _grouped_opportunities_by_event(opportunities):
        accepted_for_event = 0
        for opportunity in candidates:
            if max_orders_per_event > 0 and accepted_for_event >= max_orders_per_event:
                break
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
            enforce_plan_validity=True,
        )
            if allowed:
                filtered.append(opportunity)
                accepted_for_event += 1
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

    for opportunity in _best_opportunity_per_event(opportunities):
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
            enforce_plan_validity=True,
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

    for opportunity in _best_opportunity_per_event(opportunities):
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
            enforce_plan_validity=True,
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
                "agreement_models": int(getattr(opportunity, "agreement_models", 0) or 0),
                "total_models": int(getattr(opportunity, "total_models", 0) or 0),
                "agreement_pct": float(getattr(opportunity, "agreement_pct", 0.0) or 0.0),
                "agreement_summary": getattr(opportunity, "agreement_summary", "--"),
                "agreeing_model_names": list(getattr(opportunity, "agreeing_model_names", None) or []),
                "confidence_tier": getattr(opportunity, "confidence_tier", ""),
                "risk_label": getattr(opportunity, "risk_label", ""),
                "signal_tier": getattr(opportunity, "signal_tier", ""),
                "adversarial_score": float(getattr(opportunity, "adversarial_score", 0.0) or 0.0),
                "min_agreeing_model_edge": float(getattr(opportunity, "min_agreeing_model_edge", 0.0) or 0.0),
                "reason": reason,
                "coverage_issue_type": getattr(opportunity, "coverage_issue_type", None),
                "valid_model_count": int(getattr(opportunity, "valid_model_count", 0) or 0),
                "required_model_count": int(getattr(opportunity, "required_model_count", 0) or 0),
                "provider_failures": list(getattr(opportunity, "provider_failures", None) or []),
                "provider_failure_details": dict(getattr(opportunity, "provider_failure_details", None) or {}) or None,
                "degraded_reason": getattr(opportunity, "degraded_reason", None),
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
