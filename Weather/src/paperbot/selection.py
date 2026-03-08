from __future__ import annotations


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
        if require_token and not getattr(opportunity, "token_id", None):
            continue
        price = float(getattr(opportunity, "price_cents", 0.0))
        if min_price_cents is not None and price < min_price_cents:
            continue
        if max_price_cents is not None and price > max_price_cents:
            continue
        if max_spread is not None and float(getattr(opportunity, "spread", 0.0)) > max_spread:
            continue
        event_slug = getattr(opportunity, "event_slug", "")
        if max_orders_per_event > 0 and per_event.get(event_slug, 0) >= max_orders_per_event:
            continue
        if max_share_size is not None and plans_by_slug is not None:
            key = f"{event_slug}|{getattr(opportunity, 'market_slug', '')}|{getattr(opportunity, 'side', '')}"
            plan = plans_by_slug.get(key)
            if plan is not None and float(getattr(plan, "share_size", 0.0)) > max_share_size:
                continue
        filtered.append(opportunity)
        per_event[event_slug] = per_event.get(event_slug, 0) + 1
    return filtered
