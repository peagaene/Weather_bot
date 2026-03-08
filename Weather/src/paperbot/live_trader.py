from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from .polymarket_live import OrderPlan


@dataclass
class ExecutionResult:
    mode: str
    success: bool
    market_slug: str
    side: str
    price_cents: float
    share_size: float
    response: dict[str, Any]
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "success": self.success,
            "market_slug": self.market_slug,
            "side": self.side,
            "price_cents": self.price_cents,
            "share_size": self.share_size,
            "response": self.response,
            "error": self.error,
        }


def _sanitize_open_orders(orders: list[dict[str, Any]]) -> dict[str, Any]:
    summary_orders: list[dict[str, Any]] = []
    for order in orders[:10]:
        summary_orders.append(
            {
                "id": _extract_order_id(order),
                "side": order.get("side"),
                "status": order.get("status"),
                "price_cents": _extract_order_price_cents(order),
                "size": order.get("size"),
            }
        )
    return {
        "open_orders_count": len(orders),
        "open_orders_preview": summary_orders,
    }


def _sanitize_response_payload(response: Any) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {"raw": str(response)}

    safe: dict[str, Any] = {}
    for key in ("success", "errorMsg", "status", "orderID", "orderId", "id", "market", "asset_id"):
        if key in response:
            safe[key] = response.get(key)
    if "open_orders" in response and isinstance(response["open_orders"], list):
        safe.update(_sanitize_open_orders(response["open_orders"]))
    if not safe:
        safe["keys"] = sorted(response.keys())
    return safe


def _build_client():
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        from py_clob_client.constants import POLYGON
    except Exception as exc:
        return None, f"py-clob-client unavailable: {exc}"

    host = os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")
    chain_id = int(os.getenv("POLYMARKET_CHAIN_ID", str(POLYGON)))
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    api_key = os.getenv("POLYMARKET_API_KEY", "").strip()
    api_secret = os.getenv("POLYMARKET_API_SECRET", "").strip()
    api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()
    funder = os.getenv("POLYMARKET_FUNDER", "").strip() or None
    signature_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))

    if not private_key:
        return None, "POLYMARKET_PRIVATE_KEY not configured"

    creds = None
    if api_key and api_secret and api_passphrase:
        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)

    try:
        client = ClobClient(
            host,
            chain_id=chain_id,
            key=private_key,
            creds=creds,
            signature_type=signature_type,
            funder=funder,
        )
        if creds is None:
            client.set_api_creds(client.create_or_derive_api_creds())
        return client, None
    except Exception as exc:
        return None, str(exc)


def _extract_order_id(order: dict[str, Any]) -> str | None:
    for key in ("id", "orderID", "order_id"):
        value = order.get(key)
        if value:
            return str(value)
    return None


def _extract_order_price_cents(order: dict[str, Any]) -> float | None:
    for key in ("price", "originalPrice"):
        value = order.get(key)
        try:
            if value is None:
                continue
            numeric = float(value)
            return numeric * 100.0 if numeric <= 1.0 else numeric
        except (TypeError, ValueError):
            continue
    return None


def execute_order_plan(
    plan: OrderPlan,
    *,
    live: bool = False,
    replace_open_orders: bool = False,
    replace_price_threshold_cents: float = 1.0,
) -> ExecutionResult:
    if not live:
        return ExecutionResult(
            mode="dry-run",
            success=True,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={
                "message": "dry-run only",
                "token_id": plan.token_id,
                "limit_price_cents": plan.limit_price_cents,
                "share_size": plan.share_size,
            },
        )

    if not plan.token_id:
        return ExecutionResult(
            mode="live",
            success=False,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={},
            error="missing token_id",
        )

    client, client_error = _build_client()
    if client is None:
        return ExecutionResult(
            mode="live",
            success=False,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={},
            error=client_error,
        )

    try:
        from py_clob_client.clob_types import OpenOrderParams, OrderArgs, OrderType

        existing_orders = client.get_orders(OpenOrderParams(asset_id=plan.token_id))
        same_side_orders = [
            order for order in existing_orders
            if str(order.get("side", "")).upper() == str(plan.side).upper()
        ]
        if same_side_orders and not replace_open_orders:
            return ExecutionResult(
                mode="live",
                success=False,
                market_slug=plan.market_slug,
                side=plan.side,
                price_cents=plan.limit_price_cents,
                share_size=plan.share_size,
                response=_sanitize_open_orders(same_side_orders),
                error="open_order_exists_replace_disabled",
            )
        if same_side_orders and replace_open_orders:
            to_cancel: list[str] = []
            for order in same_side_orders:
                existing_price_cents = _extract_order_price_cents(order)
                if existing_price_cents is None:
                    order_id = _extract_order_id(order)
                    if order_id:
                        to_cancel.append(order_id)
                    continue
                if abs(existing_price_cents - plan.limit_price_cents) >= replace_price_threshold_cents:
                    order_id = _extract_order_id(order)
                    if order_id:
                        to_cancel.append(order_id)
                else:
                    return ExecutionResult(
                        mode="live",
                        success=False,
                        market_slug=plan.market_slug,
                        side=plan.side,
                        price_cents=plan.limit_price_cents,
                        share_size=plan.share_size,
                        response=_sanitize_open_orders(same_side_orders),
                        error="existing_order_close_enough",
                    )
            if to_cancel:
                client.cancel_orders(to_cancel)

        order_args = OrderArgs(
            token_id=plan.token_id,
            price=round(plan.limit_price_cents / 100.0, 6),
            size=round(plan.share_size, 4),
            side=plan.side,
        )
        signed_order = client.create_order(order_args)
        response = client.post_order(signed_order, OrderType.GTC, post_only=True)
        return ExecutionResult(
            mode="live",
            success=True,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response=_sanitize_response_payload(response),
        )
    except Exception as exc:
        return ExecutionResult(
            mode="live",
            success=False,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={},
            error=str(exc),
        )
