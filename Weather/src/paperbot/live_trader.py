from __future__ import annotations

import json
import os
import secrets
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    event_slug: str | None = None
    token_id: str | None = None
    client_order_id: str | None = None
    nonce: int | None = None
    submission_fingerprint: str | None = None
    exchange_order_id: str | None = None
    order_status: str = "unknown"
    accepted: bool = False
    filled_shares: float = 0.0
    avg_fill_price_cents: float | None = None
    fills: list[dict[str, Any]] = field(default_factory=list)

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
            "event_slug": self.event_slug,
            "token_id": self.token_id,
            "client_order_id": self.client_order_id,
            "nonce": self.nonce,
            "submission_fingerprint": self.submission_fingerprint,
            "exchange_order_id": self.exchange_order_id,
            "order_status": self.order_status,
            "accepted": self.accepted,
            "filled_shares": self.filled_shares,
            "avg_fill_price_cents": self.avg_fill_price_cents,
            "fills": self.fills,
        }


def _sanitize_open_orders(orders: list[dict[str, Any]]) -> dict[str, Any]:
    summary_orders: list[dict[str, Any]] = []
    for order in orders[:10]:
        summary_orders.append(
            {
                "id": _mask_identifier(_extract_order_id(order)),
                "side": order.get("side"),
                "status": _extract_order_status(order),
                "price_cents": _extract_order_price_cents(order),
                "requested_shares": _safe_float(order.get("size")),
                "filled_shares": _safe_float(order.get("filledSize") or order.get("filled_size")),
            }
        )
    return {
        "open_orders_count": len(orders),
        "open_orders_preview": summary_orders,
    }


def _sanitize_fills(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized: list[dict[str, Any]] = []
    for trade in trades[:20]:
        sanitized.append(
            {
                "id": _mask_identifier(str(trade.get("id") or trade.get("tradeID") or "")),
                "order_id": _mask_identifier(str(
                    trade.get("orderID")
                    or trade.get("order_id")
                    or trade.get("maker_order_id")
                    or trade.get("taker_order_id")
                    or ""
                )),
                "price_cents": _extract_trade_price_cents(trade),
                "share_size": _extract_trade_shares(trade),
                "timestamp": _extract_trade_timestamp_iso(trade),
            }
        )
    return sanitized


def _sanitize_response_payload(response: Any) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {"raw": _sanitize_error_text(response)}

    safe: dict[str, Any] = {}
    for key in ("success", "errorMsg", "status", "orderID", "orderId", "id", "market", "asset_id"):
        if key in response:
            value = response.get(key)
            if key in {"orderID", "orderId", "id", "asset_id"}:
                safe[key] = _mask_identifier(value)
            elif key == "errorMsg":
                safe[key] = _sanitize_error_text(value)
            else:
                safe[key] = value
    if "open_orders" in response and isinstance(response["open_orders"], list):
        safe.update(_sanitize_open_orders(response["open_orders"]))
    if not safe:
        safe["keys"] = sorted(response.keys())
    return safe


def _build_open_order_params(token_id: str) -> Any:
    try:
        from py_clob_client.clob_types import OpenOrderParams

        return OpenOrderParams(asset_id=token_id)
    except Exception:
        return token_id


def _client_get_orders(client: Any, token_id: str) -> list[dict[str, Any]]:
    request_payload = _build_open_order_params(token_id)
    call_patterns = (
        (request_payload,),
        (),
    )
    for args in call_patterns:
        try:
            payload = client.get_orders(*args)
        except TypeError:
            continue
        except Exception:
            return []
        if isinstance(payload, list):
            return payload
    return []


def _build_trade_params(maker_address: str, token_id: str) -> Any:
    try:
        from py_clob_client.clob_types import TradeParams

        return TradeParams(maker_address=maker_address, asset_id=token_id)
    except Exception:
        return {"maker_address": maker_address, "asset_id": token_id}


def _client_get_trades(client: Any, maker_address: str, token_id: str) -> list[dict[str, Any]]:
    request_payload = _build_trade_params(maker_address, token_id)
    call_patterns = (
        (request_payload,),
        (),
    )
    for args in call_patterns:
        try:
            payload = client.get_trades(*args)
        except TypeError:
            continue
        except Exception:
            return []
        if isinstance(payload, list):
            return payload
    return []


def _order_side_for_plan(plan: OrderPlan) -> str:
    return "BUY"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _mask_identifier(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) <= 8:
        return "***"
    return f"{text[:4]}...{text[-4:]}"


def _mask_wallet_address(value: Any) -> str | None:
    masked = _mask_identifier(value)
    return masked


def _sanitize_error_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    redacted = text
    for env_key in (
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_API_PASSPHRASE",
        "POLYMARKET_FUNDER",
    ):
        secret = os.getenv(env_key, "").strip()
        if secret:
            redacted = redacted.replace(secret, f"<redacted:{env_key.lower()}>")
    return redacted[:240]


def _build_client():
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        from py_clob_client.constants import POLYGON
        from py_clob_client.signer import Signer
    except Exception as exc:
        return None, f"py-clob-client unavailable: {exc}"

    host = os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")
    chain_id = int(os.getenv("POLYMARKET_CHAIN_ID", str(POLYGON)))
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    api_key = os.getenv("POLYMARKET_API_KEY", "").strip()
    api_secret = os.getenv("POLYMARKET_API_SECRET", "").strip()
    api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()
    funder = _resolve_funder_address()
    signature_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))

    if not private_key:
        return None, "POLYMARKET_PRIVATE_KEY not configured"

    try:
        signer_address = Signer(private_key, chain_id).address()
    except Exception as exc:
        return None, f"invalid signer setup: {exc}"

    signature_type = _resolve_signature_type(
        configured_signature_type=signature_type,
        signer_address=signer_address,
        funder=funder,
    )

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


def _resolve_funder_address() -> str | None:
    for env_key in ("POLYMARKET_FUNDER", "WEATHER_PUBLIC_WALLET_ADDRESS", "POLYMARKET_PUBLIC_ADDRESS"):
        value = os.getenv(env_key, "").strip()
        if value:
            return value
    return None


def _resolve_signature_type(*, configured_signature_type: int, signer_address: str, funder: str | None) -> int:
    signer = str(signer_address or "").strip().lower()
    funded = str(funder or "").strip().lower()
    if configured_signature_type in {1, 2}:
        return configured_signature_type
    if funded and signer and funded != signer:
        # Polymarket proxy/Magic wallets need signature_type=1.
        # Browser-wallet proxy users can still override explicitly with 2.
        return 1
    return configured_signature_type


def _parse_token_balance(value: Any, decimals: int = 6) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if "." in text:
            return float(text)
        raw = int(text)
        return raw / float(10**decimals)
    except (TypeError, ValueError):
        return None


def _extract_allowance_usd(value: Any, decimals: int = 6) -> float | None:
    if value is None:
        return None
    numeric = _parse_token_balance(value, decimals=decimals)
    if numeric is not None:
        return numeric
    if isinstance(value, dict):
        candidates = [
            _extract_allowance_usd(item, decimals=decimals)
            for item in value.values()
        ]
    elif isinstance(value, (list, tuple)):
        candidates = [
            _extract_allowance_usd(item, decimals=decimals)
            for item in value
        ]
    else:
        return None
    valid = [item for item in candidates if item is not None]
    if not valid:
        return None
    return max(valid)


def get_account_snapshot() -> dict[str, Any]:
    client, client_error = _build_client()
    if client is None:
        return {"ok": False, "error": client_error}

    try:
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

        decimals = int(os.getenv("POLYMARKET_COLLATERAL_DECIMALS", "6"))
        balance_payload = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        raw_balance = balance_payload.get("balance") if isinstance(balance_payload, dict) else None
        allowances = balance_payload.get("allowances", {}) if isinstance(balance_payload, dict) else {}
        allowance_usd = _extract_allowance_usd(allowances, decimals=decimals)
        if allowance_usd is None and allowances == {}:
            allowance_usd = 0.0
        return {
            "ok": True,
            "wallet_address": client.signer.address(),
            "wallet_address_masked": _mask_wallet_address(client.signer.address()),
            "trading_address": _resolve_funder_address() or client.signer.address(),
            "trading_address_masked": _mask_wallet_address(_resolve_funder_address() or client.signer.address()),
            "collateral_balance_raw": raw_balance,
            "collateral_balance_usd": _parse_token_balance(raw_balance, decimals=decimals),
            "collateral_allowance_usd": allowance_usd,
            "allowances": allowances,
        }
    except Exception as exc:
        return {"ok": False, "error": _sanitize_error_text(exc)}


def _extract_order_id(order: dict[str, Any]) -> str | None:
    for key in ("id", "orderID", "order_id"):
        value = order.get(key)
        if value:
            return str(value)
    return None


def _extract_order_status(order: dict[str, Any]) -> str:
    for key in ("status", "state", "orderStatus"):
        value = order.get(key)
        if value:
            return str(value).lower()
    return "unknown"


def _build_submission_fingerprint(token_id: str | None, side: str, price_cents: float, share_size: float) -> str:
    return f"{token_id or ''}|{side.upper()}|{round(price_cents,4):.4f}|{round(share_size,4):.4f}"


def _generate_submission_nonce() -> int:
    base = time.time_ns() & ((1 << 48) - 1)
    random_bits = secrets.randbits(16)
    return (base << 16) | random_bits


def _build_submission_identity(signed_order: Any) -> dict[str, Any]:
    try:
        order_dict = signed_order.dict()
    except Exception:
        return {}
    return {
        "maker": str(order_dict.get("maker") or ""),
        "signer": str(order_dict.get("signer") or ""),
        "token_id": str(order_dict.get("tokenId") or order_dict.get("token_id") or ""),
        "maker_amount": str(order_dict.get("makerAmount") or order_dict.get("maker_amount") or ""),
        "taker_amount": str(order_dict.get("takerAmount") or order_dict.get("taker_amount") or ""),
        "side": str(order_dict.get("side") or "").upper(),
        "nonce": str(order_dict.get("nonce") or ""),
        "salt": str(order_dict.get("salt") or ""),
    }


def _load_submission_identity(raw_payload: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if isinstance(raw_payload, str) and raw_payload.strip():
        try:
            payload = dict(json.loads(raw_payload))
        except Exception:
            payload = {}
    elif isinstance(raw_payload, dict):
        payload = raw_payload
    submission = payload.get("submission")
    return dict(submission) if isinstance(submission, dict) else {}


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


def _extract_trade_price_cents(trade: dict[str, Any]) -> float | None:
    for key in ("price", "price_paid", "execution_price"):
        value = trade.get(key)
        numeric = _safe_float(value)
        if numeric is None:
            continue
        return numeric * 100.0 if numeric <= 1.0 else numeric
    return None


def _extract_trade_shares(trade: dict[str, Any]) -> float | None:
    for key in ("size", "amount", "shares", "matched_amount"):
        numeric = _safe_float(trade.get(key))
        if numeric is not None:
            return numeric
    return None


def _extract_trade_timestamp_iso(trade: dict[str, Any]) -> str | None:
    for key in ("timestamp", "created_at", "time", "matched_at"):
        value = trade.get(key)
        if not value:
            continue
        if isinstance(value, (int, float)):
            scale = 1000.0 if value > 10_000_000_000 else 1.0
            return datetime.fromtimestamp(float(value) / scale, tz=timezone.utc).isoformat()
        return str(value)
    return None


def _extract_order_nonce(order: dict[str, Any]) -> str | None:
    value = order.get("nonce")
    if value is None or value == "":
        return None
    return str(value)


def _extract_order_salt(order: dict[str, Any]) -> str | None:
    value = order.get("salt")
    if value is None or value == "":
        return None
    return str(value)


def _extract_order_identity(order: dict[str, Any]) -> dict[str, str]:
    return {
        "maker": str(order.get("maker") or ""),
        "signer": str(order.get("signer") or ""),
        "token_id": str(order.get("tokenId") or order.get("token_id") or ""),
        "maker_amount": str(order.get("makerAmount") or order.get("maker_amount") or ""),
        "taker_amount": str(order.get("takerAmount") or order.get("taker_amount") or ""),
        "side": str(order.get("side") or "").upper(),
        "nonce": str(order.get("nonce") or ""),
        "salt": str(order.get("salt") or ""),
    }


def _submission_identity_matches(order: dict[str, Any], identity: dict[str, Any]) -> bool:
    if not identity:
        return False
    candidate = _extract_order_identity(order)
    required = ("token_id", "maker_amount", "taker_amount", "side")
    if any(not identity.get(field) or not candidate.get(field) for field in required):
        return False
    if any(identity[field] != candidate[field] for field in required):
        return False
    for optional in ("maker", "signer"):
        if identity.get(optional) and candidate.get(optional) and identity[optional] != candidate[optional]:
            return False
    if identity.get("nonce") and candidate.get("nonce") and identity["nonce"] != candidate["nonce"]:
        return False
    if identity.get("salt") and candidate.get("salt") and identity["salt"] != candidate["salt"]:
        return False
    return True


def _match_open_order_candidate(
    client: Any,
    *,
    token_id: str,
    order_side: str,
    price_cents: float,
    share_size: float,
    submission_identity: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    orders = _client_get_orders(client, token_id)
    if not orders:
        return None
    matches: list[dict[str, Any]] = []
    identity_matches: list[dict[str, Any]] = []
    nonce_matches: list[dict[str, Any]] = []
    for order in orders:
        if str(order.get("side", "")).upper() != order_side:
            continue
        if submission_identity and _submission_identity_matches(order, submission_identity):
            identity_matches.append(order)
            continue
        candidate = _extract_order_identity(order)
        if (
            submission_identity
            and submission_identity.get("token_id")
            and submission_identity.get("nonce")
            and candidate.get("token_id") == submission_identity.get("token_id")
            and candidate.get("side") == str(submission_identity.get("side") or "").upper()
            and candidate.get("nonce") == str(submission_identity.get("nonce") or "")
        ):
            nonce_matches.append(order)
            continue
        order_price_cents = _extract_order_price_cents(order)
        order_size = _safe_float(order.get("size"))
        if order_price_cents is None or order_size is None:
            continue
        if abs(order_price_cents - price_cents) > 0.0001:
            continue
        if abs(order_size - share_size) > 0.0001:
            continue
        matches.append(order)
    if submission_identity:
        if len(identity_matches) == 1:
            return identity_matches[0]
        if len(nonce_matches) == 1:
            return nonce_matches[0]
        return None
    if len(matches) == 1:
        return matches[0]
    return None


def _list_same_side_open_orders(client: Any, token_id: str, order_side: str) -> list[dict[str, Any]]:
    orders = _client_get_orders(client, token_id)
    return [order for order in orders if str(order.get("side", "")).upper() == order_side]


def _confirm_cancelled_orders(
    client: Any,
    *,
    token_id: str,
    order_side: str,
    cancelled_ids: list[str],
    attempts: int = 4,
    wait_seconds: float = 0.35,
) -> tuple[bool, list[dict[str, Any]]]:
    remaining: list[dict[str, Any]] = []
    cancelled_set = {item for item in cancelled_ids if item}
    if not cancelled_set:
        return True, []
    for attempt in range(attempts):
        current = _list_same_side_open_orders(client, token_id, order_side)
        remaining = [order for order in current if (_extract_order_id(order) or "") in cancelled_set]
        if not remaining:
            return True, []
        if attempt < attempts - 1:
            time.sleep(wait_seconds)
    return False, remaining


def _parse_timestamp(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value) / (1000.0 if float(value) > 10_000_000_000 else 1.0)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _aggregate_fills(trades: list[dict[str, Any]]) -> tuple[float, float | None]:
    total_shares = 0.0
    total_notional = 0.0
    for trade in trades:
        shares = _extract_trade_shares(trade)
        price_cents = _extract_trade_price_cents(trade)
        if shares is None or price_cents is None:
            continue
        total_shares += shares
        total_notional += shares * price_cents
    avg_price_cents = (total_notional / total_shares) if total_shares > 0 else None
    return round(total_shares, 4), (round(avg_price_cents, 4) if avg_price_cents is not None else None)


def _fetch_order_details(client: Any, exchange_order_id: str | None) -> dict[str, Any]:
    if not exchange_order_id:
        return {}
    try:
        payload = client.get_order(exchange_order_id)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _fetch_candidate_trades(client: Any, token_id: str, maker_candidates: list[str]) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for maker_address in maker_candidates:
        if not maker_address:
            continue
        payload = _client_get_trades(client, maker_address, token_id)
        for item in payload:
            if not isinstance(item, dict):
                continue
            trade_id = str(item.get("id") or item.get("tradeID") or uuid.uuid4().hex)
            if trade_id in seen_ids:
                continue
            seen_ids.add(trade_id)
            trades.append(item)
    return trades


def _match_execution_trades(
    trades: list[dict[str, Any]],
    *,
    exchange_order_id: str | None,
    submitted_at: float,
) -> list[dict[str, Any]]:
    if not exchange_order_id:
        return []
    matched: list[dict[str, Any]] = []
    for trade in trades:
        order_candidates = {
            str(trade.get("orderID") or ""),
            str(trade.get("order_id") or ""),
            str(trade.get("maker_order_id") or ""),
            str(trade.get("taker_order_id") or ""),
        }
        if exchange_order_id in order_candidates:
            matched.append(trade)
    return matched


def _reconcile_submission(
    client: Any,
    *,
    token_id: str,
    submitted_at: float,
    exchange_order_id: str | None,
    order_side: str,
    price_cents: float,
    share_size: float,
    submission_identity: dict[str, Any] | None = None,
) -> tuple[str, str | None, float, float | None, dict[str, Any], list[dict[str, Any]]]:
    recovered_order_id = exchange_order_id
    if not recovered_order_id:
        candidate = _match_open_order_candidate(
            client,
            token_id=token_id,
            order_side=order_side,
            price_cents=price_cents,
            share_size=share_size,
            submission_identity=submission_identity,
        )
        recovered_order_id = _extract_order_id(candidate) if candidate is not None else None
    order_payload = _fetch_order_details(client, recovered_order_id)
    maker_candidates = [str(client.get_address())]
    funder = os.getenv("POLYMARKET_FUNDER", "").strip()
    if funder:
        maker_candidates.append(funder)
    candidate_trades = _fetch_candidate_trades(client, token_id, maker_candidates)
    matched_trades = _match_execution_trades(
        candidate_trades,
        exchange_order_id=recovered_order_id,
        submitted_at=submitted_at,
    )
    filled_shares, avg_fill_price_cents = _aggregate_fills(matched_trades)
    order_status = _extract_order_status(order_payload)
    if filled_shares > 0 and order_status in {"filled"}:
        final_status = "filled"
    elif filled_shares > 0:
        final_status = "partial_fill"
    elif order_status in {"live", "open", "pending", "unfilled"}:
        final_status = "resting"
    elif order_status not in {"unknown", ""}:
        final_status = order_status
    else:
        final_status = "accepted" if recovered_order_id else "submission_unconfirmed"
    return final_status, recovered_order_id, filled_shares, avg_fill_price_cents, order_payload, matched_trades


def _recover_post_order_failure(
    client: Any,
    *,
    plan: OrderPlan,
    client_order_id: str,
    nonce: int | None,
    submission_fingerprint: str,
    submission_identity: dict[str, Any],
    submitted_at: float,
    original_error: Exception,
) -> ExecutionResult:
    order_status, reconciled_order_id, filled_shares, avg_fill_price_cents, order_payload, matched_trades = _reconcile_submission(
        client,
        token_id=plan.token_id,
        submitted_at=submitted_at,
        exchange_order_id=None,
        order_side=_order_side_for_plan(plan),
        price_cents=plan.limit_price_cents,
        share_size=plan.share_size,
        submission_identity=submission_identity,
    )
    accepted = order_status in {"accepted", "resting", "partial_fill", "filled", "live", "open"}
    success = accepted or filled_shares > 0
    error_label = "post_order_response_lost" if success else str(original_error)
    return ExecutionResult(
        mode="live",
        success=success,
        market_slug=plan.market_slug,
        side=plan.side,
        price_cents=plan.limit_price_cents,
        share_size=plan.share_size,
        response={
            "submission": submission_identity,
            "recovery": {
                "post_error": str(original_error),
                "reconciled": success,
            },
            "order": _sanitize_response_payload(order_payload),
            "fills": _sanitize_fills(matched_trades),
        },
        error=None if success else error_label,
        event_slug=plan.event_slug,
        token_id=plan.token_id,
        client_order_id=client_order_id,
        nonce=nonce,
        submission_fingerprint=submission_fingerprint,
        exchange_order_id=reconciled_order_id,
        order_status=order_status,
        accepted=accepted,
        filled_shares=filled_shares,
        avg_fill_price_cents=avg_fill_price_cents,
        fills=_sanitize_fills(matched_trades),
    )


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
                "valid": plan.valid,
                "invalid_reason": plan.invalid_reason,
            },
            event_slug=plan.event_slug,
            token_id=plan.token_id,
            order_status="dry_run",
        )

    if not plan.valid:
        return ExecutionResult(
            mode="live",
            success=False,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={},
            error=plan.invalid_reason or "invalid_plan",
            event_slug=plan.event_slug,
            token_id=plan.token_id,
            order_status="rejected",
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
            event_slug=plan.event_slug,
            token_id=plan.token_id,
            order_status="rejected",
        )

    snapshot = get_account_snapshot()
    balance_usd = snapshot.get("collateral_balance_usd") if snapshot.get("ok") else None
    allowance_usd = snapshot.get("collateral_allowance_usd") if snapshot.get("ok") else None
    if balance_usd is not None and float(balance_usd) + 1e-9 < float(plan.stake_usd):
        return ExecutionResult(
            mode="live",
            success=False,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={
                "wallet_address_masked": snapshot.get("wallet_address_masked"),
                "trading_address_masked": snapshot.get("trading_address_masked"),
                "collateral_balance_usd": balance_usd,
                "collateral_allowance_usd": allowance_usd,
            },
            error="insufficient_collateral_balance",
            event_slug=plan.event_slug,
            token_id=plan.token_id,
            order_status="rejected",
        )
    if allowance_usd is None:
        return ExecutionResult(
            mode="live",
            success=False,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={
                "collateral_balance_usd": balance_usd,
                "collateral_allowance_usd": allowance_usd,
                "allowances": snapshot.get("allowances", {}),
            },
            error="collateral_allowance_unreadable",
            event_slug=plan.event_slug,
            token_id=plan.token_id,
            order_status="rejected",
        )
    if float(allowance_usd) + 1e-9 < float(plan.stake_usd):
        return ExecutionResult(
            mode="live",
            success=False,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={
                "collateral_balance_usd": balance_usd,
                "collateral_allowance_usd": allowance_usd,
                "allowances": snapshot.get("allowances", {}),
            },
            error="insufficient_collateral_allowance",
            event_slug=plan.event_slug,
            token_id=plan.token_id,
            order_status="rejected",
        )

    client_order_id = uuid.uuid4().hex
    submitted_at = time.time()
    order_side = _order_side_for_plan(plan)
    nonce = _generate_submission_nonce()
    submission_fingerprint = _build_submission_fingerprint(
        plan.token_id,
        order_side,
        plan.limit_price_cents,
        plan.share_size,
    )
    signed_order = None
    submission_identity: dict[str, Any] = {}
    try:
        same_side_orders = _list_same_side_open_orders(client, plan.token_id, order_side)
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
                event_slug=plan.event_slug,
                token_id=plan.token_id,
                client_order_id=client_order_id,
                order_status="rejected",
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
                        event_slug=plan.event_slug,
                        token_id=plan.token_id,
                        client_order_id=client_order_id,
                        order_status="rejected",
                    )
            if to_cancel:
                client.cancel_orders(to_cancel)
                cancelled_ok, remaining_orders = _confirm_cancelled_orders(
                    client,
                    token_id=plan.token_id,
                    order_side=order_side,
                    cancelled_ids=to_cancel,
                )
                if not cancelled_ok:
                    return ExecutionResult(
                        mode="live",
                        success=False,
                        market_slug=plan.market_slug,
                        side=plan.side,
                        price_cents=plan.limit_price_cents,
                        share_size=plan.share_size,
                        response=_sanitize_open_orders(remaining_orders),
                        error="cancel_replace_not_confirmed",
                        event_slug=plan.event_slug,
                        token_id=plan.token_id,
                        client_order_id=client_order_id,
                        order_status="rejected",
                    )

        from py_clob_client.clob_types import OrderArgs, OrderType

        order_args = OrderArgs(
            token_id=plan.token_id,
            price=round(plan.limit_price_cents / 100.0, 6),
            size=round(plan.share_size, 4),
            side=order_side,
            nonce=nonce,
        )
        signed_order = client.create_order(order_args)
        submission_identity = _build_submission_identity(signed_order)
        response = client.post_order(signed_order, OrderType.GTC, post_only=True)
        exchange_order_id = str(
            response.get("orderID") or response.get("orderId") or response.get("id") or ""
        ) or None
        order_status, reconciled_order_id, filled_shares, avg_fill_price_cents, order_payload, matched_trades = _reconcile_submission(
            client,
            token_id=plan.token_id,
            submitted_at=submitted_at,
            exchange_order_id=exchange_order_id,
            order_side=order_side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            submission_identity=submission_identity,
        )
        accepted = order_status in {"accepted", "resting", "partial_fill", "filled", "live", "open"}
        success = accepted or filled_shares > 0
        return ExecutionResult(
            mode="live",
            success=success,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={
                "submission": submission_identity,
                "post": _sanitize_response_payload(response),
                "order": _sanitize_response_payload(order_payload),
                "fills": _sanitize_fills(matched_trades),
            },
            error=None if success else "order_not_accepted",
            event_slug=plan.event_slug,
            token_id=plan.token_id,
            client_order_id=client_order_id,
            nonce=nonce,
            submission_fingerprint=submission_fingerprint,
            exchange_order_id=reconciled_order_id or exchange_order_id,
            order_status=order_status,
            accepted=accepted,
            filled_shares=filled_shares,
            avg_fill_price_cents=avg_fill_price_cents,
            fills=_sanitize_fills(matched_trades),
        )
    except Exception as exc:
        if signed_order is not None and not submission_identity:
            submission_identity = _build_submission_identity(signed_order)
        if signed_order is not None and submission_identity:
            return _recover_post_order_failure(
                client,
                plan=plan,
                client_order_id=client_order_id,
                nonce=nonce,
                submission_fingerprint=submission_fingerprint,
                submission_identity=submission_identity,
                submitted_at=submitted_at,
                original_error=exc,
            )
        return ExecutionResult(
            mode="live",
            success=False,
            market_slug=plan.market_slug,
            side=plan.side,
            price_cents=plan.limit_price_cents,
            share_size=plan.share_size,
            response={"submission": submission_identity} if submission_identity else {},
            error=str(exc),
            event_slug=plan.event_slug,
            token_id=plan.token_id,
            client_order_id=client_order_id,
            nonce=nonce,
            submission_fingerprint=submission_fingerprint,
            order_status="submission_unconfirmed" if submission_identity else "unknown",
        )


def sync_live_exchange_state(storage: Any) -> dict[str, Any]:
    client, client_error = _build_client()
    if client is None:
        return {"ok": False, "error": client_error, "checked_orders": 0, "updated_orders": 0}

    statuses = ("unknown", "accepted", "resting", "partial_fill", "live", "open", "submission_unconfirmed")
    pending_orders: list[dict[str, Any]] = []
    offset = 0
    page_size = 250
    while True:
        page = storage.list_live_orders(
            statuses=statuses,
            limit=page_size,
            offset=offset,
        )
        if not page:
            break
        pending_orders.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    updated_orders = 0
    for order in pending_orders:
        token_id = str(order.get("token_id") or "")
        if not token_id:
            continue
        order_status, reconciled_order_id, filled_shares, avg_fill_price_cents, order_payload, matched_trades = _reconcile_submission(
            client,
            token_id=token_id,
            submitted_at=_parse_timestamp(order.get("first_seen_at")) or time.time(),
            exchange_order_id=str(order.get("exchange_order_id") or "") or None,
            order_side=str(order.get("side") or "BUY"),
            price_cents=float(order.get("requested_price_cents") or 0.0),
            share_size=float(order.get("requested_shares") or 0.0),
            submission_identity=_load_submission_identity(order.get("raw_response_json")),
        )
        storage.sync_live_order_state(
            client_order_id=str(order["client_order_id"]),
            exchange_order_id=reconciled_order_id or str(order.get("exchange_order_id") or "") or None,
            status=order_status,
            accepted=order_status in {"accepted", "resting", "partial_fill", "filled", "live", "open"},
            filled_shares=filled_shares,
            avg_fill_price_cents=avg_fill_price_cents,
            response={
                "submission": _load_submission_identity(order.get("raw_response_json")),
                "order": _sanitize_response_payload(order_payload),
                "fills": _sanitize_fills(matched_trades),
            },
            fills=_sanitize_fills(matched_trades),
            synced_at=datetime.now(timezone.utc).isoformat(),
        )
        updated_orders += 1

    return {"ok": True, "checked_orders": len(pending_orders), "updated_orders": updated_orders}
