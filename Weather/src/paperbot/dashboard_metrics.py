from __future__ import annotations

from typing import Any

import pandas as pd


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _cost_usd(row: pd.Series) -> float | None:
    initial_value = _to_float(row.get("initialValue"))
    if initial_value is not None:
        return initial_value
    stake_usd = _to_float(row.get("stake_usd"))
    if stake_usd is not None:
        return stake_usd
    shares = _to_float(row.get("size"))
    if shares is None:
        shares = _to_float(row.get("share_size"))
    avg_price = _to_float(row.get("avgPrice"))
    if shares is not None and avg_price is not None:
        return shares * avg_price
    entry_price_cents = _to_float(row.get("entry_price_cents"))
    if shares is not None and entry_price_cents is not None:
        return shares * (entry_price_cents / 100.0)
    return None


def _value_usd(row: pd.Series) -> float | None:
    current_value = _to_float(row.get("currentValue"))
    if current_value is not None:
        return current_value
    payout_usd = _to_float(row.get("payout_usd"))
    if payout_usd is not None:
        return payout_usd
    return _cost_usd(row)


def _open_pnl_usd(row: pd.Series) -> float | None:
    value_usd = _value_usd(row)
    cost_usd = _cost_usd(row)
    if value_usd is not None and cost_usd is not None:
        return value_usd - cost_usd
    return _to_float(row.get("cashPnl"))


def normalize_open_positions(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        result = frame.copy()
        result["dashboard_cost_usd"] = pd.Series(dtype="float64")
        result["dashboard_value_usd"] = pd.Series(dtype="float64")
        result["dashboard_open_pnl_usd"] = pd.Series(dtype="float64")
        result["dashboard_open_pnl_pct"] = pd.Series(dtype="float64")
        return result

    result = frame.copy()
    result["dashboard_cost_usd"] = result.apply(_cost_usd, axis=1)
    result["dashboard_value_usd"] = result.apply(_value_usd, axis=1)
    result["dashboard_open_pnl_usd"] = result.apply(_open_pnl_usd, axis=1)
    cost_series = pd.to_numeric(result["dashboard_cost_usd"], errors="coerce")
    pnl_series = pd.to_numeric(result["dashboard_open_pnl_usd"], errors="coerce")
    result["dashboard_open_pnl_pct"] = ((pnl_series / cost_series.replace({0.0: pd.NA})) * 100.0).astype("float64")
    return result


def compute_open_position_totals(frame: pd.DataFrame) -> tuple[float, float]:
    if frame.empty:
        return 0.0, 0.0
    value_usd = float(pd.to_numeric(frame.get("dashboard_value_usd"), errors="coerce").fillna(0.0).sum())
    open_pnl_usd = float(pd.to_numeric(frame.get("dashboard_open_pnl_usd"), errors="coerce").fillna(0.0).sum())
    return value_usd, open_pnl_usd


def build_live_snapshot_curve(snapshot_rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(snapshot_rows)
    if frame.empty:
        return frame
    frame["captured_at"] = pd.to_datetime(frame["captured_at"], errors="coerce")
    frame = frame[frame["captured_at"].notna()].sort_values("captured_at").copy()
    if frame.empty:
        return frame

    net_worth = pd.to_numeric(frame.get("total_net_worth_usd"), errors="coerce")
    stored_open_pnl = pd.to_numeric(frame.get("total_open_pnl_usd"), errors="coerce")
    frame["pnl_curve_usd"] = stored_open_pnl
    frame["pnl_curve_label"] = "PnL aberto"

    valid_net_worth = net_worth.dropna()
    if not valid_net_worth.empty:
        baseline = float(valid_net_worth.iloc[0])
        frame["pnl_curve_usd"] = net_worth - baseline
        frame["pnl_curve_label"] = "PnL da curva"
    elif stored_open_pnl.notna().any():
        frame["pnl_curve_usd"] = stored_open_pnl
        frame["pnl_curve_label"] = "PnL aberto"
    else:
        frame["pnl_curve_usd"] = 0.0
        frame["pnl_curve_label"] = "PnL"

    return frame
