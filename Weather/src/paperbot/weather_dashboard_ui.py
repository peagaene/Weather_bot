from __future__ import annotations

import re
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from paperbot.dashboard_metrics import (
    build_live_snapshot_curve,
    compute_open_position_totals,
    normalize_open_positions,
)
from paperbot.weather_dashboard_theme import PLOTLY_LAYOUT


DEFAULT_REFRESH_SECONDS = 30
DEFAULT_MONITOR_SECONDS = 60


def fmt_num(value: float | int | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "--"
    return f"{float(value):,.{digits}f}"


def fmt_usd(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "--"
    return f"${float(value):,.2f}"


def fmt_cents(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "--"
    return f"{float(value):.2f}c"


def fmt_percent(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "--"
    return f"{float(value):.2f}%"


def fmt_score(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "--"
    numeric = float(value)
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:.1f}"


def fmt_confidence_tier(value: str | None) -> str:
    mapping = {
        "lock": "LOCK",
        "strong": "STRONG",
        "safe": "SAFE",
        "near-safe": "NEAR-SAFE",
        "risky": "RISKY",
    }
    return mapping.get(str(value or "").strip().lower(), str(value or "--").upper())


def fmt_short_date(value: str | None) -> str:
    if not value:
        return "--"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    month_map = {
        1: "JAN",
        2: "FEV",
        3: "MAR",
        4: "ABR",
        5: "MAI",
        6: "JUN",
        7: "JUL",
        8: "AGO",
        9: "SET",
        10: "OUT",
        11: "NOV",
        12: "DEZ",
    }
    return f"{parsed.day:02d}-{month_map.get(parsed.month, '---')}"


def fmt_timestamp(value: str | None) -> str:
    if not value:
        return "--"
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return str(value)
    return parsed.strftime("%d/%m %H:%M:%S UTC")


def fmt_short_datetime(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return "--"
    return parsed.strftime("%d/%m %H:%M UTC")


def extract_bucket_label(row: pd.Series) -> str:
    title = str(row.get("title") or "").strip()
    match = re.search(r"between\s+(.+?)\s+on\s+", title, flags=re.IGNORECASE)
    if match:
        return match.group(1).replace("?F", "F").replace("Ã‚Â°F", "F").replace("Â°F", "F")
    match = re.search(r"([0-9]+Â°F\s+or\s+(?:higher|lower|below|above))", title, flags=re.IGNORECASE)
    if match:
        return match.group(1).replace("?F", "F").replace("Ã‚Â°F", "F").replace("Â°F", "F")
    match = re.search(r"([0-9]+-[0-9]+Â°F)", title, flags=re.IGNORECASE)
    if match:
        return match.group(1).replace("?F", "F").replace("Ã‚Â°F", "F").replace("Â°F", "F")
    return str(row.get("bucket") or row.get("slug") or row.get("market_slug") or "Mercado")


def build_polymarket_like_title(row: pd.Series) -> str:
    explicit_title = str(row.get("title") or "").strip()
    if explicit_title:
        return explicit_title
    city = str(row.get("city_name") or row.get("city_key") or "this city").strip().title()
    bucket = str(row.get("bucket") or "").strip().replace("Ã‚Â°F", "F").replace("Â°F", "F")
    date_value = pd.to_datetime(row.get("date_str"), errors="coerce")
    if pd.isna(date_value):
        date_label = str(row.get("date_str") or "").strip()
    else:
        date_label = f"{date_value.strftime('%B')} {date_value.day}"
    if "or " in bucket.lower():
        bucket_phrase = bucket
    elif bucket:
        bucket_phrase = f"between {bucket}"
    else:
        bucket_phrase = "within the target range"
    return f"Will the highest temperature in {city} be {bucket_phrase} on {date_label}?"


def normalize_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    minimum = float(series.min())
    maximum = float(series.max())
    if abs(maximum - minimum) < 1e-9:
        return pd.Series([0.5] * len(series), index=series.index, dtype=float)
    return (series - minimum) / (maximum - minimum)


def parse_bucket_bounds_from_label(label: str) -> tuple[float | None, float | None]:
    text = (label or "").upper().replace("Ã‚Â°F", "F").replace("Â°F", "F").strip()
    range_match = re.search(r"(\d+)\s*-\s*(\d+)\s*F", text)
    if range_match:
        return float(range_match.group(1)), float(range_match.group(2))
    above_match = re.search(r"(\d+)\s*F\s*OR\s*(HIGHER|ABOVE)", text)
    if above_match:
        return float(above_match.group(1)), None
    below_match = re.search(r"(\d+)\s*F\s*OR\s*(LOWER|BELOW)", text)
    if below_match:
        return None, float(below_match.group(1))
    return None, None


def compute_risk_label(row: pd.Series, range_info: dict | None) -> tuple[str, float]:
    risk_points = 0.0
    consensus = float(pd.to_numeric(pd.Series([row.get("consensus_score")]), errors="coerce").fillna(0.0).iloc[0])
    spread = float(pd.to_numeric(pd.Series([row.get("spread")]), errors="coerce").fillna(0.0).iloc[0])
    sigma = float(pd.to_numeric(pd.Series([row.get("sigma")]), errors="coerce").fillna(0.0).iloc[0])
    ensemble_prediction = float(
        pd.to_numeric(pd.Series([row.get("ensemble_prediction")]), errors="coerce").fillna(0.0).iloc[0]
    )
    low, high = parse_bucket_bounds_from_label(str(row.get("bucket") or row.get("title") or ""))
    if consensus < 0.45:
        risk_points += 3.0
    elif consensus < 0.55:
        risk_points += 2.0
    elif consensus < 0.65:
        risk_points += 1.0
    if spread > 4.5:
        risk_points += 2.0
    elif spread > 3.0:
        risk_points += 1.0
    if sigma > 3.5:
        risk_points += 1.5
    elif sigma > 2.5:
        risk_points += 0.75
    if low is not None and high is not None:
        width = high - low
        if width <= 1.0:
            risk_points += 2.0
        elif width <= 2.0:
            risk_points += 1.25
        distance_to_edge = min(abs(ensemble_prediction - low), abs(ensemble_prediction - high))
        if distance_to_edge < 0.35:
            risk_points += 1.5
        elif distance_to_edge < 0.75:
            risk_points += 0.75
        midpoint = (low + high) / 2.0
        if abs(ensemble_prediction - midpoint) < 0.25:
            risk_points -= 0.4
    else:
        risk_points += 0.35
    if range_info:
        min_price = pd.to_numeric(pd.Series([range_info.get("min_price_cents")]), errors="coerce").fillna(0.0).iloc[0]
        max_price = pd.to_numeric(pd.Series([range_info.get("max_price_cents")]), errors="coerce").fillna(0.0).iloc[0]
        samples = int(pd.to_numeric(pd.Series([range_info.get("samples")]), errors="coerce").fillna(0).iloc[0])
        width = max(0.0, float(max_price) - float(min_price))
        if width > 18.0:
            risk_points += 1.25
        elif width > 10.0:
            risk_points += 0.6
        if samples < 3:
            risk_points += 0.35
    risk_points = max(0.0, risk_points)
    if risk_points >= 4.5:
        return "Risky", risk_points
    if risk_points >= 2.25:
        return "Moderate", risk_points
    return "Safe", risk_points


def runs_frame(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame["generated_at"] = pd.to_datetime(frame["generated_at"], format="ISO8601", errors="coerce")
    return frame.sort_values("generated_at", ascending=False)


def positions_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["opened_at"] = pd.to_datetime(frame["opened_at"], format="ISO8601", errors="coerce")
    frame["resolved_at"] = pd.to_datetime(frame.get("resolved_at"), format="ISO8601", errors="coerce")
    frame["resultado"] = frame["pnl_usd"].apply(
        lambda value: "--" if pd.isna(value) else ("Positivo" if float(value) > 0 else ("Negativo" if float(value) < 0 else "Neutro"))
    )
    return frame.sort_values("opened_at", ascending=False)


def position_key(row: pd.Series) -> str:
    market_slug = str(row.get("market_slug") or row.get("slug") or "").strip()
    side = str(row.get("side") or row.get("outcome") or "").strip().upper()
    return f"{market_slug}|{side}"


def build_price_range_text(range_info: dict | None, current_price: float | int | None) -> str:
    if not range_info:
        return fmt_cents(current_price)
    min_price = range_info.get("min_price_cents")
    max_price = range_info.get("max_price_cents")
    if min_price is None or max_price is None:
        return fmt_cents(current_price)
    if abs(float(min_price) - float(max_price)) < 0.0001:
        return fmt_cents(min_price)
    return f"{fmt_cents(min_price)} - {fmt_cents(max_price)}"


def build_opportunities_frame(
    details: dict,
    open_positions: pd.DataFrame,
    recent_ranges: dict[str, dict] | None = None,
) -> pd.DataFrame:
    frame = pd.DataFrame(details.get("opportunities", []))
    if frame.empty:
        return frame
    plans = pd.DataFrame(details.get("order_plans", []))
    if not plans.empty:
        plan_map = {f"{row['market_slug']}|{row['side']}": row for _, row in plans.iterrows()}
        frame["stake_usd"] = frame.apply(
            lambda row: float(plan_map.get(f"{row['market_slug']}|{row['side']}", {}).get("stake_usd", 0.0)),
            axis=1,
        )
        frame["share_size"] = frame.apply(
            lambda row: float(plan_map.get(f"{row['market_slug']}|{row['side']}", {}).get("share_size", 0.0)),
            axis=1,
        )
    else:
        frame["stake_usd"] = 0.0
        frame["share_size"] = 0.0
    open_keys: set[str] = set()
    if not open_positions.empty:
        open_keys = {position_key(row) for _, row in open_positions.iterrows()}
    frame["status_posicao"] = frame.apply(
        lambda row: "Ja aberta" if f"{row['market_slug']}|{row['side']}" in open_keys else "Nova",
        axis=1,
    )
    frame["headline"] = frame.apply(build_polymarket_like_title, axis=1)
    frame["short_date"] = frame["date_str"].map(fmt_short_date)
    if "agreement_summary" not in frame.columns:
        frame["agreement_summary"] = frame.apply(
            lambda row: (
                f"{int(pd.to_numeric(pd.Series([row.get('agreement_models')]), errors='coerce').fillna(0).iloc[0])}/"
                f"{int(pd.to_numeric(pd.Series([row.get('total_models')]), errors='coerce').fillna(0).iloc[0])}"
                if int(pd.to_numeric(pd.Series([row.get('total_models')]), errors='coerce').fillna(0).iloc[0]) > 0
                else "--"
            ),
            axis=1,
        )
    if "agreeing_model_names" not in frame.columns:
        frame["agreeing_model_names"] = [[] for _ in range(len(frame))]
    frame["agreeing_models_text"] = frame["agreeing_model_names"].map(
        lambda value: ", ".join(value[:5]) + (" ..." if len(value) > 5 else "")
        if isinstance(value, list) and value
        else "--"
    )
    frame = frame.sort_values(["edge", "consensus_score"], ascending=[False, False]).reset_index(drop=True)
    recent_ranges = recent_ranges or {}
    frame["price_range_text"] = frame.apply(
        lambda row: build_price_range_text(recent_ranges.get(f"{row['market_slug']}|{row['side']}"), row.get("price_cents")),
        axis=1,
    )
    risk_values = frame.apply(
        lambda row: compute_risk_label(row, recent_ranges.get(f"{row['market_slug']}|{row['side']}")),
        axis=1,
    )
    frame["risk_label"] = risk_values.map(lambda item: item[0])
    frame["risk_score"] = risk_values.map(lambda item: item[1])
    edge_norm = normalize_series(pd.to_numeric(frame["edge"], errors="coerce").fillna(0.0))
    prob_norm = normalize_series(pd.to_numeric(frame["model_prob"], errors="coerce").fillna(0.0))
    price_value_norm = normalize_series(100.0 - pd.to_numeric(frame["price_cents"], errors="coerce").fillna(0.0))
    consensus_norm = normalize_series(pd.to_numeric(frame["consensus_score"], errors="coerce").fillna(0.0))
    agreement_norm = normalize_series(pd.to_numeric(frame["agreement_pct"], errors="coerce").fillna(0.0))
    if len(frame) == 1:
        rank_norm = pd.Series([1.0], index=frame.index, dtype=float)
    else:
        rank_norm = pd.Series(
            [(len(frame) - 1 - idx) / (len(frame) - 1) for idx in range(len(frame))],
            index=frame.index,
            dtype=float,
        )
    composite = (
        (edge_norm * 0.30)
        + (agreement_norm * 0.30)
        + (prob_norm * 0.15)
        + (price_value_norm * 0.10)
        + (consensus_norm * 0.15)
    )
    frame["opportunity_score"] = (2.0 + (8.0 * ((composite * 0.7) + (rank_norm * 0.3)))).clip(0.0, 10.0).round(1)
    return frame


def blocked_opportunities_frame(details: dict) -> pd.DataFrame:
    frame = pd.DataFrame(details.get("blocked_opportunities", []))
    if frame.empty:
        return frame
    frame["analyzed_at_text"] = frame["analyzed_at"].map(fmt_timestamp) if "analyzed_at" in frame.columns else "--"
    frame["short_date"] = frame["date_str"].map(fmt_short_date) if "date_str" in frame.columns else "--"
    for column in ("edge", "model_prob", "price_cents", "plan_share_size"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in (
        "confidence_tier",
        "risk_label",
        "reason",
        "coverage_issue_type",
        "degraded_reason",
        "plan_invalid_reason",
        "polymarket_url",
        "signal_tier",
        "min_agreeing_model_edge",
        "adversarial_score",
    ):
        if column not in frame.columns:
            frame[column] = None
    if "provider_failures" not in frame.columns:
        frame["provider_failures"] = [[] for _ in range(len(frame))]
    if "agreeing_model_names" not in frame.columns:
        frame["agreeing_model_names"] = [[] for _ in range(len(frame))]
    if "provider_failure_details" not in frame.columns:
        frame["provider_failure_details"] = [None for _ in range(len(frame))]
    if "valid_model_count" not in frame.columns:
        frame["valid_model_count"] = 0
    if "required_model_count" not in frame.columns:
        frame["required_model_count"] = 0
    if "agreement_summary" not in frame.columns:
        frame["agreement_summary"] = frame.apply(
            lambda row: (
                f"{int(pd.to_numeric(pd.Series([row.get('agreement_models')]), errors='coerce').fillna(0).iloc[0])}/"
                f"{int(pd.to_numeric(pd.Series([row.get('total_models')]), errors='coerce').fillna(0).iloc[0])}"
                if int(pd.to_numeric(pd.Series([row.get('total_models')]), errors='coerce').fillna(0).iloc[0]) > 0
                else "--"
            ),
            axis=1,
        )
    frame["agreeing_models_text"] = frame["agreeing_model_names"].map(
        lambda value: ", ".join(value[:5]) + (" ..." if len(value) > 5 else "")
        if isinstance(value, list) and value
        else "--"
    )
    frame["provider_failures_text"] = frame["provider_failures"].map(
        lambda value: ", ".join(value[:4]) + (" ..." if len(value) > 4 else "")
        if isinstance(value, list) and value
        else "--"
    )
    frame["coverage_detail"] = frame.apply(
        lambda row: (
            f"{row.get('coverage_issue_type') or '--'} | modelos "
            f"{int(pd.to_numeric(pd.Series([row.get('valid_model_count')]), errors='coerce').fillna(0).iloc[0])}/"
            f"{int(pd.to_numeric(pd.Series([row.get('required_model_count')]), errors='coerce').fillna(0).iloc[0])}"
        ),
        axis=1,
    )
    return frame.sort_values(["edge", "model_prob"], ascending=[False, False]).reset_index(drop=True)


def public_closed_positions_frame(activity_rows: list[dict]) -> pd.DataFrame:
    if not activity_rows:
        return pd.DataFrame()
    rows: list[dict] = []
    for item in activity_rows:
        activity_type = str(item.get("type") or "").upper()
        timestamp = pd.to_datetime(item.get("timestamp"), unit="s", utc=True, errors="coerce")
        value_usd = pd.to_numeric(pd.Series([item.get("usdcSize")]), errors="coerce").fillna(0.0).iloc[0]
        side = str(item.get("side") or "").strip().upper()
        if activity_type == "TRADE" and side != "SELL":
            continue
        if activity_type not in {"REDEEM", "CLAIM", "TRADE"}:
            continue
        share_size = pd.to_numeric(pd.Series([item.get("size")]), errors="coerce").fillna(0.0).iloc[0]
        settled_price = None
        if activity_type == "TRADE":
            trade_price = pd.to_numeric(pd.Series([item.get("price")]), errors="coerce").fillna(0.0).iloc[0]
            settled_price = trade_price * 100.0 if trade_price > 0 else None
        if activity_type in {"REDEEM", "CLAIM"}:
            settled_price = 100.0 if value_usd > 0 else 0.0
        rows.append(
            {
                "source": "public",
                "activity_type": activity_type,
                "resolved_at": timestamp,
                "title": str(item.get("title") or item.get("slug") or "Mercado").strip(),
                "market_slug": str(item.get("slug") or "").strip(),
                "event_slug": str(item.get("eventSlug") or "").strip(),
                "side": str(item.get("outcome") or "").strip().upper(),
                "share_size": share_size,
                "entry_price_cents": None,
                "settled_price_cents": settled_price,
                "stake_usd": None,
                "payout_usd": value_usd,
                "pnl_usd": None,
                "roi_percent": None,
            }
        )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame = frame[frame["resolved_at"].notna()].copy()
    if frame.empty:
        return frame
    return frame.sort_values("resolved_at", ascending=False)


def live_snapshot_curve_frame(snapshot_rows: list[dict]) -> pd.DataFrame:
    return build_live_snapshot_curve(snapshot_rows)


def panel_timer_remaining(session_key: str, interval_seconds: int | None) -> str:
    if interval_seconds is None or interval_seconds <= 0:
        return "--:--"
    last_completed = float(st.session_state.get(session_key, 0.0))
    if last_completed <= 0:
        return f"{interval_seconds // 60}:{interval_seconds % 60:02d}"
    elapsed = max(0.0, time.time() - last_completed)
    remaining = max(0, interval_seconds - int(elapsed))
    minutes, seconds = divmod(remaining, 60)
    return f"{minutes}:{seconds:02d}"


def ensure_panel_timer(session_key: str) -> None:
    if float(st.session_state.get(session_key, 0.0)) <= 0:
        st.session_state[session_key] = time.time()


def refresh_panel_data_if_due(session_key: str, interval_seconds: int | None) -> None:
    if interval_seconds is None or interval_seconds <= 0:
        return
    ensure_panel_timer(session_key)
    # O dashboard opera em refresh manual para priorizar estabilidade.
    # Mantemos o timer/sessao, mas sem invalidar cache automaticamente.
    _ = float(st.session_state.get(session_key, 0.0))


def render_panel_toolbar(*, title: str, timer_label: str, timer_value: str, button_label: str) -> bool:
    left, mid, right = st.columns([5.5, 1.2, 1.4])
    with left:
        st.markdown(
            f'<div class="section-card" style="margin-bottom:0.5rem;"><div class="section-title">{title}</div></div>',
            unsafe_allow_html=True,
        )
    with mid:
        st.markdown(
            f'<div class="section-card" style="padding:0.85rem 0.9rem; text-align:center;"><div class="muted">{timer_label}</div><div style="font-weight:700;">{timer_value}</div></div>',
            unsafe_allow_html=True,
        )
    with right:
        return st.button(button_label, key=f"toolbar_{title}_{button_label}", width="stretch")


def render_title() -> None:
    st.markdown(
        """
        <div class="top-title">
            <div class="diamond"></div>
            <div>Polymarket Weather Bot</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def compute_dashboard_metrics(
    live_positions: pd.DataFrame,
    live_open_positions: pd.DataFrame,
    live_resolved_positions: pd.DataFrame,
    public_wallet_snapshot: dict,
) -> dict[str, float | int | None]:
    wins = 0 if live_resolved_positions.empty else int((pd.to_numeric(live_resolved_positions["pnl_usd"], errors="coerce") > 0).sum())
    losses = 0 if live_resolved_positions.empty else int((pd.to_numeric(live_resolved_positions["pnl_usd"], errors="coerce") < 0).sum())
    resolved = wins + losses
    win_rate = (wins / resolved * 100.0) if resolved > 0 else None
    avg_profit = None
    if resolved > 0:
        avg_profit = float(pd.to_numeric(live_resolved_positions["pnl_usd"], errors="coerce").fillna(0.0).sum()) / resolved
    live_realized_pnl = 0.0 if live_resolved_positions.empty else float(pd.to_numeric(live_resolved_positions["pnl_usd"], errors="coerce").fillna(0.0).sum())
    portfolio_value, live_open_pnl = compute_open_position_totals(live_open_positions)
    total_live_pnl = live_realized_pnl + live_open_pnl
    saldo_value = public_wallet_snapshot.get("liquid_cash_usd") if public_wallet_snapshot.get("ok") else None
    saldo_float = float(saldo_value) if saldo_value is not None else 0.0
    total_net_worth = saldo_float + portfolio_value
    return {
        "wins": wins,
        "losses": losses,
        "resolved": resolved,
        "win_rate": win_rate,
        "avg_profit": avg_profit,
        "live_open_pnl": live_open_pnl,
        "total_live_pnl": total_live_pnl,
        "portfolio_value": portfolio_value,
        "saldo_value": saldo_value,
        "saldo_float": saldo_float,
        "total_net_worth": total_net_worth,
        "trade_count": int(len(live_positions)) if not live_positions.empty else 0,
    }


def render_account_summary_card(
    live_positions: pd.DataFrame,
    live_open_positions: pd.DataFrame,
    live_resolved_positions: pd.DataFrame,
    public_wallet_snapshot: dict,
    *,
    closed_positions_count: int,
) -> None:
    metrics = compute_dashboard_metrics(
        live_positions,
        live_open_positions,
        live_resolved_positions,
        public_wallet_snapshot,
    )
    st.markdown('<div class="section-card"><div class="section-title">Conta</div></div>', unsafe_allow_html=True)
    realized_known = not live_resolved_positions.empty
    realized_pnl_value = (
        float(pd.to_numeric(live_resolved_positions["pnl_usd"], errors="coerce").fillna(0.0).sum()) if realized_known else None
    )
    win_rate_value = metrics["win_rate"] if realized_known else None
    c1, c2 = st.columns(2)
    c1.metric("Saldo", fmt_usd(metrics["saldo_value"]))
    c2.metric("Portfolio", fmt_usd(metrics["portfolio_value"]))
    c3, c4 = st.columns(2)
    c3.metric("PnL Aberto", fmt_usd(metrics["live_open_pnl"]))
    c4.metric("Patrimonio", fmt_usd(metrics["total_net_worth"]))
    c5, c6 = st.columns(2)
    c5.metric("Abertas", 0 if live_open_positions.empty else len(live_open_positions))
    c6.metric("Fechadas", int(closed_positions_count))
    c7, c8 = st.columns(2)
    c7.metric("Win Rate", fmt_percent(win_rate_value))
    c8.metric("PnL Realizado", fmt_usd(realized_pnl_value))


def render_pnl_board(
    live_resolved_positions: pd.DataFrame,
    live_snapshot_curve: pd.DataFrame,
    *,
    show_title: bool = True,
) -> None:
    if show_title:
        st.markdown('<div class="section-card"><div class="section-title">Profit & Loss</div></div>', unsafe_allow_html=True)
    if not live_snapshot_curve.empty:
        fig = px.line(
            live_snapshot_curve,
            x="captured_at",
            y="pnl_curve_usd",
            markers=True,
            labels={"captured_at": "Horario", "pnl_curve_usd": "PnL USD"},
        )
        fig.update_traces(
            line_color="#71f06f",
            line_width=4,
            marker=dict(size=10, color="#71f06f"),
            hovertemplate="%{x}<br>PnL: $%{y:.4f}<extra></extra>",
        )
        fig.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10), showlegend=False, **PLOTLY_LAYOUT)
        st.plotly_chart(fig, width="stretch")
        return
    curve = live_resolved_positions.copy()
    curve = curve[curve["resolved_at"].notna()].copy()
    if curve.empty:
        st.info("Ainda nao ha operacoes live resolvidas para montar a curva de PnL.")
        return
    curve["pnl_usd"] = pd.to_numeric(curve["pnl_usd"], errors="coerce").fillna(0.0)
    curve = curve.groupby("resolved_at", as_index=False)["pnl_usd"].sum().sort_values("resolved_at")
    curve["pnl_acumulado_usd"] = curve["pnl_usd"].cumsum()
    fig = px.line(
        curve,
        x="resolved_at",
        y="pnl_acumulado_usd",
        markers=True,
        labels={"resolved_at": "Data", "pnl_acumulado_usd": "PnL acumulado USD"},
    )
    fig.update_traces(line_color="#71f06f", line_width=4, marker_color="#71f06f")
    fig.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10), showlegend=False, **PLOTLY_LAYOUT)
    st.plotly_chart(fig, width="stretch")


def render_market_scanner(
    opportunities: pd.DataFrame,
    *,
    default_monitor_seconds: int = DEFAULT_MONITOR_SECONDS,
    show_toolbar: bool = True,
) -> None:
    if show_toolbar:
        monitor_seconds = int(st.session_state.get("dashboard_monitor_seconds", default_monitor_seconds))
        ensure_panel_timer("dashboard_scan_last_completed_at")
        refresh_clicked = render_panel_toolbar(
            title="Market Scanner",
            timer_label="Refresh",
            timer_value="manual",
            button_label="Atualizar view",
        )
        if refresh_clicked:
            st.session_state["dashboard_scan_last_completed_at"] = time.time()
            st.cache_data.clear()
            st.rerun()
    else:
        st.markdown('<div class="section-card"><div class="section-title">Market Scanner</div></div>', unsafe_allow_html=True)
    if opportunities.empty:
        st.info("Nenhuma oportunidade encontrada no ultimo scan.")
        return
    cols = st.columns(3)
    for idx, (_, row) in enumerate(opportunities.head(6).iterrows()):
        badge_class = "badge-open" if row["status_posicao"] == "Ja aberta" else "badge-new"
        side_class = "side-no" if str(row["side"]).upper() == "NO" else "side-yes"
        market_title = str(row.get("title") or row.get("headline") or row.get("market_slug") or "Mercado")
        risk_label = str(row.get("risk_label") or "Moderate")
        confidence_label = fmt_confidence_tier(row.get("confidence_tier"))
        signal_tier = str(row.get("signal_tier") or "--")
        policy_allowed = bool(row.get("policy_allowed"))
        policy_label = "Enter" if policy_allowed else "Block"
        raw_reason = str(row.get("policy_reason") or "").strip()
        reason_label = raw_reason.replace("_", " ") if raw_reason else ""
        worst_case_edge = pd.to_numeric(pd.Series([row.get("min_agreeing_model_edge")]), errors="coerce").fillna(0.0).iloc[0]
        adversarial_score = pd.to_numeric(pd.Series([row.get("adversarial_score")]), errors="coerce").fillna(0.0).iloc[0]
        with cols[idx % 3]:
            st.markdown(
                f"""
                <div class="scan-card">
                    <div class="scan-badge {badge_class}">{row['status_posicao']}</div>
                    <div class="scan-title-text">
                        <a class="scan-link" href="{row.get('polymarket_url', '#')}" target="_blank">
                            {market_title} - <span class="{side_class}">{row['side']}</span>
                        </a>
                    </div>
                    <div class="scan-meta"><strong>Entrada:</strong> {fmt_cents(row['price_cents'])}</div>
                    <div class="scan-meta"><strong>Range:</strong> {row['price_range_text']}</div>
                    <div class="scan-meta"><strong>Nota:</strong> {fmt_score(row['opportunity_score'])}/10</div>
                    <div class="scan-meta"><strong>Tier:</strong> {signal_tier} | <strong>Adversarial:</strong> {fmt_num(adversarial_score, 1)}</div>
                    <div class="scan-meta"><strong>Confidence:</strong> {confidence_label} | <strong>Consenso:</strong> {row.get('agreement_summary', '--')} ({fmt_num(row.get('agreement_pct', 0), 0)}%)</div>
                    <div class="scan-meta"><strong>Modelos:</strong> {row.get('agreeing_models_text', '--')}</div>
                    <div class="scan-meta"><strong>Worst-case edge:</strong> {fmt_percent(worst_case_edge)}</div>
                    <div class="scan-meta"><strong>Risco:</strong> {risk_label}</div>
                    <div class="scan-meta"><strong>Policy:</strong> {policy_label}</div>
                    {f'<div class="scan-meta"><strong>Motivo:</strong> {reason_label}</div>' if reason_label and not policy_allowed else ''}
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_scanner_details(opportunities: pd.DataFrame) -> None:
    if opportunities.empty:
        st.info("Nenhuma oportunidade encontrada no ultimo scan.")
        return
    display = opportunities.copy()
    display["price_cents"] = pd.to_numeric(display["price_cents"], errors="coerce").map(fmt_cents)
    display["reference_price_cents"] = pd.to_numeric(display.get("reference_price_cents"), errors="coerce").map(fmt_cents)
    display["best_bid_cents"] = pd.to_numeric(display.get("best_bid_cents"), errors="coerce").map(fmt_cents)
    display["opportunity_score"] = display["opportunity_score"].map(lambda value: f"{fmt_score(value)}/10")
    display["model_prob"] = pd.to_numeric(display["model_prob"], errors="coerce").map(fmt_percent)
    if "confidence_tier" in display.columns:
        display["confidence_tier"] = display["confidence_tier"].map(fmt_confidence_tier)
    if "signal_tier" in display.columns:
        display["signal_tier"] = display["signal_tier"].fillna("--")
    if "agreement_summary" in display.columns:
        display["agreement_summary"] = display["agreement_summary"].fillna("--")
    if "agreement_pct" in display.columns:
        display["agreement_pct"] = pd.to_numeric(display["agreement_pct"], errors="coerce").map(fmt_percent)
    if "agreeing_models_text" in display.columns:
        display["agreeing_models_text"] = display["agreeing_models_text"].fillna("--")
    if "min_agreeing_model_edge" in display.columns:
        display["min_agreeing_model_edge"] = pd.to_numeric(display["min_agreeing_model_edge"], errors="coerce").map(fmt_percent)
    if "adversarial_score" in display.columns:
        display["adversarial_score"] = pd.to_numeric(display["adversarial_score"], errors="coerce").map(lambda value: fmt_num(value, 1))
    if "executable_quality_score" in display.columns:
        display["executable_quality_score"] = pd.to_numeric(display["executable_quality_score"], errors="coerce").map(
            lambda value: fmt_percent(float(value) * 100.0 if pd.notna(value) else None)
        )
    if "data_quality_score" in display.columns:
        display["data_quality_score"] = pd.to_numeric(display["data_quality_score"], errors="coerce").map(
            lambda value: fmt_percent(float(value) * 100.0 if pd.notna(value) else None)
        )
    if "policy_allowed" in display.columns:
        display["policy_allowed"] = display["policy_allowed"].map(lambda value: "Enter" if bool(value) else "Block")
    columns = [
        col
        for col in [
            "headline",
            "side",
            "price_cents",
            "reference_price_cents",
            "best_bid_cents",
            "price_source",
            "price_range_text",
            "opportunity_score",
            "signal_tier",
            "agreement_summary",
            "adversarial_score",
            "confidence_tier",
            "agreement_pct",
            "agreeing_models_text",
            "min_agreeing_model_edge",
            "executable_quality_score",
            "data_quality_score",
            "risk_label",
            "policy_allowed",
            "policy_reason",
            "model_prob",
            "status_posicao",
        ]
        if col in display.columns
    ]
    st.dataframe(
        display[columns].rename(
            columns={
                "headline": "Mercado",
                "side": "Lado",
                "price_cents": "Entrada",
                "reference_price_cents": "Ref",
                "best_bid_cents": "Bid",
                "price_source": "Fonte",
                "price_range_text": "Range",
                "opportunity_score": "Nota",
                "signal_tier": "Tier",
                "agreement_summary": "Consenso",
                "adversarial_score": "Adv Score",
                "confidence_tier": "Confidence",
                "agreement_pct": "Concordancia",
                "agreeing_models_text": "Modelos",
                "min_agreeing_model_edge": "Worst-case Edge",
                "executable_quality_score": "Exec Quality",
                "data_quality_score": "Data Quality",
                "risk_label": "Risco",
                "policy_allowed": "Policy",
                "policy_reason": "Motivo",
                "model_prob": "Prob. modelo",
                "status_posicao": "Status",
            }
        ),
        width="stretch",
        hide_index=True,
    )


def render_blocked_opportunities_panel(
    blocked_opportunities: pd.DataFrame,
    filter_rejections: dict[str, int],
    *,
    title: str,
    key_prefix: str,
    refresh_seconds: int = DEFAULT_REFRESH_SECONDS,
) -> None:
    refresh_panel_data_if_due(f"{key_prefix}_last_refresh_at", refresh_seconds)
    refresh_clicked = render_panel_toolbar(
        title=title,
        timer_label="Refresh",
        timer_value="manual",
        button_label="Refresh",
    )
    if refresh_clicked:
        st.session_state[f"{key_prefix}_last_refresh_at"] = time.time()
        st.cache_data.clear()
        st.rerun()
    if filter_rejections:
        chips = " ".join(
            f'<span style="display:inline-block; margin:0.2rem 0.45rem 0.2rem 0; padding:0.3rem 0.55rem; border-radius:999px; background:rgba(255,107,122,0.09); border:1px solid rgba(255,107,122,0.18); color:#ffd8dc; font-size:0.82rem;">{reason}: {qty}</span>'
            for reason, qty in list(filter_rejections.items())[:8]
        )
        st.markdown(f"<div style='margin:0.1rem 0 0.8rem 0;'>{chips}</div>", unsafe_allow_html=True)
    if blocked_opportunities.empty:
        st.info("Nenhuma oportunidade bloqueada no ultimo snapshot.")
        return
    display = blocked_opportunities[
        [
            "analyzed_at_text",
            "city_key",
            "short_date",
            "bucket",
            "side",
            "edge",
            "model_prob",
            "price_cents",
            "signal_tier",
            "agreement_summary",
            "min_agreeing_model_edge",
            "adversarial_score",
            "confidence_tier",
            "agreeing_models_text",
            "risk_label",
            "reason",
            "coverage_detail",
            "provider_failures_text",
            "plan_invalid_reason",
            "plan_share_size",
        ]
    ].copy()
    display["edge"] = display["edge"].map(fmt_percent)
    display["model_prob"] = display["model_prob"].map(fmt_percent)
    display["price_cents"] = display["price_cents"].map(fmt_cents)
    display["min_agreeing_model_edge"] = pd.to_numeric(display["min_agreeing_model_edge"], errors="coerce").map(fmt_percent)
    display["adversarial_score"] = pd.to_numeric(display["adversarial_score"], errors="coerce").map(lambda value: fmt_num(value, 1))
    display["plan_share_size"] = display["plan_share_size"].map(lambda value: fmt_num(value, 2) if not pd.isna(value) else "--")
    display["confidence_tier"] = display["confidence_tier"].map(fmt_confidence_tier)
    st.dataframe(
        display.rename(
            columns={
                "analyzed_at_text": "Analisado em",
                "city_key": "Cidade",
                "short_date": "Data",
                "bucket": "Bucket",
                "side": "Lado",
                "edge": "Edge",
                "model_prob": "Prob. modelo",
                "price_cents": "Preco",
                "signal_tier": "Tier",
                "agreement_summary": "Consenso",
                "min_agreeing_model_edge": "Worst-case Edge",
                "adversarial_score": "Adv Score",
                "confidence_tier": "Confidence",
                "agreeing_models_text": "Modelos",
                "risk_label": "Risco",
                "reason": "Motivo",
                "coverage_detail": "Coverage",
                "provider_failures_text": "Falhas provider",
                "plan_invalid_reason": "Motivo plano",
                "plan_share_size": "Shares",
            }
        ),
        width="stretch",
        hide_index=True,
    )


def render_positions_panel(state: dict, *, refresh_seconds: int = DEFAULT_REFRESH_SECONDS) -> None:
    open_positions = normalize_open_positions(state["effective_open_positions"])
    closed_positions = state["effective_closed_positions"]
    refresh_panel_data_if_due("dashboard_positions_last_refresh_at", refresh_seconds)
    refresh_clicked = render_panel_toolbar(
        title="Positions",
        timer_label="Refresh",
        timer_value="manual",
        button_label="Refresh",
    )
    if refresh_clicked:
        st.session_state["dashboard_positions_last_refresh_at"] = time.time()
        st.cache_data.clear()
        st.rerun()
    selected_view = st.radio("Visao", options=["Open", "Closed"], horizontal=True, key="positions_view_mode", label_visibility="collapsed")
    st.markdown(
        """
        <div class="positions-shell">
            <div class="positions-header">
                <div>Market</div>
                <div>Avg -> Now</div>
                <div>Traded</div>
                <div>To Win</div>
                <div>Value</div>
                <div></div>
            </div>
        """,
        unsafe_allow_html=True,
    )
    if selected_view == "Open":
        if open_positions.empty:
            st.markdown("</div>", unsafe_allow_html=True)
            st.info("Nenhuma posicao aberta.")
            return
        for _, row in open_positions.iterrows():
            title = str(row.get("title") or row.get("market_slug") or "Mercado")
            outcome = str(row.get("outcome", row.get("side", ""))).upper()
            shares = pd.to_numeric(pd.Series([row.get("size", row.get("share_size"))]), errors="coerce").fillna(0.0).iloc[0]
            avg_price = pd.to_numeric(pd.Series([row.get("avgPrice")]), errors="coerce").fillna(0.0).iloc[0]
            current_price = pd.to_numeric(pd.Series([row.get("curPrice")]), errors="coerce").fillna(0.0).iloc[0]
            traded_value = pd.to_numeric(pd.Series([row.get("dashboard_cost_usd", row.get("initialValue"))]), errors="coerce").fillna(0.0).iloc[0]
            current_value = pd.to_numeric(pd.Series([row.get("dashboard_value_usd", row.get("currentValue"))]), errors="coerce").fillna(0.0).iloc[0]
            cash_pnl = pd.to_numeric(pd.Series([row.get("dashboard_open_pnl_usd", row.get("cashPnl"))]), errors="coerce").fillna(0.0).iloc[0]
            percent_pnl = pd.to_numeric(pd.Series([row.get("dashboard_open_pnl_pct", row.get("percentPnl"))]), errors="coerce").fillna(0.0).iloc[0]
            to_win = max(0.0, float(shares) - float(traded_value))
            side_class = "side-pill-no" if outcome == "NO" else "side-pill-yes"
            market_slug = str(row.get("slug") or row.get("market_slug") or "").strip()
            event_slug = str(row.get("eventSlug") or "").strip()
            market_url = f"https://polymarket.com/event/{market_slug}" if market_slug else f"https://polymarket.com/event/{event_slug}"
            icon_url = str(row.get("icon") or "").strip()
            icon_html = f'<img class="market-thumb" src="{icon_url}" alt="market" />' if icon_url else '<div class="market-thumb"></div>'
            pnl_color = "var(--green)" if cash_pnl >= 0 else "var(--red)"
            pnl_prefix = "+" if cash_pnl >= 0 else ""
            pct_prefix = "+" if percent_pnl >= 0 else ""
            st.markdown(
                f"""
                <div class="position-row">
                    <div class="market-cell">
                        {icon_html}
                        <div>
                            <a class="market-title" href="{market_url}" target="_blank">{title}</a>
                            <div class="market-sub"><span class="side-pill {side_class}">{outcome}</span>{fmt_num(shares)} shares</div>
                        </div>
                    </div>
                    <div>
                        <div class="pos-main">{fmt_cents(avg_price * 100.0)} -> {fmt_cents(current_price * 100.0)}</div>
                        <div class="pos-sub">preco medio e atual</div>
                    </div>
                    <div>
                        <div class="pos-main">{fmt_usd(traded_value)}</div>
                        <div class="pos-sub">investido</div>
                    </div>
                    <div>
                        <div class="pos-main">{fmt_usd(to_win)}</div>
                        <div class="pos-sub">retorno bruto</div>
                    </div>
                    <div>
                        <div class="pos-main">{fmt_usd(current_value)}</div>
                        <div class="pos-sub" style="color:{pnl_color};">{pnl_prefix}{fmt_usd(cash_pnl)} ({pct_prefix}{fmt_num(percent_pnl)}%)</div>
                    </div>
                    <div>
                        <a class="pos-link" href="{market_url}" target="_blank">Abrir</a>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    else:
        if closed_positions.empty:
            st.markdown("</div>", unsafe_allow_html=True)
            st.info("Nenhuma posicao finalizada ainda.")
            return
        for _, row in closed_positions.sort_values("resolved_at", ascending=False).iterrows():
            title = str(row.get("title") or row.get("market_slug") or "Mercado")
            outcome = str(row.get("side") or "").upper()
            shares = pd.to_numeric(pd.Series([row.get("share_size")]), errors="coerce").fillna(0.0).iloc[0]
            entry_price = pd.to_numeric(pd.Series([row.get("entry_price_cents")]), errors="coerce").fillna(0.0).iloc[0]
            settled_price = pd.to_numeric(pd.Series([row.get("settled_price_cents")]), errors="coerce").fillna(0.0).iloc[0]
            traded_value = pd.to_numeric(pd.Series([row.get("stake_usd")]), errors="coerce").fillna(0.0).iloc[0]
            pnl_usd = pd.to_numeric(pd.Series([row.get("pnl_usd")]), errors="coerce").fillna(0.0).iloc[0]
            payout_usd = pd.to_numeric(pd.Series([row.get("payout_usd")]), errors="coerce").fillna(0.0).iloc[0]
            roi_percent = pd.to_numeric(pd.Series([row.get("roi_percent")]), errors="coerce").fillna(0.0).iloc[0]
            source = str(row.get("source") or "bot").lower()
            side_class = "side-pill-no" if outcome == "NO" else "side-pill-yes"
            market_slug = str(row.get("market_slug") or "").strip()
            market_url = f"https://polymarket.com/event/{market_slug}" if market_slug else "#"
            pnl_color = "var(--green)" if pnl_usd >= 0 else "var(--red)"
            pnl_prefix = "+" if pnl_usd >= 0 else ""
            roi_prefix = "+" if roi_percent >= 0 else ""
            source_label = "Polymarket" if source == "public" else "Bot"
            resolved_text = fmt_short_datetime(row.get("resolved_at")) if row.get("resolved_at") is not None else "--"
            detail_text = (
                f"{pnl_prefix}{fmt_usd(pnl_usd)} ({roi_prefix}{fmt_num(roi_percent)}%)"
                if source != "public"
                else "claim da carteira publica"
            )
            value_text = fmt_usd(pnl_usd) if source != "public" else fmt_usd(payout_usd)
            st.markdown(
                f"""
                <div class="position-row">
                    <div class="market-cell">
                        <div class="market-thumb"></div>
                        <div>
                            <a class="market-title" href="{market_url}" target="_blank">{title}</a>
                            <div class="market-sub"><span class="side-pill {side_class}">{outcome or 'SETTLED'}</span>{fmt_num(shares)} shares | {source_label} | {resolved_text}</div>
                        </div>
                    </div>
                    <div>
                        <div class="pos-main">{fmt_cents(entry_price)} -> {fmt_cents(settled_price)}</div>
                        <div class="pos-sub">entrada e liquidacao</div>
                    </div>
                    <div>
                        <div class="pos-main">{fmt_usd(traded_value)}</div>
                        <div class="pos-sub">stake</div>
                    </div>
                    <div>
                        <div class="pos-main">{fmt_usd(payout_usd)}</div>
                        <div class="pos-sub">retorno final</div>
                    </div>
                    <div>
                        <div class="pos-main">{value_text}</div>
                        <div class="pos-sub" style="color:{pnl_color};">{detail_text}</div>
                    </div>
                    <div>
                        <a class="pos-link" href="{market_url}" target="_blank">Abrir</a>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)


def _value_class(value: float | None) -> str:
    if value is None or pd.isna(value):
        return ""
    if float(value) > 0:
        return "positive"
    if float(value) < 0:
        return "negative"
    return ""


def _signed_usd(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "--"
    numeric = float(value)
    prefix = "+" if numeric > 0 else ""
    return f"{prefix}{fmt_usd(numeric)}"


def _kpi_card_html(label: str, value: str, sub: str, class_name: str = "") -> str:
    extra = f" {class_name}" if class_name else ""
    return (
        '<div class="kpi-card">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value{extra}">{value}</div>'
        f'<div class="kpi-sub">{sub}</div>'
        "</div>"
    )


def _curve_legend_card_html(label: str, value: str, sub: str, class_name: str = "") -> str:
    extra = f" {class_name}" if class_name else ""
    return (
        f'<div class="curve-legend-card{extra}">'
        f'<div class="curve-legend-label">{label}</div>'
        f'<div class="curve-legend-value">{value}</div>'
        f'<div class="curve-legend-sub">{sub}</div>'
        "</div>"
    )


def _snapshot_net_worth_metrics(snapshot_curve: pd.DataFrame) -> tuple[float | None, float | None, float | None]:
    if snapshot_curve.empty:
        return None, None, None
    curve = snapshot_curve.copy()
    net_worth = pd.to_numeric(curve.get("total_net_worth_usd"), errors="coerce")
    if net_worth.isna().all():
        baseline = 0.0
        pnl_curve = pd.to_numeric(curve.get("pnl_curve_usd"), errors="coerce").fillna(0.0)
        if pnl_curve.empty:
            return None, None, None
        starting_capital = baseline
        total_pnl = float(pnl_curve.iloc[-1])
        daily_pnl = total_pnl
        return starting_capital, total_pnl, daily_pnl
    curve = curve.assign(_net_worth=net_worth).dropna(subset=["_net_worth"]).copy()
    if curve.empty:
        return None, None, None
    starting_capital = float(curve["_net_worth"].iloc[0])
    total_pnl = float(curve["_net_worth"].iloc[-1] - starting_capital)
    curve["_day"] = pd.to_datetime(curve["captured_at"], errors="coerce").dt.floor("D")
    latest_day = curve["_day"].dropna().iloc[-1] if curve["_day"].notna().any() else None
    if latest_day is None:
        return starting_capital, total_pnl, total_pnl
    latest_day_rows = curve[curve["_day"] == latest_day]
    previous_rows = curve[curve["_day"] < latest_day]
    if latest_day_rows.empty:
        daily_pnl = total_pnl
    elif previous_rows.empty:
        daily_pnl = float(latest_day_rows["_net_worth"].iloc[-1] - starting_capital)
    else:
        daily_pnl = float(latest_day_rows["_net_worth"].iloc[-1] - previous_rows["_net_worth"].iloc[-1])
    return starting_capital, total_pnl, daily_pnl


def _paginate_frame(frame: pd.DataFrame, *, session_key: str, page_size: int = 5) -> tuple[pd.DataFrame, int, int]:
    if frame.empty:
        st.session_state[session_key] = 0
        return frame, 0, 1
    total_pages = max(1, (len(frame) + page_size - 1) // page_size)
    current_page = int(st.session_state.get(session_key, 0))
    current_page = max(0, min(current_page, total_pages - 1))
    st.session_state[session_key] = current_page
    start = current_page * page_size
    end = start + page_size
    return frame.iloc[start:end].copy(), current_page, total_pages


def _render_pagination_controls(*, session_key: str, current_page: int, total_pages: int, key_prefix: str) -> None:
    left, mid, right = st.columns([1.1, 2.2, 1.1])
    with left:
        if st.button("Anterior", key=f"{key_prefix}_prev", disabled=current_page <= 0, width="stretch"):
            st.session_state[session_key] = max(0, current_page - 1)
            st.rerun()
    with mid:
        st.markdown(
            f'<div class="muted" style="text-align:center; padding-top:0.45rem;">Pagina {current_page + 1} de {total_pages}</div>',
            unsafe_allow_html=True,
        )
    with right:
        if st.button("Proxima", key=f"{key_prefix}_next", disabled=current_page >= total_pages - 1, width="stretch"):
            st.session_state[session_key] = min(total_pages - 1, current_page + 1)
            st.rerun()


def _render_open_positions_ops_panel(open_positions: pd.DataFrame) -> None:
    normalized = normalize_open_positions(open_positions)
    ordered = normalized.sort_values(
        by=["opened_at"] if "opened_at" in normalized.columns else ["dashboard_value_usd"],
        ascending=False,
        na_position="last",
    ).copy()
    page, current_page, total_pages = _paginate_frame(ordered, session_key="dashboard_open_positions_page", page_size=5)
    st.markdown(
        f"""
        <div class="ops-panel">
            <div class="ops-head">
                <div class="ops-title">Open Positions</div>
                <div class="ops-badge">{len(ordered)} positions</div>
            </div>
            <div class="table-shell">
                <div class="table-header" style="grid-template-columns: 3.2fr 0.8fr 0.9fr 0.9fr 0.9fr 1fr;">
                    <div>Market</div>
                    <div>Outcome</div>
                    <div>Size</div>
                    <div>Entry</div>
                    <div>Current</div>
                    <div>P&L</div>
                </div>
        """,
        unsafe_allow_html=True,
    )
    if page.empty:
        st.markdown('<div class="table-row" style="grid-template-columns: 1fr;"><div class="muted">Nenhuma posicao aberta.</div></div>', unsafe_allow_html=True)
    else:
        for _, row in page.iterrows():
            title = str(row.get("title") or row.get("market_slug") or "Market")
            outcome = str(row.get("outcome") or row.get("side") or "").upper()
            shares = pd.to_numeric(pd.Series([row.get("size", row.get("share_size"))]), errors="coerce").fillna(0.0).iloc[0]
            avg_price = pd.to_numeric(pd.Series([row.get("avgPrice")]), errors="coerce").fillna(0.0).iloc[0] * 100.0
            current_price = pd.to_numeric(pd.Series([row.get("curPrice")]), errors="coerce").fillna(0.0).iloc[0] * 100.0
            open_pnl = pd.to_numeric(pd.Series([row.get("dashboard_open_pnl_usd")]), errors="coerce").fillna(0.0).iloc[0]
            opened_text = fmt_short_datetime(row.get("opened_at"))
            pnl_class = "num-positive" if open_pnl >= 0 else "num-negative"
            outcome_class = "pill-no" if outcome == "NO" else "pill-yes"
            st.markdown(
                f"""
                <div class="table-row" style="grid-template-columns: 3.2fr 0.8fr 0.9fr 0.9fr 0.9fr 1fr;">
                    <div class="market-col">
                        <div class="market-name">{title}</div>
                        <div class="market-meta">{opened_text}</div>
                    </div>
                    <div><span class="pill {outcome_class}">{outcome or '--'}</span></div>
                    <div>{fmt_num(shares)}</div>
                    <div>{fmt_cents(avg_price)}</div>
                    <div>{fmt_cents(current_price)}</div>
                    <div class="{pnl_class}">{_signed_usd(open_pnl)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.markdown("</div></div>", unsafe_allow_html=True)
    _render_pagination_controls(
        session_key="dashboard_open_positions_page",
        current_page=current_page,
        total_pages=total_pages,
        key_prefix="dashboard_open_positions",
    )


def _build_recent_trades_frame(state: dict) -> pd.DataFrame:
    candidates: list[pd.DataFrame] = []
    live_positions = state.get("live_positions", pd.DataFrame())
    if isinstance(live_positions, pd.DataFrame) and not live_positions.empty:
        frame = live_positions.copy()
        frame["trade_time"] = pd.to_datetime(frame.get("opened_at"), errors="coerce", utc=True)
        candidates.append(frame)
    closed_positions = state.get("effective_closed_positions", pd.DataFrame())
    if isinstance(closed_positions, pd.DataFrame) and not closed_positions.empty:
        frame = closed_positions.copy()
        resolved = pd.to_datetime(frame.get("resolved_at"), errors="coerce", utc=True)
        opened = pd.to_datetime(frame.get("opened_at"), errors="coerce", utc=True)
        frame["trade_time"] = resolved.fillna(opened)
        candidates.append(frame)
    if not candidates:
        return pd.DataFrame()
    combined = pd.concat(candidates, ignore_index=True, sort=False)
    combined["trade_time"] = pd.to_datetime(combined["trade_time"], errors="coerce", utc=True)
    combined = combined[combined["trade_time"].notna()].copy()
    if combined.empty:
        return combined
    return combined.sort_values("trade_time", ascending=False).drop_duplicates(
        subset=["market_slug", "side", "trade_time"], keep="first"
    )


def _render_recent_trades_ops_panel(state: dict) -> int:
    trades = _build_recent_trades_frame(state)
    page, current_page, total_pages = _paginate_frame(trades, session_key="dashboard_recent_trades_page", page_size=5)
    st.markdown(
        f"""
        <div class="ops-panel">
            <div class="ops-head">
                <div class="ops-title">Recent Trades</div>
                <div class="ops-badge">{len(trades)} trades</div>
            </div>
            <div class="table-shell">
                <div class="table-header" style="grid-template-columns: 1fr 3.1fr 0.9fr 0.9fr 0.8fr 0.9fr 0.8fr;">
                    <div>Time</div>
                    <div>Market</div>
                    <div>Side</div>
                    <div>Outcome</div>
                    <div>Size</div>
                    <div>Price</div>
                    <div>Status</div>
                </div>
        """,
        unsafe_allow_html=True,
    )
    if page.empty:
        st.markdown('<div class="table-row" style="grid-template-columns: 1fr;"><div class="muted">Nenhum trade recente.</div></div>', unsafe_allow_html=True)
    else:
        for _, row in page.iterrows():
            title = str(row.get("title") or row.get("market_slug") or "Market")
            side = str(row.get("side") or "").upper()
            outcome = str(row.get("outcome") or row.get("side") or "").upper()
            size = pd.to_numeric(pd.Series([row.get("share_size", row.get("size"))]), errors="coerce").fillna(0.0).iloc[0]
            entry_price = pd.to_numeric(pd.Series([row.get("entry_price_cents")]), errors="coerce").fillna(0.0).iloc[0]
            if entry_price <= 0:
                entry_price = pd.to_numeric(pd.Series([row.get("avgPrice")]), errors="coerce").fillna(0.0).iloc[0] * 100.0
            status = str(row.get("status") or "filled").upper()
            time_text = fmt_short_datetime(row.get("trade_time"))
            side_class = "pill-buy" if side in {"BUY", "YES"} else "pill-sell"
            outcome_class = "pill-yes" if outcome == "YES" else "pill-no"
            status_class = "pill-filled" if status in {"RESOLVED", "FILLED", "OPEN"} else "pill-closed"
            st.markdown(
                f"""
                <div class="table-row" style="grid-template-columns: 1fr 3.1fr 0.9fr 0.9fr 0.8fr 0.9fr 0.8fr;">
                    <div>{time_text}</div>
                    <div class="market-col">
                        <div class="market-name">{title}</div>
                        <div class="market-meta">{str(row.get("market_slug") or "--")}</div>
                    </div>
                    <div><span class="pill {side_class}">{side or '--'}</span></div>
                    <div><span class="pill {outcome_class}">{outcome or '--'}</span></div>
                    <div>{fmt_num(size)}</div>
                    <div>{fmt_cents(entry_price)}</div>
                    <div><span class="pill {status_class}">{status}</span></div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.markdown("</div></div>", unsafe_allow_html=True)
    _render_pagination_controls(
        session_key="dashboard_recent_trades_page",
        current_page=current_page,
        total_pages=total_pages,
        key_prefix="dashboard_recent_trades",
    )
    if trades.empty:
        return 0
    today = pd.Timestamp.now(tz="UTC").floor("D")
    daily_trades = trades[trades["trade_time"].dt.floor("D") == today]
    return int(len(daily_trades))


def _render_curve_panel(snapshot_curve: pd.DataFrame, live_open_positions: pd.DataFrame) -> None:
    st.markdown(
        """
        <div class="curve-panel">
            <div class="ops-head">
                <div class="ops-title">Profit &amp; Loss</div>
                <div class="ops-badge">wallet curve</div>
            </div>
        """,
        unsafe_allow_html=True,
    )
    if snapshot_curve.empty:
        st.markdown("</div>", unsafe_allow_html=True)
        st.info("Ainda nao ha snapshots suficientes para montar a curva de patrimonio.")
        return
    curve = snapshot_curve.copy()
    starting_capital, total_pnl, daily_pnl = _snapshot_net_worth_metrics(curve)
    latest_net_worth = pd.to_numeric(curve.get("total_net_worth_usd"), errors="coerce").dropna()
    pnl_curve = pd.to_numeric(curve.get("pnl_curve_usd"), errors="coerce").fillna(0.0)
    open_pnl = compute_open_position_totals(live_open_positions)[1] if not live_open_positions.empty else 0.0
    latest_value = float(latest_net_worth.iloc[-1]) if not latest_net_worth.empty else None
    curve_min = float(pnl_curve.min()) if not pnl_curve.empty else 0.0
    curve_max = float(pnl_curve.max()) if not pnl_curve.empty else 0.0
    legend_cards = [
        _curve_legend_card_html("Wallet Value", fmt_usd(latest_value), "patrimonio total"),
        _curve_legend_card_html("Total P&L", _signed_usd(total_pnl), "desde o primeiro snapshot"),
        _curve_legend_card_html("Daily P&L", _signed_usd(daily_pnl), "variacao do ultimo dia"),
        _curve_legend_card_html("Open P&L", _signed_usd(open_pnl), f"range {fmt_usd(curve_min)} a {fmt_usd(curve_max)}"),
    ]
    st.markdown(f'<div class="curve-legend-grid">{"".join(legend_cards)}</div>', unsafe_allow_html=True)
    times = pd.to_datetime(curve["captured_at"], errors="coerce")
    figure = go.Figure()
    figure.add_hline(y=0, line_width=1, line_color="rgba(127,146,191,0.35)")
    figure.add_trace(
        go.Scatter(
            x=times,
            y=pnl_curve.clip(lower=0.0),
            mode="lines",
            line=dict(color="rgba(87, 227, 137, 0)"),
            fill="tozeroy",
            fillcolor="rgba(87, 227, 137, 0.18)",
            hoverinfo="skip",
            showlegend=False,
        )
    )
    figure.add_trace(
        go.Scatter(
            x=times,
            y=pnl_curve.clip(upper=0.0),
            mode="lines",
            line=dict(color="rgba(255, 123, 136, 0)"),
            fill="tozeroy",
            fillcolor="rgba(255, 123, 136, 0.14)",
            hoverinfo="skip",
            showlegend=False,
        )
    )
    figure.add_trace(
        go.Scatter(
            x=times,
            y=pnl_curve,
            mode="lines",
            line=dict(color="#ff6b7a", width=3),
            hovertemplate="%{x|%d/%m %H:%M}<br>P&L: $%{y:,.2f}<extra></extra>",
            showlegend=False,
        )
    )
    figure.update_layout(
        height=330,
        margin=dict(l=12, r=12, t=8, b=8),
        xaxis_title="Time",
        yaxis_title="Profit / Loss (USD)",
        hovermode="x unified",
        **PLOTLY_LAYOUT,
    )
    figure.update_xaxes(showgrid=False, color="#93a7d4", title_font=dict(size=12))
    figure.update_yaxes(gridcolor="rgba(147,167,212,0.12)", zeroline=False, color="#93a7d4", title_font=dict(size=12))
    st.plotly_chart(figure, width="stretch")
    st.markdown("</div>", unsafe_allow_html=True)


def render_unified_dashboard(
    state: dict,
    *,
    refresh_seconds: int = DEFAULT_REFRESH_SECONDS,
    monitor_seconds: int = DEFAULT_MONITOR_SECONDS,
) -> None:
    effective_open_positions = normalize_open_positions(state.get("effective_open_positions", pd.DataFrame()))
    live_positions = state.get("live_positions", pd.DataFrame())
    live_snapshot_curve = state.get("live_snapshot_curve", pd.DataFrame())
    wallet_snapshot = state.get("public_wallet_snapshot", {}) or {}
    portfolio_value, open_pnl = compute_open_position_totals(effective_open_positions)
    starting_capital, total_pnl, daily_pnl = _snapshot_net_worth_metrics(live_snapshot_curve)
    cash_balance = wallet_snapshot.get("liquid_cash_usd") if wallet_snapshot.get("ok") else None
    cash_float = float(cash_balance) if cash_balance is not None else 0.0
    total_value = cash_float + portfolio_value
    curve_total_net_worth = pd.to_numeric(live_snapshot_curve.get("total_net_worth_usd"), errors="coerce").dropna()
    if not curve_total_net_worth.empty:
        total_value = float(curve_total_net_worth.iloc[-1])
    daily_trades = _build_recent_trades_frame(state)
    today = pd.Timestamp.now(tz="UTC").floor("D")
    daily_trade_count = 0 if daily_trades.empty else int((daily_trades["trade_time"].dt.floor("D") == today).sum())
    deployed = max(0.0, total_value - cash_float)
    total_pnl_pct = ((float(total_pnl) / float(starting_capital)) * 100.0) if starting_capital not in (None, 0) and total_pnl is not None else None
    daily_pnl_pct = (
        (float(daily_pnl) / float(total_value - float(daily_pnl))) * 100.0
        if daily_pnl is not None and (total_value - float(daily_pnl)) not in (0, 0.0)
        else None
    )
    kpis = [
        _kpi_card_html("Total Value", fmt_usd(total_value), f"starting capital {fmt_usd(starting_capital)}"),
        _kpi_card_html("Cash Balance", fmt_usd(cash_balance), f"deployed: {fmt_usd(deployed)}"),
        _kpi_card_html("Daily P&L", _signed_usd(daily_pnl), fmt_percent(daily_pnl_pct), _value_class(daily_pnl)),
        _kpi_card_html("Total P&L", _signed_usd(total_pnl), fmt_percent(total_pnl_pct), _value_class(total_pnl)),
        _kpi_card_html("Open Positions", str(len(effective_open_positions)), "active markets"),
        _kpi_card_html("Daily Trades", str(daily_trade_count), "today"),
    ]
    st.markdown(f'<div class="kpi-grid">{"".join(kpis)}</div>', unsafe_allow_html=True)
    st.markdown('<div class="ops-grid">', unsafe_allow_html=True)
    left, right = st.columns(2, gap="large")
    with left:
        _render_open_positions_ops_panel(effective_open_positions)
    with right:
        _render_recent_trades_ops_panel(state)
    st.markdown("</div>", unsafe_allow_html=True)
    _render_curve_panel(live_snapshot_curve, effective_open_positions)
