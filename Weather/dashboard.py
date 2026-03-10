from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.env import load_app_env
from paperbot.live_trader import get_account_snapshot
from paperbot.polymarket_account import fetch_account_activity, fetch_open_positions
from paperbot.reconciliation import sync_open_positions, sync_prediction_resolutions
from paperbot.storage import WeatherBotStorage
from paperbot.wallet_chain import fetch_public_wallet_snapshot, resolve_public_wallet_address
from paperbot.weather_dashboard_theme import configure_dashboard_theme
from paperbot.weather_dashboard_ui import (
    live_snapshot_curve_frame,
    positions_frame,
    public_closed_positions_frame,
    render_title,
    render_unified_dashboard,
    runs_frame,
)

load_app_env(ROOT)


def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


DB_PATH = _resolve_path(os.getenv("WEATHER_DB_PATH", "export/db/weather_bot.db"))
LATEST_JSON_PATH = _resolve_path(os.getenv("WEATHER_LATEST_JSON", "export/history/weather_model_latest.json"))
DEFAULT_REFRESH_SECONDS = int(os.getenv("WEATHER_DASHBOARD_REFRESH_SECONDS", "30"))
DEFAULT_MONITOR_SECONDS = int(os.getenv("WEATHER_MONITOR_INTERVAL_SECONDS", "60"))


configure_dashboard_theme()


def get_storage() -> WeatherBotStorage:
    return WeatherBotStorage(DB_PATH)


@st.cache_data(ttl=10)
def load_runs(limit: int = 50) -> list[dict]:
    return get_storage().list_runs(limit=limit)


@st.cache_data(ttl=10)
def load_run_details(run_id: str) -> dict:
    return get_storage().get_run_details(run_id)


@st.cache_data(ttl=10)
def load_positions(limit: int = 500) -> list[dict]:
    return get_storage().list_positions(limit=limit)


@st.cache_data(ttl=10)
def load_latest_dashboard_state() -> dict:
    runs = runs_frame(load_runs())
    positions = positions_frame(load_positions())
    live_positions = positions[positions["mode"] == "live"].copy() if not positions.empty else pd.DataFrame()
    live_open_positions = live_positions[live_positions["status"] == "open"].copy() if not live_positions.empty else pd.DataFrame()
    live_resolved_positions = live_positions[live_positions["status"] == "resolved"].copy() if not live_positions.empty else pd.DataFrame()
    latest_details = {"opportunities": [], "order_plans": []}
    if not runs.empty:
        latest_details = load_run_details(runs.iloc[0]["run_id"])
    return {
        "runs_frame": runs,
        "live_positions": live_positions,
        "live_open_positions": live_open_positions,
        "live_resolved_positions": live_resolved_positions,
        "latest_details": latest_details,
    }


@st.cache_data(ttl=10)
def load_latest_snapshot_json() -> dict:
    try:
        payload = json.loads(LATEST_JSON_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


@st.cache_data(ttl=30)
def load_wallet_snapshot() -> dict:
    return get_account_snapshot()


@st.cache_data(ttl=60)
def load_public_wallet_snapshot(clob_snapshot: dict) -> dict:
    return fetch_public_wallet_snapshot(clob_snapshot)


@st.cache_data(ttl=20)
def load_polymarket_positions(user: str | None) -> dict:
    if not user:
        return {"ok": False, "rows": [], "error": "missing_public_wallet"}
    try:
        return {"ok": True, "rows": fetch_open_positions(user), "error": None}
    except Exception as exc:
        return {"ok": False, "rows": [], "error": str(exc)}


@st.cache_data(ttl=20)
def load_polymarket_activity(user: str | None) -> dict:
    if not user:
        return {"ok": False, "rows": [], "error": "missing_public_wallet"}
    try:
        return {"ok": True, "rows": fetch_account_activity(user), "error": None}
    except Exception as exc:
        return {"ok": False, "rows": [], "error": str(exc)}


def render_sidebar() -> dict[str, float | int]:
    st.sidebar.markdown(
        """
        <div class="sidebar-shell">
            <div class="sidebar-kicker">Weather Bot</div>
            <div class="sidebar-headline">Painel operacional</div>
            <div class="sidebar-sub">Resumo rapido, scanner, posicoes e PnL em um so lugar.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    refresh_seconds = DEFAULT_REFRESH_SECONDS
    monitor_seconds = DEFAULT_MONITOR_SECONDS
    top = int(os.getenv("WEATHER_MONITOR_TOP", "5"))
    min_edge = float(os.getenv("WEATHER_MIN_EDGE", "10.0"))
    min_consensus = float(os.getenv("WEATHER_MIN_CONSENSUS", "0.35"))
    with st.sidebar.expander("Acoes", expanded=False):
        if st.button("Sincronizar resultados"):
            storage = get_storage()
            summary = sync_open_positions(storage)
            prediction_summary = sync_prediction_resolutions(storage)
            st.cache_data.clear()
            st.success(
                f"{summary['updated_positions']} posicoes e {prediction_summary['updated_predictions']} previsoes atualizadas"
            )
            st.rerun()
        if st.button("Atualizar painel"):
            st.cache_data.clear()
            st.rerun()
    st.sidebar.divider()
    st.sidebar.caption("Dashboard em refresh manual. O micro-live continua rodando separado.")
    return {
        "refresh_seconds": int(refresh_seconds),
        "monitor_seconds": int(monitor_seconds),
        "top": int(top),
        "min_edge": float(min_edge),
        "min_consensus": float(min_consensus),
    }
def _load_dashboard_state() -> dict:
    state = load_latest_dashboard_state()
    latest_snapshot = load_latest_snapshot_json()
    wallet_snapshot = load_wallet_snapshot()
    public_wallet_snapshot = load_public_wallet_snapshot(wallet_snapshot)
    public_wallet_address = resolve_public_wallet_address(wallet_snapshot)
    positions_result = load_polymarket_positions(public_wallet_address)
    activity_result = load_polymarket_activity(public_wallet_address)
    polymarket_positions = pd.DataFrame(positions_result.get("rows") or [])
    polymarket_activity = activity_result.get("rows") or []
    effective_open_positions = polymarket_positions if not polymarket_positions.empty else state["live_open_positions"]
    effective_closed_positions = public_closed_positions_frame(polymarket_activity)
    if effective_closed_positions.empty:
        effective_closed_positions = state["live_resolved_positions"]
    latest_details = latest_snapshot if latest_snapshot else state.get("latest_details", {"opportunities": [], "order_plans": []})
    effective_wallet_snapshot = dict(public_wallet_snapshot)
    if not effective_wallet_snapshot.get("ok") and wallet_snapshot.get("ok"):
        effective_wallet_snapshot["ok"] = True
        effective_wallet_snapshot["liquid_cash_usd"] = wallet_snapshot.get("collateral_balance_usd")
        effective_wallet_snapshot["source"] = "clob_fallback"
        if public_wallet_address and not effective_wallet_snapshot.get("address"):
            effective_wallet_snapshot["address"] = public_wallet_address
    saldo_value = effective_wallet_snapshot.get("liquid_cash_usd") if effective_wallet_snapshot.get("ok") else None
    portfolio_rows = effective_open_positions if not effective_open_positions.empty else pd.DataFrame()
    portfolio_rows = portfolio_rows if portfolio_rows.empty else portfolio_rows.copy()
    from paperbot.dashboard_metrics import compute_open_position_totals

    portfolio_value, open_pnl_value = compute_open_position_totals(portfolio_rows)
    total_net_worth = (float(saldo_value) if saldo_value is not None else 0.0) + portfolio_value
    last_snapshot_at = float(st.session_state.get("dashboard_live_snapshot_last_written_at", 0.0))
    now = time.time()
    if now - last_snapshot_at >= 60:
        get_storage().record_live_account_snapshot(
            captured_at=pd.Timestamp.utcnow().isoformat(),
            saldo_usd=(float(saldo_value) if saldo_value is not None else None),
            portfolio_usd=portfolio_value,
            total_net_worth_usd=total_net_worth,
            total_open_pnl_usd=open_pnl_value,
            open_positions_count=0 if portfolio_rows.empty else len(portfolio_rows),
        )
        st.session_state["dashboard_live_snapshot_last_written_at"] = now
    return {
        "runs_frame": state["runs_frame"],
        "live_positions": state["live_positions"],
        "live_open_positions": state["live_open_positions"],
        "live_resolved_positions": state["live_resolved_positions"],
        "wallet_snapshot": wallet_snapshot,
        "public_wallet_snapshot": effective_wallet_snapshot,
        "polymarket_positions": polymarket_positions,
        "polymarket_activity": polymarket_activity,
        "polymarket_positions_ok": bool(positions_result.get("ok")),
        "polymarket_positions_error": positions_result.get("error"),
        "polymarket_activity_ok": bool(activity_result.get("ok")),
        "polymarket_activity_error": activity_result.get("error"),
        "effective_open_positions": effective_open_positions,
        "effective_closed_positions": effective_closed_positions,
        "latest_details": latest_details,
        "live_snapshot_curve": live_snapshot_curve_frame(get_storage().list_live_account_snapshots()),
    }


def main() -> None:
    controls = render_sidebar()
    st.session_state["dashboard_monitor_seconds"] = int(controls["monitor_seconds"])
    st.session_state["dashboard_refresh_seconds"] = int(controls["refresh_seconds"])
    st.session_state["dashboard_monitor_top"] = int(controls["top"])
    st.session_state["dashboard_monitor_min_edge"] = float(controls["min_edge"])
    st.session_state["dashboard_monitor_min_consensus"] = float(controls["min_consensus"])
    render_title()
    st.caption("Atualizacao manual por estabilidade. O micro-live continua rodando separado.")
    try:
        state = _load_dashboard_state()
        if not state.get("polymarket_positions_ok"):
            st.warning("API publica do Polymarket indisponivel para posicoes abertas. Exibindo fallback local quando possivel.")
        if not state.get("polymarket_activity_ok"):
            st.warning("API publica do Polymarket indisponivel para historico. Exibindo fallback local quando possivel.")
        render_unified_dashboard(
            state,
            refresh_seconds=int(controls["refresh_seconds"]),
            monitor_seconds=int(controls["monitor_seconds"]),
        )
    except Exception as exc:
        st.error("Falha ao carregar o dashboard. O micro-live pode continuar operando normalmente.")
        st.exception(exc)


if __name__ == "__main__":
    main()
