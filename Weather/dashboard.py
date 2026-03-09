from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

load_dotenv(ROOT / ".env", override=False)

from paperbot.reconciliation import sync_open_positions, sync_prediction_resolutions
from paperbot.dashboard_metrics import (
    build_live_snapshot_curve,
    compute_open_position_totals,
    normalize_open_positions,
)
from paperbot.live_trader import get_account_snapshot
from paperbot.polymarket_account import fetch_account_activity, fetch_open_positions
from paperbot.storage import WeatherBotStorage
from paperbot.wallet_chain import fetch_public_wallet_snapshot


def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


DB_PATH = _resolve_path(os.getenv("WEATHER_DB_PATH", "export/db/weather_bot.db"))
LATEST_JSON_PATH = _resolve_path(os.getenv("WEATHER_LATEST_JSON", "export/history/weather_model_latest.json"))
DEFAULT_REFRESH_SECONDS = int(os.getenv("WEATHER_DASHBOARD_REFRESH_SECONDS", "30"))
DEFAULT_MONITOR_SECONDS = int(os.getenv("WEATHER_MONITOR_INTERVAL_SECONDS", "60"))

PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(17,18,22,0.0)",
    font=dict(color="#f5f1e8"),
)


st.set_page_config(page_title="Weather Bot", page_icon="WS", layout="wide")

st.markdown(
    """
    <style>
    :root {
        --bg: #07080c;
        --panel: #12141a;
        --border: #2b2f39;
        --text: #f5f1e8;
        --muted: #9f9b92;
        --green: #71f06f;
        --blue: #7aa8ff;
        --red: #ff6b7a;
    }
    .stApp {
        background:
            radial-gradient(circle at 10% 10%, rgba(113, 240, 111, 0.08), transparent 18%),
            radial-gradient(circle at 90% 0%, rgba(122, 168, 255, 0.08), transparent 20%),
            linear-gradient(180deg, #050608 0%, #090b10 100%);
        color: var(--text);
    }
    .main .block-container {
        padding-top: 1.1rem;
        padding-bottom: 2rem;
        max-width: 1500px;
    }
    h1, h2, h3, h4, h5, h6, p, label, span, div {
        color: var(--text);
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(16, 18, 23, 0.98) 0%, rgba(9, 10, 14, 0.98) 100%);
        border-right: 1px solid var(--border);
    }
    [data-testid="stSidebar"] * {
        color: var(--text);
    }
    [data-testid="stSidebar"] [role="radiogroup"] {
        gap: 0.45rem;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label {
        background: rgba(245, 241, 232, 0.04);
        border: 1px solid rgba(245, 241, 232, 0.08);
        border-radius: 14px;
        padding: 0.55rem 0.7rem;
        margin-bottom: 0.15rem;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label:hover {
        border-color: rgba(113, 240, 111, 0.35);
        background: rgba(113, 240, 111, 0.08);
    }
    .sidebar-shell {
        background: rgba(245, 241, 232, 0.03);
        border: 1px solid rgba(245, 241, 232, 0.07);
        border-radius: 18px;
        padding: 0.9rem 1rem 0.6rem 1rem;
        margin-bottom: 1rem;
    }
    .sidebar-kicker {
        color: var(--green);
        font-size: 0.72rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        font-weight: 700;
        margin-bottom: 0.25rem;
    }
    .sidebar-headline {
        font-size: 1.05rem;
        font-weight: 700;
        margin-bottom: 0.2rem;
    }
    .sidebar-sub {
        color: var(--muted);
        font-size: 0.85rem;
        line-height: 1.4;
    }
    [data-baseweb="select"] > div,
    [data-baseweb="input"] > div {
        background: #1a1d25;
        border: 1px solid var(--border);
        color: var(--text);
    }
    .stButton > button {
        background: linear-gradient(180deg, #1c2330, #121722);
        color: var(--text);
        border: 1px solid var(--border);
        border-radius: 12px;
    }
    div[data-testid="stMetric"] {
        background: rgba(245, 241, 232, 0.04);
        border: 1px solid rgba(245, 241, 232, 0.07);
        border-radius: 16px;
        padding: 0.8rem 0.9rem;
    }
    [data-testid="stDataFrame"] {
        background: rgba(10, 15, 28, 0.85);
        border: 1px solid var(--border);
        border-radius: 16px;
    }
    .top-title {
        display: flex;
        align-items: center;
        gap: 0.7rem;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        font-weight: 700;
        margin-bottom: 0.9rem;
    }
    .diamond {
        width: 12px;
        height: 12px;
        background: var(--green);
        transform: rotate(45deg);
        border-radius: 2px;
        box-shadow: 0 0 14px rgba(113, 240, 111, 0.5);
    }
    .section-card {
        background: rgba(18, 20, 26, 0.92);
        border: 1px solid var(--border);
        border-radius: 20px;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.22);
    }
    .section-title {
        font-size: 1rem;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 0.8rem;
        color: var(--text);
        font-weight: 700;
    }
    .scan-card {
        background: rgba(245, 241, 232, 0.04);
        border: 1px solid rgba(245, 241, 232, 0.07);
        border-radius: 18px;
        padding: 0.9rem;
        min-height: 210px;
    }
    .scan-badge {
        display: inline-block;
        padding: 0.2rem 0.55rem;
        border-radius: 999px;
        font-size: 0.72rem;
        font-weight: 700;
        text-transform: uppercase;
        margin-bottom: 0.7rem;
    }
    .badge-new {
        background: rgba(113, 240, 111, 0.14);
        border: 1px solid rgba(113, 240, 111, 0.2);
        color: var(--green);
    }
    .badge-open {
        background: rgba(255, 107, 122, 0.14);
        border: 1px solid rgba(255, 107, 122, 0.2);
        color: #ffb6bf;
    }
    .scan-title-text {
        font-size: 1.05rem;
        line-height: 1.35;
        min-height: 58px;
        margin-bottom: 0.8rem;
    }
    .scan-link {
        color: var(--text);
        text-decoration: none;
    }
    .scan-link:hover {
        color: #ffffff;
        text-decoration: underline;
    }
    .scan-meta {
        color: var(--muted);
        font-size: 0.9rem;
        margin-bottom: 0.2rem;
    }
    .positions-shell {
        background: rgba(18, 20, 26, 0.92);
        border: 1px solid var(--border);
        border-radius: 20px;
        padding: 1rem;
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.22);
    }
    .positions-tabs {
        display: flex;
        gap: 1.2rem;
        margin-bottom: 1rem;
        font-size: 1.05rem;
        font-weight: 700;
    }
    .positions-tab-active {
        color: var(--text);
    }
    .positions-tab-muted {
        color: #8ea4c6;
    }
    .positions-header {
        display: grid;
        grid-template-columns: 2.7fr 1.2fr 0.9fr 0.9fr 1.2fr 0.8fr;
        gap: 0.8rem;
        padding: 0 0.25rem 0.55rem 0.25rem;
        color: #8ea4c6;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .position-row {
        display: grid;
        grid-template-columns: 2.7fr 1.2fr 0.9fr 0.9fr 1.2fr 0.8fr;
        gap: 0.8rem;
        align-items: center;
        background: rgba(12, 17, 27, 0.92);
        border: 1px solid rgba(245, 241, 232, 0.05);
        border-radius: 18px;
        padding: 0.8rem 0.9rem;
        margin-bottom: 0.75rem;
    }
    .market-cell {
        display: flex;
        align-items: center;
        gap: 0.8rem;
        min-width: 0;
    }
    .market-thumb {
        width: 48px;
        height: 48px;
        border-radius: 12px;
        object-fit: cover;
        background: rgba(245, 241, 232, 0.05);
        border: 1px solid rgba(245, 241, 232, 0.06);
        flex-shrink: 0;
    }
    .market-title {
        font-weight: 700;
        line-height: 1.35;
        margin-bottom: 0.25rem;
        color: var(--text);
        text-decoration: none;
        display: block;
    }
    .market-title:hover {
        color: #ffffff;
        text-decoration: underline;
    }
    .market-sub {
        color: var(--muted);
        font-size: 0.85rem;
    }
    .side-pill {
        display: inline-block;
        padding: 0.18rem 0.45rem;
        border-radius: 999px;
        font-size: 0.78rem;
        font-weight: 700;
        margin-right: 0.35rem;
    }
    .side-pill-no {
        background: rgba(255, 107, 122, 0.12);
        color: var(--red);
        border: 1px solid rgba(255, 107, 122, 0.18);
    }
    .side-pill-yes {
        background: rgba(113, 240, 111, 0.12);
        color: var(--green);
        border: 1px solid rgba(113, 240, 111, 0.18);
    }
    .pos-main {
        font-weight: 700;
        font-size: 1rem;
    }
    .pos-sub {
        color: var(--muted);
        font-size: 0.84rem;
        margin-top: 0.15rem;
    }
    .pos-link {
        display: inline-block;
        text-align: center;
        padding: 0.6rem 0.8rem;
        border-radius: 12px;
        background: linear-gradient(180deg, #2396ff, #1479d6);
        color: #ffffff;
        text-decoration: none;
        font-weight: 700;
    }
    .pos-link:hover {
        color: #ffffff;
        text-decoration: none;
        filter: brightness(1.05);
    }
    .muted {
        color: var(--muted);
    }
    .side-no {
        color: var(--red);
        font-weight: 700;
    }
    .side-yes {
        color: var(--green);
        font-weight: 700;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
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
def load_recent_opportunity_ranges(keys: tuple[tuple[str, str], ...]) -> dict:
    return get_storage().recent_opportunity_ranges(list(keys))


@st.cache_data(ttl=10)
def load_latest_dashboard_state() -> dict:
    runs_frame = _runs_frame(load_runs())
    positions = _positions_frame(load_positions())
    live_positions = positions[positions["mode"] == "live"].copy() if not positions.empty else pd.DataFrame()
    live_open_positions = live_positions[live_positions["status"] == "open"].copy() if not live_positions.empty else pd.DataFrame()
    live_resolved_positions = (
        live_positions[live_positions["status"] == "resolved"].copy() if not live_positions.empty else pd.DataFrame()
    )
    latest_details = {"opportunities": [], "order_plans": []}
    if not runs_frame.empty:
        latest_details = load_run_details(runs_frame.iloc[0]["run_id"])
    opportunities = _opportunities_frame(latest_details, live_open_positions)
    return {
        "runs_frame": runs_frame,
        "live_positions": live_positions,
        "live_open_positions": live_open_positions,
        "live_resolved_positions": live_resolved_positions,
        "latest_details": latest_details,
        "opportunities": opportunities,
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
def load_polymarket_positions(user: str | None) -> list[dict]:
    if not user:
        return []
    return fetch_open_positions(user)


@st.cache_data(ttl=20)
def load_polymarket_activity(user: str | None) -> list[dict]:
    if not user:
        return []
    return fetch_account_activity(user)


def run_scan_now(*, top: int, min_edge: float, min_consensus: float) -> tuple[bool, str]:
    command = [
        sys.executable,
        str(ROOT / "run_weather_models.py"),
        "--top",
        str(top),
        "--min-edge",
        str(min_edge),
        "--min-consensus",
        str(min_consensus),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
    except Exception as exc:
        return False, str(exc)
    message = (completed.stdout or completed.stderr or "").strip()
    if completed.returncode != 0:
        return False, message or f"scan retornou codigo {completed.returncode}"
    return True, message or "scan executado com sucesso"


def _fmt_num(value: float | int | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "--"
    return f"{float(value):,.{digits}f}"


def _fmt_usd(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "--"
    return f"${float(value):,.2f}"


def _fmt_cents(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "--"
    return f"{float(value):.2f}c"


def _fmt_percent(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "--"
    return f"{float(value):.2f}%"


def _fmt_score(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "--"
    numeric = float(value)
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:.1f}"


def _fmt_confidence_tier(value: str | None) -> str:
    mapping = {
        "lock": "LOCK",
        "strong": "STRONG",
        "safe": "SAFE",
        "near-safe": "NEAR-SAFE",
        "risky": "RISKY",
    }
    return mapping.get(str(value or "").strip().lower(), str(value or "--").upper())


def _fmt_short_date(value: str | None) -> str:
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


def _fmt_timestamp(value: str | None) -> str:
    if not value:
        return "--"
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return str(value)
    return parsed.strftime("%d/%m %H:%M:%S UTC")


def _fmt_short_datetime(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return "--"
    return parsed.strftime("%d/%m %H:%M UTC")


def _extract_bucket_label(row: pd.Series) -> str:
    title = str(row.get("title") or "").strip()
    match = re.search(r"between\s+(.+?)\s+on\s+", title, flags=re.IGNORECASE)
    if match:
        return match.group(1).replace("?F", "F").replace("Â°F", "F").replace("°F", "F")
    match = re.search(r"([0-9]+°F\s+or\s+(?:higher|lower|below|above))", title, flags=re.IGNORECASE)
    if match:
        return match.group(1).replace("?F", "F").replace("Â°F", "F").replace("°F", "F")
    match = re.search(r"([0-9]+-[0-9]+°F)", title, flags=re.IGNORECASE)
    if match:
        return match.group(1).replace("?F", "F").replace("Â°F", "F").replace("°F", "F")
    return str(row.get("bucket") or row.get("slug") or row.get("market_slug") or "Mercado")


def _build_polymarket_like_title(row: pd.Series) -> str:
    explicit_title = str(row.get("title") or "").strip()
    if explicit_title:
        return explicit_title
    city = str(row.get("city_name") or row.get("city_key") or "this city").strip().title()
    bucket = str(row.get("bucket") or "").strip()
    bucket = bucket.replace("Â°F", "F").replace("°F", "F")
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


def _normalize_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    minimum = float(series.min())
    maximum = float(series.max())
    if abs(maximum - minimum) < 1e-9:
        return pd.Series([0.5] * len(series), index=series.index, dtype=float)
    return (series - minimum) / (maximum - minimum)


def _parse_bucket_bounds_from_label(label: str) -> tuple[float | None, float | None]:
    text = (label or "").upper().replace("Â°F", "F").replace("°F", "F").strip()
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


def _compute_risk_label(row: pd.Series, range_info: dict | None) -> tuple[str, float]:
    risk_points = 0.0

    consensus = float(pd.to_numeric(pd.Series([row.get("consensus_score")]), errors="coerce").fillna(0.0).iloc[0])
    spread = float(pd.to_numeric(pd.Series([row.get("spread")]), errors="coerce").fillna(0.0).iloc[0])
    sigma = float(pd.to_numeric(pd.Series([row.get("sigma")]), errors="coerce").fillna(0.0).iloc[0])
    ensemble_prediction = float(pd.to_numeric(pd.Series([row.get("ensemble_prediction")]), errors="coerce").fillna(0.0).iloc[0])
    bucket_label = str(row.get("bucket") or row.get("title") or "")
    low, high = _parse_bucket_bounds_from_label(bucket_label)

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
        midpoint = (low + high) / 2.0
        distance_to_edge = min(abs(ensemble_prediction - low), abs(ensemble_prediction - high))
        if distance_to_edge < 0.35:
            risk_points += 1.5
        elif distance_to_edge < 0.75:
            risk_points += 0.75
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


def _runs_frame(runs: list[dict]) -> pd.DataFrame:
    if not runs:
        return pd.DataFrame()
    frame = pd.DataFrame(runs)
    frame["generated_at"] = pd.to_datetime(frame["generated_at"], format="ISO8601", errors="coerce")
    return frame.sort_values("generated_at", ascending=False)


def _positions_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["opened_at"] = pd.to_datetime(frame["opened_at"], format="ISO8601", errors="coerce")
    frame["resolved_at"] = pd.to_datetime(frame.get("resolved_at"), format="ISO8601", errors="coerce")
    frame["resultado"] = frame["pnl_usd"].apply(
        lambda value: "--" if pd.isna(value) else ("Positivo" if float(value) > 0 else ("Negativo" if float(value) < 0 else "Neutro"))
    )
    return frame.sort_values("opened_at", ascending=False)


def _position_key(row: pd.Series) -> str:
    market_slug = str(row.get("market_slug") or row.get("slug") or "").strip()
    side = str(row.get("side") or row.get("outcome") or "").strip().upper()
    return f"{market_slug}|{side}"


def _opportunities_frame(details: dict, open_positions: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(details.get("opportunities", []))
    if frame.empty:
        return frame
    plans = pd.DataFrame(details.get("order_plans", []))
    if not plans.empty:
        plan_map = {
            f"{row['market_slug']}|{row['side']}": row
            for _, row in plans.iterrows()
        }
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
    open_keys = set()
    if not open_positions.empty:
        open_keys = {_position_key(row) for _, row in open_positions.iterrows()}
    frame["status_posicao"] = frame.apply(
        lambda row: "Ja aberta" if f"{row['market_slug']}|{row['side']}" in open_keys else "Nova",
        axis=1,
    )
    frame["headline"] = frame.apply(
        _build_polymarket_like_title,
        axis=1,
    )
    frame["short_date"] = frame["date_str"].map(_fmt_short_date)
    frame = frame.sort_values(["edge", "consensus_score"], ascending=[False, False]).reset_index(drop=True)

    keys = tuple((str(row["market_slug"]), str(row["side"])) for _, row in frame.iterrows())
    recent_ranges = load_recent_opportunity_ranges(keys)
    frame["price_range_text"] = frame.apply(
        lambda row: _build_price_range_text(
            recent_ranges.get(f"{row['market_slug']}|{row['side']}"),
            row.get("price_cents"),
        ),
        axis=1,
    )
    frame["risk_label"] = frame.apply(
        lambda row: _compute_risk_label(
            row,
            recent_ranges.get(f"{row['market_slug']}|{row['side']}"),
        )[0],
        axis=1,
    )
    frame["risk_score"] = frame.apply(
        lambda row: _compute_risk_label(
            row,
            recent_ranges.get(f"{row['market_slug']}|{row['side']}"),
        )[1],
        axis=1,
    )

    edge_norm = _normalize_series(pd.to_numeric(frame["edge"], errors="coerce").fillna(0.0))
    prob_norm = _normalize_series(pd.to_numeric(frame["model_prob"], errors="coerce").fillna(0.0))
    price_value_norm = _normalize_series(100.0 - pd.to_numeric(frame["price_cents"], errors="coerce").fillna(0.0))
    consensus_norm = _normalize_series(pd.to_numeric(frame["consensus_score"], errors="coerce").fillna(0.0))
    agreement_norm = _normalize_series(pd.to_numeric(frame["agreement_pct"], errors="coerce").fillna(0.0))
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
    frame["opportunity_score"] = (
        2.0 + (8.0 * ((composite * 0.7) + (rank_norm * 0.3)))
    ).clip(lower=0.0, upper=10.0).round(1)
    return frame


def _blocked_opportunities_frame(details: dict) -> pd.DataFrame:
    frame = pd.DataFrame(details.get("blocked_opportunities", []))
    if frame.empty:
        return frame
    if "analyzed_at" in frame.columns:
        frame["analyzed_at_text"] = frame["analyzed_at"].map(_fmt_timestamp)
    else:
        frame["analyzed_at_text"] = "--"
    if "date_str" in frame.columns:
        frame["short_date"] = frame["date_str"].map(_fmt_short_date)
    else:
        frame["short_date"] = "--"
    for column in ("edge", "model_prob", "price_cents", "plan_share_size"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ("confidence_tier", "risk_label", "reason", "plan_invalid_reason", "polymarket_url", "signal_tier", "min_agreeing_model_edge", "adversarial_score"):
        if column not in frame.columns:
            frame[column] = None
    return frame.sort_values(["edge", "model_prob"], ascending=[False, False]).reset_index(drop=True)


def _public_closed_positions_frame(activity_rows: list[dict]) -> pd.DataFrame:
    if not activity_rows:
        return pd.DataFrame()
    rows: list[dict] = []
    for item in activity_rows:
        activity_type = str(item.get("type") or "").upper()
        timestamp = pd.to_datetime(item.get("timestamp"), unit="s", utc=True, errors="coerce")
        value_usd = pd.to_numeric(pd.Series([item.get("usdcSize")]), errors="coerce").fillna(0.0).iloc[0]
        side = str(item.get("side") or "").strip().upper()
        if activity_type in {"TRADE"} and side != "SELL":
            continue
        if activity_type not in {"REDEEM", "CLAIM", "TRADE"}:
            continue
        settled_price = None
        entry_price = None
        share_size = pd.to_numeric(pd.Series([item.get("size")]), errors="coerce").fillna(0.0).iloc[0]
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
                "entry_price_cents": entry_price,
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


def _build_price_range_text(range_info: dict | None, current_price: float | int | None) -> str:
    if not range_info:
        return _fmt_cents(current_price)
    min_price = range_info.get("min_price_cents")
    max_price = range_info.get("max_price_cents")
    if min_price is None or max_price is None:
        return _fmt_cents(current_price)
    if abs(float(min_price) - float(max_price)) < 0.0001:
        return _fmt_cents(min_price)
    return f"{_fmt_cents(min_price)} - {_fmt_cents(max_price)}"


def _live_pnl_curve_frame(live_resolved_positions: pd.DataFrame) -> pd.DataFrame:
    if live_resolved_positions.empty:
        return pd.DataFrame()
    curve = live_resolved_positions.copy()
    curve = curve[curve["resolved_at"].notna()].copy()
    if curve.empty:
        return pd.DataFrame()
    curve["pnl_usd"] = pd.to_numeric(curve["pnl_usd"], errors="coerce").fillna(0.0)
    curve = (
        curve.groupby("resolved_at", as_index=False)["pnl_usd"]
        .sum()
        .sort_values("resolved_at")
    )
    curve["pnl_acumulado_usd"] = curve["pnl_usd"].cumsum()
    return curve


def _live_snapshot_curve_frame(snapshot_rows: list[dict]) -> pd.DataFrame:
    return build_live_snapshot_curve(snapshot_rows)


def _panel_timer_remaining(session_key: str, interval_seconds: int | None) -> str:
    if interval_seconds is None or interval_seconds <= 0:
        return "--:--"
    last_completed = float(st.session_state.get(session_key, 0.0))
    if last_completed <= 0:
        return f"{interval_seconds // 60}:{interval_seconds % 60:02d}"
    elapsed = max(0.0, time.time() - last_completed)
    remaining = max(0, interval_seconds - int(elapsed))
    minutes, seconds = divmod(remaining, 60)
    return f"{minutes}:{seconds:02d}"


def _ensure_panel_timer(session_key: str) -> None:
    if float(st.session_state.get(session_key, 0.0)) <= 0:
        st.session_state[session_key] = time.time()


def _refresh_panel_data_if_due(session_key: str, interval_seconds: int | None) -> None:
    if interval_seconds is None or interval_seconds <= 0:
        return
    _ensure_panel_timer(session_key)
    last_completed = float(st.session_state.get(session_key, 0.0))
    now = time.time()
    if now - last_completed >= interval_seconds:
        st.session_state[session_key] = now
        st.cache_data.clear()


def render_panel_toolbar(
    *,
    title: str,
    timer_label: str,
    timer_value: str,
    button_label: str,
) -> bool:
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
    return False


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
                f"{summary['updated_positions']} posicoes e "
                f"{prediction_summary['updated_predictions']} previsoes atualizadas"
            )
            st.rerun()
        if st.button("Atualizar painel"):
            st.cache_data.clear()
            st.rerun()

    st.sidebar.divider()
    if monitor_seconds > 0:
        st.sidebar.caption(f"Scanner live ativo: {int(monitor_seconds)}s")
    else:
        st.sidebar.caption("Scanner live desligado.")
    return {
        "refresh_seconds": int(refresh_seconds),
        "monitor_seconds": int(monitor_seconds),
        "top": int(top),
        "min_edge": float(min_edge),
        "min_consensus": float(min_consensus),
    }


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


def _compute_dashboard_metrics(
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
    metrics = _compute_dashboard_metrics(
        live_positions,
        live_open_positions,
        live_resolved_positions,
        public_wallet_snapshot,
    )

    with st.container():
        st.markdown('<div class="section-card"><div class="section-title">Conta</div></div>', unsafe_allow_html=True)
        realized_known = not live_resolved_positions.empty
        realized_pnl_value = (
            float(pd.to_numeric(live_resolved_positions["pnl_usd"], errors="coerce").fillna(0.0).sum())
            if realized_known
            else None
        )
        win_rate_value = metrics["win_rate"] if realized_known else None
        c1, c2 = st.columns(2)
        c1.metric("Saldo", _fmt_usd(metrics["saldo_value"]))
        c2.metric("Portfolio", _fmt_usd(metrics["portfolio_value"]))
        c3, c4 = st.columns(2)
        c3.metric("PnL Aberto", _fmt_usd(metrics["live_open_pnl"]))
        c4.metric("Patrimonio", _fmt_usd(metrics["total_net_worth"]))
        c5, c6 = st.columns(2)
        c5.metric("Abertas", 0 if live_open_positions.empty else len(live_open_positions))
        c6.metric("Fechadas", int(closed_positions_count))
        c7, c8 = st.columns(2)
        c7.metric("Win Rate", _fmt_percent(win_rate_value))
        c8.metric("PnL Realizado", _fmt_usd(realized_pnl_value))


def render_overview_strip(
    live_positions: pd.DataFrame,
    live_open_positions: pd.DataFrame,
    live_resolved_positions: pd.DataFrame,
    public_wallet_snapshot: dict,
    live_snapshot_curve: pd.DataFrame,
) -> None:
    metrics = _compute_dashboard_metrics(
        live_positions,
        live_open_positions,
        live_resolved_positions,
        public_wallet_snapshot,
    )
    pnl_delta = metrics["total_live_pnl"]
    pnl_delta_text = f"{'+' if pnl_delta and float(pnl_delta) >= 0 else ''}{_fmt_usd(pnl_delta)}"
    pnl_delta_color = "var(--green)" if (pnl_delta or 0.0) >= 0 else "var(--red)"
    st.markdown(
        f"""
        <div class="section-card">
            <div class="section-title">Portfolio</div>
            <div style="display:flex; justify-content:space-between; gap:1rem; align-items:flex-start; flex-wrap:wrap;">
                <div>
                    <div style="font-size:2.2rem; font-weight:800; margin-bottom:0.2rem;">{_fmt_usd(metrics['portfolio_value'])}</div>
                    <div style="color:{pnl_delta_color}; font-weight:700;">{pnl_delta_text} total</div>
                </div>
                <div style="text-align:right;">
                    <div style="color:var(--muted); font-size:0.88rem; margin-bottom:0.25rem;">Available to trade</div>
                    <div style="font-size:1.9rem; font-weight:800;">{_fmt_usd(metrics['saldo_value'])}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_pnl_board(
    live_open_positions: pd.DataFrame,
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

    curve = _live_pnl_curve_frame(live_resolved_positions)
    if curve.empty:
        st.info("Ainda nao ha operacoes live resolvidas para montar a curva de PnL.")
        return

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


def render_market_scanner(opportunities: pd.DataFrame, *, show_toolbar: bool = True) -> None:
    refresh_clicked = False
    if show_toolbar:
        monitor_seconds = int(st.session_state.get("dashboard_monitor_seconds", DEFAULT_MONITOR_SECONDS))
        _ensure_panel_timer("dashboard_scan_last_completed_at")
        refresh_clicked = render_panel_toolbar(
            title="Market Scanner",
            timer_label="Atualiza em",
            timer_value=_panel_timer_remaining("dashboard_scan_last_completed_at", monitor_seconds),
            button_label="Refresh",
        )
    else:
        st.markdown('<div class="section-card"><div class="section-title">Market Scanner</div></div>', unsafe_allow_html=True)
    if refresh_clicked:
        ok, message = run_scan_now(
            top=int(st.session_state.get("dashboard_monitor_top", int(os.getenv("WEATHER_MONITOR_TOP", "5")))),
            min_edge=float(st.session_state.get("dashboard_monitor_min_edge", float(os.getenv("WEATHER_MIN_EDGE", "10.0")))),
            min_consensus=float(st.session_state.get("dashboard_monitor_min_consensus", float(os.getenv("WEATHER_MIN_CONSENSUS", "0.35")))),
        )
        st.session_state["dashboard_scan_last_completed_at"] = time.time()
        st.session_state["dashboard_scan_last_ok"] = ok
        st.session_state["dashboard_scan_last_message"] = message
        st.cache_data.clear()
        st.rerun()
    if opportunities.empty:
        st.info("Nenhuma oportunidade encontrada no ultimo scan.")
        return

    cols = st.columns(3)
    top_rows = list(opportunities.head(6).iterrows())
    for idx, (_, row) in enumerate(top_rows):
        badge_class = "badge-open" if row["status_posicao"] == "Ja aberta" else "badge-new"
        side_class = "side-no" if str(row["side"]).upper() == "NO" else "side-yes"
        market_title = str(row.get("title") or row.get("headline") or row.get("market_slug") or "Mercado")
        risk_label = str(row.get("risk_label") or "Moderate")
        confidence_label = _fmt_confidence_tier(row.get("confidence_tier"))
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
                    <div class="scan-meta"><strong>Entrada:</strong> {_fmt_cents(row['price_cents'])}</div>
                    <div class="scan-meta"><strong>Range:</strong> {row['price_range_text']}</div>
                    <div class="scan-meta"><strong>Nota:</strong> {_fmt_score(row['opportunity_score'])}/10</div>
                    <div class="scan-meta"><strong>Tier:</strong> {signal_tier} | <strong>Adversarial:</strong> {_fmt_num(adversarial_score, 1)}</div>
                    <div class="scan-meta"><strong>Confidence:</strong> {confidence_label} ({_fmt_num(row.get('agreement_pct', 0), 0)}%)</div>
                    <div class="scan-meta"><strong>Worst-case edge:</strong> {_fmt_percent(worst_case_edge)}</div>
                    <div class="scan-meta"><strong>Risco:</strong> {risk_label}</div>
                    <div class="scan-meta"><strong>Policy:</strong> {policy_label}</div>
                    {f'<div class="scan-meta"><strong>Motivo:</strong> {reason_label}</div>' if reason_label and not policy_allowed else ''}
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_trade_history(live_resolved_positions: pd.DataFrame, *, show_toolbar: bool = True) -> None:
    refresh_clicked = False
    if show_toolbar:
        _refresh_panel_data_if_due("dashboard_history_last_refresh_at", int(st.session_state.get("dashboard_refresh_seconds", DEFAULT_REFRESH_SECONDS)))
        refresh_clicked = render_panel_toolbar(
            title="Trade History",
            timer_label="Atualiza em",
            timer_value=_panel_timer_remaining("dashboard_history_last_refresh_at", int(st.session_state.get("dashboard_refresh_seconds", DEFAULT_REFRESH_SECONDS))),
            button_label="Refresh",
        )
    else:
        st.markdown('<div class="section-card"><div class="section-title">Trade History</div></div>', unsafe_allow_html=True)
    if refresh_clicked:
        st.session_state["dashboard_history_last_refresh_at"] = time.time()
        st.cache_data.clear()
        st.rerun()
    if live_resolved_positions.empty:
        st.info("Sem historico de operacoes live finalizadas ainda.")
        return

    display = live_resolved_positions.copy()
    if "city_key" not in display.columns:
        display["city_key"] = display.get("title", display.get("market_slug", "--"))
    if "entry_price_cents" not in display.columns:
        display["entry_price_cents"] = None
    if "settled_price_cents" not in display.columns:
        display["settled_price_cents"] = None
    if "stake_usd" not in display.columns:
        display["stake_usd"] = None
    if "pnl_usd" not in display.columns:
        display["pnl_usd"] = None
    if "market_slug" not in display.columns:
        display["market_slug"] = display.get("event_slug", "--")
    if "activity_type" not in display.columns:
        display["activity_type"] = "BOT"
    display = display[
        ["resolved_at", "activity_type", "city_key", "side", "entry_price_cents", "settled_price_cents", "stake_usd", "pnl_usd", "market_slug"]
    ].copy()
    display["resolved_at"] = display["resolved_at"].dt.strftime("%d/%m %H:%M")
    display["entry_price_cents"] = display["entry_price_cents"].map(_fmt_cents)
    display["settled_price_cents"] = display["settled_price_cents"].map(_fmt_cents)
    display["stake_usd"] = display["stake_usd"].map(_fmt_usd)
    display["pnl_usd"] = display["pnl_usd"].map(_fmt_usd)
    display = display.rename(
        columns={
            "resolved_at": "Data",
            "activity_type": "Tipo",
            "city_key": "Cidade",
            "side": "Lado",
            "entry_price_cents": "Entrada",
            "settled_price_cents": "Saida",
            "stake_usd": "Stake",
            "pnl_usd": "PnL",
            "market_slug": "Mercado",
        }
    )
    st.dataframe(display, width="stretch", hide_index=True)


def render_scanner_details(opportunities: pd.DataFrame) -> None:
    if opportunities.empty:
        st.info("Nenhuma oportunidade encontrada no ultimo scan.")
        return
    display = opportunities.copy()
    display["price_cents"] = pd.to_numeric(display["price_cents"], errors="coerce").map(_fmt_cents)
    display["reference_price_cents"] = pd.to_numeric(display.get("reference_price_cents"), errors="coerce").map(_fmt_cents)
    display["best_bid_cents"] = pd.to_numeric(display.get("best_bid_cents"), errors="coerce").map(_fmt_cents)
    display["opportunity_score"] = display["opportunity_score"].map(lambda value: f"{_fmt_score(value)}/10")
    display["model_prob"] = pd.to_numeric(display["model_prob"], errors="coerce").map(_fmt_percent)
    if "confidence_tier" in display.columns:
        display["confidence_tier"] = display["confidence_tier"].map(_fmt_confidence_tier)
    if "signal_tier" in display.columns:
        display["signal_tier"] = display["signal_tier"].fillna("--")
    if "agreement_pct" in display.columns:
        display["agreement_pct"] = pd.to_numeric(display["agreement_pct"], errors="coerce").map(lambda value: _fmt_percent(value))
    if "min_agreeing_model_edge" in display.columns:
        display["min_agreeing_model_edge"] = pd.to_numeric(display["min_agreeing_model_edge"], errors="coerce").map(_fmt_percent)
    if "adversarial_score" in display.columns:
        display["adversarial_score"] = pd.to_numeric(display["adversarial_score"], errors="coerce").map(lambda value: _fmt_num(value, 1))
    if "executable_quality_score" in display.columns:
        display["executable_quality_score"] = pd.to_numeric(display["executable_quality_score"], errors="coerce").map(lambda value: _fmt_percent(float(value) * 100.0 if pd.notna(value) else None))
    if "data_quality_score" in display.columns:
        display["data_quality_score"] = pd.to_numeric(display["data_quality_score"], errors="coerce").map(lambda value: _fmt_percent(float(value) * 100.0 if pd.notna(value) else None))
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
            "adversarial_score",
            "confidence_tier",
            "agreement_pct",
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
    display = display[columns].rename(
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
            "adversarial_score": "Adv Score",
            "confidence_tier": "Confidence",
            "agreement_pct": "Concordancia",
            "min_agreeing_model_edge": "Worst-case Edge",
            "executable_quality_score": "Exec Quality",
            "data_quality_score": "Data Quality",
            "risk_label": "Risco",
            "policy_allowed": "Policy",
            "policy_reason": "Motivo",
            "model_prob": "Prob. modelo",
            "status_posicao": "Status",
        }
    )
    st.dataframe(display, width="stretch", hide_index=True)


def render_unified_dashboard(state: dict) -> None:
    effective_open_positions = state["effective_open_positions"]
    raw_predictions = state.get("latest_details", {}).get("raw_predictions", [])
    degraded_count = sum(1 for item in raw_predictions if str(item.get("degraded_reason") or "").strip())
    blocked_count = sum(1 for item in raw_predictions if not bool(item.get("policy_allowed")))
    if degraded_count > 0:
        st.warning(f"Ultimo scan em modo degradado: {degraded_count} previsoes com dados degradados.")
    elif blocked_count > 0 and not state["opportunities"].empty:
        st.info(f"Ultimo scan bloqueou {blocked_count} previsoes por politica; oportunidades restantes continuam validas.")
    elif blocked_count > 0 and state["opportunities"].empty:
        st.info(f"Ultimo scan bloqueou {blocked_count} previsoes por politica; nenhuma entrada liberada.")

    left, right = st.columns([1.2, 1.0], gap="large")
    with left:
        render_market_scanner(state["opportunities"], show_toolbar=True)
        st.markdown('<div class="section-card"><div class="section-title">Scanner Details</div></div>', unsafe_allow_html=True)
        render_scanner_details(state["opportunities"])
    with right:
        render_positions_panel(state)

    render_blocked_opportunities_panel(
        state["blocked_opportunities"],
        state["filter_rejections"],
        title="Sinais Bloqueados",
        key_prefix="unified_blocked",
    )

    bottom_left, bottom_right = st.columns([0.95, 2.05], gap="large")
    with bottom_left:
        render_account_summary_card(
            state["live_positions"],
            effective_open_positions,
            state["live_resolved_positions"],
            state["public_wallet_snapshot"],
            closed_positions_count=0 if state["effective_closed_positions"].empty else len(state["effective_closed_positions"]),
        )
    with bottom_right:
        render_pnl_board(
            effective_open_positions,
            state["live_resolved_positions"],
            state["live_snapshot_curve"],
            show_title=True,
        )


def render_blocked_opportunities_panel(
    blocked_opportunities: pd.DataFrame,
    filter_rejections: dict[str, int],
    *,
    title: str,
    key_prefix: str,
) -> None:
    _refresh_panel_data_if_due(f"{key_prefix}_last_refresh_at", int(st.session_state.get("dashboard_refresh_seconds", DEFAULT_REFRESH_SECONDS)))
    refresh_clicked = render_panel_toolbar(
        title=title,
        timer_label="Atualiza em",
        timer_value=_panel_timer_remaining(f"{key_prefix}_last_refresh_at", int(st.session_state.get("dashboard_refresh_seconds", DEFAULT_REFRESH_SECONDS))),
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
            "min_agreeing_model_edge",
            "adversarial_score",
            "confidence_tier",
            "risk_label",
            "reason",
            "plan_invalid_reason",
            "plan_share_size",
        ]
    ].copy()
    display["edge"] = display["edge"].map(_fmt_percent)
    display["model_prob"] = display["model_prob"].map(_fmt_percent)
    display["price_cents"] = display["price_cents"].map(_fmt_cents)
    display["min_agreeing_model_edge"] = pd.to_numeric(display["min_agreeing_model_edge"], errors="coerce").map(_fmt_percent)
    display["adversarial_score"] = pd.to_numeric(display["adversarial_score"], errors="coerce").map(lambda value: _fmt_num(value, 1))
    display["plan_share_size"] = display["plan_share_size"].map(lambda value: _fmt_num(value, 2) if not pd.isna(value) else "--")
    display["confidence_tier"] = display["confidence_tier"].map(_fmt_confidence_tier)
    display = display.rename(
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
            "min_agreeing_model_edge": "Worst-case Edge",
            "adversarial_score": "Adv Score",
            "confidence_tier": "Confidence",
            "risk_label": "Risco",
            "reason": "Motivo",
            "plan_invalid_reason": "Motivo plano",
            "plan_share_size": "Shares",
        }
    )
    st.dataframe(display, width="stretch", hide_index=True)


def render_positions_panel(state: dict) -> None:
    open_positions = state["effective_open_positions"]
    closed_positions = state["effective_closed_positions"]
    _refresh_panel_data_if_due("dashboard_positions_last_refresh_at", int(st.session_state.get("dashboard_refresh_seconds", DEFAULT_REFRESH_SECONDS)))
    refresh_clicked = render_panel_toolbar(
        title="Positions",
        timer_label="Atualiza em",
        timer_value=_panel_timer_remaining("dashboard_positions_last_refresh_at", int(st.session_state.get("dashboard_refresh_seconds", DEFAULT_REFRESH_SECONDS))),
        button_label="Refresh",
    )
    if refresh_clicked:
        st.session_state["dashboard_positions_last_refresh_at"] = time.time()
        st.cache_data.clear()
        st.rerun()
    selected_view = st.radio(
        "Visao",
        options=["Open", "Closed"],
        horizontal=True,
        key="positions_view_mode",
        label_visibility="collapsed",
    )
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
                            <div class="market-sub"><span class="side-pill {side_class}">{outcome}</span>{_fmt_num(shares)} shares</div>
                        </div>
                    </div>
                    <div>
                        <div class="pos-main">{_fmt_cents(avg_price * 100.0)} -> {_fmt_cents(current_price * 100.0)}</div>
                        <div class="pos-sub">preco medio e atual</div>
                    </div>
                    <div>
                        <div class="pos-main">{_fmt_usd(traded_value)}</div>
                        <div class="pos-sub">investido</div>
                    </div>
                    <div>
                        <div class="pos-main">{_fmt_usd(to_win)}</div>
                        <div class="pos-sub">retorno bruto</div>
                    </div>
                    <div>
                        <div class="pos-main">{_fmt_usd(current_value)}</div>
                        <div class="pos-sub" style="color:{pnl_color};">{pnl_prefix}{_fmt_usd(cash_pnl)} ({pct_prefix}{_fmt_num(percent_pnl)}%)</div>
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
            resolved_text = _fmt_short_datetime(row.get("resolved_at")) if row.get("resolved_at") is not None else "--"
            st.markdown(
                f"""
                <div class="position-row">
                    <div class="market-cell">
                        <div class="market-thumb"></div>
                        <div>
                            <a class="market-title" href="{market_url}" target="_blank">{title}</a>
                            <div class="market-sub"><span class="side-pill {side_class}">{outcome or 'SETTLED'}</span>{_fmt_num(shares)} shares | {source_label} | {resolved_text}</div>
                        </div>
                    </div>
                    <div>
                        <div class="pos-main">{_fmt_cents(entry_price)} -> {_fmt_cents(settled_price)}</div>
                        <div class="pos-sub">entrada e liquidacao</div>
                    </div>
                    <div>
                        <div class="pos-main">{_fmt_usd(traded_value)}</div>
                        <div class="pos-sub">stake</div>
                    </div>
                    <div>
                        <div class="pos-main">{_fmt_usd(payout_usd)}</div>
                        <div class="pos-sub">retorno final</div>
                    </div>
                    <div>
                        <div class="pos-main">{_fmt_usd(pnl_usd) if source != 'public' else _fmt_usd(payout_usd)}</div>
                        <div class="pos-sub" style="color:{pnl_color};">{(pnl_prefix + _fmt_usd(pnl_usd) + f' ({roi_prefix}{_fmt_num(roi_percent)}%)') if source != 'public' else 'claim da carteira publica'}</div>
                    </div>
                    <div>
                        <a class="pos-link" href="{market_url}" target="_blank">Abrir</a>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)


def maybe_run_live_scan(*, monitor_seconds: int, top: int, min_edge: float, min_consensus: float) -> None:
    if monitor_seconds <= 0:
        return
    now = time.time()
    last_completed = float(st.session_state.get("dashboard_scan_last_completed_at", 0.0))
    running = bool(st.session_state.get("dashboard_scan_running", False))
    if running or (last_completed and now - last_completed < monitor_seconds):
        return
    st.session_state["dashboard_scan_running"] = True
    ok, message = run_scan_now(top=top, min_edge=min_edge, min_consensus=min_consensus)
    st.session_state["dashboard_scan_running"] = False
    st.session_state["dashboard_scan_last_completed_at"] = time.time()
    st.session_state["dashboard_scan_last_ok"] = ok
    st.session_state["dashboard_scan_last_message"] = message
    st.cache_data.clear()


def _load_dashboard_state() -> dict:
    state = load_latest_dashboard_state()
    latest_snapshot = load_latest_snapshot_json()
    runs_frame = state["runs_frame"]
    live_positions = state["live_positions"]
    live_open_positions = state["live_open_positions"]
    live_resolved_positions = state["live_resolved_positions"]
    wallet_snapshot = load_wallet_snapshot()
    public_wallet_snapshot = load_public_wallet_snapshot(wallet_snapshot)
    public_wallet_address = public_wallet_snapshot.get("address") if public_wallet_snapshot.get("ok") else None
    polymarket_positions = pd.DataFrame(load_polymarket_positions(public_wallet_address))
    polymarket_activity = load_polymarket_activity(public_wallet_address)
    effective_open_positions = polymarket_positions if not polymarket_positions.empty else live_open_positions
    effective_open_positions = normalize_open_positions(effective_open_positions)
    public_closed_positions = _public_closed_positions_frame(polymarket_activity)
    effective_closed_positions = public_closed_positions if not public_closed_positions.empty else live_resolved_positions
    latest_details = latest_snapshot if latest_snapshot else state.get("latest_details", {"opportunities": [], "order_plans": []})
    opportunities = _opportunities_frame(latest_details, effective_open_positions)
    blocked_snapshot = {
        "blocked_opportunities": latest_snapshot.get("blocked_opportunities", []),
        "filter_rejections": latest_snapshot.get("filter_rejections", {}),
    }
    blocked_opportunities = _blocked_opportunities_frame(blocked_snapshot)
    saldo_value = public_wallet_snapshot.get("liquid_cash_usd") if public_wallet_snapshot.get("ok") else None
    portfolio_value, open_pnl_value = compute_open_position_totals(effective_open_positions)
    total_net_worth = (float(saldo_value) if saldo_value is not None else 0.0) + portfolio_value
    last_snapshot_at = float(st.session_state.get("dashboard_live_snapshot_last_written_at", 0.0))
    now = time.time()
    if now - last_snapshot_at >= 60:
        captured_at = pd.Timestamp.utcnow().isoformat()
        get_storage().record_live_account_snapshot(
            captured_at=captured_at,
            saldo_usd=(float(saldo_value) if saldo_value is not None else None),
            portfolio_usd=portfolio_value,
            total_net_worth_usd=total_net_worth,
            total_open_pnl_usd=open_pnl_value,
            open_positions_count=0 if effective_open_positions.empty else len(effective_open_positions),
        )
        st.session_state["dashboard_live_snapshot_last_written_at"] = now
    return {
        "runs_frame": runs_frame,
        "live_positions": live_positions,
        "live_open_positions": live_open_positions,
        "live_resolved_positions": live_resolved_positions,
        "effective_closed_positions": effective_closed_positions,
        "wallet_snapshot": wallet_snapshot,
        "public_wallet_snapshot": public_wallet_snapshot,
        "polymarket_positions": polymarket_positions,
        "polymarket_activity": polymarket_activity,
        "effective_open_positions": effective_open_positions,
        "latest_details": latest_details,
        "opportunities": opportunities,
        "blocked_opportunities": blocked_opportunities,
        "filter_rejections": blocked_snapshot["filter_rejections"],
        "live_snapshot_curve": _live_snapshot_curve_frame(get_storage().list_live_account_snapshots()),
    }


def main() -> None:
    controls = render_sidebar()
    refresh_interval = None if int(controls["refresh_seconds"]) <= 0 else int(controls["refresh_seconds"])
    scanner_interval = None if int(controls["monitor_seconds"]) <= 0 else int(controls["monitor_seconds"])
    st.session_state["dashboard_monitor_seconds"] = int(controls["monitor_seconds"])
    st.session_state["dashboard_refresh_seconds"] = int(controls["refresh_seconds"])
    st.session_state["dashboard_monitor_top"] = int(controls["top"])
    st.session_state["dashboard_monitor_min_edge"] = float(controls["min_edge"])
    st.session_state["dashboard_monitor_min_consensus"] = float(controls["min_consensus"])

    render_title()
    panel_interval = 1

    @st.fragment(run_every=panel_interval)
    def render_panel() -> None:
        state = _load_dashboard_state()
        render_unified_dashboard(state)

    @st.fragment(run_every=scanner_interval if scanner_interval is not None else refresh_interval)
    def run_background_scanner() -> None:
        maybe_run_live_scan(
            monitor_seconds=int(controls["monitor_seconds"]),
            top=int(controls["top"]),
            min_edge=float(controls["min_edge"]),
            min_consensus=float(controls["min_consensus"]),
        )

    run_background_scanner()
    render_panel()


if __name__ == "__main__":
    main()
