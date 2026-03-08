from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit.components.v1 import html

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.realtime_signal import aggregate_candles, append_prediction_log, build_signal_for_interval, fetch_candles, split_closed_and_live_candles


ASSET_OPTIONS = ["BTC-USD", "ETH-USD"]
TIMEFRAME_OPTIONS = [5, 15]


st.set_page_config(page_title="Crypto Signal Dashboard", layout="wide", initial_sidebar_state="collapsed")

st.markdown(
    """
    <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(255, 255, 255, 0.03), transparent 32%),
                radial-gradient(circle at bottom right, rgba(255, 255, 255, 0.02), transparent 28%),
                linear-gradient(180deg, #010101 0%, #0a0a0a 100%);
            color: #e5eefb;
        }
        .stApp [data-testid="stHeader"] {
            background: rgba(1, 1, 1, 0.0);
        }
        .stApp [data-testid="stSidebar"],
        .stApp [data-testid="collapsedControl"] {
            display: none;
        }
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 1.5rem;
            max-width: 1500px;
        }
        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(8, 8, 8, 0.96), rgba(18, 18, 18, 0.92));
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 16px;
            padding: 10px 14px;
            box-shadow: 0 12px 32px rgba(0, 0, 0, 0.20);
        }
        div[data-testid="stMetric"] label,
        div[data-testid="stMetric"] [data-testid="stMetricLabel"] {
            color: #8fa6c3 !important;
        }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: #f8fbff !important;
        }
        div[data-testid="stDataFrame"] {
            border-radius: 16px;
            overflow: hidden;
            border: 1px solid rgba(148, 163, 184, 0.10);
        }
        .terminal-shell, .control-shell {
            background: linear-gradient(180deg, rgba(5, 5, 5, 0.98), rgba(12, 12, 12, 0.95));
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 22px;
            box-shadow: 0 18px 60px rgba(0, 0, 0, 0.28);
        }
        .terminal-shell {
            padding: 16px;
            margin-top: 12px;
        }
        .control-shell {
            padding: 14px 16px 8px 16px;
            margin: 10px 0 14px 0;
        }
        .terminal-topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            padding: 8px 10px 14px 10px;
            border-bottom: 1px solid rgba(148, 163, 184, 0.10);
            margin-bottom: 12px;
        }
        .terminal-title {
            font-size: 1.05rem;
            font-weight: 700;
            color: #f8fbff;
        }
        .terminal-subtitle {
            font-size: 0.85rem;
            color: #8fa6c3;
        }
        .section-label {
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #7f93ad;
            margin-bottom: 8px;
        }
        div[data-testid="stSelectbox"] > label {
            color: #8fa6c3 !important;
        }
        div[data-testid="stSelectbox"] [data-baseweb="select"] > div {
            background: linear-gradient(180deg, rgba(8, 8, 8, 0.96), rgba(18, 18, 18, 0.92));
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 16px;
            min-height: 84px;
            box-shadow: 0 12px 32px rgba(0, 0, 0, 0.20);
        }
        div[data-testid="stSelectbox"] [data-baseweb="select"] > div > div {
            padding-top: 10px;
            padding-bottom: 10px;
        }
        div[data-testid="stSelectbox"] [data-baseweb="select"] span {
            color: #f8fbff;
            font-size: 1.55rem;
            font-weight: 700;
            line-height: 1.2;
            font-family: inherit;
        }
        div[data-testid="stSelectbox"] svg {
            color: #8fa6c3;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def _signal_palette(signal: str) -> tuple[str, str]:
    if signal == "UP":
        return "#22c55e", "#e5eefb"
    if signal == "DOWN":
        return "#ef4444", "#e5eefb"
    return "#f59e0b", "#e5eefb"


def _format_countdown(seconds_left: float) -> str:
    total_seconds = max(0, int(seconds_left))
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _bucket_start(ts: pd.Timestamp, horizon_minutes: int) -> pd.Timestamp:
    minute_bucket = (ts.minute // horizon_minutes) * horizon_minutes
    return ts.replace(minute=minute_bucket, second=0, microsecond=0)


def _bucket_end(ts: pd.Timestamp, horizon_minutes: int) -> pd.Timestamp:
    return _bucket_start(ts, horizon_minutes) + timedelta(minutes=horizon_minutes)


def _restore_signal(payload: dict):
    from paperbot.realtime_signal import HorizonSignal

    return HorizonSignal(**payload)


def _render_signal_card(signal, timer_prefix: str, candle_close: pd.Timestamp, prediction_expires: pd.Timestamp) -> None:
    accent, text_color = _signal_palette(signal.signal)
    candle_target_ms = int(candle_close.timestamp() * 1000)
    prediction_target_ms = int(prediction_expires.timestamp() * 1000)
    candle_initial = _format_countdown((candle_close - pd.Timestamp.utcnow().tz_localize(None)).total_seconds())
    prediction_initial = _format_countdown((prediction_expires - pd.Timestamp.utcnow().tz_localize(None)).total_seconds())
    html(
        f"""
        <div style="
            background:linear-gradient(180deg, #080808 0%, #121212 100%);
            border:1px solid rgba(148, 163, 184, 0.14);
            border-left:4px solid rgba(148, 163, 184, 0.18);
            border-radius:16px;
            padding:18px 18px 14px 18px;
            min-height:280px;
            font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
        ">
            <div style="font-size:0.85rem; text-transform:uppercase; letter-spacing:0.08em; color:#475569;">
                Horizon {signal.horizon}
            </div>
            <div style="font-size:2rem; font-weight:700; color:{accent}; margin-top:6px;">
                {signal.signal}
            </div>
            <div style="font-size:0.95rem; color:{text_color}; margin-top:10px;">
                Confidence: <strong>{signal.confidence:.2%}</strong>
            </div>
            <div style="font-size:0.95rem; color:{text_color}; margin-top:4px;">
                Prob Up: <strong>{signal.prob_up:.2%}</strong>
            </div>
            <div style="font-size:0.95rem; color:{text_color}; margin-top:4px;">
                Prob Down: <strong>{signal.prob_down:.2%}</strong>
            </div>
            <div style="font-size:0.95rem; color:{text_color}; margin-top:4px;">
                Score: <strong>{signal.score:.4f}</strong>
            </div>
            <div id="{timer_prefix}-candle" style="font-size:0.95rem; color:{text_color}; margin-top:12px;">
                Candle closes in: <strong>{candle_initial}</strong>
            </div>
            <div id="{timer_prefix}-prediction" style="font-size:0.95rem; color:{text_color}; margin-top:4px;">
                Prediction expires in: <strong>{prediction_initial}</strong>
            </div>
        </div>
        <script>
            (function() {{
                const candleRoot = document.getElementById("{timer_prefix}-candle");
                const predictionRoot = document.getElementById("{timer_prefix}-prediction");
                if (!candleRoot || !predictionRoot) return;
                const candleTarget = {candle_target_ms};
                const predictionTarget = {prediction_target_ms};
                function formatLeft(ms) {{
                    const total = Math.max(0, Math.floor(ms / 1000));
                    const hours = Math.floor(total / 3600);
                    const minutes = Math.floor((total % 3600) / 60);
                    const seconds = total % 60;
                    if (hours > 0) {{
                        return `${{String(hours).padStart(2, '0')}}:${{String(minutes).padStart(2, '0')}}:${{String(seconds).padStart(2, '0')}}`;
                    }}
                    return `${{String(minutes).padStart(2, '0')}}:${{String(seconds).padStart(2, '0')}}`;
                }}
                function tick() {{
                    candleRoot.innerHTML = "Candle closes in: <strong>" + formatLeft(candleTarget - Date.now()) + "</strong>";
                    predictionRoot.innerHTML = "Prediction expires in: <strong>" + formatLeft(predictionTarget - Date.now()) + "</strong>";
                }}
                tick();
                const interval = setInterval(tick, 1000);
                window.addEventListener("beforeunload", () => clearInterval(interval), {{ once: true }});
            }})();
        </script>
        """,
        height=330,
        scrolling=False,
    )


def _render_top_timer(label: str, target_ts: pd.Timestamp, timer_prefix: str) -> None:
    target_ms = int(target_ts.timestamp() * 1000)
    initial = _format_countdown((target_ts - pd.Timestamp.utcnow().tz_localize(None)).total_seconds())
    html(
        f"""
        <div style="
            background: linear-gradient(180deg, rgba(8, 8, 8, 0.96), rgba(18, 18, 18, 0.92));
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 16px;
            padding: 10px 14px;
            box-shadow: 0 12px 32px rgba(0, 0, 0, 0.20);
            font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
            min-height: 84px;
        ">
            <div style="font-size:0.85rem; color:#8fa6c3; margin-bottom:10px;">{label}</div>
            <div id="{timer_prefix}-value" style="font-size:1.55rem; font-weight:700; color:#f8fbff;">{initial}</div>
        </div>
        <script>
            (function() {{
                const root = document.getElementById("{timer_prefix}-value");
                if (!root) return;
                const target = {target_ms};
                function formatLeft(ms) {{
                    const total = Math.max(0, Math.floor(ms / 1000));
                    const hours = Math.floor(total / 3600);
                    const minutes = Math.floor((total % 3600) / 60);
                    const seconds = total % 60;
                    if (hours > 0) {{
                        return `${{String(hours).padStart(2, '0')}}:${{String(minutes).padStart(2, '0')}}:${{String(seconds).padStart(2, '0')}}`;
                    }}
                    return `${{String(minutes).padStart(2, '0')}}:${{String(seconds).padStart(2, '0')}}`;
                }}
                function tick() {{
                    root.textContent = formatLeft(target - Date.now());
                }}
                tick();
                const interval = setInterval(tick, 1000);
                window.addEventListener("beforeunload", () => clearInterval(interval), {{ once: true }});
            }})();
        </script>
        """,
        height=104,
        scrolling=False,
    )


def _schedule_refresh_at(target_ts: pd.Timestamp, refresh_key: str) -> None:
    target_ms = int(target_ts.timestamp() * 1000)
    html(
        f"""
        <script>
            (function() {{
                const target = {target_ms};
                const key = "refresh-scheduled::{refresh_key}";
                const now = Date.now();
                const delay = Math.max(500, target - now + 800);
                const stored = window.sessionStorage.getItem(key);
                if (stored === String(target)) return;
                window.sessionStorage.setItem(key, String(target));
                window.setTimeout(function() {{
                    window.parent.location.reload();
                }}, delay);
            }})();
        </script>
        """,
        height=0,
        scrolling=False,
    )


def _render_signal_summary(signal) -> None:
    signal_class = signal.signal.lower()
    result_label = signal.features.get("_last_result_label", "Pending")
    result_class = str(signal.features.get("_last_result_class", "")).lower()
    signal_color = "#f59e0b"
    if signal_class == "up":
        signal_color = "#22c55e"
    elif signal_class == "down":
        signal_color = "#ef4444"
    result_color = "#f8fbff"
    if result_class == "up":
        result_color = "#22c55e"
    elif result_class == "down":
        result_color = "#ef4444"
    elif result_class == "neutral":
        result_color = "#f59e0b"
    html(
        f"""
        <div style="display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:10px; margin-top:10px; font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
            <div style="background:linear-gradient(180deg, rgba(8,8,8,0.96), rgba(18,18,18,0.92)); border:1px solid rgba(148,163,184,0.12); border-radius:14px; padding:12px; min-height:88px;">
                <div style="font-size:0.7rem; text-transform:uppercase; letter-spacing:0.08em; color:#7f93ad; margin-bottom:8px;">Signal</div>
                <div style="font-size:1.15rem; font-weight:700; color:{signal_color}; line-height:1.1;">{signal.signal}</div>
            </div>
            <div style="background:linear-gradient(180deg, rgba(8,8,8,0.96), rgba(18,18,18,0.92)); border:1px solid rgba(148,163,184,0.12); border-radius:14px; padding:12px; min-height:88px;">
                <div style="font-size:0.7rem; text-transform:uppercase; letter-spacing:0.08em; color:#7f93ad; margin-bottom:8px;">Confidence</div>
                <div style="font-size:1.15rem; font-weight:700; color:#f8fbff; line-height:1.1;">{signal.confidence:.2%}</div>
            </div>
            <div style="background:linear-gradient(180deg, rgba(8,8,8,0.96), rgba(18,18,18,0.92)); border:1px solid rgba(148,163,184,0.12); border-radius:14px; padding:12px; min-height:88px;">
                <div style="font-size:0.7rem; text-transform:uppercase; letter-spacing:0.08em; color:#7f93ad; margin-bottom:8px;">Result</div>
                <div style="font-size:1.15rem; font-weight:700; color:{result_color}; line-height:1.1;">{result_label}</div>
            </div>
        </div>
        """,
        height=120,
        scrolling=False,
    )


def _signal_state_key(product_id: str, timeframe_minutes: int) -> str:
    return f"signal_state::{product_id}::{timeframe_minutes}"


def _signal_history_size(candle_limit: int, timeframe_minutes: int) -> int:
    return max(candle_limit, 50) * timeframe_minutes


def _load_signal_state(product_id: str, timeframe_minutes: int, candle_limit: int) -> dict:
    refresh_time = pd.Timestamp.utcnow().tz_convert(None)
    current_bucket = _bucket_start(refresh_time, timeframe_minutes).isoformat()
    key = _signal_state_key(product_id, timeframe_minutes)
    stored = st.session_state.get(key)

    if stored and stored.get("bucket") == current_bucket:
        return stored

    base_candles = _load_live_candles(product_id, _signal_history_size(candle_limit, timeframe_minutes), 60)
    candles = aggregate_candles(base_candles, timeframe_minutes)
    closed_candles, live_candle = split_closed_and_live_candles(
        candles,
        now=refresh_time.tz_localize("UTC"),
        granularity_seconds=timeframe_minutes * 60,
    )
    signal_frame = closed_candles if len(closed_candles) >= 40 else candles.iloc[:-1].reset_index(drop=True)
    if len(signal_frame) < 40:
        raise RuntimeError("not enough closed candles to compute signal")

    signal = build_signal_for_interval(
        signal_frame,
        horizon_minutes=timeframe_minutes,
        bar_interval_minutes=timeframe_minutes,
    )
    price_source = live_candle if not live_candle.empty else candles.tail(1)
    latest_price = float(price_source["close"].iloc[-1])
    price_change = 0.0
    if len(signal_frame) > 5:
        price_change = (latest_price / float(signal_frame["close"].iloc[-6]) - 1.0) * 100.0
    last_result_label = "Pending"
    last_result_class = ""
    previous_summary = st.session_state.get(f"{key}::last_result")
    if previous_summary:
        last_result_label = previous_summary.get("label", last_result_label)
        last_result_class = previous_summary.get("class", last_result_class)

    if stored and stored.get("bucket") != current_bucket and not closed_candles.empty:
        previous_signal = _restore_signal(stored["signal"])
        resolved_candle = closed_candles.iloc[-1]
        candle_open = float(resolved_candle["open"])
        candle_close = float(resolved_candle["close"])
        if previous_signal.signal == "UP":
            is_win = candle_close > candle_open
            last_result_label = "Win" if is_win else "Loss"
            last_result_class = "up" if is_win else "down"
        elif previous_signal.signal == "DOWN":
            is_win = candle_close < candle_open
            last_result_label = "Win" if is_win else "Loss"
            last_result_class = "up" if is_win else "down"
        else:
            last_result_label = "No Trade"
            last_result_class = "neutral"
        st.session_state[f"{key}::last_result"] = {
            "label": last_result_label,
            "class": last_result_class,
        }
        append_prediction_log(
            product_id=product_id,
            timeframe_minutes=timeframe_minutes,
            bucket=str(stored.get("bucket", "")),
            signal=previous_signal,
            latest_price=candle_close,
            result_label=last_result_label,
        )

    signal.features["_last_result_label"] = last_result_label
    signal.features["_last_result_class"] = last_result_class

    stored = {
        "bucket": current_bucket,
        "next_close": _bucket_end(refresh_time, timeframe_minutes).isoformat(),
        "signal": asdict(signal),
        "latest_price": latest_price,
        "price_change": price_change,
    }
    st.session_state[key] = stored
    append_prediction_log(
        product_id=product_id,
        timeframe_minutes=timeframe_minutes,
        bucket=current_bucket,
        signal=signal,
        latest_price=latest_price,
        result_label="",
    )
    return stored


def _render_live_chart_component(product_id: str, timeframe_minutes: int, candle_limit: int) -> None:
    normalized_product = product_id.lower().replace("-", "-").replace("_", "-")
    component_id = f"live-chart-{normalized_product}-{timeframe_minutes}m"
    granularity_seconds = 60
    chart_limit = max(candle_limit * timeframe_minutes, timeframe_minutes * 50)
    html(
        f"""
        <div style="border:1px solid rgba(148, 163, 184, 0.14); border-radius:16px; padding:10px; background:linear-gradient(180deg, #080808 0%, #121212 100%);">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
                <div style="font-size:1rem; font-weight:600; color:#e5eefb;">Live Candles</div>
                <div id="{component_id}-status" style="font-size:0.85rem; color:#8fa6c3;">Connecting...</div>
            </div>
            <div id="{component_id}" style="height:680px;"></div>
            <div id="{component_id}-error" style="display:none; margin-top:8px; color:#ef4444; font-size:0.85rem; font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;"></div>
        </div>
        <script>
            (function() {{
                const root = document.getElementById("{component_id}");
                const status = document.getElementById("{component_id}-status");
                const errorRoot = document.getElementById("{component_id}-error");
                if (!root || !status || !errorRoot) return;

                const productId = {json.dumps(product_id)};
                const timeframeMinutes = {int(timeframe_minutes)};
                const candleLimit = {int(chart_limit)};
                const tickerEndpoint = "https://api.exchange.coinbase.com/products/" + productId + "/ticker";
                const websocketEndpoint = "wss://ws-feed.exchange.coinbase.com";
                const maxCandlesPerRequest = 300;
                let aggregatedData = [];
                let socket = null;
                let reconnectTimer = null;

                function showError(message) {{
                    status.textContent = "Update failed";
                    errorRoot.style.display = "block";
                    errorRoot.textContent = message;
                }}

                function normalize(rows) {{
                    return rows.slice().sort((a, b) => a[0] - b[0]).map((row) => ({{
                        time: new Date(row[0] * 1000).toISOString(),
                        low: Number(row[1]),
                        high: Number(row[2]),
                        open: Number(row[3]),
                        close: Number(row[4]),
                        volume: Number(row[5]),
                    }})).slice(-candleLimit);
                }}

                function localIso(dateIso) {{
                    const d = new Date(dateIso);
                    const year = d.getFullYear();
                    const month = String(d.getMonth() + 1).padStart(2, "0");
                    const day = String(d.getDate()).padStart(2, "0");
                    const hour = String(d.getHours()).padStart(2, "0");
                    const minute = String(d.getMinutes()).padStart(2, "0");
                    const second = String(d.getSeconds()).padStart(2, "0");
                    return `${{year}}-${{month}}-${{day}}T${{hour}}:${{minute}}:${{second}}`;
                }}

                function buildCandlesUrl(startIso, endIso) {{
                    const params = new URLSearchParams({{
                        granularity: "{granularity_seconds}",
                        start: startIso,
                        end: endIso,
                    }});
                    return "https://api.exchange.coinbase.com/products/" + productId + "/candles?" + params.toString();
                }}

                function bucketKey(dateIso) {{
                    const d = new Date(dateIso);
                    d.setUTCSeconds(0, 0);
                    d.setUTCMinutes(Math.floor(d.getUTCMinutes() / timeframeMinutes) * timeframeMinutes);
                    return d.toISOString();
                }}

                function aggregate(rows) {{
                    const buckets = new Map();
                    rows.forEach((row) => {{
                        const key = bucketKey(row.time);
                        if (!buckets.has(key)) {{
                            buckets.set(key, {{
                                time: key,
                                displayTime: localIso(key),
                                open: row.open,
                                high: row.high,
                                low: row.low,
                                close: row.close,
                                volume: row.volume,
                            }});
                            return;
                        }}
                        const current = buckets.get(key);
                        current.high = Math.max(current.high, row.high);
                        current.low = Math.min(current.low, row.low);
                        current.close = row.close;
                        current.volume += row.volume;
                    }});
                    return Array.from(buckets.values()).slice(-Math.max(80, Math.floor(candleLimit / timeframeMinutes)));
                }}

                function upsertLivePrice(lastPrice) {{
                    if (!Array.isArray(aggregatedData) || aggregatedData.length === 0) return;
                    const now = new Date();
                    const currentBucket = bucketKey(now.toISOString());
                    const price = Number(lastPrice);
                    if (!Number.isFinite(price)) return;
                    const last = aggregatedData[aggregatedData.length - 1];
                    if (!last || last.time !== currentBucket) {{
                        aggregatedData.push({{
                            time: currentBucket,
                            displayTime: localIso(currentBucket),
                            open: price,
                            high: price,
                            low: price,
                            close: price,
                            volume: 0,
                        }});
                        aggregatedData = aggregatedData.slice(-Math.max(80, Math.floor(candleLimit / timeframeMinutes)));
                        return;
                    }}
                    last.high = Math.max(last.high, price);
                    last.low = Math.min(last.low, price);
                    last.close = price;
                }}

                function render(data) {{
                    if (!window.Plotly) {{
                        showError("Plotly nao carregou no navegador.");
                        return;
                    }}
                    const x = data.map((item) => item.displayTime || item.time);
                    const open = data.map((item) => item.open);
                    const high = data.map((item) => item.high);
                    const low = data.map((item) => item.low);
                    const close = data.map((item) => item.close);
                    const volume = data.map((item) => item.volume);
                    const traces = [
                        {{
                            x, open, high, low, close,
                            type: "candlestick",
                            name: "Price",
                            increasing: {{ line: {{ color: "#22c55e" }} }},
                            decreasing: {{ line: {{ color: "#ef4444" }} }},
                            xaxis: "x",
                            yaxis: "y",
                        }},
                        {{
                            x, y: volume,
                            type: "bar",
                            name: "Volume",
                            marker: {{ color: "#64748b", opacity: 0.85 }},
                            xaxis: "x2",
                            yaxis: "y2",
                        }},
                    ];
                    const layout = {{
                        margin: {{ l: 28, r: 64, t: 20, b: 28 }},
                        showlegend: false,
                        paper_bgcolor: "#101010",
                        plot_bgcolor: "#101010",
                        xaxis: {{ domain: [0, 1], anchor: "y", rangeslider: {{ visible: false }}, showgrid: false, automargin: true, tickfont: {{ color: "#94a3b8", size: 11 }} }},
                        yaxis: {{ domain: [0.30, 1], title: "Price", titlefont: {{ color: "#cbd5e1", size: 12 }}, side: "right", automargin: true, tickfont: {{ color: "#cbd5e1", size: 11 }}, showgrid: true, gridcolor: "rgba(148,163,184,0.10)", color: "#cbd5e1" }},
                        xaxis2: {{ domain: [0, 1], anchor: "y2", showgrid: false, automargin: true, tickfont: {{ color: "#64748b", size: 10 }} }},
                        yaxis2: {{ domain: [0, 0.22], title: "Volume", titlefont: {{ color: "#94a3b8", size: 11 }}, side: "right", automargin: true, tickfont: {{ color: "#94a3b8", size: 10 }}, showgrid: true, gridcolor: "rgba(148,163,184,0.10)", color: "#94a3b8" }},
                        height: 660,
                        uirevision: "keep-zoom",
                        font: {{ color: "#cbd5e1" }},
                    }};
                    const config = {{ responsive: true, displayModeBar: false }};
                    if (!root.dataset.initialized) {{
                        Plotly.newPlot(root, traces, layout, config);
                        root.dataset.initialized = "true";
                    }} else {{
                        Plotly.update(root, {{
                            x: [x, x],
                            open: [open],
                            high: [high],
                            low: [low],
                            close: [close],
                            y: [volume],
                        }}, layout, [0, 1]);
                    }}
                    errorRoot.style.display = "none";
                }}

                async function fetchHistoryCandles() {{
                    let remaining = candleLimit;
                    let endCursor = new Date();
                    let rows = [];
                    while (remaining > 0) {{
                        const batch = Math.min(remaining, maxCandlesPerRequest);
                        const startCursor = new Date(endCursor.getTime() - batch * {granularity_seconds} * 1000);
                        const response = await fetch(buildCandlesUrl(startCursor.toISOString(), endCursor.toISOString()), {{ headers: {{ "Accept": "application/json" }} }});
                        if (!response.ok) throw new Error("HTTP " + response.status);
                        const payload = await response.json();
                        rows = payload.concat(rows);
                        endCursor = startCursor;
                        remaining -= batch;
                    }}
                    return rows;
                }}

                async function loadCandles() {{
                    try {{
                        status.textContent = "Updating...";
                        const raw = await fetchHistoryCandles();
                        const normalized = normalize(raw);
                        const data = aggregate(normalized);
                        if (!Array.isArray(data) || data.length === 0) throw new Error("Nenhum candle recebido.");
                        aggregatedData = data;
                        render(aggregatedData);
                        status.textContent = "Live " + new Date().toLocaleTimeString();
                    }} catch (error) {{
                        showError(String(error));
                    }}
                }}

                async function loadTicker() {{
                    try {{
                        const response = await fetch(tickerEndpoint, {{ headers: {{ "Accept": "application/json" }} }});
                        if (!response.ok) throw new Error("HTTP " + response.status);
                        const payload = await response.json();
                        upsertLivePrice(payload.price);
                        if (aggregatedData.length > 0) {{
                            render(aggregatedData);
                            status.textContent = "Live " + new Date().toLocaleTimeString();
                        }}
                    }} catch (error) {{
                        showError(String(error));
                    }}
                }}

                function connectTickerSocket() {{
                    try {{
                        if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {{
                            return;
                        }}
                        socket = new WebSocket(websocketEndpoint);
                        socket.onopen = function() {{
                            status.textContent = "Streaming...";
                            socket.send(JSON.stringify({{
                                type: "subscribe",
                                product_ids: [productId],
                                channels: ["ticker"]
                            }}));
                        }};
                        socket.onmessage = function(event) {{
                            try {{
                                const payload = JSON.parse(event.data);
                                if (payload.type === "ticker" && payload.product_id === productId && payload.price) {{
                                    upsertLivePrice(payload.price);
                                    if (aggregatedData.length > 0) {{
                                        render(aggregatedData);
                                        status.textContent = "Live " + new Date().toLocaleTimeString();
                                    }}
                                }}
                            }} catch (error) {{
                            }}
                        }};
                        socket.onerror = function() {{
                            status.textContent = "Socket reconnecting...";
                        }};
                        socket.onclose = function() {{
                            if (reconnectTimer) {{
                                clearTimeout(reconnectTimer);
                            }}
                            reconnectTimer = window.setTimeout(connectTickerSocket, 2000);
                        }};
                    }} catch (error) {{
                        status.textContent = "Socket failed";
                    }}
                }}

                function start() {{
                    loadCandles();
                    const candleInterval = setInterval(loadCandles, 60000);
                    loadTicker();
                    connectTickerSocket();
                    window.addEventListener("beforeunload", () => {{
                        clearInterval(candleInterval);
                        if (reconnectTimer) {{
                            clearTimeout(reconnectTimer);
                        }}
                        if (socket) {{
                            socket.close();
                        }}
                    }}, {{ once: true }});
                }}

                if (window.Plotly) {{
                    start();
                    return;
                }}
                const script = document.createElement("script");
                script.src = "https://cdn.plot.ly/plotly-2.35.2.min.js";
                script.async = true;
                script.onload = start;
                script.onerror = function() {{
                    showError("Falha ao carregar biblioteca do grafico.");
                }};
                document.head.appendChild(script);
            }})();
        </script>
        """,
        height=730,
        scrolling=False,
    )


@st.cache_data(ttl=2, show_spinner=False)
def _load_live_candles(product_id_value: str, candle_limit_value: int, granularity_seconds_value: int) -> pd.DataFrame:
    return fetch_candles(
        product_id_value,
        candle_limit=candle_limit_value,
        granularity_seconds=granularity_seconds_value,
    )


st.markdown(
    """
    <div class="terminal-topbar">
        <div>
            <div class="terminal-title">Crypto Direction Terminal</div>
            <div class="terminal-subtitle">Candles em tempo real e leitura direcional por timeframe.</div>
        </div>
        <div class="terminal-subtitle">Layout inspirado em plataforma de trade, sem modulo de entrada.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

product_id = st.session_state.get("asset_select", "BTC-USD")
selected_timeframe = st.session_state.get("timeframe_select", 5)

st.markdown('<div class="control-shell">', unsafe_allow_html=True)
control1, control2 = st.columns([1.6, 0.8], gap="medium")
with control1:
    candle_limit = st.slider("Candles no grafico", min_value=60, max_value=300, value=180, step=30)
with control2:
    show_table = st.checkbox("Mostrar tabela", value=False)
    reload_clicked = st.button("Recarregar", width="stretch")
st.markdown("</div>", unsafe_allow_html=True)

if reload_clicked:
    st.rerun()

state = None
state_error = None
try:
    state = _load_signal_state(product_id, selected_timeframe, candle_limit)
except Exception as exc:
    state_error = exc

chart_col, signal_col = st.columns([3.9, 1.35], gap="large")
with chart_col:
    st.markdown('<div class="terminal-shell">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">Live Chart</div>', unsafe_allow_html=True)
    _render_live_chart_component(product_id, selected_timeframe, candle_limit)
    st.markdown("</div>", unsafe_allow_html=True)

with signal_col:
    st.markdown('<div class="terminal-shell">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">Signal Panel</div>', unsafe_allow_html=True)
    if state_error:
        st.error(f"Falha ao calcular sinal: {state_error}")
    else:
        next_timeframe_close = pd.Timestamp(state["next_close"])
        frozen_signal = _restore_signal(state["signal"])
        timer_prefix = f"{product_id}-{selected_timeframe}m".lower().replace(" ", "-").replace("_", "-")
        _render_signal_card(frozen_signal, timer_prefix, next_timeframe_close, next_timeframe_close)
        st.markdown('<div class="section-label" style="margin-top:10px;">Signal Details</div>', unsafe_allow_html=True)
        _render_signal_summary(frozen_signal)
        _schedule_refresh_at(next_timeframe_close, f"{product_id}-{selected_timeframe}m")
    st.markdown("</div>", unsafe_allow_html=True)

metrics1, metrics2, metrics3, metrics4 = st.columns([0.95, 1.05, 1.2, 1.2])
with metrics1:
    st.selectbox("Asset", options=ASSET_OPTIONS, index=ASSET_OPTIONS.index(product_id), key="asset_select")
with metrics2:
    st.selectbox("Timeframe", options=TIMEFRAME_OPTIONS, index=TIMEFRAME_OPTIONS.index(selected_timeframe), format_func=lambda x: f"{x}m", key="timeframe_select")
with metrics3:
    if state:
        st.metric("Current Price", f"${float(state['latest_price']):,.2f}", delta=f"{float(state['price_change']):.2f}%")
    else:
        st.metric("Current Price", "--")
with metrics4:
    if state:
        _render_top_timer("Candle Ends In", pd.Timestamp(state["next_close"]), f"top-candle-{selected_timeframe}")
    else:
        st.metric("Candle Ends In", "--")

if show_table:
    try:
        candles = aggregate_candles(
            _load_live_candles(product_id, _signal_history_size(candle_limit, selected_timeframe), 60),
            selected_timeframe,
        ).tail(candle_limit)
        st.markdown('<div class="section-label" style="margin-top:18px;">Candle Table</div>', unsafe_allow_html=True)
        candle_display = candles.copy()
        candle_display["time"] = candle_display["time"].dt.tz_convert(None)
        st.dataframe(
            candle_display[["time", "open", "high", "low", "close", "volume"]].sort_values("time", ascending=False),
            width="stretch",
            hide_index=True,
        )
    except Exception as exc:
        st.error(f"Falha ao carregar candles: {exc}")
