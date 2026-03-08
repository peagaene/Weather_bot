from __future__ import annotations

import argparse
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


COINBASE_EXCHANGE_BASE_URL = "https://api.exchange.coinbase.com"
MAX_CANDLES_PER_REQUEST = 300


@dataclass
class HorizonSignal:
    horizon: str
    price: float
    signal: str
    confidence: float
    prob_up: float
    prob_down: float
    score: float
    features: dict[str, float]


@dataclass
class SignalSnapshot:
    ts: str
    product_id: str
    price: float
    horizons: list[HorizonSignal]


@dataclass
class BacktestResult:
    product_id: str
    timeframe_minutes: int
    total_rows: int
    actionable_rows: int
    wins: int
    losses: int
    no_trade: int
    accuracy: float
    output_csv: str


def _request_json(url: str) -> list:
    request = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "paper-bot-realtime-signal/1.0",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=8.0) as response:
            payload = response.read().decode("utf-8")
            data = json.loads(payload)
            if not isinstance(data, list):
                raise RuntimeError("unexpected response format from Coinbase candles endpoint")
            return data
    except urllib.error.HTTPError as err:
        raise RuntimeError(f"Coinbase candles request failed with HTTP {err.code}") from err
    except urllib.error.URLError as err:
        raise RuntimeError(f"Coinbase candles request failed: {err}") from err


def fetch_candles(
    product_id: str,
    *,
    granularity_seconds: int = 60,
    candle_limit: int = 180,
    base_url: str = COINBASE_EXCHANGE_BASE_URL,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pd.DataFrame:
    def _to_exchange_iso(ts: datetime) -> str:
        return ts.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _frame_from_rows(rows: list) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["time", "low", "high", "open", "close", "volume"])
        frame = pd.DataFrame(
            rows,
            columns=["time", "low", "high", "open", "close", "volume"],
        )
        for column in ["low", "high", "open", "close", "volume"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame["time"] = pd.to_datetime(frame["time"], unit="s", utc=True)
        return frame.sort_values("time").dropna().reset_index(drop=True)

    if candle_limit <= MAX_CANDLES_PER_REQUEST and start is None and end is None:
        params = urllib.parse.urlencode({"granularity": str(granularity_seconds)})
        url = f"{base_url.rstrip('/')}/products/{product_id}/candles?{params}"
        frame = _frame_from_rows(_request_json(url))
        if frame.empty:
            raise RuntimeError("empty candle response")
        if len(frame) > candle_limit:
            frame = frame.tail(candle_limit).reset_index(drop=True)
        return frame

    end_cursor = (end or datetime.now(timezone.utc)).astimezone(timezone.utc)
    start_cursor = start.astimezone(timezone.utc) if start else None
    remaining = max(1, int(candle_limit))
    chunks: list[pd.DataFrame] = []

    while remaining > 0:
        batch_size = min(remaining, MAX_CANDLES_PER_REQUEST)
        batch_start = end_cursor - timedelta(seconds=granularity_seconds * batch_size)
        if start_cursor is not None and batch_start < start_cursor:
            batch_start = start_cursor

        params = {
            "granularity": str(granularity_seconds),
            "start": _to_exchange_iso(batch_start),
            "end": _to_exchange_iso(end_cursor),
        }
        url = f"{base_url.rstrip('/')}/products/{product_id}/candles?{urllib.parse.urlencode(params)}"
        frame = _frame_from_rows(_request_json(url))
        if not frame.empty:
            chunks.append(frame)
        if start_cursor is not None and batch_start <= start_cursor:
            break
        end_cursor = batch_start
        remaining -= batch_size

    if not chunks:
        raise RuntimeError("empty candle response")

    merged = (
        pd.concat(chunks, ignore_index=True)
        .drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )
    if len(merged) > candle_limit:
        merged = merged.tail(candle_limit).reset_index(drop=True)
    return merged


def split_closed_and_live_candles(
    frame: pd.DataFrame,
    *,
    now: pd.Timestamp | None = None,
    granularity_seconds: int = 60,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return frame.copy(), frame.copy()

    current_time = now
    if current_time is None:
        current_time = pd.Timestamp.now(tz="UTC")
    elif current_time.tzinfo is None:
        current_time = current_time.tz_localize("UTC")
    else:
        current_time = current_time.tz_convert("UTC")

    ordered = frame.sort_values("time").reset_index(drop=True)
    last_open = ordered["time"].iloc[-1]
    last_close = last_open + pd.Timedelta(seconds=granularity_seconds)

    if current_time < last_close and len(ordered) > 1:
        return ordered.iloc[:-1].reset_index(drop=True), ordered.iloc[-1:].reset_index(drop=True)
    return ordered, ordered.iloc[0:0].copy()


def aggregate_candles(frame: pd.DataFrame, timeframe_minutes: int) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    working = frame.copy().sort_values("time").set_index("time")
    rule = f"{int(timeframe_minutes)}min"
    aggregated = pd.DataFrame(
        {
            "open": working["open"].resample(rule, label="left", closed="left").first(),
            "high": working["high"].resample(rule, label="left", closed="left").max(),
            "low": working["low"].resample(rule, label="left", closed="left").min(),
            "close": working["close"].resample(rule, label="left", closed="left").last(),
            "volume": working["volume"].resample(rule, label="left", closed="left").sum(),
        }
    ).dropna()
    return aggregated.reset_index()


def _sigmoid(value: float) -> float:
    clipped = max(-12.0, min(12.0, value))
    return 1.0 / (1.0 + math.exp(-clipped))


def _rsi(close: pd.Series, period: int = 14) -> float:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.rolling(period).mean().iloc[-1]
    avg_loss = loss.rolling(period).mean().iloc[-1]
    if pd.isna(avg_gain) or pd.isna(avg_loss):
        return 50.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _rolling_volatility(close: pd.Series, window: int = 20) -> float:
    returns = close.pct_change().dropna()
    if len(returns) < max(5, window // 2):
        return 0.0
    value = returns.tail(window).std()
    if pd.isna(value):
        return 0.0
    return float(value)


def _atr(frame: pd.DataFrame, window: int = 14) -> float:
    if len(frame) < max(3, window):
        return 0.0
    high = frame["high"]
    low = frame["low"]
    prev_close = frame["close"].shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    value = tr.rolling(window).mean().iloc[-1]
    if pd.isna(value):
        return 0.0
    return float(value)


def _safe_pct_change(series: pd.Series, periods: int) -> float:
    if len(series) <= periods:
        return 0.0
    value = series.pct_change(periods).iloc[-1]
    if pd.isna(value):
        return 0.0
    return float(value)


def build_signal(frame: pd.DataFrame, horizon_minutes: int) -> HorizonSignal:
    return build_signal_for_interval(frame, horizon_minutes=horizon_minutes, bar_interval_minutes=1)


def build_signal_for_interval(
    frame: pd.DataFrame,
    *,
    horizon_minutes: int,
    bar_interval_minutes: int,
) -> HorizonSignal:
    if len(frame) < 40:
        raise RuntimeError("not enough candles to compute signal")

    close = frame["close"]
    volume = frame["volume"]
    price = float(close.iloc[-1])

    interval = max(1, int(bar_interval_minutes))
    lookback = max(2, math.ceil(horizon_minutes / interval))
    short_ma_window = max(3, lookback)
    long_ma_window = max(short_ma_window + 2, lookback * 3)
    breakout_window = max(10, lookback * 4)

    momentum = float(close.pct_change(lookback).iloc[-1])
    trend = float((close.rolling(short_ma_window).mean().iloc[-1] - close.rolling(long_ma_window).mean().iloc[-1]) / price)
    rsi_value = _rsi(close, period=14)
    rsi_centered = (rsi_value - 50.0) / 50.0
    volatility = _rolling_volatility(close, window=20)
    volume_ratio = float(volume.tail(short_ma_window).mean() / max(1e-12, volume.tail(long_ma_window).mean()))
    breakout = float((price - close.tail(breakout_window).min()) / max(1e-12, close.tail(breakout_window).max() - close.tail(breakout_window).min()))
    ret_1 = _safe_pct_change(close, 1)
    ret_3 = _safe_pct_change(close, min(3, len(close) - 1))
    ret_8 = _safe_pct_change(close, min(8, len(close) - 1))
    atr_value = _atr(frame, window=14)
    atr_pct = atr_value / max(1e-12, price)
    candle_range = float((frame["high"].iloc[-1] - frame["low"].iloc[-1]) / max(1e-12, price))
    candle_body = float((frame["close"].iloc[-1] - frame["open"].iloc[-1]) / max(1e-12, price))
    ma_gap = float((close.rolling(short_ma_window).mean().iloc[-1] - price) / max(1e-12, price))
    trend_strength = abs(trend)

    score = (
        momentum * 9.0
        + trend * 14.0
        + rsi_centered * 0.9
        + (volume_ratio - 1.0) * 0.35
        + (breakout - 0.5) * 1.2
        - volatility * 6.0
        + ret_1 * 2.5
        + ret_3 * 3.5
        + ret_8 * 2.0
        + candle_body * 4.0
        - atr_pct * 2.2
        - candle_range * 1.2
        - abs(ma_gap) * 1.8
        + trend_strength * 1.1
    )
    prob_up = _sigmoid(score)
    prob_down = 1.0 - prob_up

    if prob_up >= 0.55:
        signal = "UP"
    elif prob_up <= 0.45:
        signal = "DOWN"
    else:
        signal = "NEUTRAL"

    confidence = abs(prob_up - 0.5) * 2.0
    return HorizonSignal(
        horizon=f"{horizon_minutes}m",
        price=round(price, 2),
        signal=signal,
        confidence=round(confidence, 4),
        prob_up=round(prob_up, 4),
        prob_down=round(prob_down, 4),
        score=round(score, 4),
        features={
            "momentum": round(momentum, 6),
            "trend": round(trend, 6),
            "rsi": round(rsi_value, 2),
            "volatility": round(volatility, 6),
            "volume_ratio": round(volume_ratio, 4),
            "breakout_position": round(breakout, 4),
            "ret_1": round(ret_1, 6),
            "ret_3": round(ret_3, 6),
            "ret_8": round(ret_8, 6),
            "atr_pct": round(atr_pct, 6),
            "candle_range": round(candle_range, 6),
            "candle_body": round(candle_body, 6),
            "ma_gap": round(ma_gap, 6),
            "trend_strength": round(trend_strength, 6),
        },
    )


def append_prediction_log(
    *,
    product_id: str,
    timeframe_minutes: int,
    bucket: str,
    signal: HorizonSignal,
    latest_price: float,
    result_label: str | None = None,
) -> None:
    export_dir = Path(os.getenv("PAPERBOT_SIGNAL_EXPORT_DIR", "export")) / "signals"
    export_dir.mkdir(parents=True, exist_ok=True)
    out_path = export_dir / "prediction_log.csv"
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "product_id": product_id,
        "timeframe_minutes": timeframe_minutes,
        "bucket": bucket,
        "signal": signal.signal,
        "confidence": signal.confidence,
        "prob_up": signal.prob_up,
        "prob_down": signal.prob_down,
        "score": signal.score,
        "latest_price": round(latest_price, 6),
        "result": result_label or "",
        **{f"feature_{k}": v for k, v in signal.features.items() if not k.startswith("_")},
    }
    frame = pd.DataFrame([row])
    if out_path.exists():
        frame.to_csv(out_path, mode="a", header=False, index=False)
    else:
        frame.to_csv(out_path, index=False)


def run_signal_backtest(
    *,
    product_id: str,
    timeframe_minutes: int,
    candle_limit: int,
    output_csv: str | None = None,
) -> BacktestResult:
    base_limit = max(candle_limit * timeframe_minutes + 80, 300)
    base_frame = fetch_candles(product_id, granularity_seconds=60, candle_limit=base_limit)
    frame = aggregate_candles(base_frame, timeframe_minutes)
    if len(frame) < 60:
        raise RuntimeError("not enough historical candles to run backtest")

    rows: list[dict] = []
    wins = 0
    losses = 0
    no_trade = 0

    for idx in range(40, len(frame) - 1):
        history = frame.iloc[: idx + 1].reset_index(drop=True)
        next_candle = frame.iloc[idx + 1]
        signal = build_signal_for_interval(
            history,
            horizon_minutes=timeframe_minutes,
            bar_interval_minutes=timeframe_minutes,
        )
        next_open = float(next_candle["open"])
        next_close = float(next_candle["close"])
        if signal.signal == "UP":
            result = "Win" if next_close > next_open else "Loss"
        elif signal.signal == "DOWN":
            result = "Win" if next_close < next_open else "Loss"
        else:
            result = "No Trade"

        if result == "Win":
            wins += 1
        elif result == "Loss":
            losses += 1
        else:
            no_trade += 1

        row = {
            "bucket": history["time"].iloc[-1].isoformat(),
            "product_id": product_id,
            "timeframe_minutes": timeframe_minutes,
            "signal": signal.signal,
            "confidence": signal.confidence,
            "prob_up": signal.prob_up,
            "prob_down": signal.prob_down,
            "score": signal.score,
            "current_close": float(history["close"].iloc[-1]),
            "next_open": next_open,
            "next_close": next_close,
            "result": result,
        }
        for feature_key, feature_value in signal.features.items():
            if feature_key.startswith("_"):
                continue
            row[f"feature_{feature_key}"] = feature_value
        rows.append(row)

    actionable_rows = wins + losses
    accuracy = wins / actionable_rows if actionable_rows else 0.0
    output_path = Path(output_csv) if output_csv else Path(os.getenv("PAPERBOT_SIGNAL_EXPORT_DIR", "export")) / "signals" / f"backtest_{product_id.replace('-', '_')}_{timeframe_minutes}m.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_path, index=False)

    return BacktestResult(
        product_id=product_id,
        timeframe_minutes=timeframe_minutes,
        total_rows=len(rows),
        actionable_rows=actionable_rows,
        wins=wins,
        losses=losses,
        no_trade=no_trade,
        accuracy=round(accuracy, 4),
        output_csv=str(output_path),
    )


def generate_snapshot(product_id: str, horizons: Iterable[int], candle_limit: int) -> SignalSnapshot:
    frame = fetch_candles(product_id, candle_limit=candle_limit)
    closed_frame, live_frame = split_closed_and_live_candles(frame)
    signal_frame = closed_frame if not closed_frame.empty else frame
    price_source = live_frame if not live_frame.empty else frame
    price = float(price_source["close"].iloc[-1])
    signals = [build_signal_for_interval(signal_frame, horizon_minutes=h, bar_interval_minutes=1) for h in horizons]
    return SignalSnapshot(
        ts=datetime.now(timezone.utc).isoformat(),
        product_id=product_id,
        price=round(price, 2),
        horizons=signals,
    )


def format_snapshot(snapshot: SignalSnapshot) -> str:
    lines = [
        f"time={snapshot.ts}",
        f"product={snapshot.product_id}",
        f"price={snapshot.price}",
    ]
    for signal in snapshot.horizons:
        lines.append(
            f"{signal.horizon}: signal={signal.signal} confidence={signal.confidence:.4f} "
            f"prob_up={signal.prob_up:.4f} score={signal.score:.4f}"
        )
    return "\n".join(lines)


def _parse_horizons(raw: str) -> list[int]:
    horizons: list[int] = []
    for item in raw.split(","):
        value = item.strip().lower().removesuffix("m")
        if not value:
            continue
        minute_value = int(value)
        if minute_value <= 0:
            raise ValueError("horizons must be positive integers")
        horizons.append(minute_value)
    if not horizons:
        raise ValueError("at least one horizon is required")
    return horizons


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate real-time BTC/crypto direction signals.")
    parser.add_argument("--product-id", default="BTC-USD", help="Coinbase product id, ex: BTC-USD or ETH-USD")
    parser.add_argument("--horizons", default="5,15", help="Prediction horizons in minutes, ex: 5,15")
    parser.add_argument("--candle-limit", type=int, default=180, help="How many 1m candles to load")
    parser.add_argument("--poll-seconds", type=float, default=0.0, help="If > 0, keep polling at this interval")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text")
    parser.add_argument("--backtest", action="store_true", help="Run offline backtest instead of live snapshot")
    parser.add_argument("--timeframe", type=int, default=5, help="Backtest timeframe in minutes")
    parser.add_argument("--output-csv", default=None, help="Optional output CSV path for backtest")
    args = parser.parse_args(argv)

    if args.backtest:
        result = run_signal_backtest(
            product_id=args.product_id,
            timeframe_minutes=args.timeframe,
            candle_limit=args.candle_limit,
            output_csv=args.output_csv,
        )
        print(json.dumps(asdict(result), indent=2))
        return

    horizons = _parse_horizons(args.horizons)
    while True:
        snapshot = generate_snapshot(args.product_id, horizons, args.candle_limit)
        if args.json:
            print(json.dumps(asdict(snapshot), indent=2))
        else:
            print(format_snapshot(snapshot))
        if args.poll_seconds <= 0:
            return
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
