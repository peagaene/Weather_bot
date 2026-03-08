from __future__ import annotations

import argparse
import csv
import json
import decimal
import os
from collections import Counter
from pathlib import Path
from typing import Dict

from .config import RuntimeConfig, RiskConfig, StrategyConfig
from .engine import TradingEngine
from .execution import PaperExecutionEngine
from .feeds import PolymarketFeed, SyntheticFeed
from .risk import RiskEngine
from .state import MarketState
from .strategy import MomentumStrategy


def _parse_market_map(raw: str | None) -> Dict[str, str]:
    if not raw:
        return {}
    raw = raw.strip()
    if not raw:
        return {}
    if raw.startswith("{") and raw.endswith("}"):
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            raise ValueError("Invalid POLYMARKET_TOKEN_MAP JSON format. Expected dict.")
        return {str(k).strip(): str(v).strip() for k, v in obj.items() if str(k).strip() and str(v).strip()}

    mapping: Dict[str, str] = {}
    for pair in raw.split(","):
        part = pair.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"Invalid mapping pair '{part}'. Use symbol:token_id")
        symbol, token_id = part.split(":", 1)
        symbol = symbol.strip()
        token_id = token_id.strip()
        if symbol and token_id:
            mapping[symbol] = token_id
    return mapping


def load_env_value(name: str, default: str) -> str:
    value = os.environ.get(name, str(default))
    path = Path(__file__).resolve().parents[2] / ".env"
    if path.exists() and name not in os.environ:
        for line in path.read_text(encoding="utf-8").splitlines():
            if "=" not in line or line.strip().startswith("#"):
                continue
            k, v = line.split("=", 1)
            if k.strip() == name:
                value = v.strip()
                break
    return value


def build_config(argv=None) -> RuntimeConfig:
    parser = argparse.ArgumentParser(description="Legacy paper trading bot simulator")
    parser.add_argument("--symbols", default=load_env_value("PAPERBOT_SYMBOLS", "BTCUSDC,ETHUSDC"))
    parser.add_argument("--ticks", type=int, default=int(load_env_value("PAPERBOT_TICKS", "300")))
    parser.add_argument("--interval", type=float, default=float(load_env_value("PAPERBOT_INTERVAL_SECONDS", "1.0")))

    parser.add_argument("--risk-max-trade", type=float, default=float(load_env_value("RISK_MAX_RISK_PER_TRADE", "0.01")))
    parser.add_argument("--risk-max-daily-loss", type=float, default=float(load_env_value("RISK_MAX_DAILY_LOSS", "0.03")))
    parser.add_argument("--risk-max-symbol", type=float, default=float(load_env_value("RISK_MAX_EXPOSURE_PER_SYMBOL", "250.0")))
    parser.add_argument("--risk-max-total", type=float, default=float(load_env_value("RISK_MAX_EXPOSURE_TOTAL", "600.0")))
    parser.add_argument("--risk-stop", type=float, default=float(load_env_value("RISK_STOP_LOSS_PCT", "0.08")))
    parser.add_argument("--risk-take", type=float, default=float(load_env_value("RISK_TAKE_PROFIT_PCT", "0.12")))
    parser.add_argument("--risk-min-score", type=float, default=float(load_env_value("RISK_MIN_SIGNAL_SCORE", "0.55")))

    parser.add_argument("--strategy-window", type=int, default=int(load_env_value("STRAT_WINDOW", "12")))
    parser.add_argument("--strategy-threshold", type=float, default=float(load_env_value("STRAT_THRESHOLD", "0.0025")))
    parser.add_argument("--strategy-size-bps", type=float, default=float(load_env_value("STRAT_SIZE_BPS", "0.20")))
    parser.add_argument(
        "--feed-mode",
        default=load_env_value("FEED_MODE", "polymarket"),
        choices=["synthetic", "polymarket"],
    )
    parser.add_argument(
        "--polymarket-token-map",
        default=load_env_value("POLYMARKET_TOKEN_MAP", ""),
        help="Mapeamento de simbolo para token id (ex.: BTCUSDC:0x123,ETHUSDC:0x456).",
    )
    parser.add_argument(
        "--polymarket-clob-base-url",
        default=load_env_value("POLYMARKET_CLOB_BASE_URL", "https://clob.polymarket.com"),
    )
    parser.add_argument(
        "--polymarket-gamma-base-url",
        default=load_env_value("POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"),
    )
    parser.add_argument(
        "--polymarket-request-timeout",
        type=float,
        default=float(load_env_value("POLYMARKET_REQUEST_TIMEOUT", "4.0")),
    )
    parser.add_argument(
        "--paper-close-on-end",
        action="store_true",
        help="Close all open positions at the end of the simulation to realize PnL.",
    )
    args = parser.parse_args(argv)
    token_map = _parse_market_map(args.polymarket_token_map)

    risk = RiskConfig(
        max_risk_per_trade=args.risk_max_trade,
        max_daily_loss=args.risk_max_daily_loss,
        max_exposure_per_symbol=args.risk_max_symbol,
        max_exposure_total=args.risk_max_total,
        stop_loss_pct=args.risk_stop,
        take_profit_pct=args.risk_take,
        min_signal_score=args.risk_min_score,
    )
    strategy = StrategyConfig(
        window=args.strategy_window,
        threshold=args.strategy_threshold,
        size_bps=args.strategy_size_bps,
        min_score=args.risk_min_score,
    )
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    return RuntimeConfig(
        symbols=symbols,
        ticks=args.ticks,
        interval_seconds=args.interval,
        risk=risk,
        strategy=strategy,
        close_positions_on_end=args.paper_close_on_end,
        feed_mode=args.feed_mode,
        polymarket_token_map=token_map,
        polymarket_clob_base_url=args.polymarket_clob_base_url,
        polymarket_gamma_base_url=args.polymarket_gamma_base_url,
        polymarket_request_timeout=args.polymarket_request_timeout,
    )


def _validate_config(config: RuntimeConfig) -> None:
    if not config.symbols:
        raise ValueError("No symbols provided.")
    if config.ticks <= 0:
        raise ValueError("Ticks must be positive.")
    if config.ticks > 1_000_000:
        raise ValueError("Ticks too high.")
    if config.strategy.window < 2:
        raise ValueError("Strategy window must be >= 2.")
    if config.strategy.size_bps <= 0:
        raise ValueError("Strategy size bps must be greater than 0.")
    if config.risk.max_exposure_total <= 0:
        raise ValueError("Max total exposure must be positive.")
    if config.risk.max_exposure_per_symbol <= 0:
        raise ValueError("Max per-symbol exposure must be positive.")
    if config.interval_seconds <= 0:
        raise ValueError("Interval seconds must be positive.")
    if not (0 < config.risk.max_risk_per_trade < 1):
        raise ValueError("Risk per trade must be between 0 and 1.")
    if not (0 < config.risk.max_daily_loss < 1):
        raise ValueError("Max daily loss must be between 0 and 1.")
    if not (0 <= config.risk.min_signal_score <= 1):
        raise ValueError("Min signal score must be between 0 and 1.")
    if config.feed_mode != "synthetic" and config.feed_mode != "polymarket":
        raise ValueError("feed_mode must be 'synthetic' or 'polymarket'")


def _build_feed(config: RuntimeConfig, symbols: list[str]):
    if config.feed_mode == "synthetic":
        return SyntheticFeed(
            symbols=symbols,
            spread_bps=7.0,
            base_prices={s: 100.0 + idx * 60 for idx, s in enumerate(symbols)},
        )
    return PolymarketFeed(
        symbols=symbols,
        symbol_to_token_id=config.polymarket_token_map,
        clob_base_url=config.polymarket_clob_base_url,
        gamma_base_url=config.polymarket_gamma_base_url,
        request_timeout=config.polymarket_request_timeout,
    )


def run_simulation(argv=None):
    config = build_config(argv)
    _validate_config(config)
    feed = _build_feed(config, config.symbols)
    state = MarketState(equity=1000.0, cash=1000.0, starting_equity=1000.0)
    strategy = MomentumStrategy(config.strategy)
    risk = RiskEngine(config.risk)
    execution = PaperExecutionEngine()
    engine = TradingEngine(config, feed, strategy, risk, execution, state)
    return config, engine.run(config.ticks, close_positions=config.close_positions_on_end)


def _build_run_summary(result) -> dict:
    action_counts = Counter(item["action"] for item in result.actions)
    reject_reasons = Counter(
        item["reason"] for item in result.actions if item.get("action") == "reject" and item.get("reason")
    )
    action_reasons = Counter(item["reason"] for item in result.actions if item.get("reason"))
    return {
        "ticks": result.total_ticks,
        "final_equity": result.final_equity,
        "realized_pnl": result.realized_pnl,
        "unrealized_pnl": result.unrealized_pnl,
        "total_exposure": result.total_exposure,
        "action_counts": dict(action_counts),
        "top_reject_reasons": dict(reject_reasons.most_common(5)),
        "top_action_reasons": dict(action_reasons.most_common(5)),
    }


_CSV_HISTORY_FIELDS: list[str] = [
    "step",
    "tick",
    "ts",
    "symbol",
    "price",
    "action",
    "reason",
    "score",
    "signal",
    "signal_size",
    "fill_size",
    "fill_price",
    "fill_pnl",
    "qty",
    "equity",
    "cash",
    "realized_pnl",
    "unrealized_pnl",
    "total_exposure",
]


def _normalize_csv_row(row: dict) -> dict:
    normalized: dict[str, str | float | int] = {}
    for field in _CSV_HISTORY_FIELDS:
        value = row.get(field, "")
        if isinstance(value, (dict, list, tuple)):
            value = json.dumps(value, ensure_ascii=False)
        if isinstance(value, decimal.Decimal):
            value = float(value)
        normalized[field] = value
    return normalized


def _save_results(result, out_json: str | None = None, out_csv: str | None = None) -> None:
    if out_json:
        payload = _build_run_summary(result)
        payload["actions"] = result.actions
        payload["history"] = result.history
        Path(out_json).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if out_csv:
        with Path(out_csv).open("w", encoding="utf-8", newline="") as f:
            if not result.history:
                return
            writer = csv.DictWriter(f, fieldnames=_CSV_HISTORY_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for row in result.history:
                if not isinstance(row, dict):
                    continue
                writer.writerow(_normalize_csv_row(row))


def main(argv=None):
    config, result = run_simulation(argv)
    summary = _build_run_summary(result)

    print("Paper run finished")
    print(json.dumps(
        {
            "ticks": summary["ticks"],
            "final_equity": summary["final_equity"],
            "realized_pnl": summary["realized_pnl"],
            "unrealized_pnl": summary["unrealized_pnl"],
            "total_exposure": summary["total_exposure"],
            "action_counts": summary["action_counts"],
            "actions": result.actions[:20],
            "actions_total": len(result.actions),
        },
        indent=2,
    ))

    out_json = os.getenv("PAPERBOT_EXPORT_JSON")
    out_csv = os.getenv("PAPERBOT_EXPORT_CSV")
    if out_json or out_csv:
        if out_json:
            Path(out_json).parent.mkdir(parents=True, exist_ok=True)
        if out_csv:
            Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
        _save_results(result, out_json or None, out_csv or None)


if __name__ == "__main__":
    main()

