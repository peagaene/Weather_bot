from __future__ import annotations

import argparse
import json
import os
import uuid
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.env import load_app_env
from paperbot.history import append_csv_rows, write_json
from paperbot.live_trader import execute_order_plan, sync_live_exchange_state
from paperbot.polymarket_live import build_order_plan, summarize_plan
from paperbot.polymarket_weather import scan_weather_model_opportunities
from paperbot.reconciliation import sync_open_positions, sync_prediction_resolutions
from paperbot.selection import explain_blocked_opportunities, filter_opportunities, summarize_filter_rejections
from paperbot.storage import WeatherBotStorage
from paperbot.trading_state import FileLock, TradingStateStore
from paperbot.weather_models import (
    _load_source_weight_profile,
    _load_truth_weight_profile,
    fetch_meteostat_observed_daily_highs,
    fetch_noaa_isd_observed_daily_highs,
    fetch_nws_observed_daily_highs,
    nws_station_id,
)
from paperbot.degendoppler import CITY_CONFIGS, _parse_bucket_range
from run_prediction_analysis import generate_prediction_analysis
from run_source_weight_analysis import generate_source_weight_analysis
from run_truth_weight_analysis import generate_truth_weight_analysis

load_app_env(ROOT)

def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


def _ensure_policy_profile_fresh() -> None:
    enabled = str(os.getenv("WEATHER_POLICY_AUTO_REFRESH_PROFILE", "1")).strip().lower() not in {"0", "false", "no"}
    if not enabled:
        return
    db_path = _resolve_path(os.getenv("WEATHER_DB_PATH", "export/db/weather_bot.db"))
    if not db_path.exists():
        return
    analysis_path = _resolve_path(os.getenv("WEATHER_PREDICTION_ANALYSIS_PATH", "export/analysis/prediction_analysis.json"))
    profile_path = _resolve_path(os.getenv("WEATHER_POLICY_PROFILE_PATH", "export/analysis/policy_profile.json"))
    max_age_hours = max(1.0, float(os.getenv("WEATHER_POLICY_PROFILE_MAX_AGE_HOURS", "24") or 24))
    max_age_seconds = max_age_hours * 3600.0
    min_samples = max(1, int(os.getenv("WEATHER_POLICY_PROFILE_MIN_SAMPLES", "25") or 25))
    top_segments = max(1, int(os.getenv("WEATHER_POLICY_PROFILE_TOP_SEGMENTS", "10") or 10))
    now = time.time()
    if profile_path.exists():
        age_seconds = max(0.0, now - profile_path.stat().st_mtime)
        if age_seconds <= max_age_seconds:
            return
    try:
        generate_prediction_analysis(
            db_path=db_path,
            min_samples=min_samples,
            top_segments=top_segments,
            output_json=analysis_path,
            policy_profile_json=profile_path,
        )
        print(f"policy profile atualizado: {profile_path}")
    except Exception as exc:
        print(f"AVISO: falha ao atualizar policy profile: {_sanitize_text(exc)}")


def _ensure_source_weight_profile_fresh() -> None:
    enabled = str(os.getenv("WEATHER_SOURCE_WEIGHT_AUTO_REFRESH", "1")).strip().lower() not in {"0", "false", "no"}
    if not enabled:
        return
    db_path = _resolve_path(os.getenv("WEATHER_DB_PATH", "export/db/weather_bot.db"))
    if not db_path.exists():
        return
    analysis_path = _resolve_path(
        os.getenv("WEATHER_SOURCE_WEIGHT_ANALYSIS_PATH", "export/analysis/source_weight_analysis.json")
    )
    profile_path = _resolve_path(
        os.getenv("WEATHER_SOURCE_WEIGHT_PROFILE_PATH", "export/analysis/source_weight_profile.json")
    )
    max_age_hours = max(1.0, float(os.getenv("WEATHER_SOURCE_WEIGHT_PROFILE_MAX_AGE_HOURS", "24") or 24))
    max_age_seconds = max_age_hours * 3600.0
    min_samples = max(1, int(os.getenv("WEATHER_SOURCE_WEIGHT_MIN_SAMPLES", "50") or 50))
    top_segments = max(1, int(os.getenv("WEATHER_SOURCE_WEIGHT_TOP_SEGMENTS", "10") or 10))
    now = time.time()
    if profile_path.exists():
        age_seconds = max(0.0, now - profile_path.stat().st_mtime)
        if age_seconds <= max_age_seconds:
            return
    try:
        generate_source_weight_analysis(
            db_path=db_path,
            min_samples=min_samples,
            top_segments=top_segments,
            output_json=analysis_path,
            profile_json=profile_path,
        )
        _load_source_weight_profile.cache_clear()
        print(f"source weight profile atualizado: {profile_path}")
    except Exception as exc:
        print(f"AVISO: falha ao atualizar source weight profile: {_sanitize_text(exc)}")


def _ensure_truth_weight_profile_fresh() -> None:
    enabled = str(os.getenv("WEATHER_TRUTH_WEIGHT_AUTO_REFRESH", "1")).strip().lower() not in {"0", "false", "no"}
    if not enabled:
        return
    db_path = _resolve_path(os.getenv("WEATHER_DB_PATH", "export/db/weather_bot.db"))
    if not db_path.exists():
        return
    analysis_path = _resolve_path(
        os.getenv("WEATHER_TRUTH_WEIGHT_ANALYSIS_PATH", "export/analysis/truth_weight_analysis.json")
    )
    profile_path = _resolve_path(
        os.getenv("WEATHER_TRUTH_WEIGHT_PROFILE_PATH", "export/analysis/truth_weight_profile.json")
    )
    max_age_hours = max(1.0, float(os.getenv("WEATHER_TRUTH_WEIGHT_PROFILE_MAX_AGE_HOURS", "24") or 24))
    max_age_seconds = max_age_hours * 3600.0
    min_samples = max(1, int(os.getenv("WEATHER_TRUTH_WEIGHT_MIN_SAMPLES", "20") or 20))
    top_segments = max(1, int(os.getenv("WEATHER_TRUTH_WEIGHT_TOP_SEGMENTS", "10") or 10))
    now = time.time()
    if profile_path.exists():
        age_seconds = max(0.0, now - profile_path.stat().st_mtime)
        if age_seconds <= max_age_seconds:
            return
    try:
        generate_truth_weight_analysis(
            db_path=db_path,
            min_samples=min_samples,
            top_segments=top_segments,
            output_json=analysis_path,
            profile_json=profile_path,
        )
        _load_truth_weight_profile.cache_clear()
        print(f"truth weight profile atualizado: {profile_path}")
    except Exception as exc:
        print(f"AVISO: falha ao atualizar truth weight profile: {_sanitize_text(exc)}")


def _build_history_rows(run_id: str, selected: list, plans: list, executions: list, generated_at: str) -> list[dict]:
    rows: list[dict] = []
    for rank, (opportunity, plan, execution) in enumerate(zip(selected, plans, executions), start=1):
        sanitized_execution = _sanitize_execution_for_export(execution.as_dict())
        rows.append(
            {
                "run_id": run_id,
                "generated_at": generated_at,
                "rank": rank,
                **opportunity.as_dict(),
                **{f"plan_{k}": v for k, v in asdict(plan).items()},
                **{f"exec_{k}": v for k, v in sanitized_execution.items()},
            }
        )
    return rows


def _sanitize_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for env_key in (
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_API_PASSPHRASE",
        "POLYMARKET_FUNDER",
    ):
        secret = os.getenv(env_key, "").strip()
        if secret:
            text = text.replace(secret, f"<redacted:{env_key.lower()}>")
    return text[:240]


def _sanitize_identifier(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) <= 8:
        return "***"
    return f"{text[:4]}...{text[-4:]}"


def _sanitize_execution_for_export(item: dict) -> dict:
    sanitized = dict(item)
    sanitized["response"] = {}
    sanitized["fills"] = []
    sanitized["has_response"] = bool(item.get("response"))
    sanitized["fills_count"] = len(item.get("fills") or [])
    sanitized["client_order_id"] = _sanitize_identifier(item.get("client_order_id"))
    sanitized["exchange_order_id"] = _sanitize_identifier(item.get("exchange_order_id"))
    sanitized["nonce"] = None
    sanitized["submission_fingerprint"] = None
    sanitized["error"] = _sanitize_text(item.get("error"))
    return sanitized


def _build_export_payload(
    *,
    generated_at: str,
    run_id: str,
    filters: dict,
    raw_count: int,
    selected: list,
    blocked_opportunities: list[dict],
    rejection_summary: dict,
    plans: list,
    executions: list,
    execute_count: int,
) -> dict:
    return {
        "generated_at": generated_at,
        "run_id": run_id,
        "filters": dict(filters),
        "configured_execute_top": int(filters.get("execute_top", 0) or 0),
        "actual_execute_count": int(execute_count),
        "raw_count": raw_count,
        "count": len(selected),
        "filter_rejections": rejection_summary,
        "blocked_opportunities": blocked_opportunities,
        "opportunities": [item.as_dict() for item in selected],
        "order_plans": [plan.as_dict() for plan in plans],
        "executions": [_sanitize_execution_for_export(item.as_dict()) for item in executions],
    }


def _build_safe_share_payload(payload: dict) -> dict:
    safe_payload = dict(payload)
    safe_payload["opportunities"] = [
        {
            key: value
            for key, value in item.items()
            if key not in {"token_id", "market_id", "polymarket_url", "model_predictions"}
        }
        for item in payload.get("opportunities", [])
    ]
    safe_payload["blocked_opportunities"] = [
        {
            key: value
            for key, value in item.items()
            if key not in {"token_id", "market_id", "polymarket_url", "model_predictions"}
        }
        for item in payload.get("blocked_opportunities", [])
    ]
    safe_payload["order_plans"] = [
        {
            key: value
            for key, value in item.items()
            if key not in {"token_id", "polymarket_url", "bankroll_usd"}
        }
        for item in payload.get("order_plans", [])
    ]
    return safe_payload


def _count_ambiguous_live_orders(storage: WeatherBotStorage) -> int:
    total = 0
    offset = 0
    page_size = 250
    while True:
        page = storage.list_live_orders(
            statuses=("submission_unconfirmed",),
            limit=page_size,
            offset=offset,
        )
        if not page:
            break
        total += len(page)
        if len(page) < page_size:
            break
        offset += page_size
    return total


def _bucket_alignment_context(bucket_label: str, forecast_temp_f: float, side: str) -> tuple[bool | None, bool | None, float | None]:
    min_value, max_value = _parse_bucket_range(bucket_label)
    if min_value is None and max_value is None:
        return None, None, None
    numeric = float(forecast_temp_f)
    in_bucket = True
    delta = 0.0
    if min_value is not None and numeric < float(min_value):
        in_bucket = False
        delta = float(min_value) - numeric
    if max_value is not None and numeric > float(max_value):
        in_bucket = False
        delta = max(delta, numeric - float(max_value))
    aligns_with_trade_side = in_bucket if side == "YES" else (not in_bucket)
    return aligns_with_trade_side, in_bucket, round(delta, 4)


def _build_forecast_source_snapshot_rows(
    *,
    run_id: str,
    captured_at: str,
    raw_predictions: list[dict],
) -> list[dict]:
    rows: list[dict] = []
    for item in raw_predictions:
        model_predictions = item.get("model_predictions")
        if not isinstance(model_predictions, dict):
            continue
        effective_weights = item.get("effective_weights")
        if not isinstance(effective_weights, dict):
            effective_weights = {}
        for source_name, forecast_temp in model_predictions.items():
            try:
                forecast_temp_f = float(forecast_temp)
            except (TypeError, ValueError):
                continue
            aligns_with_trade_side, source_in_bucket, source_delta_f = _bucket_alignment_context(
                str(item.get("bucket") or ""),
                forecast_temp_f,
                str(item.get("side") or ""),
            )
            rows.append(
                {
                    "run_id": run_id,
                    "captured_at": captured_at,
                    "city_key": item.get("city_key"),
                    "city_name": item.get("city_name"),
                    "day_label": item.get("day_label"),
                    "date_str": item.get("date_str"),
                    "event_slug": item.get("event_slug"),
                    "market_slug": item.get("market_slug"),
                    "market_id": item.get("market_id"),
                    "bucket": item.get("bucket"),
                    "side": item.get("side"),
                    "source_name": source_name,
                    "forecast_temp_f": round(forecast_temp_f, 4),
                    "effective_weight": effective_weights.get(source_name),
                    "agreement_models": item.get("agreement_models"),
                    "total_models": item.get("total_models"),
                    "agreement_pct": item.get("agreement_pct"),
                    "aligns_with_trade_side": aligns_with_trade_side,
                    "source_in_bucket": source_in_bucket,
                    "source_delta_f": source_delta_f,
                    "raw_context": {
                        "signal_tier": item.get("signal_tier"),
                        "confidence_tier": item.get("confidence_tier"),
                        "policy_allowed": bool(item.get("policy_allowed")),
                        "policy_reason": item.get("policy_reason"),
                        "price_source": item.get("price_source"),
                        "reference_price_cents": item.get("reference_price_cents"),
                        "coverage_score": item.get("coverage_score"),
                    },
                }
            )
    return rows


def _build_station_observation_rows(*, captured_at: str) -> list[dict]:
    rows: list[dict] = []
    for city in CITY_CONFIGS:
        if city.supports_nws:
            station_id = nws_station_id(city)
            if station_id:
                try:
                    observed_daily_highs = fetch_nws_observed_daily_highs(city, limit=72)
                except Exception:
                    observed_daily_highs = {}
                for local_date, observed_high_f in observed_daily_highs.items():
                    try:
                        numeric_high = float(observed_high_f)
                    except (TypeError, ValueError):
                        continue
                    rows.append(
                        {
                            "captured_at": captured_at,
                            "city_key": city.key,
                            "city_name": city.display_name,
                            "station_id": station_id,
                            "local_date": local_date,
                            "observed_high_f": round(numeric_high, 4),
                            "source": "nws_station_observation",
                            "raw_context": {
                                "timezone_name": city.timezone_name,
                                "regime_tags": list(city.regime_tags),
                            },
                        }
                    )
        elif city.market_temp_unit == "C":
            try:
                station_id, observed_daily_highs = fetch_meteostat_observed_daily_highs(city, lookback_days=7)
            except Exception:
                station_id, observed_daily_highs = None, {}
            source_name = "meteostat_daily_observation"
            if not observed_daily_highs:
                try:
                    station_id, observed_daily_highs = fetch_noaa_isd_observed_daily_highs(city, lookback_days=7)
                except Exception:
                    station_id, observed_daily_highs = None, {}
                if observed_daily_highs:
                    source_name = "noaa_isd_observation"
            for local_date, observed_high_f in observed_daily_highs.items():
                try:
                    numeric_high = float(observed_high_f)
                except (TypeError, ValueError):
                    continue
                rows.append(
                    {
                        "captured_at": captured_at,
                        "city_key": city.key,
                        "city_name": city.display_name,
                        "station_id": station_id or f"point:{city.lat:.4f},{city.lon:.4f}",
                        "local_date": local_date,
                        "observed_high_f": round(numeric_high, 4),
                        "source": source_name,
                        "raw_context": {
                            "timezone_name": city.timezone_name,
                            "regime_tags": list(city.regime_tags),
                        },
                    }
                )
    return rows


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Scan Polymarket weather markets using direct forecast models.")
    parser.add_argument("--days-ahead", type=int, default=3, help="How many day buckets to scan: 1-3.")
    parser.add_argument("--min-edge", type=float, default=float(os.getenv("WEATHER_MIN_EDGE", "10.0")), help="Minimum edge in percentage points.")
    parser.add_argument("--min-model-prob", type=float, default=float(os.getenv("WEATHER_MIN_MODEL_PROB", "15.0")), help="Minimum model probability to keep.")
    parser.add_argument("--min-consensus", type=float, default=float(os.getenv("WEATHER_MIN_CONSENSUS", "0.35")), help="Minimum ensemble consensus score from 0 to 1.")
    parser.add_argument("--top", type=int, default=10, help="How many opportunities to print.")
    parser.add_argument("--show-blocked", type=int, default=10, help="How many blocked opportunities to show with rejection reasons.")
    parser.add_argument("--bankroll", type=float, default=float(os.getenv("PAPERBOT_BANKROLL_USD", "1000")))
    parser.add_argument("--kelly-fraction", type=float, default=float(os.getenv("PAPERBOT_KELLY_FRACTION", "0.25")))
    parser.add_argument("--min-stake-usd", type=float, default=float(os.getenv("PAPERBOT_MIN_STAKE_USD", "5.0")))
    parser.add_argument("--max-stake-usd", type=float, default=float(os.getenv("PAPERBOT_MAX_STAKE_USD", "0.0")))
    parser.add_argument("--min-price-cents", type=float, default=float(os.getenv("WEATHER_MIN_PRICE_CENTS", "10")))
    parser.add_argument("--max-price-cents", type=float, default=float(os.getenv("WEATHER_MAX_PRICE_CENTS", "65")))
    parser.add_argument("--max-spread", type=float, default=float(os.getenv("WEATHER_MAX_MODEL_SPREAD", "4.0")), help="Maximum allowed disagreement between models in Fahrenheit.")
    parser.add_argument("--max-share-size", type=float, default=float(os.getenv("WEATHER_MAX_SHARE_SIZE", "400")))
    parser.add_argument("--max-orders-per-event", type=int, default=int(os.getenv("WEATHER_MAX_ORDERS_PER_EVENT", "1")))
    parser.add_argument("--history-csv", default=os.getenv("WEATHER_HISTORY_CSV", "export/history/weather_model_scan_log.csv"))
    parser.add_argument("--latest-json", default=os.getenv("WEATHER_LATEST_JSON", "export/history/weather_model_latest.json"))
    parser.add_argument("--db-path", default=os.getenv("WEATHER_DB_PATH", "export/db/weather_bot.db"))
    parser.add_argument("--state-json", default=os.getenv("WEATHER_STATE_JSON", "export/state/trading_state.json"))
    parser.add_argument("--lock-file", default=os.getenv("WEATHER_LOCK_FILE", "export/state/trading_state.lock"))
    parser.add_argument("--scan-lock-file", default=os.getenv("WEATHER_SCAN_LOCK_FILE", "export/state/scan.lock"))
    parser.add_argument("--no-history", action="store_true", help="Do not append this run to persistent history files.")
    parser.add_argument("--live", action="store_true", help="Actually post orders to Polymarket. Default is dry-run.")
    parser.add_argument("--execute-top", type=int, default=int(os.getenv("WEATHER_EXECUTE_TOP", "1")), help="How many top plans to execute in dry-run/live mode.")
    parser.add_argument("--daily-live-limit", type=int, default=int(os.getenv("WEATHER_DAILY_LIVE_LIMIT", "3")))
    parser.add_argument("--bucket-live-limit", type=int, default=int(os.getenv("WEATHER_BUCKET_LIVE_LIMIT", "2")))
    parser.add_argument("--city-cooldown-minutes", type=int, default=int(os.getenv("WEATHER_CITY_COOLDOWN_MINUTES", "180")))
    parser.add_argument("--event-cooldown-minutes", type=int, default=int(os.getenv("WEATHER_EVENT_COOLDOWN_MINUTES", "720")))
    parser.add_argument("--bucket-cooldown-minutes", type=int, default=int(os.getenv("WEATHER_BUCKET_COOLDOWN_MINUTES", "360")))
    parser.add_argument("--replace-open-orders", action="store_true", help="Cancel and replace similar open orders before posting a new live order.")
    parser.add_argument("--replace-price-threshold-cents", type=float, default=float(os.getenv("WEATHER_REPLACE_PRICE_THRESHOLD_CENTS", "1.0")))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-sync-resolutions", action="store_true", help="Do not refresh open positions against resolved market outcomes.")
    parser.add_argument("--export-json", default=None)
    parser.add_argument("--export-csv", default=None)
    parser.add_argument("--safe-share", action="store_true", help="Redact market URLs, token ids and operational fields from stdout/JSON exports.")
    args = parser.parse_args(argv)
    _ensure_policy_profile_fresh()
    _ensure_source_weight_profile_fresh()
    _ensure_truth_weight_profile_fresh()

    if args.live and args.no_history:
        parser.error("--live requires persistent history/storage; remove --no-history.")

    generated_at = datetime.now(timezone.utc).isoformat()
    run_id = uuid.uuid4().hex[:12]
    live_lock_path = _resolve_path(args.lock_file)
    scan_lock_path = _resolve_path(args.scan_lock_file)
    base_filters = {
        "min_edge": args.min_edge,
        "min_model_prob": args.min_model_prob,
        "min_consensus": args.min_consensus,
        "min_price_cents": args.min_price_cents,
        "max_price_cents": args.max_price_cents,
        "min_stake_usd": args.min_stake_usd,
        "max_stake_usd": args.max_stake_usd,
        "max_spread": args.max_spread,
        "max_share_size": args.max_share_size,
        "max_orders_per_event": args.max_orders_per_event,
        "lock_file": str(live_lock_path),
        "live": args.live,
        "execute_top": args.execute_top,
        "daily_live_limit": args.daily_live_limit,
        "bucket_live_limit": args.bucket_live_limit,
        "city_cooldown_minutes": args.city_cooldown_minutes,
        "event_cooldown_minutes": args.event_cooldown_minutes,
        "bucket_cooldown_minutes": args.bucket_cooldown_minutes,
        "replace_open_orders": args.replace_open_orders,
        "replace_price_threshold_cents": args.replace_price_threshold_cents,
    }
    storage = None if args.no_history else WeatherBotStorage(_resolve_path(args.db_path))
    if storage is not None:
        storage.init_run(run_id=run_id, generated_at=generated_at, filters=base_filters)
    live_sync_result = None
    if args.live and storage is not None:
        live_sync_result = sync_live_exchange_state(storage)
        if not live_sync_result.get("ok"):
            message = f"live pre-sync failed: {live_sync_result.get('error') or 'unknown_error'}"
            if args.json:
                print(json.dumps({"ok": False, "error": _sanitize_text(message)}, indent=2))
                return
            print(_sanitize_text(message))
            return
    try:
        scan_lock = FileLock(scan_lock_path, timeout_seconds=1.0, poll_seconds=0.1, stale_seconds=180.0)
        with scan_lock:
            raw_opportunities = scan_weather_model_opportunities(
                days_ahead=args.days_ahead,
                min_edge=args.min_edge,
                min_model_prob=args.min_model_prob,
                min_consensus=args.min_consensus,
            )
            preselected = raw_opportunities
            candidate_plans = [
                build_order_plan(
                    opportunity,
                    bankroll_usd=args.bankroll,
                    kelly_fraction=args.kelly_fraction,
                    max_price_cents=args.max_price_cents,
                    min_stake_usd=args.min_stake_usd,
                    max_stake_usd=(args.max_stake_usd if args.max_stake_usd > 0 else None),
                )
                for opportunity in preselected
            ]
            plan_index = {
                f"{opportunity.event_slug}|{opportunity.market_slug}|{opportunity.side}": plan
                for opportunity, plan in zip(preselected, candidate_plans)
            }
            opportunities = filter_opportunities(
                preselected,
                min_price_cents=args.min_price_cents,
                max_price_cents=args.max_price_cents,
                max_spread=args.max_spread,
                max_share_size=args.max_share_size,
                require_token=True,
                max_orders_per_event=args.max_orders_per_event,
                plans_by_slug=plan_index,
            )
            rejection_summary = summarize_filter_rejections(
                preselected,
                min_price_cents=args.min_price_cents,
                max_price_cents=args.max_price_cents,
                max_spread=args.max_spread,
                max_share_size=args.max_share_size,
                require_token=True,
                max_orders_per_event=args.max_orders_per_event,
                plans_by_slug=plan_index,
            )
            blocked_opportunities = explain_blocked_opportunities(
                preselected,
                min_price_cents=args.min_price_cents,
                max_price_cents=args.max_price_cents,
                max_spread=args.max_spread,
                max_share_size=args.max_share_size,
                require_token=True,
                max_orders_per_event=args.max_orders_per_event,
                plans_by_slug=plan_index,
                limit=max(0, args.show_blocked),
            )
            for item in blocked_opportunities:
                item["analyzed_at"] = generated_at
            selected = opportunities[: max(0, args.top)]
            plans = [
                plan_index[f"{opportunity.event_slug}|{opportunity.market_slug}|{opportunity.side}"]
                for opportunity in selected
            ]
            execute_count = max(0, min(args.execute_top, len(plans)))
            state_store = TradingStateStore(_resolve_path(args.state_json))
            executions = []
            for idx, (opportunity, plan) in enumerate(zip(selected, plans)):
                should_live = args.live and idx < execute_count
                bucket_key = f"{opportunity.event_slug}|{opportunity.market_slug}|{opportunity.side}"
                if should_live:
                    with FileLock(live_lock_path):
                        state_store = TradingStateStore(_resolve_path(args.state_json))
                        if storage is not None:
                            ambiguous_order_count = _count_ambiguous_live_orders(storage)
                            if ambiguous_order_count > 0:
                                from paperbot.live_trader import ExecutionResult

                                executions.append(
                                    ExecutionResult(
                                        mode="live",
                                        success=False,
                                        market_slug=plan.market_slug,
                                        side=plan.side,
                                        price_cents=plan.limit_price_cents,
                                        share_size=plan.share_size,
                                        response={"submission_unconfirmed_count": ambiguous_order_count},
                                        error="submission_unconfirmed_pending_recovery",
                                    )
                                )
                                continue
                        decision = state_store.can_execute(
                            city_key=opportunity.city_key,
                            event_slug=opportunity.event_slug,
                            bucket_key=bucket_key,
                            daily_live_limit=args.daily_live_limit,
                            bucket_live_limit=args.bucket_live_limit,
                            city_cooldown_minutes=args.city_cooldown_minutes,
                            event_cooldown_minutes=args.event_cooldown_minutes,
                            bucket_cooldown_minutes=args.bucket_cooldown_minutes,
                        )
                        if not decision.ok:
                            from paperbot.live_trader import ExecutionResult

                            executions.append(
                                ExecutionResult(
                                    mode="live",
                                    success=False,
                                    market_slug=plan.market_slug,
                                    side=plan.side,
                                    price_cents=plan.limit_price_cents,
                                    share_size=plan.share_size,
                                    response={},
                                    error=decision.reason,
                                )
                            )
                            continue
                        execution = execute_order_plan(
                            plan,
                            live=True,
                            replace_open_orders=args.replace_open_orders,
                            replace_price_threshold_cents=args.replace_price_threshold_cents,
                        )
                        executions.append(execution)
                        if storage is not None:
                            storage.append_live_execution(
                                run_id=run_id,
                                generated_at=generated_at,
                                rank=idx + 1,
                                opportunity=opportunity.as_dict(),
                                plan=plan.as_dict(),
                                execution=execution.as_dict(),
                            )
                        if execution.accepted or execution.order_status == "submission_unconfirmed" or (execution.filled_shares or 0.0) > 0:
                            state_store.record_live_execution(
                                city_key=opportunity.city_key,
                                event_slug=opportunity.event_slug,
                                bucket_key=bucket_key,
                            )
                else:
                    executions.append(execute_order_plan(plan, live=False))
    except TimeoutError:
        message = f"scan skipped: another scan is already running ({scan_lock_path})"
        if args.json:
            print(json.dumps({"ok": False, "error": _sanitize_text(message)}, indent=2))
            return
        print(_sanitize_text(message))
        return
    payload = _build_export_payload(
        generated_at=generated_at,
        run_id=run_id,
        filters=base_filters,
        raw_count=len(raw_opportunities),
        selected=selected,
        blocked_opportunities=blocked_opportunities,
        rejection_summary=rejection_summary,
        plans=plans,
        executions=executions,
        execute_count=execute_count,
    )

    export_payload = _build_safe_share_payload(payload) if args.safe_share else payload
    if args.export_json:
        write_json(_resolve_path(args.export_json), export_payload)
    if args.export_csv:
        append_csv_rows(_resolve_path(args.export_csv), _build_history_rows(run_id, selected, plans, executions, generated_at))
    if not args.no_history:
        history_rows = _build_history_rows(run_id, selected, plans, executions, generated_at)
        append_csv_rows(_resolve_path(args.history_csv), history_rows)
        write_json(_resolve_path(args.latest_json), payload)
    sync_summary = None
    live_sync_error = None
    if not args.no_history and storage is not None:
        storage.persist_run(
            run_id=run_id,
            generated_at=generated_at,
            raw_count=len(raw_opportunities),
            count_selected=len(selected),
            filters=payload["filters"],
            raw_predictions=[item.as_dict() for item in raw_opportunities],
            opportunities=payload["opportunities"],
            order_plans=payload["order_plans"],
            executions=payload["executions"],
        )
        try:
            forecast_source_rows = _build_forecast_source_snapshot_rows(
                run_id=run_id,
                captured_at=generated_at,
                raw_predictions=[item.as_dict() for item in raw_opportunities],
            )
            payload["forecast_source_snapshot_count"] = storage.record_forecast_source_snapshots(forecast_source_rows)
        except Exception as exc:
            payload["forecast_source_snapshot_error"] = _sanitize_text(exc)
        try:
            station_rows = _build_station_observation_rows(captured_at=generated_at)
            payload["station_observation_count"] = storage.record_station_observation_daily_highs(station_rows)
        except Exception as exc:
            payload["station_observation_error"] = _sanitize_text(exc)
        if args.live:
            payload["live_order_sync"] = sync_live_exchange_state(storage)
            if not payload["live_order_sync"].get("ok"):
                live_sync_error = payload["live_order_sync"].get("error") or "unknown_error"
                payload["degraded_mode"] = True
                payload["degraded_reason"] = f"live_post_sync_failed:{live_sync_error}"
        if not args.no_sync_resolutions:
            sync_summary = sync_open_positions(storage)
            payload["resolution_sync"] = sync_summary
            prediction_sync_summary = sync_prediction_resolutions(storage)
            payload["prediction_resolution_sync"] = prediction_sync_summary

    export_payload = _build_safe_share_payload(payload) if args.safe_share else payload

    if args.json:
        print(json.dumps(export_payload, indent=2))
        return

    if live_sync_error:
        print(f"AVISO: post-sync live falhou: {_sanitize_text(live_sync_error)}")

    if not selected:
        print("Nenhuma oportunidade encontrada com os filtros atuais.")
        if rejection_summary:
            print("Principais motivos de bloqueio:")
            for reason, qty in rejection_summary.items():
                print(f"  - {reason}: {qty}")
        if blocked_opportunities:
            print("Oportunidades bloqueadas:")
            for item in blocked_opportunities:
                print(
                    f"  - {item['city_key']} {item['date_str']} {item['side']} {item['bucket']} "
                    f"edge={item['edge']:.2f} model={item['model_prob']:.2f}% price={item['price_cents']:.2f}c "
                    f"conf={item['confidence_tier']} risk={item['risk_label']} motivo={item['reason']}"
                )
        if sync_summary:
            print(
                f"Resolucao atualizada: {sync_summary['updated_positions']} posicoes, "
                f"{sync_summary['checked_markets']} mercados checados"
            )
        return

    print(f"Oportunidades encontradas: {len(selected)} de {len(raw_opportunities)} apos filtros")
    for idx, (opportunity, plan, execution) in enumerate(zip(selected, plans, executions), start=1):
        print(
            f"{idx:02d}. {opportunity.city_key} {opportunity.date_str} {opportunity.side} {opportunity.bucket} "
            f"edge={opportunity.edge:.2f} model={opportunity.model_prob:.2f}% price={opportunity.price_cents:.2f}c "
            f"ensemble={opportunity.ensemble_prediction:.1f}F consensus={opportunity.consensus_score:.2f}"
        )
        print(f"    {summarize_plan(plan)}")
        print(
            f"    execution: {execution.mode} success={execution.success}"
            + (f" error={_sanitize_text(execution.error)}" if execution.error else "")
        )
    if blocked_opportunities:
        print("Bloqueadas pelos filtros:")
        for item in blocked_opportunities:
            print(
                f"  - {item['city_key']} {item['date_str']} {item['side']} {item['bucket']} "
                f"edge={item['edge']:.2f} model={item['model_prob']:.2f}% price={item['price_cents']:.2f}c "
                f"conf={item['confidence_tier']} risk={item['risk_label']} motivo={item['reason']}"
            )
    if sync_summary:
        print(
            f"Resolucao atualizada: {sync_summary['updated_positions']} posicoes, "
            f"{sync_summary['checked_markets']} mercados checados"
        )


if __name__ == "__main__":
    main()
