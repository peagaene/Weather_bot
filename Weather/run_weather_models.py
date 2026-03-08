from __future__ import annotations

import argparse
import json
import os
import uuid
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

load_dotenv(ROOT / ".env", override=False)

from paperbot.history import append_csv_rows, write_json
from paperbot.live_trader import execute_order_plan
from paperbot.polymarket_live import build_order_plan, summarize_plan
from paperbot.polymarket_weather import scan_weather_model_opportunities
from paperbot.selection import filter_opportunities
from paperbot.trading_state import FileLock, TradingStateStore


def _build_history_rows(run_id: str, selected: list, plans: list, executions: list, generated_at: str) -> list[dict]:
    rows: list[dict] = []
    for rank, (opportunity, plan, execution) in enumerate(zip(selected, plans, executions), start=1):
        rows.append(
            {
                "run_id": run_id,
                "generated_at": generated_at,
                "rank": rank,
                **opportunity.as_dict(),
                **{f"plan_{k}": v for k, v in asdict(plan).items()},
                **{f"exec_{k}": v for k, v in execution.as_dict().items()},
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
    parser.add_argument("--bankroll", type=float, default=float(os.getenv("PAPERBOT_BANKROLL_USD", "1000")))
    parser.add_argument("--kelly-fraction", type=float, default=float(os.getenv("PAPERBOT_KELLY_FRACTION", "0.25")))
    parser.add_argument("--min-price-cents", type=float, default=float(os.getenv("WEATHER_MIN_PRICE_CENTS", "10")))
    parser.add_argument("--max-price-cents", type=float, default=float(os.getenv("WEATHER_MAX_PRICE_CENTS", "65")))
    parser.add_argument("--max-spread", type=float, default=float(os.getenv("WEATHER_MAX_MODEL_SPREAD", "4.0")), help="Maximum allowed disagreement between models in Fahrenheit.")
    parser.add_argument("--max-share-size", type=float, default=float(os.getenv("WEATHER_MAX_SHARE_SIZE", "400")))
    parser.add_argument("--max-orders-per-event", type=int, default=int(os.getenv("WEATHER_MAX_ORDERS_PER_EVENT", "1")))
    parser.add_argument("--history-csv", default=os.getenv("WEATHER_HISTORY_CSV", "export/history/weather_model_scan_log.csv"))
    parser.add_argument("--latest-json", default=os.getenv("WEATHER_LATEST_JSON", "export/history/weather_model_latest.json"))
    parser.add_argument("--state-json", default=os.getenv("WEATHER_STATE_JSON", "export/state/trading_state.json"))
    parser.add_argument("--lock-file", default=os.getenv("WEATHER_LOCK_FILE", "export/state/trading_state.lock"))
    parser.add_argument("--no-history", action="store_true", help="Do not append this run to persistent history files.")
    parser.add_argument("--live", action="store_true", help="Actually post orders to Polymarket. Default is dry-run.")
    parser.add_argument("--execute-top", type=int, default=int(os.getenv("WEATHER_EXECUTE_TOP", "1")), help="How many top plans to execute in dry-run/live mode.")
    parser.add_argument("--daily-live-limit", type=int, default=int(os.getenv("WEATHER_DAILY_LIVE_LIMIT", "3")))
    parser.add_argument("--city-cooldown-minutes", type=int, default=int(os.getenv("WEATHER_CITY_COOLDOWN_MINUTES", "180")))
    parser.add_argument("--event-cooldown-minutes", type=int, default=int(os.getenv("WEATHER_EVENT_COOLDOWN_MINUTES", "720")))
    parser.add_argument("--bucket-cooldown-minutes", type=int, default=int(os.getenv("WEATHER_BUCKET_COOLDOWN_MINUTES", "360")))
    parser.add_argument("--replace-open-orders", action="store_true", help="Cancel and replace similar open orders before posting a new live order.")
    parser.add_argument("--replace-price-threshold-cents", type=float, default=float(os.getenv("WEATHER_REPLACE_PRICE_THRESHOLD_CENTS", "1.0")))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--export-json", default=None)
    parser.add_argument("--export-csv", default=None)
    args = parser.parse_args(argv)

    raw_opportunities = scan_weather_model_opportunities(
        days_ahead=args.days_ahead,
        min_edge=args.min_edge,
        min_model_prob=args.min_model_prob,
        min_consensus=args.min_consensus,
    )
    preselected = raw_opportunities[: max(args.top * 3, args.top)]
    candidate_plans = [
        build_order_plan(
            opportunity,
            bankroll_usd=args.bankroll,
            kelly_fraction=args.kelly_fraction,
            max_price_cents=args.max_price_cents,
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
    selected = opportunities[: max(0, args.top)]
    plans = [
        plan_index[f"{opportunity.event_slug}|{opportunity.market_slug}|{opportunity.side}"]
        for opportunity in selected
    ]
    execute_count = max(0, min(args.execute_top, len(plans)))
    state_store = TradingStateStore(Path(args.state_json))
    live_lock_path = Path(args.lock_file)
    executions = []
    for idx, (opportunity, plan) in enumerate(zip(selected, plans)):
        should_live = args.live and idx < execute_count
        bucket_key = f"{opportunity.event_slug}|{opportunity.market_slug}|{opportunity.side}"
        if should_live:
            with FileLock(live_lock_path):
                state_store = TradingStateStore(Path(args.state_json))
                decision = state_store.can_execute(
                    city_key=opportunity.city_key,
                    event_slug=opportunity.event_slug,
                    bucket_key=bucket_key,
                    daily_live_limit=args.daily_live_limit,
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
                if execution.success:
                    state_store.record_live_execution(
                        city_key=opportunity.city_key,
                        event_slug=opportunity.event_slug,
                        bucket_key=bucket_key,
                    )
        else:
            executions.append(execute_order_plan(plan, live=False))
    generated_at = datetime.now(timezone.utc).isoformat()
    run_id = uuid.uuid4().hex[:12]

    payload = {
        "generated_at": generated_at,
        "run_id": run_id,
        "filters": {
            "min_edge": args.min_edge,
            "min_model_prob": args.min_model_prob,
            "min_consensus": args.min_consensus,
            "min_price_cents": args.min_price_cents,
            "max_price_cents": args.max_price_cents,
            "max_spread": args.max_spread,
            "max_share_size": args.max_share_size,
            "max_orders_per_event": args.max_orders_per_event,
            "lock_file": str(live_lock_path),
            "live": args.live,
            "execute_top": execute_count,
            "daily_live_limit": args.daily_live_limit,
            "city_cooldown_minutes": args.city_cooldown_minutes,
            "event_cooldown_minutes": args.event_cooldown_minutes,
            "bucket_cooldown_minutes": args.bucket_cooldown_minutes,
            "replace_open_orders": args.replace_open_orders,
            "replace_price_threshold_cents": args.replace_price_threshold_cents,
        },
        "raw_count": len(raw_opportunities),
        "count": len(selected),
        "opportunities": [item.as_dict() for item in selected],
        "order_plans": [plan.as_dict() for plan in plans],
        "executions": [item.as_dict() for item in executions],
    }

    if args.export_json:
        write_json(Path(args.export_json), payload)
    if args.export_csv:
        append_csv_rows(Path(args.export_csv), _build_history_rows(run_id, selected, plans, executions, generated_at))
    if not args.no_history:
        history_rows = _build_history_rows(run_id, selected, plans, executions, generated_at)
        append_csv_rows(Path(args.history_csv), history_rows)
        write_json(Path(args.latest_json), payload)

    if args.json:
        print(json.dumps(payload, indent=2))
        return

    if not selected:
        print("Nenhuma oportunidade encontrada com os filtros atuais.")
        return

    print(f"Oportunidades encontradas: {len(selected)} de {len(raw_opportunities)} apos filtros")
    for idx, (opportunity, plan, execution) in enumerate(zip(selected, plans, executions), start=1):
        models_text = ", ".join(f"{k}:{v:.1f}" for k, v in sorted(opportunity.model_predictions.items()))
        print(
            f"{idx:02d}. {opportunity.city_key} {opportunity.date_str} {opportunity.side} {opportunity.bucket} "
            f"edge={opportunity.edge:.2f} model={opportunity.model_prob:.2f}% price={opportunity.price_cents:.2f}c "
            f"ensemble={opportunity.ensemble_prediction:.1f}F consensus={opportunity.consensus_score:.2f}"
        )
        print(f"    {summarize_plan(plan)}")
        print(f"    models: {models_text}")
        print(
            f"    execution: {execution.mode} success={execution.success}"
            + (f" error={execution.error}" if execution.error else "")
        )
        print(f"    {plan.polymarket_url}")


if __name__ == "__main__":
    main()
