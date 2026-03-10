from __future__ import annotations

import argparse
import json
import os
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
import sys
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.env import load_app_env
from paperbot.degendoppler import CITY_CONFIGS

load_app_env(ROOT)

CITY_TIMEZONES = {city.key: city.timezone_name for city in CITY_CONFIGS}


def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_iso(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _fetch_rows(db_path: Path) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
    conn = _connect(db_path)
    try:
        predictions = conn.execute(
            """
            SELECT
                run_id,
                generated_at,
                city_key,
                date_str,
                event_slug,
                market_slug,
                event_title,
                bucket,
                side,
                price_cents,
                model_prob,
                confidence_tier,
                policy_allowed,
                policy_reason,
                coverage_ok,
                degraded_reason,
                price_source,
                settled_price_cents,
                pnl_usd,
                roi_percent,
                resolved_at
            FROM scan_predictions
            WHERE settled_price_cents IS NOT NULL
            ORDER BY generated_at ASC, id ASC
            """
        ).fetchall()
        runs = conn.execute(
            """
            SELECT run_id, generated_at, raw_count, count_selected
            FROM bot_runs
            ORDER BY generated_at ASC
            """
        ).fetchall()
        return predictions, runs
    finally:
        conn.close()


def _fetch_market_history_rows(
    db_path: Path,
    *,
    lookback_hours: int,
) -> list[sqlite3.Row]:
    conn = _connect(db_path)
    try:
        query = """
            SELECT
                captured_at,
                market_slug,
                yes_price_cents,
                no_price_cents,
                yes_best_ask_cents,
                no_best_ask_cents,
                yes_best_bid_cents,
                no_best_bid_cents,
                last_trade_price
            FROM market_history_snapshots
        """
        params: list[Any] = []
        if lookback_hours > 0:
            query += " WHERE captured_at >= datetime('now', ?)"
            params.append(f"-{lookback_hours} hours")
        query += " ORDER BY market_slug ASC, captured_at ASC, id ASC"
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()


def _filter_recent_rows(
    rows: list[sqlite3.Row],
    *,
    timestamp_key: str,
    lookback_hours: int,
) -> list[sqlite3.Row]:
    if lookback_hours <= 0:
        return rows
    parsed = [(_parse_iso(row[timestamp_key]), row) for row in rows]
    available = [(ts, row) for ts, row in parsed if ts is not None]
    if not available:
        return rows
    latest = max(ts for ts, _ in available)
    threshold = latest.timestamp() - (lookback_hours * 3600.0)
    return [row for ts, row in available if ts.timestamp() >= threshold]


def _row_value(row: sqlite3.Row | dict[str, Any], key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return row[key]


def _build_point_in_time_validation(
    rows: list[sqlite3.Row],
    *,
    min_trades: int,
    max_brier_score: float,
    max_non_executable_ratio: float,
) -> dict[str, Any]:
    first_seen: dict[str, sqlite3.Row] = {}
    leakage_count = 0
    usable_rows: list[sqlite3.Row] = []

    for row in rows:
        generated_at = _parse_iso(_row_value(row, "generated_at"))
        city_key = str(_row_value(row, "city_key") or "").strip().upper()
        try:
            target_date = datetime.fromisoformat(str(_row_value(row, "date_str")))
        except ValueError:
            target_date = None
        if generated_at is None or target_date is None:
            continue
        timezone_name = CITY_TIMEZONES.get(city_key, "UTC")
        generated_local_date = generated_at.astimezone(ZoneInfo(timezone_name)).date()
        if generated_local_date > target_date.date():
            leakage_count += 1
            continue
        market_key = f"{_row_value(row, 'event_slug')}|{_row_value(row, 'market_slug')}|{_row_value(row, 'side')}"
        if market_key in first_seen:
            continue
        first_seen[market_key] = row
        usable_rows.append(row)

    if not usable_rows:
        return {
            "present": True,
            "passed": False,
            "reason": "lookahead_or_date_leakage_detected" if leakage_count > 0 else "no_usable_point_in_time_predictions",
            "sample_count": 0,
            "leakage_count": leakage_count,
            "brier_score": None,
            "non_executable_price_ratio": None,
        }

    brier_components: list[float] = []
    non_executable = 0
    for row in usable_rows:
        probability = _safe_float(_row_value(row, "model_prob"))
        settled = _safe_float(_row_value(row, "settled_price_cents"))
        if probability is None or settled is None:
            continue
        predicted = max(0.0, min(1.0, probability / 100.0))
        outcome = max(0.0, min(1.0, settled / 100.0))
        brier_components.append((predicted - outcome) ** 2)
        if str(_row_value(row, "price_source") or "").lower() == "gamma_outcome_price":
            non_executable += 1

    sample_count = len(brier_components)
    brier_score = (sum(brier_components) / sample_count) if sample_count > 0 else None
    non_executable_ratio = (non_executable / len(usable_rows)) if usable_rows else None
    blocking_reasons = [
        reason
        for reason, blocked in (
            ("insufficient_point_in_time_samples", sample_count < min_trades),
            ("lookahead_or_date_leakage_detected", leakage_count > 0),
            ("excess_non_executable_price_source", (non_executable_ratio or 0.0) > max_non_executable_ratio),
            ("brier_score_too_high", brier_score is None or brier_score > max_brier_score),
        )
        if blocked
    ]
    return {
        "present": True,
        "passed": not blocking_reasons,
        "reason": "" if not blocking_reasons else ",".join(blocking_reasons),
        "sample_count": sample_count,
        "usable_market_count": len(usable_rows),
        "leakage_count": leakage_count,
        "brier_score": round(brier_score, 6) if brier_score is not None else None,
        "non_executable_price_ratio": round(non_executable_ratio, 6) if non_executable_ratio is not None else None,
        "blocking_reasons": blocking_reasons,
    }


def _build_run_gap_summary(
    runs: list[sqlite3.Row],
    *,
    expected_interval_seconds: int,
    gap_ratio: float,
) -> dict[str, Any]:
    timestamps = [_parse_iso(row["generated_at"]) for row in runs]
    ordered = [item for item in timestamps if item is not None]
    if len(ordered) < 2:
        return {
            "runs": len(runs),
            "expected_interval_seconds": expected_interval_seconds,
            "observed_median_interval_seconds": None,
            "large_gap_count": 0,
            "largest_gap_seconds": None,
        }

    diffs = [
        (curr - prev).total_seconds()
        for prev, curr in zip(ordered, ordered[1:])
        if curr >= prev
    ]
    threshold = expected_interval_seconds * gap_ratio
    large_gaps = [diff for diff in diffs if diff > threshold]
    latest_session_start_index = 0
    for index, diff in enumerate(diffs, start=1):
        if diff > threshold:
            latest_session_start_index = index
    session_timestamps = ordered[latest_session_start_index:]
    session_diffs = [
        (curr - prev).total_seconds()
        for prev, curr in zip(session_timestamps, session_timestamps[1:])
        if curr >= prev
    ]
    session_large_gaps = [diff for diff in session_diffs if diff > threshold]
    return {
        "runs": len(runs),
        "expected_interval_seconds": expected_interval_seconds,
        "observed_median_interval_seconds": round(median(diffs), 2) if diffs else None,
        "large_gap_count": len(large_gaps),
        "largest_gap_seconds": round(max(large_gaps), 2) if large_gaps else None,
        "current_session_runs": len(session_timestamps),
        "current_session_large_gap_count": len(session_large_gaps),
        "current_session_largest_gap_seconds": round(max(session_large_gaps), 2) if session_large_gaps else None,
    }

def _group_trade_summary(trades: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for trade in trades:
        name = str(trade.get(key) or "unknown")
        item = grouped.setdefault(
            name,
            {"group": name, "count": 0, "wins": 0, "losses": 0, "pnl_usd": 0.0, "roi_sum": 0.0},
        )
        pnl = float(trade["pnl_usd"])
        item["count"] += 1
        item["pnl_usd"] += pnl
        item["roi_sum"] += float(trade["roi_percent"])
        if pnl > 0:
            item["wins"] += 1
        elif pnl < 0:
            item["losses"] += 1
    output: list[dict[str, Any]] = []
    for item in grouped.values():
        count = int(item["count"])
        output.append(
            {
                "group": item["group"],
                "count": count,
                "wins": int(item["wins"]),
                "losses": int(item["losses"]),
                "win_rate_percent": round((int(item["wins"]) / count) * 100.0, 4) if count else None,
                "pnl_usd": round(float(item["pnl_usd"]), 6),
                "avg_roi_percent": round(float(item["roi_sum"]) / count, 4) if count else None,
            }
        )
    output.sort(key=lambda item: (-item["count"], item["group"]))
    return output


def _build_market_history_index(rows: list[sqlite3.Row]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        captured_at = _parse_iso(row["captured_at"])
        market_slug = str(row["market_slug"] or "").strip()
        if not market_slug or captured_at is None:
            continue
        index[market_slug].append(
            {
                "captured_at": captured_at,
                "yes_price_cents": _safe_float(row["yes_price_cents"]),
                "no_price_cents": _safe_float(row["no_price_cents"]),
                "yes_best_ask_cents": _safe_float(row["yes_best_ask_cents"]),
                "no_best_ask_cents": _safe_float(row["no_best_ask_cents"]),
                "yes_best_bid_cents": _safe_float(row["yes_best_bid_cents"]),
                "no_best_bid_cents": _safe_float(row["no_best_bid_cents"]),
                "last_trade_price": _safe_float(row["last_trade_price"]),
            }
        )
    return index


def _select_market_snapshot(
    snapshots: list[dict[str, Any]],
    *,
    target_time: datetime | None,
) -> dict[str, Any] | None:
    if not snapshots:
        return None
    if target_time is None:
        return snapshots[-1]
    eligible = [item for item in snapshots if item["captured_at"] <= target_time]
    if eligible:
        return eligible[-1]
    return min(
        snapshots,
        key=lambda item: abs((item["captured_at"] - target_time).total_seconds()),
    )


def _snapshot_side_fields(side: str, snapshot: dict[str, Any] | None) -> tuple[float | None, float | None, float | None]:
    if snapshot is None:
        return None, None, None
    if str(side or "").upper() == "NO":
        return (
            _safe_float(snapshot.get("no_price_cents")),
            _safe_float(snapshot.get("no_best_ask_cents")),
            _safe_float(snapshot.get("no_best_bid_cents")),
        )
    return (
        _safe_float(snapshot.get("yes_price_cents")),
        _safe_float(snapshot.get("yes_best_ask_cents")),
        _safe_float(snapshot.get("yes_best_bid_cents")),
    )


def _build_market_history_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    enriched = [trade for trade in trades if trade.get("market_history_used")]
    if not enriched:
        return {
            "trades_with_market_history": 0,
            "coverage_ratio": 0.0,
            "avg_entry_delta_cents": None,
            "avg_observed_spread_cents": None,
        }
    entry_deltas = [
        abs(float(trade["entry_vs_snapshot_delta_cents"]))
        for trade in enriched
        if trade.get("entry_vs_snapshot_delta_cents") is not None
    ]
    spreads = [
        float(trade["observed_spread_cents"])
        for trade in enriched
        if trade.get("observed_spread_cents") is not None
    ]
    return {
        "trades_with_market_history": len(enriched),
        "coverage_ratio": round(len(enriched) / len(trades), 6) if trades else 0.0,
        "avg_entry_delta_cents": round(sum(entry_deltas) / len(entry_deltas), 6) if entry_deltas else None,
        "avg_observed_spread_cents": round(sum(spreads) / len(spreads), 6) if spreads else None,
    }


def _event_replay(
    rows: list[sqlite3.Row],
    *,
    market_history_index: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    market_history_index = market_history_index or {}
    simulated_trades: list[dict[str, Any]] = []
    skipped: dict[str, int] = defaultdict(int)
    first_seen: dict[str, sqlite3.Row] = {}

    for row in rows:
        market_key = f"{row['event_slug']}|{row['market_slug']}|{row['side']}"
        if market_key in first_seen:
            skipped["duplicate_market_after_entry"] += 1
            continue

        if row["degraded_reason"] and not bool(row["policy_allowed"]):
            skipped[f"degraded:{row['degraded_reason']}"] += 1
            continue
        if not bool(row["policy_allowed"]):
            skipped[f"policy:{row['policy_reason'] or 'blocked'}"] += 1
            continue
        if str(row["price_source"] or "").lower() == "gamma_outcome_price":
            skipped["non_executable_price_source"] += 1
            continue

        entry_price_cents = _safe_float(row["price_cents"])
        settled_price_cents = _safe_float(row["settled_price_cents"])
        generated_at = _parse_iso(row["generated_at"])
        resolved_at = _parse_iso(row["resolved_at"])
        if (
            entry_price_cents is None
            or entry_price_cents <= 0
            or settled_price_cents is None
            or generated_at is None
        ):
            skipped["invalid_row"] += 1
            continue

        snapshot = _select_market_snapshot(
            market_history_index.get(str(row["market_slug"] or "").strip(), []),
            target_time=generated_at,
        )
        snapshot_last_price, snapshot_best_ask, snapshot_best_bid = _snapshot_side_fields(
            str(row["side"] or ""),
            snapshot,
        )
        observed_spread_cents = None
        if snapshot_best_ask is not None and snapshot_best_bid is not None:
            observed_spread_cents = round(max(0.0, snapshot_best_ask - snapshot_best_bid), 6)
        entry_vs_snapshot_delta_cents = None
        if snapshot_best_ask is not None:
            entry_vs_snapshot_delta_cents = round(entry_price_cents - snapshot_best_ask, 6)
        elif snapshot_last_price is not None:
            entry_vs_snapshot_delta_cents = round(entry_price_cents - snapshot_last_price, 6)

        stake_usd = 1.0
        shares = round(stake_usd / (entry_price_cents / 100.0), 6)
        payout_usd = round(shares * (settled_price_cents / 100.0), 6)
        pnl_usd = round(payout_usd - stake_usd, 6)
        roi_percent = round((pnl_usd / stake_usd) * 100.0, 4)
        first_seen[market_key] = row
        simulated_trades.append(
            {
                "market_key": market_key,
                "generated_at": generated_at.isoformat(),
                "resolved_at": resolved_at.isoformat() if resolved_at else None,
                "city_key": row["city_key"],
                "event_slug": row["event_slug"],
                "market_slug": row["market_slug"],
                "event_title": row["event_title"],
                "bucket": row["bucket"],
                "side": row["side"],
                "entry_price_cents": round(entry_price_cents, 4),
                "settled_price_cents": round(settled_price_cents, 4),
                "stake_usd": stake_usd,
                "shares": shares,
                "payout_usd": payout_usd,
                "pnl_usd": pnl_usd,
                "roi_percent": roi_percent,
                "confidence_tier": row["confidence_tier"],
                "policy_reason": row["policy_reason"],
                "market_history_used": snapshot is not None,
                "market_snapshot_at": snapshot["captured_at"].isoformat() if snapshot is not None else None,
                "market_snapshot_price_cents": round(snapshot_last_price, 6) if snapshot_last_price is not None else None,
                "market_snapshot_best_ask_cents": round(snapshot_best_ask, 6) if snapshot_best_ask is not None else None,
                "market_snapshot_best_bid_cents": round(snapshot_best_bid, 6) if snapshot_best_bid is not None else None,
                "observed_spread_cents": observed_spread_cents,
                "entry_vs_snapshot_delta_cents": entry_vs_snapshot_delta_cents,
            }
        )

    wins = sum(1 for trade in simulated_trades if trade["pnl_usd"] > 0)
    losses = sum(1 for trade in simulated_trades if trade["pnl_usd"] < 0)
    pnl_total = round(sum(trade["pnl_usd"] for trade in simulated_trades), 6)
    return {
        "trades": simulated_trades,
        "simulated_trades": len(simulated_trades),
        "wins": wins,
        "losses": losses,
        "win_rate_percent": round((wins / len(simulated_trades)) * 100.0, 4)
        if simulated_trades
        else None,
        "pnl_usd": pnl_total,
        "skipped": dict(sorted(skipped.items())),
        "market_history_summary": _build_market_history_summary(simulated_trades),
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Replay minimo orientado a eventos com scans gravados em ordem temporal."
    )
    parser.add_argument("--db-path", default="export/db/weather_bot.db")
    parser.add_argument("--report-json", default="export/replay/replay_report.json")
    parser.add_argument("--gate-json", default="export/replay/replay_gate.json")
    parser.add_argument(
        "--expected-interval-seconds",
        type=int,
        default=int(os.getenv("WEATHER_AUTO_TRADE_INTERVAL_SECONDS", os.getenv("WEATHER_MONITOR_INTERVAL_SECONDS", "300"))),
    )
    parser.add_argument("--max-gap-ratio", type=float, default=2.5)
    parser.add_argument("--min-simulated-trades", type=int, default=2)
    parser.add_argument("--max-point-in-time-brier", type=float, default=0.25)
    parser.add_argument("--max-non-executable-ratio", type=float, default=0.20)
    parser.add_argument("--recent-lookback-hours", type=int, default=72)
    parser.add_argument("--approve", action="store_true", help="Marca o gate como aprovado.")
    args = parser.parse_args(argv)

    db_path = _resolve_path(args.db_path)
    report_path = _resolve_path(args.report_json)
    gate_path = _resolve_path(args.gate_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.parent.mkdir(parents=True, exist_ok=True)

    prediction_rows, run_rows = _fetch_rows(db_path)
    market_history_rows = _fetch_market_history_rows(
        db_path,
        lookback_hours=max(0, int(args.recent_lookback_hours)),
    )
    prediction_rows = _filter_recent_rows(
        prediction_rows,
        timestamp_key="generated_at",
        lookback_hours=max(0, int(args.recent_lookback_hours)),
    )
    run_rows = _filter_recent_rows(
        run_rows,
        timestamp_key="generated_at",
        lookback_hours=max(0, int(args.recent_lookback_hours)),
    )
    gap_summary = _build_run_gap_summary(
        run_rows,
        expected_interval_seconds=max(1, args.expected_interval_seconds),
        gap_ratio=max(1.0, args.max_gap_ratio),
    )
    replay = _event_replay(
        prediction_rows,
        market_history_index=_build_market_history_index(market_history_rows),
    )
    trades = replay["trades"]
    point_in_time_validation = _build_point_in_time_validation(
        prediction_rows,
        min_trades=max(1, args.min_simulated_trades),
        max_brier_score=max(0.0, args.max_point_in_time_brier),
        max_non_executable_ratio=max(0.0, min(1.0, args.max_non_executable_ratio)),
    )
    eligible_for_manual_review = (
        replay["simulated_trades"] >= args.min_simulated_trades
        and gap_summary["current_session_large_gap_count"] == 0
    )
    production_validation = {
        "replay_is_only_supporting_evidence": True,
        "point_in_time_validation": point_in_time_validation,
        "sufficient_for_production_validation": bool(point_in_time_validation.get("passed")) and eligible_for_manual_review,
        "blocking_reasons": [
            reason
            for reason, is_blocked in (
                ("insufficient_replay_coverage", replay["simulated_trades"] < args.min_simulated_trades),
                ("scan_gaps_detected", gap_summary["current_session_large_gap_count"] > 0),
                ("missing_or_failed_point_in_time_validation", not bool(point_in_time_validation.get("passed"))),
            )
            if is_blocked
        ],
    }

    report = {
        "db_path": str(db_path),
        "simulation_mode": "recorded_scan_event_replay",
        "limitations": [
            "usa apenas scans realmente gravados no banco",
            "nao reconstrui scans perdidos fora do historico salvo",
            "assume fill imediato no preco executavel gravado",
            "usa snapshots historicos de mercado apenas como contexto auxiliar quando disponiveis",
            "nao simula latencia, slippage ou filas de maker",
            "nao e suficiente sozinho para validar prontidao de producao",
        ],
        "recent_lookback_hours": int(args.recent_lookback_hours),
        "resolved_prediction_rows": len(prediction_rows),
        "run_gap_summary": gap_summary,
        "replay_summary": {
            "simulated_trades": replay["simulated_trades"],
            "wins": replay["wins"],
            "losses": replay["losses"],
            "win_rate_percent": replay["win_rate_percent"],
            "pnl_usd": replay["pnl_usd"],
            "skipped": replay["skipped"],
        },
        "market_history_summary": replay["market_history_summary"],
        "by_confidence": _group_trade_summary(trades, "confidence_tier"),
        "by_policy": _group_trade_summary(trades, "policy_reason"),
        "by_city": _group_trade_summary(trades, "city_key"),
        "trades": trades,
        "eligible_for_manual_review": eligible_for_manual_review,
        "production_validation": production_validation,
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    approved = bool(args.approve) and bool(production_validation["sufficient_for_production_validation"])
    gate = {
        "approved": approved,
        "manual_approval_requested": bool(args.approve),
        "eligible_for_manual_review": eligible_for_manual_review,
        "sufficient_for_production_validation": production_validation["sufficient_for_production_validation"],
        "blocking_reasons": production_validation["blocking_reasons"],
        "report_path": str(report_path),
        "simulation_mode": "recorded_scan_event_replay",
        "simulated_trades": replay["simulated_trades"],
        "wins": replay["wins"],
        "losses": replay["losses"],
        "pnl_usd": replay["pnl_usd"],
        "large_gap_count": gap_summary["large_gap_count"],
        "current_session_large_gap_count": gap_summary["current_session_large_gap_count"],
    }
    gate_path.write_text(json.dumps(gate, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Replay report salvo em {report_path}")
    print(
        "Replay gate salvo em "
        f"{gate_path} | approved={gate['approved']} | eligible_for_manual_review={eligible_for_manual_review}"
    )


if __name__ == "__main__":
    main()
