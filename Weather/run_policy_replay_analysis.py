from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.degendoppler import _parse_bucket_range, convert_temperature, infer_market_temp_unit
from paperbot.env import load_app_env
from paperbot.policy import apply_trade_policy
from paperbot.polymarket_weather import (
    _execution_quality_score,
    _fee_adjusted_price,
    _model_edge_statistics,
    _model_side_agreement_pct,
    _signal_tier,
    _confidence_tier_from_agreement_with_count,
)

load_app_env(ROOT)


def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _json_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _bucket_bounds_from_label(label: str) -> tuple[float, float, str]:
    low_value, high_value = _parse_bucket_range(label)
    bucket_unit = infer_market_temp_unit(label, default="F")
    lower_floor = -100.0 if bucket_unit == "F" else -80.0
    upper_cap = 200.0 if bucket_unit == "F" else 120.0
    normalized_label = str(label or "").lower()
    if "below" in normalized_label or "lower" in normalized_label:
        upper = (high_value if high_value is not None else 20) + 0.5
        return lower_floor, float(upper), bucket_unit
    if "above" in normalized_label or "higher" in normalized_label:
        lower = (low_value if low_value is not None else 50) - 0.5
        return float(lower), upper_cap, bucket_unit
    if low_value is not None and high_value is not None:
        return float(low_value) - 0.5, float(high_value) + 0.5, bucket_unit
    return lower_floor, upper_cap, bucket_unit


def _provider_failures_from_row(row: sqlite3.Row) -> list[str]:
    failures = _json_loads(row["provider_failures_json"], []) if "provider_failures_json" in row.keys() else []
    normalized = [str(item).strip().lower() for item in failures if str(item).strip()]
    if normalized:
        return sorted(set(normalized))
    degraded_reason = str(row["degraded_reason"] or "")
    if "provider_failures:" not in degraded_reason:
        return []
    parts = []
    for chunk in degraded_reason.split(";"):
        chunk = chunk.strip()
        if not chunk.startswith("provider_failures:"):
            continue
        raw = chunk.split(":", 1)[1]
        parts.extend(item.strip().lower() for item in raw.split(",") if item.strip())
    return sorted(set(parts))


def _heuristic_data_quality_score(row: sqlite3.Row, provider_failures: list[str]) -> float:
    if "data_quality_score" in row.keys() and row["data_quality_score"] is not None:
        return _safe_float(row["data_quality_score"])
    coverage_ok = bool(row["coverage_ok"]) if "coverage_ok" in row.keys() else False
    coverage_score = _safe_float(row["coverage_score"], 1.0 if coverage_ok else 0.0)
    score = 0.9 if coverage_ok else 0.55
    if provider_failures:
        score -= min(0.35, 0.05 * len(provider_failures))
    degraded_reason = str(row["degraded_reason"] or "")
    if "insufficient_model_coverage" in degraded_reason:
        score = min(score, 0.4)
    score = min(score, coverage_score + 0.15) if coverage_score > 0 else score
    return round(max(0.0, min(1.0, score)), 4)


def _reconstruct_policy_context(row: sqlite3.Row) -> dict[str, Any]:
    model_predictions = _json_loads(row["model_predictions_json"], {})
    if not isinstance(model_predictions, dict):
        model_predictions = {}
    predictions = {str(k): float(v) for k, v in model_predictions.items() if v is not None}
    low, high, bucket_unit = _bucket_bounds_from_label(str(row["bucket"] or ""))
    agreement_models, total_models, agreement_pct = _model_side_agreement_pct(
        predictions,
        low,
        high,
        str(row["side"] or ""),
        bucket_unit,
    )
    confidence_tier = str(row["confidence_tier"] or "").strip().lower()
    if not confidence_tier:
        confidence_tier = _confidence_tier_from_agreement_with_count(agreement_pct, total_models)
    sigma = max(1.0, _safe_float(row["sigma"], 1.5))
    horizon_lookup = {"today": 0, "tomorrow": 1, "day2": 2}
    horizon_days = horizon_lookup.get(str(row["day_label"] or "").strip().lower())
    mean_agreeing_model_edge = _safe_float(row["mean_agreeing_model_edge"]) if "mean_agreeing_model_edge" in row.keys() else 0.0
    min_agreeing_model_edge = _safe_float(row["min_agreeing_model_edge"]) if "min_agreeing_model_edge" in row.keys() else 0.0
    agreeing_model_count = int(row["agreeing_model_count"] or 0) if "agreeing_model_count" in row.keys() and row["agreeing_model_count"] is not None else 0
    if predictions and ("min_agreeing_model_edge" not in row.keys() or row["min_agreeing_model_edge"] is None):
        ensemble = SimpleNamespace(predictions=predictions, sigma=sigma)
        (
            mean_agreeing_model_edge,
            min_agreeing_model_edge,
            agreeing_model_count,
            _,
            _,
        ) = _model_edge_statistics(
            ensemble,
            low=low,
            high=high,
            bucket_unit=bucket_unit,
            side=str(row["side"] or ""),
            break_even_price=_fee_adjusted_price(_safe_float(row["price_cents"])),
            horizon_days=horizon_days,
        )
    executable_quality_score = (
        _safe_float(row["executable_quality_score"])
        if "executable_quality_score" in row.keys() and row["executable_quality_score"] is not None
        else _execution_quality_score(
            price_source=str(row["price_source"] or ""),
            entry_price_cents=_safe_float(row["price_cents"]),
            best_bid_cents=(_safe_float(row["best_bid_cents"]) if "best_bid_cents" in row.keys() else None),
            order_min_size=(_safe_float(row["order_min_size"]) if "order_min_size" in row.keys() else None),
        )
    )
    provider_failures = _provider_failures_from_row(row)
    data_quality_score = _heuristic_data_quality_score(row, provider_failures)
    signal_tier = str(row["signal_tier"] or "").strip().upper() if "signal_tier" in row.keys() else ""
    if not signal_tier:
        signal_tier, _, _ = _signal_tier(
            model_prob=_safe_float(row["model_prob"]),
            mean_agreeing_model_edge=mean_agreeing_model_edge,
            min_agreeing_model_edge=min_agreeing_model_edge,
            agreement_pct=agreement_pct,
            executable_quality_score=executable_quality_score,
            data_quality_score=data_quality_score,
            consensus_score=_safe_float(row["consensus_score"]),
        )
    return {
        "model_predictions": predictions,
        "agreement_models": agreement_models,
        "total_models": total_models,
        "agreement_pct": round(agreement_pct, 2),
        "confidence_tier": confidence_tier or "risky",
        "signal_tier": signal_tier or "C",
        "mean_agreeing_model_edge": mean_agreeing_model_edge,
        "min_agreeing_model_edge": min_agreeing_model_edge,
        "agreeing_model_count": agreeing_model_count,
        "executable_quality_score": executable_quality_score,
        "data_quality_score": data_quality_score,
        "coverage_score": _safe_float(row["coverage_score"], 1.0 if bool(row["coverage_ok"]) else 0.0)
        if "coverage_score" in row.keys()
        else (1.0 if bool(row["coverage_ok"]) else 0.0),
        "valid_model_count": int(row["valid_model_count"] or agreement_models or total_models or len(predictions))
        if "valid_model_count" in row.keys() and row["valid_model_count"] is not None
        else max(agreement_models, total_models, len(predictions)),
        "required_model_count": int(row["required_model_count"] or 0) if "required_model_count" in row.keys() and row["required_model_count"] is not None else 0,
        "provider_failures": provider_failures,
        "provider_failure_details": _json_loads(row["provider_failure_details_json"], {}) if "provider_failure_details_json" in row.keys() else {},
    }


def _build_group_summary(rows: list[dict[str, Any]], keys: tuple[str, ...], min_samples: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        group_key = tuple(row.get(key) for key in keys)
        grouped.setdefault(group_key, []).append(row)
    output = []
    for group_key, items in grouped.items():
        if len(items) < min_samples:
            continue
        wins = sum(1 for item in items if item["won"])
        pnl = sum(float(item.get("pnl_usd") or 0.0) for item in items)
        output.append(
            {
                "segment": " / ".join(str(value) for value in group_key),
                "keys": {key: value for key, value in zip(keys, group_key)},
                "sample_count": len(items),
                "win_rate": round(wins / len(items), 4),
                "total_pnl_usd": round(pnl, 4),
                "avg_pnl_usd": round(pnl / len(items), 4),
            }
        )
    output.sort(key=lambda item: (-float(item["win_rate"]), -float(item["avg_pnl_usd"]), -int(item["sample_count"])))
    return output


def _segment_recommendation(
    *,
    signal_count: int,
    resolved_count: int,
    win_rate: float | None,
    avg_edge: float,
    avg_pnl_usd: float,
    total_pnl_usd: float,
    target_win_rate: float,
    min_samples: int,
) -> str:
    if resolved_count <= 0:
        return "observe"
    if total_pnl_usd <= 0 or avg_pnl_usd <= 0:
        return "block"
    if resolved_count < max(5, min_samples // 2):
        return "observe"
    if win_rate is None:
        return "observe"
    if win_rate >= target_win_rate and avg_edge >= 10.0 and signal_count >= min_samples:
        return "keep"
    if win_rate >= max(0.0, target_win_rate - 0.08) and avg_edge >= 8.0 and total_pnl_usd > 0:
        return "expand"
    if win_rate >= max(0.0, target_win_rate - 0.15) and total_pnl_usd > 0:
        return "observe"
    return "block"


def _build_segment_recommendations(
    rows: list[dict[str, Any]],
    *,
    keys: tuple[str, ...],
    min_samples: int,
    target_win_rate: float,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        group_key = tuple(row.get(key) for key in keys)
        grouped.setdefault(group_key, []).append(row)
    output: list[dict[str, Any]] = []
    for group_key, items in grouped.items():
        signal_count = len(items)
        resolved_items = [item for item in items if item["resolved"]]
        resolved_count = len(resolved_items)
        if signal_count < min_samples and resolved_count < max(3, min_samples // 2):
            continue
        wins = sum(1 for item in resolved_items if item["won"])
        losses = sum(1 for item in resolved_items if item["lost"])
        total_pnl = sum(float(item.get("pnl_usd") or 0.0) for item in resolved_items)
        total_roi = sum(float(item.get("roi_percent") or 0.0) for item in resolved_items)
        total_edge = sum(float(item.get("edge") or 0.0) for item in items)
        win_rate = round(wins / resolved_count, 4) if resolved_count else None
        avg_edge = round(total_edge / signal_count, 4) if signal_count else 0.0
        avg_pnl = round(total_pnl / resolved_count, 4) if resolved_count else 0.0
        avg_roi = round(total_roi / resolved_count, 4) if resolved_count else 0.0
        recommendation = _segment_recommendation(
            signal_count=signal_count,
            resolved_count=resolved_count,
            win_rate=win_rate,
            avg_edge=avg_edge,
            avg_pnl_usd=avg_pnl,
            total_pnl_usd=total_pnl,
            target_win_rate=target_win_rate,
            min_samples=min_samples,
        )
        output.append(
            {
                "segment": " / ".join(str(value) for value in group_key),
                "keys": {key: value for key, value in zip(keys, group_key)},
                "signal_count": signal_count,
                "resolved_count": resolved_count,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "avg_edge": avg_edge,
                "avg_pnl_usd": avg_pnl,
                "total_pnl_usd": round(total_pnl, 4),
                "avg_roi_percent": avg_roi,
                "recommendation": recommendation,
            }
        )
    output.sort(
        key=lambda item: (
            {"keep": 0, "expand": 1, "observe": 2, "block": 3}.get(str(item["recommendation"]), 9),
            -(float(item["total_pnl_usd"])),
            -(float(item["avg_pnl_usd"])),
            -(float(item["win_rate"] or 0.0)),
            -(int(item["signal_count"])),
        )
    )
    return output


def generate_policy_replay_analysis(
    *,
    db_path: Path,
    min_samples: int,
    target_win_rate: float,
    output_json: Path | None = None,
) -> dict[str, Any]:
    conn = _connect(db_path)
    try:
        available = _fetch_columns(conn, "scan_predictions")
        columns = [
            "generated_at", "city_key", "day_label", "date_str", "bucket", "side", "edge", "price_cents",
            "model_prob", "market_prob", "ensemble_prediction", "consensus_score", "spread", "sigma", "confidence_tier",
            "coverage_ok", "degraded_reason", "price_source", "best_bid_cents", "settled_price_cents",
            "pnl_usd", "roi_percent", "resolved_at", "model_predictions_json", "policy_allowed", "policy_reason",
        ]
        optional = [
            "coverage_score", "signal_tier", "mean_agreeing_model_edge", "min_agreeing_model_edge",
            "agreeing_model_count", "executable_quality_score", "data_quality_score", "valid_model_count",
            "required_model_count", "provider_failures_json", "provider_failure_details_json", "order_min_size",
        ]
        selected_columns = [column for column in columns + optional if column in available]
        rows = conn.execute(f"SELECT {', '.join(selected_columns)} FROM scan_predictions ORDER BY generated_at ASC, id ASC").fetchall()
    finally:
        conn.close()

    replay_rows: list[dict[str, Any]] = []
    exact_replay_rows = 0
    exact_allowed = 0
    exact_allowed_resolved = 0
    exact_wins = 0
    exact_pnl_total = 0.0
    exact_roi_total = 0.0
    exact_required = {
        "signal_tier",
        "min_agreeing_model_edge",
        "executable_quality_score",
        "data_quality_score",
        "coverage_score",
        "valid_model_count",
        "required_model_count",
    }
    for row in rows:
        reconstructed = _reconstruct_policy_context(row)
        opportunity = SimpleNamespace(
            city_key=row["city_key"],
            day_label=row["day_label"],
            bucket=row["bucket"],
            side=row["side"],
            edge=_safe_float(row["edge"]),
            price_cents=_safe_float(row["price_cents"]),
            model_prob=_safe_float(row["model_prob"]),
            consensus_score=_safe_float(row["consensus_score"]),
            spread=_safe_float(row["spread"]),
            sigma=_safe_float(row["sigma"]),
            ensemble_prediction=_safe_float(row["ensemble_prediction"]),
            coverage_ok=bool(row["coverage_ok"]),
            degraded_reason=row["degraded_reason"],
            agreement_models=reconstructed["agreement_models"],
            total_models=reconstructed["total_models"],
            agreement_pct=reconstructed["agreement_pct"],
            confidence_tier=reconstructed["confidence_tier"],
            signal_tier=reconstructed["signal_tier"],
            min_agreeing_model_edge=reconstructed["min_agreeing_model_edge"],
            executable_quality_score=reconstructed["executable_quality_score"],
            data_quality_score=reconstructed["data_quality_score"],
            coverage_score=reconstructed["coverage_score"],
            valid_model_count=reconstructed["valid_model_count"],
            required_model_count=reconstructed["required_model_count"],
            provider_failures=reconstructed["provider_failures"],
            provider_failure_details=reconstructed["provider_failure_details"],
        )
        decision = apply_trade_policy(opportunity)
        settled = _safe_float(row["settled_price_cents"], default=-1.0)
        replay_item = {
            "city_key": row["city_key"],
            "day_label": row["day_label"],
            "bucket": row["bucket"],
            "side": row["side"],
            "confidence_tier": reconstructed["confidence_tier"],
            "signal_tier": reconstructed["signal_tier"],
            "price_cents": _safe_float(row["price_cents"]),
            "consensus_score": _safe_float(row["consensus_score"]),
            "spread": _safe_float(row["spread"]),
            "edge": _safe_float(row["edge"]),
            "allowed": bool(decision.allowed),
            "reason": decision.reason,
            "resolved": row["resolved_at"] is not None,
            "won": row["resolved_at"] is not None and settled >= 99.999,
            "lost": row["resolved_at"] is not None and settled < 99.999,
            "pnl_usd": _safe_float(row["pnl_usd"]),
            "roi_percent": _safe_float(row["roi_percent"]),
        }
        replay_rows.append(replay_item)
        has_enriched_runtime_fields = (
            ("agreement_summary" in row.keys() and row["agreement_summary"] not in (None, ""))
            or ("executable_quality_score" in row.keys() and _safe_float(row["executable_quality_score"]) > 0.0)
            or ("data_quality_score" in row.keys() and _safe_float(row["data_quality_score"]) > 0.0)
            or ("coverage_score" in row.keys() and _safe_float(row["coverage_score"]) > 0.0)
            or ("min_agreeing_model_edge" in row.keys() and _safe_float(row["min_agreeing_model_edge"]) > 0.0)
        )
        if (
            has_enriched_runtime_fields
            and exact_required.issubset(set(row.keys()))
            and all(row[key] is not None for key in exact_required)
        ):
            exact_replay_rows += 1
            if decision.allowed:
                exact_allowed += 1
                if replay_item["resolved"]:
                    exact_allowed_resolved += 1
                    if replay_item["won"]:
                        exact_wins += 1
                    exact_pnl_total += float(replay_item["pnl_usd"])
                    exact_roi_total += float(replay_item["roi_percent"])

    allowed = [row for row in replay_rows if row["allowed"]]
    allowed_resolved = [row for row in allowed if row["resolved"]]
    wins = sum(1 for row in allowed_resolved if row["won"])
    pnl_total = sum(float(row["pnl_usd"]) for row in allowed_resolved)
    roi_total = sum(float(row["roi_percent"]) for row in allowed_resolved)
    blocked_reason_counts: dict[str, int] = {}
    for row in replay_rows:
        if row["allowed"]:
            continue
        blocked_reason_counts[row["reason"]] = blocked_reason_counts.get(row["reason"], 0) + 1
    historical_allowed = [row for row, source in zip(replay_rows, rows) if bool(source["policy_allowed"])]
    historical_allowed_resolved = [row for row in historical_allowed if row["resolved"]]
    historical_wins = sum(1 for row in historical_allowed_resolved if row["won"])
    historical_pnl = sum(float(row["pnl_usd"]) for row in historical_allowed_resolved)
    historical_roi = sum(float(row["roi_percent"]) for row in historical_allowed_resolved)
    current_segment_recommendations = {
        "by_city_day": _build_segment_recommendations(
            allowed,
            keys=("city_key", "day_label"),
            min_samples=min_samples,
            target_win_rate=target_win_rate,
        )[:20],
        "by_confidence_signal": _build_segment_recommendations(
            allowed,
            keys=("confidence_tier", "signal_tier"),
            min_samples=min_samples,
            target_win_rate=target_win_rate,
        )[:20],
        "by_city_day_confidence": _build_segment_recommendations(
            allowed,
            keys=("city_key", "day_label", "confidence_tier"),
            min_samples=min_samples,
            target_win_rate=target_win_rate,
        )[:20],
    }
    historical_segment_recommendations = {
        "by_city_day": _build_segment_recommendations(
            historical_allowed,
            keys=("city_key", "day_label"),
            min_samples=min_samples,
            target_win_rate=target_win_rate,
        )[:20],
        "by_confidence_signal": _build_segment_recommendations(
            historical_allowed,
            keys=("confidence_tier", "signal_tier"),
            min_samples=min_samples,
            target_win_rate=target_win_rate,
        )[:20],
        "by_city_day_confidence": _build_segment_recommendations(
            historical_allowed,
            keys=("city_key", "day_label", "confidence_tier"),
            min_samples=min_samples,
            target_win_rate=target_win_rate,
        )[:20],
    }
    best_historical_roi_segments = sorted(
        historical_segment_recommendations["by_city_day_confidence"],
        key=lambda item: (
            {"keep": 0, "expand": 1, "observe": 2, "block": 3}.get(str(item["recommendation"]), 9),
            -(float(item["avg_roi_percent"])),
            -(float(item["total_pnl_usd"])),
            -(float(item["win_rate"] or 0.0)),
        ),
    )[:10]
    payload = {
        "db_path": str(db_path),
        "total_scan_predictions": len(replay_rows),
        "resolved_scan_predictions": sum(1 for row in replay_rows if row["resolved"]),
        "historical_policy_baseline": {
            "allowed_signals": len(historical_allowed),
            "allowed_resolved": len(historical_allowed_resolved),
            "wins": historical_wins,
            "losses": len(historical_allowed_resolved) - historical_wins,
            "win_rate": round(historical_wins / len(historical_allowed_resolved), 4) if historical_allowed_resolved else None,
            "total_pnl_usd": round(historical_pnl, 4),
            "avg_roi_percent": round(historical_roi / len(historical_allowed_resolved), 4) if historical_allowed_resolved else None,
        },
        "current_policy_replay": {
            "allowed_signals": len(allowed),
            "allowed_resolved": len(allowed_resolved),
            "wins": wins,
            "losses": len(allowed_resolved) - wins,
            "win_rate": round(wins / len(allowed_resolved), 4) if allowed_resolved else None,
            "total_pnl_usd": round(pnl_total, 4),
            "avg_roi_percent": round(roi_total / len(allowed_resolved), 4) if allowed_resolved else None,
        },
        "exact_current_policy_replay_on_modern_rows": {
            "exact_rows": exact_replay_rows,
            "allowed_signals": exact_allowed,
            "allowed_resolved": exact_allowed_resolved,
            "wins": exact_wins,
            "losses": exact_allowed_resolved - exact_wins,
            "win_rate": round(exact_wins / exact_allowed_resolved, 4) if exact_allowed_resolved else None,
            "total_pnl_usd": round(exact_pnl_total, 4),
            "avg_roi_percent": round(exact_roi_total / exact_allowed_resolved, 4) if exact_allowed_resolved else None,
        },
        "target_win_rate": target_win_rate,
        "current_policy_segment_recommendations": current_segment_recommendations,
        "historical_policy_segment_recommendations": historical_segment_recommendations,
        "best_historical_roi_segments": best_historical_roi_segments,
        "top_block_reasons_under_current_policy": dict(sorted(blocked_reason_counts.items(), key=lambda kv: kv[1], reverse=True)[:12]),
    }
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Reprocessa o historico de scan_predictions com a policy atual.")
    parser.add_argument("--db-path", default="export/db/weather_bot.db")
    parser.add_argument("--output-json", default="export/analysis/policy_replay_analysis.json")
    parser.add_argument("--min-samples", type=int, default=25)
    parser.add_argument("--target-win-rate", type=float, default=0.90)
    args = parser.parse_args(argv)

    payload = generate_policy_replay_analysis(
        db_path=_resolve_path(args.db_path),
        min_samples=max(1, int(args.min_samples)),
        target_win_rate=max(0.0, min(1.0, float(args.target_win_rate))),
        output_json=_resolve_path(args.output_json) if args.output_json else None,
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
