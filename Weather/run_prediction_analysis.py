from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.env import load_app_env

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


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _canonical_bucket_key(label: Any) -> str:
    text = str(label or "").upper()
    for token in ("°F", "ºF", "Â°F", "ÂºF", "Ã‚Â°F", "Ã‚ÂºF", "Ãƒâ€šÃ‚Â°F", "Ãƒâ€šÃ‚ÂºF"):
        text = text.replace(token, "F")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    text = re.sub(r"\bF\b", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _fetch_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row["name"])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def _available_prediction_columns(conn: sqlite3.Connection) -> list[str]:
    preferred = [
        "generated_at",
        "city_key",
        "date_str",
        "day_label",
        "event_slug",
        "market_slug",
        "bucket",
        "side",
        "edge",
        "ev_percent",
        "price_cents",
        "model_prob",
        "market_prob",
        "weighted_score",
        "consensus_score",
        "spread",
        "sigma",
        "agreement_pct",
        "confidence_tier",
        "signal_tier",
        "policy_allowed",
        "policy_reason",
        "coverage_ok",
        "degraded_reason",
        "price_source",
        "settled_price_cents",
        "pnl_usd",
        "roi_percent",
        "resolved_at",
    ]
    existing = _fetch_columns(conn, "scan_predictions")
    return [column for column in preferred if column in existing]


def _fetch_resolved_predictions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    columns = _available_prediction_columns(conn)
    query = f"""
        SELECT {", ".join(columns)}
        FROM scan_predictions
        WHERE resolved_at IS NOT NULL
        ORDER BY generated_at ASC, id ASC
    """
    return [dict(row) for row in conn.execute(query).fetchall()]


def _compute_row_metrics(row: dict[str, Any]) -> dict[str, Any]:
    settled_price_cents = _safe_float(row.get("settled_price_cents"))
    pnl_usd = _safe_float(row.get("pnl_usd"))
    roi_percent = _safe_float(row.get("roi_percent"))
    return {
        **row,
        "won": bool(settled_price_cents is not None and settled_price_cents >= 99.999),
        "lost": bool(settled_price_cents is not None and settled_price_cents <= 0.001),
        "pnl_usd_value": pnl_usd if pnl_usd is not None else 0.0,
        "roi_percent_value": roi_percent if roi_percent is not None else 0.0,
        "bucket_key": _canonical_bucket_key(row.get("bucket")),
    }


def _group_summary(rows: list[dict[str, Any]], key: str, *, min_samples: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        raw_value = row.get(key)
        group_key = str(raw_value if raw_value not in (None, "") else "__missing__")
        grouped.setdefault(group_key, []).append(row)

    output: list[dict[str, Any]] = []
    for group_key, items in grouped.items():
        if len(items) < min_samples:
            continue
        wins = sum(1 for item in items if item["won"])
        losses = sum(1 for item in items if item["lost"])
        pnl_sum = sum(float(item["pnl_usd_value"]) for item in items)
        roi_sum = sum(float(item["roi_percent_value"]) for item in items)
        output.append(
            {
                key: None if group_key == "__missing__" else group_key,
                "sample_count": len(items),
                "wins": wins,
                "losses": losses,
                "win_rate": round(wins / len(items), 4) if items else None,
                "avg_pnl_usd": round(pnl_sum / len(items), 4) if items else None,
                "total_pnl_usd": round(pnl_sum, 4),
                "avg_roi_percent": round(roi_sum / len(items), 4) if items else None,
            }
        )
    output.sort(
        key=lambda item: (
            -int(item["sample_count"]),
            -float(item["win_rate"] or 0.0),
            -float(item["avg_pnl_usd"] or 0.0),
        )
    )
    return output


def _build_summary(rows: list[dict[str, Any]], *, min_samples: int, top_segments: int) -> dict[str, Any]:
    sample_count = len(rows)
    wins = sum(1 for row in rows if row["won"])
    losses = sum(1 for row in rows if row["lost"])
    pnl_sum = sum(float(row["pnl_usd_value"]) for row in rows)
    roi_sum = sum(float(row["roi_percent_value"]) for row in rows)

    group_keys = [
        key
        for key in ("city_key", "side", "policy_allowed", "confidence_tier", "policy_reason", "bucket", "day_label")
        if any(key in row for row in rows)
    ]
    by_group = {
        key: _group_summary(rows, key, min_samples=min_samples)[:top_segments]
        for key in group_keys
    }

    best_segments = []
    worst_segments = []
    bucket_segments = by_group.get("bucket") or []
    if bucket_segments:
        best_segments = sorted(
            bucket_segments,
            key=lambda item: (-float(item["avg_pnl_usd"] or 0.0), -int(item["sample_count"])),
        )[:top_segments]
        worst_segments = sorted(
            bucket_segments,
            key=lambda item: (float(item["avg_pnl_usd"] or 0.0), -int(item["sample_count"])),
        )[:top_segments]

    return {
        "resolved_predictions": sample_count,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / sample_count, 4) if sample_count else None,
        "avg_pnl_usd": round(pnl_sum / sample_count, 4) if sample_count else None,
        "total_pnl_usd": round(pnl_sum, 4),
        "avg_roi_percent": round(roi_sum / sample_count, 4) if sample_count else None,
        "segments": by_group,
        "best_buckets": best_segments,
        "worst_buckets": worst_segments,
    }


def _build_policy_profile(rows: list[dict[str, Any]]) -> dict[str, Any]:
    city_segments = _group_summary(rows, "city_key", min_samples=200)
    bucket_segments = _group_summary(rows, "bucket_key", min_samples=200)
    day_segments = _group_summary(rows, "day_label", min_samples=200)

    blocked_city_keys = [
        str(item["city_key"]).upper()
        for item in city_segments
        if float(item["avg_pnl_usd"] or 0.0) <= -0.5 or float(item["win_rate"] or 0.0) <= 0.05
    ]
    caution_city_keys = [
        str(item["city_key"]).upper()
        for item in city_segments
        if str(item["city_key"]).upper() not in blocked_city_keys
        and (float(item["avg_pnl_usd"] or 0.0) < 0.0 or float(item["win_rate"] or 0.0) < 0.4)
    ]
    caution_buckets = [
        str(item["bucket_key"])
        for item in bucket_segments
        if float(item["avg_pnl_usd"] or 0.0) <= -0.3 or float(item["win_rate"] or 0.0) <= 0.1
    ]
    preferred_day_labels = [
        str(item["day_label"]).lower()
        for item in day_segments
        if float(item["avg_pnl_usd"] or 0.0) > 0.0 and float(item["win_rate"] or 0.0) >= 0.5
    ]
    return {
        "generated_from_resolved_predictions": len(rows),
        "blocked_city_keys": sorted(set(blocked_city_keys)),
        "caution_city_keys": sorted(set(caution_city_keys)),
        "caution_buckets": sorted(set(caution_buckets)),
        "preferred_day_labels": sorted(set(preferred_day_labels)),
    }


def generate_prediction_analysis(
    *,
    db_path: Path,
    min_samples: int,
    top_segments: int,
    output_json: Path | None = None,
    policy_profile_json: Path | None = None,
) -> dict[str, Any]:
    conn = _connect(db_path)
    try:
        rows = [_compute_row_metrics(row) for row in _fetch_resolved_predictions(conn)]
    finally:
        conn.close()

    payload = {
        "db_path": str(db_path),
        "min_samples": max(1, int(min_samples)),
        "top_segments": max(1, int(top_segments)),
        "summary": _build_summary(
            rows,
            min_samples=max(1, int(min_samples)),
            top_segments=max(1, int(top_segments)),
        ),
        "policy_profile": _build_policy_profile(rows),
    }

    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    if policy_profile_json is not None:
        policy_profile_json.parent.mkdir(parents=True, exist_ok=True)
        policy_profile_json.write_text(
            json.dumps(payload["policy_profile"], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return payload


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Analisa o historico de scan_predictions para calibracao do bot.")
    parser.add_argument("--db-path", default="export/db/weather_bot.db")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--policy-profile-json", default="export/analysis/policy_profile.json")
    parser.add_argument("--min-samples", type=int, default=25)
    parser.add_argument("--top-segments", type=int, default=10)
    args = parser.parse_args(argv)

    db_path = _resolve_path(args.db_path)
    output_path = _resolve_path(args.output_json) if args.output_json else None
    profile_path = _resolve_path(args.policy_profile_json) if args.policy_profile_json else None
    payload = generate_prediction_analysis(
        db_path=db_path,
        min_samples=max(1, int(args.min_samples)),
        top_segments=max(1, int(args.top_segments)),
        output_json=output_path,
        policy_profile_json=profile_path,
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
