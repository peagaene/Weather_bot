from __future__ import annotations

import argparse
import json
import math
import sqlite3
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent


PROVIDER_NOTES = {
    "tomorrow": "Historical rows before 2026-03-11 may include unit contamination; re-baseline recommended.",
    "weatherstack": "Current account plan does not support forecast; keep disabled for live influence.",
    "weatherbit": "API key still provisioning or not fully activated.",
}

PROVIDER_MANUAL_RECOMMENDATIONS = {
    "weatherstack": "disable",
}

ROLL_OUT_PROVIDER_NAMES = {
    "tomorrow",
    "weatherapi",
    "visualcrossing",
    "openweather",
    "weatherbit",
    "meteosource",
    "pirateweather",
    "met_norway",
    "brightsky",
    "meteoblue",
    "weatherstack",
    "accuweather",
}

IMPLEMENTED_PROVIDERS = sorted(ROLL_OUT_PROVIDER_NAMES)

RECOMMENDATION_TO_STAGE = {
    "candidate_live_influence": "eligible_for_live_influence",
    "candidate_weighting": "eligible_for_weighting",
    "observe": "observation_only",
    "disable": "rejected_or_low_value",
    "rebaseline": "observation_only",
}

RECOMMENDATION_TO_MULTIPLIER = {
    "candidate_live_influence": 1.05,
    "candidate_weighting": 1.0,
    "observe": 0.9,
    "disable": 0.0,
    "rebaseline": 0.5,
}


def _resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return ROOT / path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _recommend_provider(row: dict[str, Any]) -> str:
    sample_count = int(row.get("sample_count") or 0)
    mae = float(row.get("mae") or 999.0)
    rmse = float(row.get("rmse") or 999.0)
    bias = abs(float(row.get("bias") or 0.0))
    within_2f_rate = float(row.get("within_2f_rate") or 0.0)
    source_name = str(row.get("source_name") or "").strip().lower()
    if source_name == "weatherstack":
        return "disable"
    if source_name == "tomorrow" and mae > 20.0:
        return "rebaseline"
    if sample_count < 50:
        return "observe"
    if mae <= 5.5 and rmse <= 6.5 and bias <= 4.0 and within_2f_rate >= 0.18 and sample_count >= 250:
        return "candidate_live_influence"
    if mae <= 7.0 and bias <= 6.0 and within_2f_rate >= 0.12:
        return "candidate_weighting"
    if mae >= 10.0 or within_2f_rate < 0.05:
        return "disable"
    return "observe"


def generate_provider_benchmark(*, db_path: Path, min_samples: int, limit: int) -> dict[str, Any]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT
                f.source_name,
                COUNT(*) AS sample_count,
                ROUND(AVG(ABS(f.forecast_temp_f - s.observed_high_f)), 4) AS mae,
                AVG((f.forecast_temp_f - s.observed_high_f) * (f.forecast_temp_f - s.observed_high_f)) AS mse_raw,
                ROUND(AVG(f.forecast_temp_f - s.observed_high_f), 4) AS bias,
                ROUND(AVG(CASE WHEN ABS(f.forecast_temp_f - s.observed_high_f) <= 1.0 THEN 1.0 ELSE 0.0 END), 4) AS within_1f_rate,
                ROUND(AVG(CASE WHEN ABS(f.forecast_temp_f - s.observed_high_f) <= 2.0 THEN 1.0 ELSE 0.0 END), 4) AS within_2f_rate
            FROM forecast_source_snapshots f
            INNER JOIN station_observation_daily_highs s
                ON s.city_key = f.city_key
               AND s.local_date = f.date_str
            WHERE f.forecast_temp_f IS NOT NULL
              AND s.observed_high_f IS NOT NULL
            GROUP BY f.source_name
            HAVING COUNT(*) >= ?
            ORDER BY mae ASC, mse_raw ASC, ABS(bias) ASC, sample_count DESC
            LIMIT ?
            """,
            (max(1, int(min_samples)), max(1, int(limit))),
        ).fetchall()
    finally:
        conn.close()

    providers: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        mse_raw = float(item.pop("mse_raw") or 0.0)
        item["rmse"] = round(math.sqrt(max(0.0, mse_raw)), 4)
        item["recommendation"] = _recommend_provider(item)
        note = PROVIDER_NOTES.get(str(item["source_name"]).strip().lower())
        if note:
            item["note"] = note
        providers.append(item)

    keep = [item for item in providers if item["recommendation"] == "candidate_live_influence"]
    weight = [item for item in providers if item["recommendation"] == "candidate_weighting"]
    observe = [item for item in providers if item["recommendation"] == "observe"]
    disable = [item for item in providers if item["recommendation"] in {"disable", "rebaseline"}]

    return {
        "db_path": str(db_path),
        "min_samples": max(1, int(min_samples)),
        "provider_count": len(providers),
        "top_ranked": providers,
        "candidate_live_influence": keep,
        "candidate_weighting": weight,
        "observe": observe,
        "disable_or_rebaseline": disable,
        "provider_rollout_profile": _build_rollout_profile(providers),
    }


def _build_rollout_profile(providers: list[dict[str, Any]]) -> dict[str, Any]:
    by_name = {str(item["source_name"]).strip().lower(): item for item in providers}
    provider_states: dict[str, Any] = {}
    for provider_name in IMPLEMENTED_PROVIDERS:
        item = by_name.get(provider_name)
        recommendation = str((item or {}).get("recommendation") or PROVIDER_MANUAL_RECOMMENDATIONS.get(provider_name, "observe"))
        stage = RECOMMENDATION_TO_STAGE.get(recommendation, "observation_only")
        multiplier = float(RECOMMENDATION_TO_MULTIPLIER.get(recommendation, 0.9))
        provider_state = {
            "stage": stage,
            "recommendation": recommendation,
            "weight_multiplier": multiplier,
            "benchmark_available": item is not None,
        }
        if item is not None:
            provider_state.update(
                {
                    "sample_count": int(item.get("sample_count") or 0),
                    "mae": float(item.get("mae") or 0.0),
                    "rmse": float(item.get("rmse") or 0.0),
                    "bias": float(item.get("bias") or 0.0),
                    "within_2f_rate": float(item.get("within_2f_rate") or 0.0),
                }
            )
            note = item.get("note")
            if note:
                provider_state["note"] = str(note)
        provider_states[provider_name] = provider_state
    return {
        "generated_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "providers": provider_states,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Benchmark providers against observed truth in the local database.")
    parser.add_argument("--db-path", default="export/db/weather_bot.db")
    parser.add_argument("--output-json", default="export/analysis/provider_benchmark.json")
    parser.add_argument("--profile-json", default="export/analysis/provider_rollout_profile.json")
    parser.add_argument("--min-samples", type=int, default=50)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args(argv)

    payload = generate_provider_benchmark(
        db_path=_resolve_path(args.db_path),
        min_samples=args.min_samples,
        limit=args.limit,
    )
    output_path = _resolve_path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    profile_path = _resolve_path(args.profile_json)
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(
        json.dumps(payload["provider_rollout_profile"], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
