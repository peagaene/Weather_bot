from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.degendoppler import CITY_CONFIGS
from paperbot.env import load_app_env

load_app_env(ROOT)


CITY_BY_KEY = {city.key: city for city in CITY_CONFIGS}


@dataclass
class TruthMetricAccumulator:
    sample_count: int = 0
    abs_error_sum: float = 0.0
    sq_error_sum: float = 0.0
    bias_sum: float = 0.0
    within_1f_count: int = 0
    within_2f_count: int = 0

    def record(self, forecast_temp_f: float, observed_high_f: float) -> None:
        error = float(forecast_temp_f) - float(observed_high_f)
        abs_error = abs(error)
        self.sample_count += 1
        self.abs_error_sum += abs_error
        self.sq_error_sum += error * error
        self.bias_sum += error
        if abs_error <= 1.0:
            self.within_1f_count += 1
        if abs_error <= 2.0:
            self.within_2f_count += 1

    def as_metric(self) -> dict[str, float | int]:
        sample_count = max(1, self.sample_count)
        mae = self.abs_error_sum / sample_count
        rmse = math.sqrt(self.sq_error_sum / sample_count)
        bias = self.bias_sum / sample_count
        return {
            "sample_count": self.sample_count,
            "mae": round(mae, 4),
            "rmse": round(rmse, 4),
            "bias": round(bias, 4),
            "within_1f_rate": round(self.within_1f_count / sample_count, 4),
            "within_2f_rate": round(self.within_2f_count / sample_count, 4),
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


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _fetch_joined_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    query = """
        SELECT
            f.captured_at,
            f.city_key,
            f.day_label,
            f.date_str,
            f.source_name,
            f.forecast_temp_f,
            s.observed_high_f
        FROM forecast_source_snapshots f
        INNER JOIN station_observation_daily_highs s
            ON s.city_key = f.city_key
           AND s.local_date = f.date_str
        WHERE f.forecast_temp_f IS NOT NULL
          AND s.observed_high_f IS NOT NULL
        ORDER BY f.captured_at ASC, f.id ASC
    """
    return [dict(row) for row in conn.execute(query).fetchall()]


def _group_metric_rows(rows: list[dict[str, Any]], *, min_samples: int) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, TruthMetricAccumulator]] = {
        "global": {},
        "city": {},
        "city_day_label": {},
        "regime": {},
        "regime_day_label": {},
    }

    for row in rows:
        source_name = str(row.get("source_name") or "").strip().lower()
        city_key = str(row.get("city_key") or "").strip().upper()
        day_label = str(row.get("day_label") or "").strip().lower()
        forecast_temp_f = _safe_float(row.get("forecast_temp_f"))
        observed_high_f = _safe_float(row.get("observed_high_f"))
        if not source_name or forecast_temp_f is None or observed_high_f is None:
            continue

        groups["global"].setdefault(source_name, TruthMetricAccumulator()).record(forecast_temp_f, observed_high_f)
        if city_key:
            groups["city"].setdefault(f"{city_key}|{source_name}", TruthMetricAccumulator()).record(
                forecast_temp_f,
                observed_high_f,
            )
            if day_label:
                groups["city_day_label"].setdefault(f"{city_key}|{day_label}|{source_name}", TruthMetricAccumulator()).record(
                    forecast_temp_f,
                    observed_high_f,
                )
            city = CITY_BY_KEY.get(city_key)
            if city is not None:
                for regime_tag in city.regime_tags:
                    groups["regime"].setdefault(f"{regime_tag}|{source_name}", TruthMetricAccumulator()).record(
                        forecast_temp_f,
                        observed_high_f,
                    )
                    if day_label:
                        groups["regime_day_label"].setdefault(
                            f"{regime_tag}|{day_label}|{source_name}",
                            TruthMetricAccumulator(),
                        ).record(forecast_temp_f, observed_high_f)

    def finalize(source: dict[str, TruthMetricAccumulator]) -> dict[str, dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        for key, accumulator in source.items():
            metric = accumulator.as_metric()
            if int(metric["sample_count"]) < min_samples:
                continue
            output[key] = metric
        return output

    return {group_name: finalize(group) for group_name, group in groups.items()}


def _metric_to_multiplier(metric: dict[str, Any]) -> float:
    mae = float(metric.get("mae") or 99.0)
    rmse = float(metric.get("rmse") or 99.0)
    bias = abs(float(metric.get("bias") or 0.0))
    within_1f_rate = float(metric.get("within_1f_rate") or 0.0)
    within_2f_rate = float(metric.get("within_2f_rate") or 0.0)
    raw = 1.18 - (mae * 0.09) - (rmse * 0.03) - (bias * 0.03) + (within_1f_rate * 0.16) + (within_2f_rate * 0.08)
    return round(max(0.7, min(1.35, raw)), 4)


def _sorted_metric_table(group: dict[str, dict[str, Any]], key_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group_key, metric in group.items():
        rows.append({key_name: group_key, **metric, "multiplier": _metric_to_multiplier(metric)})
    rows.sort(
        key=lambda item: (
            -int(item["sample_count"]),
            float(item["mae"]),
            float(item["rmse"]),
            abs(float(item["bias"])),
        )
    )
    return rows


def _build_truth_weight_profile(
    metric_groups: dict[str, dict[str, dict[str, Any]]],
    *,
    min_samples: int,
    joined_rows: int,
) -> dict[str, Any]:
    global_weights = {
        source_name: _metric_to_multiplier(metric)
        for source_name, metric in sorted(metric_groups["global"].items())
    }

    cities: dict[str, Any] = {}
    for key, metric in metric_groups["city"].items():
        city_key, source_name = key.split("|", 1)
        city_payload = cities.setdefault(city_key, {"model_weight_multiplier": {}, "day_labels": {}})
        city_payload["model_weight_multiplier"][source_name] = _metric_to_multiplier(metric)
    for key, metric in metric_groups["city_day_label"].items():
        city_key, day_label, source_name = key.split("|", 2)
        city_payload = cities.setdefault(city_key, {"model_weight_multiplier": {}, "day_labels": {}})
        day_payload = city_payload["day_labels"].setdefault(day_label, {"model_weight_multiplier": {}})
        day_payload["model_weight_multiplier"][source_name] = _metric_to_multiplier(metric)

    regimes: dict[str, Any] = {}
    for key, metric in metric_groups["regime"].items():
        regime_tag, source_name = key.split("|", 1)
        regime_payload = regimes.setdefault(regime_tag, {"model_weight_multiplier": {}, "day_labels": {}})
        regime_payload["model_weight_multiplier"][source_name] = _metric_to_multiplier(metric)
    for key, metric in metric_groups["regime_day_label"].items():
        regime_tag, day_label, source_name = key.split("|", 2)
        regime_payload = regimes.setdefault(regime_tag, {"model_weight_multiplier": {}, "day_labels": {}})
        day_payload = regime_payload["day_labels"].setdefault(day_label, {"model_weight_multiplier": {}})
        day_payload["model_weight_multiplier"][source_name] = _metric_to_multiplier(metric)

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "generated_from_truth_matches": int(joined_rows),
        "min_samples": int(min_samples),
        "global": {"model_weight_multiplier": global_weights},
        "cities": cities,
        "regimes": regimes,
        "metadata": {
            "city_regimes": {key: list(city.regime_tags) for key, city in CITY_BY_KEY.items()},
        },
    }


def generate_truth_weight_analysis(
    *,
    db_path: Path,
    min_samples: int,
    top_segments: int,
    output_json: Path | None = None,
    profile_json: Path | None = None,
) -> dict[str, Any]:
    conn = _connect(db_path)
    try:
        rows = _fetch_joined_rows(conn)
    finally:
        conn.close()

    metric_groups = _group_metric_rows(rows, min_samples=max(1, int(min_samples)))
    profile = _build_truth_weight_profile(
        metric_groups,
        min_samples=max(1, int(min_samples)),
        joined_rows=len(rows),
    )
    payload = {
        "db_path": str(db_path),
        "min_samples": max(1, int(min_samples)),
        "top_segments": max(1, int(top_segments)),
        "truth_matches": len(rows),
        "metrics": {
            "global": _sorted_metric_table(metric_groups["global"], "source_name")[:top_segments],
            "city": _sorted_metric_table(metric_groups["city"], "city_source")[:top_segments],
            "city_day_label": _sorted_metric_table(metric_groups["city_day_label"], "city_day_label_source")[:top_segments],
            "regime": _sorted_metric_table(metric_groups["regime"], "regime_source")[:top_segments],
            "regime_day_label": _sorted_metric_table(metric_groups["regime_day_label"], "regime_day_label_source")[:top_segments],
        },
        "truth_weight_profile": profile,
    }
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    if profile_json is not None:
        profile_json.parent.mkdir(parents=True, exist_ok=True)
        profile_json.write_text(json.dumps(profile, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def _print_summary(payload: dict[str, Any]) -> None:
    print(f"truth_matches={payload['truth_matches']}")
    print("global truth multipliers:")
    global_models = payload["truth_weight_profile"].get("global", {}).get("model_weight_multiplier", {})
    for model_name, multiplier in sorted(global_models.items(), key=lambda item: (-float(item[1]), item[0])):
        print(f"  {model_name}: {multiplier}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Gera pesos por source usando truth observado da estacao oficial.")
    parser.add_argument("--db-path", default="export/db/weather_bot.db")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--profile-json", default="export/analysis/truth_weight_profile.json")
    parser.add_argument("--min-samples", type=int, default=20)
    parser.add_argument("--top-segments", type=int, default=10)
    args = parser.parse_args(argv)

    db_path = _resolve_path(args.db_path)
    output_json = _resolve_path(args.output_json) if args.output_json else None
    profile_json = _resolve_path(args.profile_json) if args.profile_json else None
    payload = generate_truth_weight_analysis(
        db_path=db_path,
        min_samples=args.min_samples,
        top_segments=args.top_segments,
        output_json=output_json,
        profile_json=profile_json,
    )
    _print_summary(payload)
    if output_json is None:
        print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
