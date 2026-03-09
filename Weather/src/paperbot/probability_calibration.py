from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_PROBABILITY_CALIBRATION_PATH = str(
    Path(__file__).resolve().parents[2] / "export" / "calibration" / "weather_probability_calibration.json"
)
PROBABILITY_CALIBRATION_PATH = os.getenv(
    "WEATHER_PROBABILITY_CALIBRATION_PATH",
    DEFAULT_PROBABILITY_CALIBRATION_PATH,
)


@dataclass
class ProbabilityCalibrationResult:
    raw_probability: float
    calibrated_probability: float
    source: str
    bin_count: int


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


def infer_horizon_days(generated_at: Any, date_str: Any) -> int | None:
    created = _parse_iso(generated_at)
    if created is None:
        return None
    try:
        target = datetime.fromisoformat(str(date_str)).date()
    except ValueError:
        return None
    return (target - created.date()).days


def _bucket_probability(probability: float, bin_size: int) -> tuple[int, int]:
    clamped = max(0.0, min(1.0, float(probability)))
    low = int((clamped * 100.0) // bin_size) * bin_size
    if low >= 100:
        low = 100 - bin_size
    high = min(100, low + bin_size)
    return low, high


def _group_key(city_key: str | None = None, horizon_days: int | None = None) -> str:
    if city_key and horizon_days is not None:
        return f"city:{city_key}:h{horizon_days}"
    if city_key:
        return f"city:{city_key}"
    return "global"


def build_probability_calibration(
    rows: list[dict[str, Any]],
    *,
    bin_size: int = 10,
    prior_sample_weight: float = 5.0,
    min_group_samples: int = 5,
) -> dict[str, Any]:
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            raw_probability = max(0.0, min(1.0, float(row["model_prob"]) / 100.0))
            outcome = max(0.0, min(1.0, float(row["settled_price_cents"]) / 100.0))
        except (KeyError, TypeError, ValueError):
            continue
        city_key = str(row.get("city_key") or "").strip() or None
        horizon_days = infer_horizon_days(row.get("generated_at"), row.get("date_str"))
        normalized_rows.append(
            {
                "raw_probability": raw_probability,
                "outcome": outcome,
                "city_key": city_key,
                "horizon_days": horizon_days,
            }
        )

    global_base_rate = (
        sum(item["outcome"] for item in normalized_rows) / len(normalized_rows)
        if normalized_rows
        else 0.5
    )

    grouped: dict[str, dict[str, Any]] = {}
    for item in normalized_rows:
        keys = [_group_key()]
        if item["city_key"]:
            keys.append(_group_key(item["city_key"]))
            if item["horizon_days"] is not None:
                keys.append(_group_key(item["city_key"], item["horizon_days"]))
        for key in keys:
            bucket_low, bucket_high = _bucket_probability(item["raw_probability"], bin_size)
            group = grouped.setdefault(
                key,
                {
                    "sample_count": 0,
                    "outcome_sum": 0.0,
                    "positive_count": 0,
                    "bins": {},
                },
            )
            group["sample_count"] += 1
            group["outcome_sum"] += item["outcome"]
            if item["outcome"] >= 0.5:
                group["positive_count"] += 1
            bucket_key = f"{bucket_low}-{bucket_high}"
            bucket = group["bins"].setdefault(
                bucket_key,
                {
                    "low": bucket_low,
                    "high": bucket_high,
                    "sample_count": 0,
                    "raw_probability_sum": 0.0,
                    "outcome_sum": 0.0,
                    "positive_count": 0,
                },
            )
            bucket["sample_count"] += 1
            bucket["raw_probability_sum"] += item["raw_probability"]
            bucket["outcome_sum"] += item["outcome"]
            if item["outcome"] >= 0.5:
                bucket["positive_count"] += 1

    def _materialize_group(group: dict[str, Any]) -> dict[str, Any]:
        bins_output: list[dict[str, Any]] = []
        sample_count = int(group["sample_count"])
        base_rate = (float(group["outcome_sum"]) / sample_count) if sample_count > 0 else global_base_rate
        positive_count = int(group.get("positive_count", 0))
        negative_count = max(0, sample_count - positive_count)
        has_outcome_diversity = positive_count > 0 and negative_count > 0
        for item in sorted(group["bins"].values(), key=lambda entry: entry["low"]):
            count = int(item["sample_count"])
            raw_mean = float(item["raw_probability_sum"]) / count if count > 0 else 0.0
            observed_mean = float(item["outcome_sum"]) / count if count > 0 else base_rate
            calibrated = (
                (float(item["outcome_sum"]) + (prior_sample_weight * base_rate))
                / (count + prior_sample_weight)
            )
            bins_output.append(
                {
                    "low": int(item["low"]),
                    "high": int(item["high"]),
                    "sample_count": count,
                    "positive_count": int(item.get("positive_count", 0)),
                    "raw_mean": round(raw_mean, 6),
                    "observed_mean": round(observed_mean, 6),
                    "calibrated_mean": round(calibrated, 6),
                }
            )
        return {
            "sample_count": sample_count,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "base_rate": round(base_rate, 6),
            "usable": sample_count >= min_group_samples and has_outcome_diversity,
            "has_outcome_diversity": has_outcome_diversity,
            "bins": bins_output,
        }

    global_group = _materialize_group(grouped.get("global", {"sample_count": 0, "outcome_sum": 0.0, "bins": {}}))
    cities: dict[str, Any] = {}
    for key, group in grouped.items():
        if not key.startswith("city:"):
            continue
        parts = key.split(":")
        city_key = parts[1]
        city_item = cities.setdefault(city_key, {"sample_count": 0, "bins": [], "usable": False, "horizon_days": {}})
        materialized = _materialize_group(group)
        if len(parts) == 2:
            city_item.update(materialized)
        elif len(parts) == 3 and parts[2].startswith("h"):
            city_item["horizon_days"][parts[2][1:]] = materialized

    return {
        "bin_size": int(bin_size),
        "prior_sample_weight": float(prior_sample_weight),
        "global": global_group,
        "cities": cities,
        "total_samples": len(normalized_rows),
    }


@lru_cache(maxsize=1)
def load_probability_calibration(path: str | None = None) -> dict[str, Any]:
    target = (path or PROBABILITY_CALIBRATION_PATH).strip()
    if not target:
        return {}
    try:
        with open(target, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def apply_probability_calibration(
    raw_probability: float,
    *,
    city_key: str,
    horizon_days: int | None,
    calibration_payload: dict[str, Any] | None = None,
) -> ProbabilityCalibrationResult:
    payload = calibration_payload if calibration_payload is not None else load_probability_calibration()
    clamped = max(0.0, min(1.0, float(raw_probability)))
    groups: list[tuple[str, dict[str, Any] | None]] = []
    city_payload = payload.get("cities", {}).get(city_key) if isinstance(payload.get("cities"), dict) else None
    horizon_payload = None
    if isinstance(city_payload, dict) and horizon_days is not None:
        horizon_payload = (city_payload.get("horizon_days") or {}).get(str(horizon_days))
    groups.append((f"city:{city_key}:h{horizon_days}", horizon_payload if isinstance(horizon_payload, dict) else None))
    groups.append((f"city:{city_key}", city_payload if isinstance(city_payload, dict) else None))
    groups.append(("global", payload.get("global") if isinstance(payload.get("global"), dict) else None))

    probability_percent = clamped * 100.0
    for source, group in groups:
        if not isinstance(group, dict) or not group.get("usable"):
            continue
        bins = group.get("bins")
        if not isinstance(bins, list):
            continue
        for bin_item in bins:
            try:
                low = float(bin_item["low"])
                high = float(bin_item["high"])
                count = int(bin_item.get("sample_count", 0))
                calibrated_mean = float(bin_item["calibrated_mean"])
            except (KeyError, TypeError, ValueError):
                continue
            upper_inclusive = probability_percent <= high if high >= 100 else probability_percent < high
            if probability_percent < low or not upper_inclusive:
                continue
            blend_strength = min(0.8, max(0.0, count / 20.0))
            calibrated_probability = (clamped * (1.0 - blend_strength)) + (calibrated_mean * blend_strength)
            return ProbabilityCalibrationResult(
                raw_probability=round(clamped, 6),
                calibrated_probability=round(max(0.0, min(1.0, calibrated_probability)), 6),
                source=source,
                bin_count=count,
            )
    return ProbabilityCalibrationResult(
        raw_probability=round(clamped, 6),
        calibrated_probability=round(clamped, 6),
        source="raw",
        bin_count=0,
    )
