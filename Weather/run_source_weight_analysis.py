from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from collections import defaultdict
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
class ModelMetricAccumulator:
    sample_count: int = 0
    directional_hits: int = 0
    edge_correct_sum: float = 0.0
    edge_wrong_sum: float = 0.0
    correct_count: int = 0
    wrong_count: int = 0
    pnl_proxy_sum: float = 0.0

    def record(self, *, correct: bool, edge: float) -> None:
        self.sample_count += 1
        magnitude = max(0.25, min(3.0, abs(float(edge)) / 100.0))
        if correct:
            self.directional_hits += 1
            self.correct_count += 1
            self.edge_correct_sum += float(edge)
            self.pnl_proxy_sum += magnitude
        else:
            self.wrong_count += 1
            self.edge_wrong_sum += float(edge)
            self.pnl_proxy_sum -= magnitude

    def as_metric(self) -> dict[str, float | int | None]:
        hit_rate = (self.directional_hits / self.sample_count) if self.sample_count else 0.0
        avg_edge_correct = (self.edge_correct_sum / self.correct_count) if self.correct_count else None
        avg_edge_wrong = (self.edge_wrong_sum / self.wrong_count) if self.wrong_count else None
        avg_pnl_proxy = (self.pnl_proxy_sum / self.sample_count) if self.sample_count else 0.0
        return {
            "sample_count": self.sample_count,
            "hit_rate_directional": round(hit_rate, 4),
            "avg_edge_when_correct": round(avg_edge_correct, 4) if avg_edge_correct is not None else None,
            "avg_edge_when_wrong": round(avg_edge_wrong, 4) if avg_edge_wrong is not None else None,
            "avg_pnl_proxy": round(avg_pnl_proxy, 4),
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


def _parse_bucket_bounds(label: Any) -> tuple[float | None, float | None]:
    text = str(label or "").upper()
    for token in ("Â°F", "ÂºF", "Ã‚Â°F", "Ã‚ÂºF", "Ãƒâ€šÃ‚Â°F", "Ãƒâ€šÃ‚ÂºF", "ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°F", "ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºF"):
        text = text.replace(token, "F")
    text = text.replace("°", "").replace("º", "").replace("F", "").strip()
    if not text:
        return None, None
    normalized = " ".join(text.split()).lower()
    if "or below" in normalized or "or lower" in normalized:
        digits = "".join(ch for ch in normalized if ch.isdigit())
        return None, float(digits) if digits else None
    if "or above" in normalized or "or higher" in normalized:
        digits = "".join(ch for ch in normalized if ch.isdigit())
        return float(digits) if digits else None, None
    if "-" in normalized:
        left, right = normalized.split("-", 1)
        left_digits = "".join(ch for ch in left if ch.isdigit())
        right_digits = "".join(ch for ch in right if ch.isdigit())
        return (float(left_digits) if left_digits else None, float(right_digits) if right_digits else None)
    digits = "".join(ch for ch in normalized if ch.isdigit())
    if not digits:
        return None, None
    value = float(digits)
    return value, value


def _prediction_side_for_bucket(prediction: float, bucket: str) -> str | None:
    low, high = _parse_bucket_bounds(bucket)
    if low is not None and prediction < low:
        return "NO"
    if high is not None and prediction > high:
        return "NO"
    return "YES" if low is not None or high is not None else None


def _winning_side(selected_side: str, settled_price_cents: float | None) -> str | None:
    if settled_price_cents is None:
        return None
    chosen = str(selected_side or "").strip().upper()
    if chosen not in {"YES", "NO"}:
        return None
    if settled_price_cents >= 99.999:
        return chosen
    if settled_price_cents <= 0.001:
        return "NO" if chosen == "YES" else "YES"
    return None


def _infer_horizon_days(row: dict[str, Any]) -> int | None:
    label = str(row.get("day_label") or "").strip().lower()
    if label == "today":
        return 0
    if label == "tomorrow":
        return 1
    date_str = str(row.get("date_str") or "").strip()
    generated_at = str(row.get("generated_at") or "").strip()
    if not date_str or not generated_at:
        return None
    try:
        target_date = datetime.fromisoformat(date_str).date()
    except ValueError:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return None
    try:
        generated_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int((target_date - generated_dt.date()).days)


def _fetch_resolved_predictions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    query = """
        SELECT generated_at, city_key, day_label, date_str, bucket, side, edge, settled_price_cents, model_predictions_json
        FROM scan_predictions
        WHERE resolved_at IS NOT NULL
          AND model_predictions_json IS NOT NULL
          AND model_predictions_json != '{}'
        ORDER BY generated_at ASC, id ASC
    """
    return [dict(row) for row in conn.execute(query).fetchall()]


def _group_metric_rows(rows: list[dict[str, Any]], *, min_samples: int) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, ModelMetricAccumulator]] = {
        "global": defaultdict(ModelMetricAccumulator),
        "city": defaultdict(ModelMetricAccumulator),
        "city_horizon": defaultdict(ModelMetricAccumulator),
        "regime": defaultdict(ModelMetricAccumulator),
        "regime_horizon": defaultdict(ModelMetricAccumulator),
    }
    city_regimes = {key: list(city.regime_tags) for key, city in CITY_BY_KEY.items()}

    for row in rows:
        winning_side = _winning_side(str(row.get("side") or ""), _safe_float(row.get("settled_price_cents")))
        if winning_side is None:
            continue
        edge = _safe_float(row.get("edge")) or 0.0
        city_key = str(row.get("city_key") or "").strip().upper()
        horizon_days = _infer_horizon_days(row)
        try:
            model_predictions = json.loads(str(row.get("model_predictions_json") or "{}"))
        except Exception:
            continue
        if not isinstance(model_predictions, dict):
            continue
        for model_name, raw_prediction in model_predictions.items():
            prediction = _safe_float(raw_prediction)
            if prediction is None:
                continue
            model_side = _prediction_side_for_bucket(prediction, str(row.get("bucket") or ""))
            if model_side is None:
                continue
            correct = model_side == winning_side
            model_key = str(model_name).strip().lower()
            groups["global"][model_key].record(correct=correct, edge=edge)
            if city_key:
                groups["city"][f"{city_key}|{model_key}"].record(correct=correct, edge=edge)
                if horizon_days is not None:
                    groups["city_horizon"][f"{city_key}|{horizon_days}|{model_key}"].record(correct=correct, edge=edge)
                for regime_tag in city_regimes.get(city_key, []):
                    groups["regime"][f"{regime_tag}|{model_key}"].record(correct=correct, edge=edge)
                    if horizon_days is not None:
                        groups["regime_horizon"][f"{regime_tag}|{horizon_days}|{model_key}"].record(correct=correct, edge=edge)

    def finalize(source: dict[str, ModelMetricAccumulator]) -> dict[str, dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        for key, accumulator in source.items():
            metric = accumulator.as_metric()
            if int(metric["sample_count"]) < min_samples:
                continue
            output[key] = metric
        return output

    return {group_name: finalize(group) for group_name, group in groups.items()}


def _metric_to_multiplier(metric: dict[str, Any]) -> float:
    hit_rate = float(metric.get("hit_rate_directional") or 0.5)
    avg_pnl_proxy = float(metric.get("avg_pnl_proxy") or 0.0)
    edge_correct = _safe_float(metric.get("avg_edge_when_correct")) or 0.0
    edge_wrong = _safe_float(metric.get("avg_edge_when_wrong")) or 0.0
    edge_delta = max(-30.0, min(30.0, edge_correct - edge_wrong))
    raw = 1.0 + ((hit_rate - 0.5) * 0.8) + (avg_pnl_proxy * 0.18) + ((edge_delta / 100.0) * 0.08)
    return round(max(0.7, min(1.35, raw)), 4)


def _profile_section_from_group(group: dict[str, dict[str, Any]]) -> dict[str, float]:
    return {
        model_name: _metric_to_multiplier(metric)
        for model_name, metric in sorted(group.items())
    }


def _sorted_metric_table(group: dict[str, dict[str, Any]], key_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group_key, metric in group.items():
        rows.append({key_name: group_key, **metric, "multiplier": _metric_to_multiplier(metric)})
    rows.sort(
        key=lambda item: (
            -int(item["sample_count"]),
            -float(item["hit_rate_directional"]),
            -float(item["avg_pnl_proxy"]),
        )
    )
    return rows


def _build_source_weight_profile(
    metric_groups: dict[str, dict[str, dict[str, Any]]],
    *,
    min_samples: int,
    resolved_predictions: int,
) -> dict[str, Any]:
    global_weights = _profile_section_from_group(metric_groups["global"])
    cities: dict[str, Any] = {}
    for key, metric in metric_groups["city"].items():
        city_key, model_name = key.split("|", 1)
        city_payload = cities.setdefault(city_key, {"model_weight_multiplier": {}, "horizon_days": {}})
        city_payload["model_weight_multiplier"][model_name] = _metric_to_multiplier(metric)
    for key, metric in metric_groups["city_horizon"].items():
        city_key, horizon_days, model_name = key.split("|", 2)
        city_payload = cities.setdefault(city_key, {"model_weight_multiplier": {}, "horizon_days": {}})
        horizon_payload = city_payload["horizon_days"].setdefault(horizon_days, {"model_weight_multiplier": {}})
        horizon_payload["model_weight_multiplier"][model_name] = _metric_to_multiplier(metric)

    regimes: dict[str, Any] = {}
    for key, metric in metric_groups["regime"].items():
        regime_tag, model_name = key.split("|", 1)
        regime_payload = regimes.setdefault(regime_tag, {"model_weight_multiplier": {}, "horizon_days": {}})
        regime_payload["model_weight_multiplier"][model_name] = _metric_to_multiplier(metric)
    for key, metric in metric_groups["regime_horizon"].items():
        regime_tag, horizon_days, model_name = key.split("|", 2)
        regime_payload = regimes.setdefault(regime_tag, {"model_weight_multiplier": {}, "horizon_days": {}})
        horizon_payload = regime_payload["horizon_days"].setdefault(horizon_days, {"model_weight_multiplier": {}})
        horizon_payload["model_weight_multiplier"][model_name] = _metric_to_multiplier(metric)

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "generated_from_resolved_predictions": int(resolved_predictions),
        "min_samples": int(min_samples),
        "global": {"model_weight_multiplier": global_weights},
        "cities": cities,
        "regimes": regimes,
        "metadata": {
            "city_regimes": {key: list(city.regime_tags) for key, city in CITY_BY_KEY.items()},
        },
    }


def generate_source_weight_analysis(
    *,
    db_path: Path,
    min_samples: int,
    top_segments: int,
    output_json: Path | None = None,
    profile_json: Path | None = None,
) -> dict[str, Any]:
    conn = _connect(db_path)
    try:
        rows = _fetch_resolved_predictions(conn)
    finally:
        conn.close()

    metric_groups = _group_metric_rows(rows, min_samples=max(1, int(min_samples)))
    profile = _build_source_weight_profile(
        metric_groups,
        min_samples=max(1, int(min_samples)),
        resolved_predictions=len(rows),
    )
    payload = {
        "db_path": str(db_path),
        "min_samples": max(1, int(min_samples)),
        "top_segments": max(1, int(top_segments)),
        "resolved_predictions": len(rows),
        "metrics": {
            "global": _sorted_metric_table(metric_groups["global"], "model")[:top_segments],
            "city": _sorted_metric_table(metric_groups["city"], "city_model")[:top_segments],
            "city_horizon": _sorted_metric_table(metric_groups["city_horizon"], "city_horizon_model")[:top_segments],
            "regime": _sorted_metric_table(metric_groups["regime"], "regime_model")[:top_segments],
            "regime_horizon": _sorted_metric_table(metric_groups["regime_horizon"], "regime_horizon_model")[:top_segments],
        },
        "source_weight_profile": profile,
    }

    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    if profile_json is not None:
        profile_json.parent.mkdir(parents=True, exist_ok=True)
        profile_json.write_text(json.dumps(profile, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def _print_summary(payload: dict[str, Any]) -> None:
    print(f"resolved_predictions={payload['resolved_predictions']}")
    print("global model multipliers:")
    global_models = payload["source_weight_profile"].get("global", {}).get("model_weight_multiplier", {})
    for model_name, multiplier in sorted(global_models.items(), key=lambda item: (-float(item[1]), item[0])):
        print(f"  {model_name}: {multiplier}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Gera pesos automáticos por source/cidade/regime a partir do histórico resolvido.")
    parser.add_argument("--db-path", default="export/db/weather_bot.db")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--profile-json", default="export/analysis/source_weight_profile.json")
    parser.add_argument("--min-samples", type=int, default=50)
    parser.add_argument("--top-segments", type=int, default=10)
    args = parser.parse_args(argv)

    db_path = _resolve_path(args.db_path)
    output_path = _resolve_path(args.output_json) if args.output_json else None
    profile_path = _resolve_path(args.profile_json) if args.profile_json else None
    payload = generate_source_weight_analysis(
        db_path=db_path,
        min_samples=max(1, int(args.min_samples)),
        top_segments=max(1, int(args.top_segments)),
        output_json=output_path,
        profile_json=profile_path,
    )
    _print_summary(payload)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
