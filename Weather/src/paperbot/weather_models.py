from __future__ import annotations

import json
import importlib
import os
import re
import subprocess
import statistics
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .degendoppler import CITY_CONFIGS, CityConfig


OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ENSEMBLE_BASE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
WEATHER_GOV_BASE_URL = "https://api.weather.gov"
TOMORROW_API_KEY = os.getenv("TOMORROW_API_KEY", "").strip()
WEATHERAPI_KEY = os.getenv("WEATHERAPI_KEY", "").strip()
VISUAL_CROSSING_API_KEY = os.getenv("VISUAL_CROSSING_API_KEY", "").strip()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "").strip()
WEATHER_ENABLE_MOS = os.getenv("WEATHER_ENABLE_MOS", "1").strip().lower() not in {"0", "false", "no"}
WEATHER_ENABLE_HRRR = os.getenv("WEATHER_ENABLE_HRRR", "0").strip().lower() in {"1", "true", "yes"}
WEATHER_CALIBRATION_PATH = os.getenv(
    "WEATHER_CALIBRATION_PATH",
    str(Path(__file__).resolve().parents[2] / "export" / "calibration" / "weather_model_calibration.json"),
)
WEATHER_PROVIDER_CACHE_DIR = Path(
    os.getenv(
        "WEATHER_PROVIDER_CACHE_DIR",
        str(Path(__file__).resolve().parents[2] / "export" / "cache" / "provider_responses"),
    )
)
WEATHER_PROVIDER_CACHE_TTL_SECONDS = int(os.getenv("WEATHER_PROVIDER_CACHE_TTL_SECONDS", "300") or 300)
WEATHER_PROVIDER_CACHE_MAX_STALE_SECONDS = int(
    os.getenv("WEATHER_PROVIDER_CACHE_MAX_STALE_SECONDS", "900") or 900
)
WEATHER_PROVIDER_MAX_WORKERS = int(os.getenv("WEATHER_PROVIDER_MAX_WORKERS", "4") or 4)
WEATHER_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS = int(
    os.getenv("WEATHER_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS", "1800") or 1800
)
WEATHER_PROVIDER_AUTH_COOLDOWN_SECONDS = int(
    os.getenv("WEATHER_PROVIDER_AUTH_COOLDOWN_SECONDS", "21600") or 21600
)

PROVIDER_CACHE_POLICY: dict[str, dict[str, int]] = {
    "open_meteo": {"ttl": 300, "max_stale": 900},
    "open_meteo_ensemble": {"ttl": 300, "max_stale": 900},
    "weather_gov": {"ttl": 900, "max_stale": 1800},
    "tomorrow": {"ttl": 3600, "max_stale": 10800},
    "weatherapi": {"ttl": 1800, "max_stale": 7200},
    "visualcrossing": {"ttl": 1800, "max_stale": 7200},
    "openweather": {"ttl": 1800, "max_stale": 7200},
    "mos": {"ttl": 10800, "max_stale": 21600},
    "hrrr": {"ttl": 3600, "max_stale": 7200},
}

OPEN_METEO_MODELS: dict[str, str | None] = {
    "best_match": None,
    "gfs": "gfs_seamless",
    "ecmwf": "ecmwf_ifs04",
    "icon": "icon_seamless",
    "gem": "gem_seamless",
    "jma": "jma_seamless",
}

MODEL_WEIGHTS: dict[str, float] = {
    "best_match": 1.2,
    "ecmwf": 1.2,
    "nws": 1.1,
    "tomorrow": 1.0,
    "weatherapi": 0.95,
    "visualcrossing": 0.95,
    "openweather": 0.85,
    "mos": 0.95,
    "hrrr": 1.15,
    "gfs": 1.0,
    "icon": 1.0,
    "gem": 0.9,
    "jma": 0.8,
}

ENSEMBLE_MODELS: dict[str, str] = {
    "gfs_ens": "gfs_seamless",
    "ecmwf_ens": "ecmwf_ifs025",
    "icon_ens": "icon_seamless",
}
OPTIONAL_PROVIDER_NAMES = ("tomorrow", "weatherapi", "visualcrossing", "openweather", "hrrr")

MOS_STATION_CODES: dict[str, str] = {
    "NYC": "KLGA",
    "CHI": "KORD",
    "ATL": "KATL",
    "SEA": "KSEA",
    "MIA": "KMIA",
    "DAL": "KDFW",
}
MOS_PRODUCT_SPECS: dict[str, dict[str, str]] = {
    "mav": {
        "provider": "mos",
        "base_dir": "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs_mos/prod",
        "dir_prefix": "gfs_mos",
        "file_pattern": r"mdl_gfsmav\.t(\d{2})z",
    },
    "mex": {
        "provider": "mos",
        "base_dir": "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs_mos/prod",
        "dir_prefix": "gfs_mos",
        "file_pattern": r"mdl_gfsmex\.t(\d{2})z",
    },
    "met": {
        "provider": "mos",
        "base_dir": "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam_mos/prod",
        "dir_prefix": "nam_mos",
        "file_pattern": r"mdl_nammet\.t(\d{2})z",
    },
}
HRRR_BASE_DIR = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"
WEATHER_HRRR_STEP_HOURS = max(1, int(os.getenv("WEATHER_HRRR_STEP_HOURS", "3") or 3))
WEATHER_HRRR_EXTENDED_STEP_HOURS = max(
    WEATHER_HRRR_STEP_HOURS,
    int(os.getenv("WEATHER_HRRR_EXTENDED_STEP_HOURS", "6") or 6),
)
WEATHER_HRRR_TARGET_DAYS = max(1, int(os.getenv("WEATHER_HRRR_TARGET_DAYS", "2") or 2))
WEATHER_HRRR_MAX_FORECAST_HOURS = max(6, int(os.getenv("WEATHER_HRRR_MAX_FORECAST_HOURS", "36") or 36))
WEATHER_HRRR_REQUEST_TIMEOUT_SECONDS = max(20, int(os.getenv("WEATHER_HRRR_REQUEST_TIMEOUT_SECONDS", "90") or 90))
WEATHER_HRRR_PARSE_TIMEOUT_SECONDS = max(20, int(os.getenv("WEATHER_HRRR_PARSE_TIMEOUT_SECONDS", "90") or 90))
WEATHER_HRRR_CONDA_ENV = os.getenv("WEATHER_HRRR_CONDA_ENV", os.getenv("CONDA_DEFAULT_ENV", "base")).strip() or "base"
WEATHER_HRRR_CONDA_BAT = os.getenv("WEATHER_HRRR_CONDA_BAT", "").strip()
WEATHER_HRRR_RUNTIME_CHECK_TTL_SECONDS = max(
    300,
    int(os.getenv("WEATHER_HRRR_RUNTIME_CHECK_TTL_SECONDS", "21600") or 21600),
)
WEATHER_NWS_RETRY_ATTEMPTS = max(1, int(os.getenv("WEATHER_NWS_RETRY_ATTEMPTS", "3") or 3))
WEATHER_NWS_RETRY_BACKOFF_SECONDS = max(0.1, float(os.getenv("WEATHER_NWS_RETRY_BACKOFF_SECONDS", "0.75") or 0.75))
HRRR_RUNTIME_ERROR: str | None = None

PROVIDER_COOLDOWNS: dict[str, float] = {}


@dataclass
class ModelForecast:
    model_name: str
    date: str
    high: float
    low: float | None = None
    source: str = ""


@dataclass
class EnsembleForecast:
    city_key: str
    city_name: str
    date: str
    predictions: dict[str, float]
    blended_high: float
    min_high: float
    max_high: float
    spread: float
    sigma: float
    consensus_score: float
    probabilistic_spread: float | None = None
    probabilistic_member_count: int = 0
    probabilistic_family_highs: dict[str, list[float]] | None = None
    valid_model_count: int = 0
    required_model_count: int = 0
    coverage_ok: bool = False
    coverage_issue_type: str | None = None
    degraded_reason: str | None = None
    provider_failures: list[str] | None = None
    provider_failure_details: dict[str, str] | None = None
    effective_weights: dict[str, float] | None = None
    data_quality_score: float = 0.0
    coverage_score: float = 0.0


@dataclass
class ForecastFetchBundle:
    forecasts_by_model: dict[str, list[ModelForecast]]
    provider_failures: list[str]
    provider_failure_details: dict[str, str] | None = None


def _cache_file_for_url(url: str) -> Path:
    WEATHER_PROVIDER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    digest = sha256(url.encode("utf-8")).hexdigest()
    return WEATHER_PROVIDER_CACHE_DIR / f"{digest}.json"


def _load_cached_response(url: str, *, max_age_seconds: int) -> Any | None:
    cache_file = _cache_file_for_url(url)
    try:
        payload = json.loads(cache_file.read_text(encoding="utf-8"))
    except Exception:
        return None
    fetched_at = float(payload.get("fetched_at", 0.0) or 0.0)
    if fetched_at <= 0:
        return None
    age_seconds = max(0.0, time.time() - fetched_at)
    if age_seconds > max_age_seconds:
        return None
    return payload.get("data")


def _write_cached_response(url: str, data: Any) -> None:
    cache_file = _cache_file_for_url(url)
    payload = {
        "fetched_at": time.time(),
        "data": data,
    }
    try:
        cache_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        return


def _cache_key_for_name(name: str) -> str:
    return f"cache://{name}"


@lru_cache(maxsize=1)
def _load_calibration_config() -> dict[str, Any]:
    path = WEATHER_CALIBRATION_PATH.strip()
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_calibration(city_key: str, horizon_days: int | None) -> tuple[dict[str, float], dict[str, float]]:
    payload = _load_calibration_config()
    global_cfg = payload.get("global") if isinstance(payload, dict) else None
    cities_cfg = payload.get("cities") if isinstance(payload, dict) else None
    city_cfg = cities_cfg.get(city_key) if isinstance(cities_cfg, dict) else None
    horizon_cfg = None
    if isinstance(city_cfg, dict):
        horizon_map = city_cfg.get("horizon_days")
        if isinstance(horizon_map, dict) and horizon_days is not None:
            horizon_cfg = horizon_map.get(str(horizon_days))

    bias_map: dict[str, float] = {}
    weight_map: dict[str, float] = {}
    sources = [global_cfg, city_cfg, horizon_cfg]
    for source in sources:
        if not isinstance(source, dict):
            continue
        raw_bias = source.get("model_bias_f")
        if isinstance(raw_bias, dict):
            for model_name, value in raw_bias.items():
                try:
                    bias_map[str(model_name)] = float(value)
                except (TypeError, ValueError):
                    continue
        raw_weight = source.get("model_weight_multiplier")
        if isinstance(raw_weight, dict):
            for model_name, value in raw_weight.items():
                try:
                    weight_map[str(model_name)] = float(value)
                except (TypeError, ValueError):
                    continue
    return bias_map, weight_map


def _apply_model_calibration(
    city_key: str,
    horizon_days: int | None,
    predictions: dict[str, float],
) -> tuple[dict[str, float], dict[str, float]]:
    bias_map, weight_multiplier_map = _resolve_calibration(city_key, horizon_days)
    adjusted_predictions: dict[str, float] = {}
    effective_weights: dict[str, float] = {}
    for model_name, value in predictions.items():
        adjusted_predictions[model_name] = float(value) + float(bias_map.get(model_name, 0.0))
        effective_weights[model_name] = max(
            0.05,
            float(MODEL_WEIGHTS.get(model_name, 1.0))
            * float(weight_multiplier_map.get(model_name, 1.0))
            * _horizon_weight_multiplier(model_name, horizon_days),
        )
    return adjusted_predictions, effective_weights


def _horizon_weight_multiplier(model_name: str, horizon_days: int | None) -> float:
    if horizon_days is None:
        return 1.0
    name = str(model_name or "").strip().lower()
    if horizon_days <= 0:
        if name in {"nws", "tomorrow", "weatherapi", "openweather", "visualcrossing", "best_match"}:
            return 1.2
        if name == "mos":
            return 1.35
        if name == "hrrr":
            return 1.55
        if name in {"gfs", "icon", "gem", "jma"}:
            return 0.85
        if name == "ecmwf":
            return 0.92
    if horizon_days == 1:
        if name in {"nws", "tomorrow", "weatherapi", "best_match"}:
            return 1.1
        if name == "mos":
            return 1.25
        if name == "hrrr":
            return 1.4
        if name in {"gfs", "icon", "gem", "jma"}:
            return 0.93
    if horizon_days == 2:
        if name == "mos":
            return 1.12
        if name == "hrrr":
            return 1.05
    return 1.0


def _compute_coverage_score(
    *,
    valid_model_count: int,
    min_models: int,
    provider_failures: list[str] | None,
    probabilistic_member_count: int,
    predictions: dict[str, float],
    horizon_days: int | None,
) -> float:
    min_models = max(1, int(min_models))
    failures = list(provider_failures or [])
    model_coverage = min(1.0, valid_model_count / float(min_models))
    probabilistic_support = min(1.0, probabilistic_member_count / 30.0) if probabilistic_member_count > 0 else 0.0
    core_names = {"best_match", "ecmwf", "gfs", "icon", "nws", "mos"}
    if horizon_days is not None and horizon_days <= 1:
        core_names.add("hrrr")
    core_hits = sum(1 for name in core_names if name in predictions)
    core_target = 5 if horizon_days is None or horizon_days > 1 else 6
    core_support = min(1.0, core_hits / float(core_target))
    failure_penalty = min(0.45, sum(_provider_failure_penalty(name, horizon_days) for name in failures))
    score = (model_coverage * 0.5) + (core_support * 0.35) + (probabilistic_support * 0.15) - failure_penalty
    return round(max(0.0, min(1.0, score)), 4)


def _compute_data_quality_score(
    *,
    valid_model_count: int,
    min_models: int,
    provider_failures: list[str] | None,
    probabilistic_member_count: int,
    predictions: dict[str, float],
    horizon_days: int | None,
    coverage_ok: bool,
) -> float:
    model_coverage_score = min(1.0, valid_model_count / max(float(min_models), 1.0))
    failure_penalty = min(
        0.45,
        sum(_provider_failure_penalty(name, horizon_days, for_data_quality=True) for name in (provider_failures or [])),
    )
    probabilistic_score = min(1.0, probabilistic_member_count / 40.0) if probabilistic_member_count > 0 else 0.0
    score = (model_coverage_score * 0.6) + (probabilistic_score * 0.2) + 0.2
    if horizon_days is not None and horizon_days <= 1:
        near_term_support = sum(
            1
            for model_name in ("nws", "tomorrow", "weatherapi", "openweather", "visualcrossing", "best_match", "mos", "hrrr")
            if model_name in predictions
        )
        score += min(0.12, near_term_support * 0.02)
    if not coverage_ok:
        score -= 0.2
    score -= failure_penalty
    return round(max(0.0, min(1.0, score)), 4)


def _weighted_median(values: list[float], weights: list[float]) -> float:
    ordered = sorted(zip(values, weights), key=lambda item: item[0])
    total_weight = sum(weight for _, weight in ordered)
    if total_weight <= 0:
        return statistics.median(values)
    cumulative = 0.0
    midpoint = total_weight / 2.0
    for value, weight in ordered:
        cumulative += max(weight, 0.0)
        if cumulative >= midpoint:
            return value
    return ordered[-1][0]


def _median_absolute_deviation(values: list[float], center: float | None = None) -> float:
    if not values:
        return 0.0
    pivot = statistics.median(values) if center is None else center
    deviations = [abs(value - pivot) for value in values]
    return statistics.median(deviations)


def _robust_weighted_blend(
    predictions: dict[str, float],
    *,
    weights_by_model: dict[str, float] | None = None,
) -> tuple[float, float, float]:
    values = [float(value) for value in predictions.values()]
    weights = [
        float((weights_by_model or {}).get(model_name, MODEL_WEIGHTS.get(model_name, 1.0)))
        for model_name in predictions
    ]
    if not values:
        raise ValueError("predictions cannot be empty")
    if len(values) == 1:
        only_value = values[0]
        return only_value, only_value, 0.0

    anchor = _weighted_median(values, weights)
    mad = _median_absolute_deviation(values, center=anchor)
    robust_scale = max(mad * 1.4826, 0.75)

    adjusted_weights: list[float] = []
    weighted_sum = 0.0
    total_weight = 0.0
    for (model_name, value), base_weight in zip(predictions.items(), weights):
        deviation_units = abs(float(value) - anchor) / robust_scale
        # Smoothly downweight outliers instead of hard-clipping them.
        outlier_penalty = 1.0 / (1.0 + deviation_units * deviation_units)
        adjusted_weight = base_weight * outlier_penalty
        adjusted_weights.append(adjusted_weight)
        weighted_sum += float(value) * adjusted_weight
        total_weight += adjusted_weight

    if total_weight <= 0:
        robust_mean = anchor
    else:
        robust_mean = weighted_sum / total_weight

    return robust_mean, anchor, robust_scale


def _provider_cache_policy(provider_name: str | None) -> tuple[int, int]:
    policy = PROVIDER_CACHE_POLICY.get(str(provider_name or "").strip().lower(), {})
    ttl = int(policy.get("ttl", WEATHER_PROVIDER_CACHE_TTL_SECONDS))
    max_stale = int(policy.get("max_stale", WEATHER_PROVIDER_CACHE_MAX_STALE_SECONDS))
    return ttl, max_stale


def _provider_cooldown_until(provider_name: str | None) -> float:
    if not provider_name:
        return 0.0
    return float(PROVIDER_COOLDOWNS.get(str(provider_name).strip().lower(), 0.0) or 0.0)


def _provider_in_cooldown(provider_name: str | None) -> bool:
    until = _provider_cooldown_until(provider_name)
    return until > time.time()


def _set_provider_cooldown(provider_name: str | None, seconds: int) -> None:
    if not provider_name or seconds <= 0:
        return
    PROVIDER_COOLDOWNS[str(provider_name).strip().lower()] = time.time() + float(seconds)


def _clear_provider_cooldown(provider_name: str | None) -> None:
    if not provider_name:
        return
    PROVIDER_COOLDOWNS.pop(str(provider_name).strip().lower(), None)


def _provider_failure_penalty(
    provider_name: str,
    horizon_days: int | None,
    *,
    for_data_quality: bool = False,
) -> float:
    name = str(provider_name or "").strip().lower()
    if not name:
        return 0.0
    base_penalty = 0.08 if for_data_quality else 0.06
    if name in {"tomorrow", "weatherapi", "visualcrossing", "openweather"}:
        return base_penalty * 0.75
    if name == "nws":
        if horizon_days is not None and horizon_days <= 1:
            return base_penalty * 0.7
        return base_penalty * 0.85
    if name == "hrrr":
        return base_penalty * 1.05 if horizon_days is not None and horizon_days <= 1 else base_penalty * 0.9
    if name == "mos":
        return base_penalty * 1.0
    return base_penalty


def _request_json(
    url: str,
    *,
    timeout: float = 20.0,
    headers: dict[str, str] | None = None,
    provider_name: str | None = None,
) -> Any:
    ttl_seconds, max_stale_seconds = _provider_cache_policy(provider_name)
    if _provider_in_cooldown(provider_name):
        stale = _load_cached_response(url, max_age_seconds=max_stale_seconds)
        if stale is not None:
            return stale
        remaining = max(1, int(_provider_cooldown_until(provider_name) - time.time()))
        raise RuntimeError(f"provider {provider_name} cooling down for {remaining}s")
    cached = _load_cached_response(url, max_age_seconds=ttl_seconds)
    if cached is not None:
        return cached
    request = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "weather-bot/1.0 (https://api.weather.gov; contact=local-script)",
            "Accept": "application/geo+json, application/json",
            **(headers or {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
            data = json.loads(payload)
            _write_cached_response(url, data)
            _clear_provider_cooldown(provider_name)
            return data
    except urllib.error.HTTPError as err:
        if err.code == 401:
            _set_provider_cooldown(provider_name, WEATHER_PROVIDER_AUTH_COOLDOWN_SECONDS)
        if err.code == 429:
            _set_provider_cooldown(provider_name, WEATHER_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS)
        if err.code == 429:
            stale = _load_cached_response(url, max_age_seconds=max_stale_seconds)
            if stale is not None:
                return stale
        raise RuntimeError(f"HTTP {err.code} fetching {url}") from err
    except urllib.error.URLError as err:
        stale = _load_cached_response(url, max_age_seconds=max_stale_seconds)
        if stale is not None:
            return stale
        raise RuntimeError(f"Network error fetching {url}: {err}") from err
    except json.JSONDecodeError as err:
        raise RuntimeError(f"Invalid JSON returned by {url}") from err


def _request_text(
    url: str,
    *,
    timeout: float = 30.0,
    headers: dict[str, str] | None = None,
    provider_name: str | None = None,
) -> str:
    ttl_seconds, max_stale_seconds = _provider_cache_policy(provider_name)
    if _provider_in_cooldown(provider_name):
        stale = _load_cached_response(url, max_age_seconds=max_stale_seconds)
        if isinstance(stale, str):
            return stale
        remaining = max(1, int(_provider_cooldown_until(provider_name) - time.time()))
        raise RuntimeError(f"provider {provider_name} cooling down for {remaining}s")
    cached = _load_cached_response(url, max_age_seconds=ttl_seconds)
    if isinstance(cached, str):
        return cached
    request = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "weather-bot/1.0 (https://api.weather.gov; contact=local-script)",
            "Accept": "text/plain, text/html;q=0.9, */*;q=0.8",
            **(headers or {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8", "ignore")
            _write_cached_response(url, payload)
            _clear_provider_cooldown(provider_name)
            return payload
    except urllib.error.HTTPError as err:
        if err.code == 401:
            _set_provider_cooldown(provider_name, WEATHER_PROVIDER_AUTH_COOLDOWN_SECONDS)
        if err.code == 429:
            _set_provider_cooldown(provider_name, WEATHER_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS)
        stale = _load_cached_response(url, max_age_seconds=max_stale_seconds)
        if isinstance(stale, str):
            return stale
        raise RuntimeError(f"HTTP {err.code} fetching {url}") from err
    except urllib.error.URLError as err:
        stale = _load_cached_response(url, max_age_seconds=max_stale_seconds)
        if isinstance(stale, str):
            return stale
        raise RuntimeError(f"Network error fetching {url}: {err}") from err


def _request_json_with_retry(
    url: str,
    *,
    provider_name: str,
    attempts: int,
    timeout: float = 20.0,
    headers: dict[str, str] | None = None,
) -> Any:
    last_error: Exception | None = None
    max_attempts = max(1, int(attempts))
    for attempt_idx in range(max_attempts):
        try:
            return _request_json(url, timeout=timeout, headers=headers, provider_name=provider_name)
        except Exception as exc:
            last_error = exc
            if attempt_idx >= max_attempts - 1:
                break
            time.sleep(WEATHER_NWS_RETRY_BACKOFF_SECONDS * (attempt_idx + 1))
    assert last_error is not None
    raise last_error


def _summarize_provider_error(exc: Exception) -> str:
    text = str(exc or "").strip() or exc.__class__.__name__
    text = " ".join(text.split())
    if len(text) > 160:
        text = text[:157] + "..."
    return text


def _serialize_model_forecasts(rows: list[ModelForecast]) -> list[dict[str, Any]]:
    return [
        {
            "model_name": row.model_name,
            "date": row.date,
            "high": row.high,
            "low": row.low,
            "source": row.source,
        }
        for row in rows
    ]


def _restore_model_forecasts(payload: Any) -> list[ModelForecast]:
    output: list[ModelForecast] = []
    if not isinstance(payload, list):
        return output
    for item in payload:
        if not isinstance(item, dict):
            continue
        try:
            output.append(
                ModelForecast(
                    model_name=str(item.get("model_name") or ""),
                    date=str(item["date"]),
                    high=float(item["high"]),
                    low=(float(item["low"]) if item.get("low") is not None else None),
                    source=str(item.get("source") or ""),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return output


def _coverage_issue_type(
    *,
    valid_model_count: int,
    min_models: int,
    provider_failures: list[str] | None,
    provider_failure_details: dict[str, str] | None,
) -> str | None:
    failures = list(provider_failures or [])
    details = dict(provider_failure_details or {})
    all_rate_limited = bool(failures) and all("HTTP 429" in str(details.get(name, "")) for name in failures)
    if valid_model_count < min_models and failures:
        return "mixed_rate_limited" if all_rate_limited else "mixed"
    if failures:
        return "rate_limited" if all_rate_limited else "provider_failure"
    if valid_model_count < min_models:
        return "insufficient_data"
    return None


def _list_directory_links(url: str, *, provider_name: str) -> list[str]:
    text = _request_text(url, provider_name=provider_name)
    return re.findall(r'href="([^"]+)"', text, flags=re.IGNORECASE)


def _find_latest_nomads_file(
    *,
    base_dir_url: str,
    dir_prefix: str,
    file_pattern: str,
    provider_name: str,
    max_dirs: int = 3,
) -> str | None:
    directory_links = _list_directory_links(base_dir_url.rstrip("/") + "/", provider_name=provider_name)
    dir_regex = re.compile(rf"^{re.escape(dir_prefix)}\.(\d{{8}})/$")
    file_regex = re.compile(file_pattern)
    candidate_dirs = sorted(
        (match.group(0) for link in directory_links if (match := dir_regex.match(link))),
        reverse=True,
    )
    for directory in candidate_dirs[:max_dirs]:
        directory_url = f"{base_dir_url.rstrip('/')}/{directory}"
        try:
            file_links = _list_directory_links(directory_url, provider_name=provider_name)
        except Exception:
            continue
        matches: list[tuple[int, str]] = []
        for link in file_links:
            match = file_regex.match(link)
            if not match:
                continue
            matches.append((int(match.group(1)), link))
        if matches:
            matches.sort(reverse=True)
            return f"{directory_url.rstrip('/')}/{matches[0][1]}"
    return None


def _extract_station_block(text: str, station_code: str) -> str | None:
    pattern = re.compile(
        rf"(?ms)^\s*{re.escape(station_code)}\b.*?(?=^\s*[A-Z0-9]{{4,5}}\s{{2,}}.*GUIDANCE|\Z)"
    )
    match = pattern.search(text)
    if not match:
        return None
    return match.group(0).strip()


def _parse_issue_date_from_block(block: str) -> date | None:
    header = block.splitlines()[0] if block else ""
    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", header)
    if not match:
        return None
    month, day_value, year = (int(part) for part in match.groups())
    return date(year, month, day_value)


def _resolve_forecast_dates(issue_date: date | None, day_numbers: list[int]) -> list[str]:
    if issue_date is None:
        return []
    cursor = issue_date
    output: list[str] = []
    for day_number in day_numbers:
        for _ in range(40):
            if cursor.day == day_number:
                output.append(cursor.isoformat())
                cursor += timedelta(days=1)
                break
            cursor += timedelta(days=1)
    return output


def _parse_mos_nx_line(block: str, date_count: int) -> tuple[list[float | None], list[float | None]]:
    nx_line = next((line for line in block.splitlines() if line.startswith(" N/X") or line.startswith("N/X")), "")
    values = [float(item) for item in re.findall(r"-?\d+", nx_line)]
    if not values:
        return [], []
    highs = values[1::2]
    if len(highs) < date_count and values:
        highs.append(values[-1])
    lows = values[0::2]
    return lows[:date_count], highs[:date_count]


def _parse_mav_or_met_block(block: str) -> dict[str, tuple[float | None, float]]:
    issue_date = _parse_issue_date_from_block(block)
    dt_line = next((line for line in block.splitlines() if line.startswith(" DT") or line.startswith("DT ")), "")
    day_numbers = [int(item) for item in re.findall(r"/[A-Z]{3}\s+(\d{1,2})", dt_line)]
    forecast_dates = _resolve_forecast_dates(issue_date, day_numbers)
    lows, highs = _parse_mos_nx_line(block, len(forecast_dates))
    output: dict[str, tuple[float | None, float]] = {}
    for idx, date_str in enumerate(forecast_dates):
        high = highs[idx] if idx < len(highs) else None
        if high is None:
            continue
        low = lows[idx] if idx < len(lows) else None
        output[date_str] = (low, high)
    return output


def _parse_mex_block(block: str) -> dict[str, tuple[float | None, float]]:
    issue_date = _parse_issue_date_from_block(block)
    lines = block.splitlines()
    day_line = next((line for line in lines if re.match(r"\s+[A-Z]{3}\s+\d{1,2}\|", line)), "")
    day_numbers = [int(item) for item in re.findall(r"[A-Z]{3}\s+(\d{1,2})", day_line)]
    forecast_dates = _resolve_forecast_dates(issue_date, day_numbers)
    nx_line = next((line for line in lines if line.startswith(" N/X") or line.startswith("N/X")), "")
    segments = [segment.strip() for segment in nx_line.split("|")]
    output: dict[str, tuple[float | None, float]] = {}
    for date_str, segment in zip(forecast_dates, segments):
        values = [float(item) for item in re.findall(r"-?\d+", segment)]
        if not values:
            continue
        low = values[0] if len(values) >= 2 else None
        high = values[1] if len(values) >= 2 else values[0]
        output[date_str] = (low, high)
    return output


def _combine_guidance_rows(guidance_rows: list[dict[str, tuple[float | None, float]]]) -> list[ModelForecast]:
    by_date: dict[str, dict[str, list[float]]] = {}
    for rows in guidance_rows:
        for date_str, (low, high) in rows.items():
            bucket = by_date.setdefault(date_str, {"highs": [], "lows": []})
            bucket["highs"].append(float(high))
            if low is not None:
                bucket["lows"].append(float(low))
    output: list[ModelForecast] = []
    for date_str in sorted(by_date.keys()):
        highs = by_date[date_str]["highs"]
        lows = by_date[date_str]["lows"]
        if not highs:
            continue
        output.append(
            ModelForecast(
                model_name="mos",
                date=date_str,
                high=float(round(statistics.median(highs), 2)),
                low=(float(round(statistics.median(lows), 2)) if lows else None),
                source="noaa-mos",
            )
        )
    return output


def _mos_station_code(city: CityConfig) -> str | None:
    return MOS_STATION_CODES.get(city.key)


def _fetch_mos_daily(city: CityConfig) -> list[ModelForecast]:
    if not WEATHER_ENABLE_MOS:
        return []
    station_code = _mos_station_code(city)
    if not station_code:
        return []
    guidance_rows: list[dict[str, tuple[float | None, float]]] = []
    for product_key, spec in MOS_PRODUCT_SPECS.items():
        url = _find_latest_nomads_file(
            base_dir_url=spec["base_dir"],
            dir_prefix=spec["dir_prefix"],
            file_pattern=spec["file_pattern"],
            provider_name=spec["provider"],
        )
        if not url:
            continue
        text = _request_text(url, provider_name=spec["provider"])
        block = _extract_station_block(text, station_code)
        if not block:
            continue
        rows = _parse_mex_block(block) if product_key == "mex" else _parse_mav_or_met_block(block)
        if rows:
            guidance_rows.append(rows)
    return _combine_guidance_rows(guidance_rows)


@lru_cache(maxsize=1)
def _hrrr_runtime_available() -> bool:
    global HRRR_RUNTIME_ERROR
    try:
        if _hrrr_inprocess_runtime_available():
            HRRR_RUNTIME_ERROR = None
            return True
        conda_bat = _hrrr_conda_bat()
        if conda_bat is None or not conda_bat.exists():
            HRRR_RUNTIME_ERROR = "conda runtime not found"
            return False
        cache_key = f"hrrr_runtime://{conda_bat}|{WEATHER_HRRR_CONDA_ENV}"
        cached = _load_cached_response(cache_key, max_age_seconds=WEATHER_HRRR_RUNTIME_CHECK_TTL_SECONDS)
        if isinstance(cached, dict):
            cached_ok = bool(cached.get("ok"))
            cached_error = str(cached.get("error") or "").strip() or None
            HRRR_RUNTIME_ERROR = None if cached_ok else cached_error
            return cached_ok
        result = subprocess.run(
            [
                str(conda_bat),
                "run",
                "-n",
                WEATHER_HRRR_CONDA_ENV,
                "python",
                "-c",
                "import eccodes, cfgrib, xarray; print('ok')",
            ],
            capture_output=True,
            text=True,
            timeout=WEATHER_HRRR_PARSE_TIMEOUT_SECONDS,
            check=False,
        )
        if result.returncode == 0 and "ok" in (result.stdout or ""):
            HRRR_RUNTIME_ERROR = None
            _write_cached_response(cache_key, {"ok": True, "error": None})
            return True
        HRRR_RUNTIME_ERROR = _summarize_provider_error(
            RuntimeError((result.stderr or result.stdout or "conda runtime failed").strip())
        )
        _write_cached_response(cache_key, {"ok": False, "error": HRRR_RUNTIME_ERROR})
        return False
    except Exception as exc:
        HRRR_RUNTIME_ERROR = _summarize_provider_error(exc)
        return False


@lru_cache(maxsize=1)
def _hrrr_inprocess_runtime_available() -> bool:
    global HRRR_RUNTIME_ERROR
    cache_key = f"hrrr_runtime_inprocess://{os.getenv('CONDA_DEFAULT_ENV', '') or 'python'}"
    cached = _load_cached_response(cache_key, max_age_seconds=WEATHER_HRRR_RUNTIME_CHECK_TTL_SECONDS)
    if isinstance(cached, dict):
        cached_ok = bool(cached.get("ok"))
        cached_error = str(cached.get("error") or "").strip() or None
        HRRR_RUNTIME_ERROR = None if cached_ok else cached_error
        return cached_ok
    try:
        importlib.import_module("eccodes")
        importlib.import_module("cfgrib")
        importlib.import_module("xarray")
        HRRR_RUNTIME_ERROR = None
        _write_cached_response(cache_key, {"ok": True, "error": None})
        return True
    except Exception as exc:
        HRRR_RUNTIME_ERROR = _summarize_provider_error(exc)
        _write_cached_response(cache_key, {"ok": False, "error": HRRR_RUNTIME_ERROR})
        return False


@lru_cache(maxsize=1)
def _hrrr_parser_script_path() -> Path:
    handle, raw_path = tempfile.mkstemp(prefix=f"weather_hrrr_parser_{os.getpid()}_", suffix=".py")
    os.close(handle)
    parser_path = Path(raw_path)
    parser_path.write_text(
        "\n".join(
            [
                "import json",
                "import sys",
                "import xarray as xr",
                "path = sys.argv[1]",
                "target_lat = float(sys.argv[2])",
                "target_lon = float(sys.argv[3])",
                "ds = xr.open_dataset(path, engine='cfgrib')",
                "var_name = list(ds.data_vars)[0]",
                "arr = ds[var_name]",
                "lat = ds['latitude'].values",
                "lon = ds['longitude'].values",
                "dist = (lat - target_lat) ** 2 + (lon - target_lon) ** 2",
                "flat_idx = int(dist.reshape(-1).argmin())",
                "value = float(arr.values.reshape(-1)[flat_idx])",
                "valid = str(ds['valid_time'].values.reshape(-1)[0]) if 'valid_time' in ds.coords else str(ds.coords['time'].values)",
                "units = arr.attrs.get('units') or arr.attrs.get('GRIB_units')",
                "print(json.dumps({'valid': valid, 'value': value, 'units': units}))",
            ]
        ),
        encoding="utf-8",
    )
    return parser_path


def _hrrr_conda_bat() -> Path | None:
    if WEATHER_HRRR_CONDA_BAT:
        candidate = Path(WEATHER_HRRR_CONDA_BAT)
        if candidate.exists():
            return candidate
    conda_exe = os.getenv("CONDA_EXE", "").strip()
    if conda_exe:
        conda_path = Path(conda_exe)
        if conda_path.exists():
            if conda_path.name.lower() == "conda.bat":
                return conda_path
            sibling = conda_path.parent / "conda.bat"
            if sibling.exists():
                return sibling
            condabin = conda_path.parent.parent / "condabin" / "conda.bat"
            if condabin.exists():
                return condabin
    for candidate in (
        Path.home() / "anaconda3" / "condabin" / "conda.bat",
        Path.home() / "miniconda3" / "condabin" / "conda.bat",
        Path("C:/Users/pe_hn/anaconda3/condabin/conda.bat"),
    ):
        if candidate.exists():
            return candidate
    return None


def _find_latest_hrrr_cycle(*, max_dirs: int = 2) -> tuple[str, int] | None:
    directory_links = _list_directory_links(HRRR_BASE_DIR.rstrip("/") + "/", provider_name="hrrr")
    dir_regex = re.compile(r"^hrrr\.(\d{8})/$")
    candidate_dirs = sorted(
        (match.group(0) for link in directory_links if (match := dir_regex.match(link))),
        reverse=True,
    )
    file_regex = re.compile(r"^hrrr\.t(\d{2})z\.wrfsfcf(\d{2})\.grib2$")
    fallback: tuple[str, int] | None = None
    for directory in candidate_dirs[:max_dirs]:
        directory_url = f"{HRRR_BASE_DIR.rstrip('/')}/{directory}conus/"
        try:
            file_links = _list_directory_links(directory_url, provider_name="hrrr")
        except Exception:
            continue
        by_cycle: dict[int, list[int]] = {}
        for link in file_links:
            match = file_regex.match(link)
            if not match:
                continue
            by_cycle.setdefault(int(match.group(1)), []).append(int(match.group(2)))
        for cycle_hour in sorted(by_cycle.keys(), reverse=True):
            max_step = max(by_cycle[cycle_hour]) if by_cycle[cycle_hour] else -1
            if max_step >= WEATHER_HRRR_MAX_FORECAST_HOURS:
                return directory.rstrip("/").split(".")[-1], cycle_hour
        if by_cycle and fallback is None:
            fallback_hour = max(by_cycle.keys())
            fallback = (directory.rstrip("/").split(".")[-1], fallback_hour)
    return fallback


def _hrrr_filter_url(city: CityConfig, date_token: str, cycle_hour: int, step_hour: int) -> str:
    params = {
        "dir": f"/hrrr.{date_token}/conus",
        "file": f"hrrr.t{cycle_hour:02d}z.wrfsfcf{step_hour:02d}.grib2",
        "var_TMP": "on",
        "lev_2_m_above_ground": "on",
        "subregion": "",
        "leftlon": str(round(city.lon - 0.2, 3)),
        "rightlon": str(round(city.lon + 0.2, 3)),
        "toplat": str(round(city.lat + 0.2, 3)),
        "bottomlat": str(round(city.lat - 0.2, 3)),
    }
    return f"https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl?{urllib.parse.urlencode(params)}"


def _hrrr_daily_cache_key(
    city: CityConfig,
    date_token: str,
    cycle_hour: int,
    target_dates: list[str] | tuple[str, ...] | set[str],
) -> str:
    ordered_target_dates = ",".join(sorted(str(item) for item in target_dates))
    return (
        "hrrr_daily://"
        f"{city.key}/{date_token}/t{cycle_hour:02d}/dates={ordered_target_dates}/"
        f"step={WEATHER_HRRR_STEP_HOURS}/extended={WEATHER_HRRR_EXTENDED_STEP_HOURS}"
    )


def _parse_hrrr_valid_datetime(valid_text: str) -> datetime:
    parsed = datetime.fromisoformat(valid_text.replace("Z", "+00:00").replace(".000000000", ""))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hrrr_target_local_dates(
    city: CityConfig,
    *,
    reference: datetime | None = None,
    target_days: int = WEATHER_HRRR_TARGET_DAYS,
) -> list[str]:
    local_tz = ZoneInfo(city.timezone_name)
    now = (reference or datetime.now(timezone.utc)).astimezone(local_tz)
    return [(now + timedelta(days=offset)).date().isoformat() for offset in range(max(1, target_days))]


def _hrrr_step_schedule(max_forecast_hours: int) -> list[int]:
    near_term_cutoff = min(24, max_forecast_hours)
    steps: list[int] = []
    for hour in range(0, near_term_cutoff + 1, WEATHER_HRRR_STEP_HOURS):
        steps.append(hour)
    extended_start = near_term_cutoff + WEATHER_HRRR_EXTENDED_STEP_HOURS
    for hour in range(extended_start, max_forecast_hours + 1, WEATHER_HRRR_EXTENDED_STEP_HOURS):
        if hour not in steps:
            steps.append(hour)
    if max_forecast_hours not in steps:
        steps.append(max_forecast_hours)
    return sorted(set(steps))


def _run_hrrr_subset_parser(grib_path: Path, city: CityConfig) -> tuple[str, float]:
    if _hrrr_inprocess_runtime_available():
        return _run_hrrr_subset_parser_inprocess(grib_path, city)
    conda_bat = _hrrr_conda_bat()
    if conda_bat is None:
        raise RuntimeError("conda runtime not found")
    parser_script = _hrrr_parser_script_path()
    result = subprocess.run(
        [
            str(conda_bat),
            "run",
            "-n",
            WEATHER_HRRR_CONDA_ENV,
            "python",
            str(parser_script),
            str(grib_path),
            str(city.lat),
            str(city.lon),
        ],
        capture_output=True,
        text=True,
        timeout=WEATHER_HRRR_PARSE_TIMEOUT_SECONDS,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "hrrr parser failed").strip())
    try:
        payload = json.loads((result.stdout or "").strip().splitlines()[-1])
        valid = str(payload["valid"])
        value = float(payload["value"])
        units = str(payload.get("units") or "")
    except Exception as exc:
        raise RuntimeError(f"invalid HRRR parser output: {result.stdout!r}") from exc
    if units.upper() == "K":
        value = ((value - 273.15) * 9.0 / 5.0) + 32.0
    return valid, value


def _run_hrrr_subset_parser_inprocess(grib_path: Path, city: CityConfig) -> tuple[str, float]:
    xr = importlib.import_module("xarray")
    ds = xr.open_dataset(str(grib_path), engine="cfgrib")
    try:
        var_name = next(iter(ds.data_vars))
        arr = ds[var_name]
        lat = ds["latitude"].values
        lon = ds["longitude"].values
        dist = (lat - float(city.lat)) ** 2 + (lon - float(city.lon)) ** 2
        flat_idx = int(dist.reshape(-1).argmin())
        value = float(arr.values.reshape(-1)[flat_idx])
        if "valid_time" in ds.coords:
            valid = str(ds["valid_time"].values.reshape(-1)[0])
        elif "time" in ds.coords:
            valid = str(ds.coords["time"].values)
        else:
            raise RuntimeError("HRRR dataset missing valid time coordinate")
        units = str(arr.attrs.get("units") or arr.attrs.get("GRIB_units") or "")
    finally:
        try:
            ds.close()
        except Exception:
            pass
    if units.upper() == "K":
        value = ((value - 273.15) * 9.0 / 5.0) + 32.0
    return valid, value


def _fetch_hrrr_daily(city: CityConfig) -> list[ModelForecast]:
    if not WEATHER_ENABLE_HRRR:
        return []
    if not _hrrr_runtime_available():
        raise RuntimeError(f"HRRR runtime unavailable: {HRRR_RUNTIME_ERROR or 'missing dependency'}")
    cycle = _find_latest_hrrr_cycle()
    if cycle is None:
        raise RuntimeError("HRRR cycle not found")
    date_token, cycle_hour = cycle
    target_dates = set(_hrrr_target_local_dates(city))
    aggregate_cache_key = _hrrr_daily_cache_key(city, date_token, cycle_hour, target_dates)
    hrrr_ttl, _ = _provider_cache_policy("hrrr")
    cached_daily = _load_cached_response(aggregate_cache_key, max_age_seconds=hrrr_ttl)
    if isinstance(cached_daily, list):
        restored: list[ModelForecast] = []
        for item in cached_daily:
            if not isinstance(item, dict):
                continue
            try:
                restored.append(
                    ModelForecast(
                        model_name="hrrr",
                        date=str(item["date"]),
                        high=float(item["high"]),
                        low=None,
                        source="noaa-hrrr",
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        if restored:
            return restored
    by_date: dict[str, float] = {}
    local_tz = ZoneInfo(city.timezone_name)
    missing_steps = 0
    for step_hour in _hrrr_step_schedule(WEATHER_HRRR_MAX_FORECAST_HOURS):
        url = _hrrr_filter_url(city, date_token, cycle_hour, step_hour)
        cached = _load_cached_response(url, max_age_seconds=hrrr_ttl)
        if isinstance(cached, dict) and "valid" in cached and "temp_f" in cached:
            valid_text = str(cached["valid"])
            temp_f = float(cached["temp_f"])
        else:
            grib_path = Path(tempfile.gettempdir()) / f"hrrr_{city.key}_{date_token}_{cycle_hour:02d}_{step_hour:02d}.grib2"
            request = urllib.request.Request(url=url, headers={"User-Agent": "weather-bot/1.0", "Accept": "*/*"})
            try:
                with urllib.request.urlopen(request, timeout=WEATHER_HRRR_REQUEST_TIMEOUT_SECONDS) as response:
                    grib_path.write_bytes(response.read())
            except urllib.error.HTTPError as err:
                if err.code == 429:
                    _set_provider_cooldown("hrrr", WEATHER_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS)
                    raise RuntimeError(f"HTTP {err.code} fetching {url}") from err
                if err.code == 404:
                    missing_steps += 1
                    continue
                raise RuntimeError(f"HTTP {err.code} fetching {url}") from err
            except urllib.error.URLError as err:
                raise RuntimeError(f"Network error fetching {url}: {err}") from err
            try:
                valid_text, temp_f = _run_hrrr_subset_parser(grib_path, city)
            finally:
                try:
                    grib_path.unlink(missing_ok=True)
                except Exception:
                    pass
            _write_cached_response(url, {"valid": valid_text, "temp_f": temp_f})
        valid_dt = _parse_hrrr_valid_datetime(valid_text)
        local_date = valid_dt.astimezone(local_tz).date().isoformat()
        if local_date not in target_dates:
            if by_date and local_date > max(target_dates):
                break
            continue
        current = by_date.get(local_date)
        if current is None or temp_f > current:
            by_date[local_date] = temp_f
    if not by_date:
        if missing_steps > 0:
            raise RuntimeError(f"HRRR steps unavailable for cycle {date_token} t{cycle_hour:02d}z")
        raise RuntimeError("HRRR returned no usable daily maxima")
    output = [
        ModelForecast(model_name="hrrr", date=date_str, high=round(high, 2), low=None, source="noaa-hrrr")
        for date_str, high in sorted(by_date.items())
    ]
    _write_cached_response(
        aggregate_cache_key,
        [{"date": item.date, "high": item.high} for item in output],
    )
    return output


def _effective_min_models(base_min_models: int) -> int:
    disabled_optional = sum(
        1
        for provider_name in OPTIONAL_PROVIDER_NAMES
        if _provider_in_cooldown(provider_name) and _provider_cooldown_until(provider_name) - time.time() > 300
    )
    adjusted = max(3, int(base_min_models) - disabled_optional)
    return adjusted


def _fetch_open_meteo_model(city: CityConfig, model_key: str) -> list[ModelForecast]:
    params = {
        "latitude": str(city.lat),
        "longitude": str(city.lon),
        "daily": "temperature_2m_max,temperature_2m_min",
        "temperature_unit": "fahrenheit",
        "timezone": "auto",
        "forecast_days": "7",
    }
    model_name = OPEN_METEO_MODELS[model_key]
    if model_name:
        params["models"] = model_name
    url = f"{OPEN_METEO_BASE_URL}?{urllib.parse.urlencode(params)}"
    data = _request_json(url, provider_name="open_meteo")
    daily = data.get("daily") if isinstance(data, dict) else None
    if not isinstance(daily, dict):
        return []

    output: list[ModelForecast] = []
    times = daily.get("time") or []
    highs = daily.get("temperature_2m_max") or []
    lows = daily.get("temperature_2m_min") or []
    for idx, date in enumerate(times):
        try:
            output.append(
                ModelForecast(
                    model_name=model_key,
                    date=str(date),
                    high=float(highs[idx]),
                    low=float(lows[idx]),
                    source="open-meteo",
                )
            )
        except (IndexError, TypeError, ValueError):
            continue
    return output


def _fetch_open_meteo_ensemble_daily_highs(city: CityConfig, model_name: str, open_meteo_model: str) -> dict[str, list[float]]:
    params = {
        "latitude": str(city.lat),
        "longitude": str(city.lon),
        "hourly": "temperature_2m",
        "temperature_unit": "fahrenheit",
        "timezone": "auto",
        "forecast_days": "4",
        "models": open_meteo_model,
    }
    url = f"{OPEN_METEO_ENSEMBLE_BASE_URL}?{urllib.parse.urlencode(params)}"
    data = _request_json(url, provider_name="open_meteo_ensemble")
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not isinstance(hourly, dict):
        return {}
    times = hourly.get("time") or []
    if not isinstance(times, list) or not times:
        return {}

    member_series: dict[str, list[float]] = {}
    for key, values in hourly.items():
        if not key.startswith("temperature_2m_member"):
            continue
        if not isinstance(values, list):
            continue
        member_series[key] = values

    by_date: dict[str, list[float]] = {}
    for _, values in member_series.items():
        maxima_by_date: dict[str, float] = {}
        for idx, timestamp in enumerate(times):
            try:
                value = float(values[idx])
            except (IndexError, TypeError, ValueError):
                continue
            date_key = str(timestamp)[:10]
            previous = maxima_by_date.get(date_key)
            if previous is None or value > previous:
                maxima_by_date[date_key] = value
        for date_key, day_max in maxima_by_date.items():
            by_date.setdefault(date_key, []).append(day_max)
    return by_date


def _fetch_nws_daily(city: CityConfig) -> list[ModelForecast]:
    daily_cache_key = _cache_key_for_name(f"nws_daily/{city.key}")
    try:
        points_url = f"{WEATHER_GOV_BASE_URL}/points/{city.lat},{city.lon}"
        points = _request_json_with_retry(
            points_url,
            provider_name="weather_gov",
            attempts=WEATHER_NWS_RETRY_ATTEMPTS,
        )
        properties = points.get("properties") if isinstance(points, dict) else None
        if not isinstance(properties, dict):
            return []
        forecast_url = properties.get("forecast")
        if not forecast_url:
            return []

        forecast = _request_json_with_retry(
            str(forecast_url),
            provider_name="weather_gov",
            attempts=WEATHER_NWS_RETRY_ATTEMPTS,
        )
        periods = (((forecast or {}).get("properties") or {}).get("periods")) or []
        by_date: dict[str, float] = {}
        for period in periods:
            if not isinstance(period, dict):
                continue
            if not period.get("isDaytime"):
                continue
            start = str(period.get("startTime") or "")
            if not start:
                continue
            date = start[:10]
            temperature = period.get("temperature")
            if temperature is None:
                continue
            try:
                temp_f = float(temperature)
            except (TypeError, ValueError):
                continue
            current = by_date.get(date)
            if current is None or temp_f > current:
                by_date[date] = temp_f
        rows = [
            ModelForecast(model_name="nws", date=date, high=high, low=None, source="weather.gov")
            for date, high in sorted(by_date.items())
        ]
        if rows:
            _write_cached_response(daily_cache_key, _serialize_model_forecasts(rows))
        return rows
    except Exception:
        _, max_stale_seconds = _provider_cache_policy("weather_gov")
        stale_rows = _restore_model_forecasts(_load_cached_response(daily_cache_key, max_age_seconds=max_stale_seconds))
        if stale_rows:
            return stale_rows
        raise


def _fetch_tomorrow_daily(city: CityConfig) -> list[ModelForecast]:
    if not TOMORROW_API_KEY:
        return []
    params = {
        "location": f"{city.lat},{city.lon}",
        "apikey": TOMORROW_API_KEY,
    }
    url = f"https://api.tomorrow.io/v4/weather/forecast?{urllib.parse.urlencode(params)}"
    data = _request_json(url, provider_name="tomorrow")
    daily = (((data or {}).get("timelines") or {}).get("daily")) or []
    output: list[ModelForecast] = []
    for item in daily:
        if not isinstance(item, dict):
            continue
        values = item.get("values") or {}
        try:
            output.append(
                ModelForecast(
                    model_name="tomorrow",
                    date=str(item.get("time") or "")[:10],
                    high=float(values.get("temperatureMax")),
                    low=float(values.get("temperatureMin")) if values.get("temperatureMin") is not None else None,
                    source="tomorrow.io",
                )
            )
        except (TypeError, ValueError):
            continue
    return output


def _fetch_weatherapi_daily(city: CityConfig) -> list[ModelForecast]:
    if not WEATHERAPI_KEY:
        return []
    params = {
        "key": WEATHERAPI_KEY,
        "q": f"{city.lat},{city.lon}",
        "days": "3",
        "aqi": "no",
        "alerts": "no",
    }
    url = f"https://api.weatherapi.com/v1/forecast.json?{urllib.parse.urlencode(params)}"
    data = _request_json(url, provider_name="weatherapi")
    forecast_days = ((((data or {}).get("forecast")) or {}).get("forecastday")) or []
    output: list[ModelForecast] = []
    for item in forecast_days:
        day = item.get("day") or {}
        try:
            output.append(
                ModelForecast(
                    model_name="weatherapi",
                    date=str(item.get("date") or ""),
                    high=float(day.get("maxtemp_f")),
                    low=float(day.get("mintemp_f")) if day.get("mintemp_f") is not None else None,
                    source="weatherapi.com",
                )
            )
        except (TypeError, ValueError):
            continue
    return output


def _fetch_visualcrossing_daily(city: CityConfig) -> list[ModelForecast]:
    if not VISUAL_CROSSING_API_KEY:
        return []
    location = f"{city.lat},{city.lon}"
    params = {
        "unitGroup": "us",
        "include": "days",
        "key": VISUAL_CROSSING_API_KEY,
        "contentType": "json",
    }
    url = (
        "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{urllib.parse.quote(location, safe='')}?{urllib.parse.urlencode(params)}"
    )
    data = _request_json(url, provider_name="visualcrossing")
    days = (data or {}).get("days") or []
    output: list[ModelForecast] = []
    for item in days[:7]:
        try:
            output.append(
                ModelForecast(
                    model_name="visualcrossing",
                    date=str(item.get("datetime") or ""),
                    high=float(item.get("tempmax")),
                    low=float(item.get("tempmin")) if item.get("tempmin") is not None else None,
                    source="visualcrossing",
                )
            )
        except (TypeError, ValueError):
            continue
    return output


def _fetch_openweather_daily(city: CityConfig) -> list[ModelForecast]:
    if not OPENWEATHER_API_KEY:
        return []
    params = {
        "lat": str(city.lat),
        "lon": str(city.lon),
        "appid": OPENWEATHER_API_KEY,
        "units": "imperial",
    }
    url = f"https://api.openweathermap.org/data/2.5/forecast?{urllib.parse.urlencode(params)}"
    data = _request_json(url, provider_name="openweather")
    rows = (data or {}).get("list") or []
    by_date: dict[str, dict[str, float]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        dt_text = str(item.get("dt_txt") or "")
        date_key = dt_text[:10]
        main = item.get("main") or {}
        temp_max = main.get("temp_max")
        temp_min = main.get("temp_min")
        try:
            temp_max_f = float(temp_max)
        except (TypeError, ValueError):
            continue
        current = by_date.setdefault(date_key, {"high": temp_max_f, "low": temp_min if temp_min is not None else temp_max_f})
        current["high"] = max(float(current["high"]), temp_max_f)
        if temp_min is not None:
            try:
                current["low"] = min(float(current["low"]), float(temp_min))
            except (TypeError, ValueError):
                pass
    output: list[ModelForecast] = []
    for date_key in sorted(by_date.keys())[:3]:
        values = by_date[date_key]
        output.append(
            ModelForecast(
                model_name="openweather",
                date=date_key,
                high=float(values["high"]),
                low=float(values["low"]) if values.get("low") is not None else None,
                source="openweathermap",
            )
        )
    return output


def fetch_all_model_forecasts(city: CityConfig) -> ForecastFetchBundle:
    results: dict[str, list[ModelForecast]] = {}
    failures: list[str] = []
    failure_details: dict[str, str] = {}
    fetchers: dict[str, Any] = {
        **{model_key: (lambda mk=model_key: _fetch_open_meteo_model(city, mk)) for model_key in OPEN_METEO_MODELS},
        "nws": lambda: _fetch_nws_daily(city),
        "mos": lambda: _fetch_mos_daily(city),
    }
    optional_fetchers = {
        "tomorrow": _fetch_tomorrow_daily,
        "weatherapi": _fetch_weatherapi_daily,
        "visualcrossing": _fetch_visualcrossing_daily,
        "openweather": _fetch_openweather_daily,
    }
    if WEATHER_ENABLE_HRRR:
        optional_fetchers["hrrr"] = _fetch_hrrr_daily
    disabled_optional: set[str] = {
        key for key in optional_fetchers if _provider_in_cooldown(key) and _provider_cooldown_until(key) - time.time() > 300
    }
    for key, fetcher in optional_fetchers.items():
        fetchers[key] = lambda fn=fetcher: fn(city)

    with ThreadPoolExecutor(max_workers=min(max(1, WEATHER_PROVIDER_MAX_WORKERS), len(fetchers))) as executor:
        future_map = {executor.submit(fetcher): key for key, fetcher in fetchers.items()}
        for future in as_completed(future_map):
            key = future_map[future]
            try:
                results[key] = future.result()
            except Exception as exc:
                results[key] = []
                summary = _summarize_provider_error(exc)
                if key in disabled_optional and ("HTTP 401" in summary or "cooling down" in summary):
                    continue
                failures.append(key)
                failure_details[key] = summary
    return ForecastFetchBundle(
        forecasts_by_model=results,
        provider_failures=sorted(set(failures)),
        provider_failure_details={key: failure_details[key] for key in sorted(failure_details)},
    )


def fetch_probabilistic_daily_highs(
    city: CityConfig,
    date_strs: list[str],
) -> tuple[dict[str, dict[str, list[float]]], list[str], dict[str, str]]:
    by_date: dict[str, dict[str, list[float]]] = {date_str: {} for date_str in date_strs}
    failures: list[str] = []
    failure_details: dict[str, str] = {}
    if _provider_in_cooldown("open_meteo_ensemble") and _provider_cooldown_until("open_meteo_ensemble") - time.time() > 60:
        remaining = max(1, int(_provider_cooldown_until("open_meteo_ensemble") - time.time()))
        names = sorted(ENSEMBLE_MODELS.keys())
        return by_date, names, {name: f"provider open_meteo_ensemble cooling down for {remaining}s" for name in names}
    for model_family, open_meteo_model in ENSEMBLE_MODELS.items():
        try:
            ensemble_rows = _fetch_open_meteo_ensemble_daily_highs(city, model_family, open_meteo_model)
        except Exception as exc:
            failures.append(model_family)
            failure_details[model_family] = _summarize_provider_error(exc)
            continue
        for date_str in date_strs:
            values = ensemble_rows.get(date_str) or []
            if values:
                by_date.setdefault(date_str, {})[model_family] = values
    return by_date, sorted(set(failures)), {key: failure_details[key] for key in sorted(failure_details)}


def build_ensemble_for_date(
    city: CityConfig,
    forecasts_by_model: dict[str, list[ModelForecast]],
    date_str: str,
    probabilistic_highs: dict[str, list[float]] | None = None,
    provider_failures: list[str] | None = None,
    provider_failure_details: dict[str, str] | None = None,
    horizon_days: int | None = None,
) -> EnsembleForecast | None:
    predictions: dict[str, float] = {}
    for model_name, entries in forecasts_by_model.items():
        match = next((entry for entry in entries if entry.date == date_str), None)
        if match is None:
            continue
        predictions[model_name] = match.high

    if not predictions:
        return None

    calibrated_predictions, effective_weights = _apply_model_calibration(city.key, horizon_days, predictions)
    values = list(calibrated_predictions.values())
    deterministic_blended, deterministic_anchor, robust_scale = _robust_weighted_blend(
        calibrated_predictions,
        weights_by_model=effective_weights,
    )
    probabilistic_values_by_family = {
        family: [float(value) for value in values]
        for family, values in (probabilistic_highs or {}).items()
        if values
    }
    family_means = [statistics.mean(values) for values in probabilistic_values_by_family.values()]
    family_spreads = [statistics.pstdev(values) for values in probabilistic_values_by_family.values() if len(values) > 1]
    probabilistic_mean = statistics.mean(family_means) if family_means else None
    probabilistic_spread = statistics.mean(family_spreads) if family_spreads else None
    if probabilistic_mean is not None:
        blended = (deterministic_blended * 0.70) + (deterministic_anchor * 0.10) + (probabilistic_mean * 0.20)
    else:
        blended = (deterministic_blended * 0.85) + (deterministic_anchor * 0.15)
    spread = statistics.pstdev(values) if len(values) > 1 else 0.0
    sigma_candidates = [1.5, 1.2 * spread, 1.1 * robust_scale, 2.0 if len(values) < 3 else 0.0]
    if probabilistic_spread is not None:
        sigma_candidates.append(float(probabilistic_spread))
    sigma = max(sigma_candidates)
    model_consensus = max(0.0, min(1.0, 1.0 - (max(spread, robust_scale) / 6.0)))
    if probabilistic_spread is None:
        consensus = model_consensus
    else:
        probabilistic_consensus = max(0.0, min(1.0, 1.0 - (float(probabilistic_spread) / 8.0)))
        consensus = (model_consensus * 0.65) + (probabilistic_consensus * 0.35)
    valid_model_count = len(predictions)
    min_models = _effective_min_models(int(os.getenv("WEATHER_MIN_VALID_MODELS", "5") or 5))
    coverage_score = _compute_coverage_score(
        valid_model_count=valid_model_count,
        min_models=min_models,
        provider_failures=provider_failures,
        probabilistic_member_count=sum(len(values) for values in probabilistic_values_by_family.values()),
        predictions=calibrated_predictions,
        horizon_days=horizon_days,
    )
    coverage_ok = coverage_score >= float(os.getenv("WEATHER_MIN_COVERAGE_SCORE", "0.62"))
    coverage_issue_type = _coverage_issue_type(
        valid_model_count=valid_model_count,
        min_models=min_models,
        provider_failures=provider_failures,
        provider_failure_details=provider_failure_details,
    )
    degraded_reasons: list[str] = []
    if valid_model_count < min_models:
        degraded_reasons.append(f"insufficient_model_coverage:{valid_model_count}")
    if provider_failures:
        degraded_reasons.append(f"provider_failures:{','.join(sorted(provider_failures))}")
    return EnsembleForecast(
        city_key=city.key,
        city_name=city.display_name,
        date=date_str,
        predictions={key: round(val, 2) for key, val in calibrated_predictions.items()},
        blended_high=round(blended, 2),
        min_high=round(min(values), 2),
        max_high=round(max(values), 2),
        spread=round(spread, 4),
        sigma=round(sigma, 4),
        consensus_score=round(consensus, 4),
        probabilistic_spread=(round(probabilistic_spread, 4) if probabilistic_spread is not None else None),
        probabilistic_member_count=sum(len(values) for values in probabilistic_values_by_family.values()),
        probabilistic_family_highs={
            family: [round(float(value), 4) for value in values]
            for family, values in probabilistic_values_by_family.items()
        }
        if probabilistic_values_by_family
        else None,
        valid_model_count=valid_model_count,
        required_model_count=min_models,
        coverage_ok=coverage_ok,
        coverage_issue_type=coverage_issue_type,
        degraded_reason=";".join(degraded_reasons) if degraded_reasons else None,
        provider_failures=sorted(provider_failures or []),
        provider_failure_details=dict(provider_failure_details or {}) or None,
        effective_weights={key: round(value, 4) for key, value in effective_weights.items()},
        data_quality_score=_compute_data_quality_score(
            valid_model_count=valid_model_count,
            min_models=min_models,
            provider_failures=provider_failures,
            probabilistic_member_count=sum(len(values) for values in probabilistic_values_by_family.values()),
            predictions=calibrated_predictions,
            horizon_days=horizon_days,
            coverage_ok=coverage_ok,
        ),
        coverage_score=coverage_score,
    )


def fetch_city_ensembles(city: CityConfig, date_strs: list[str]) -> dict[str, EnsembleForecast]:
    fetch_bundle = fetch_all_model_forecasts(city)
    probabilistic_by_date, probabilistic_failures, probabilistic_failure_details = fetch_probabilistic_daily_highs(city, date_strs)
    provider_failures = sorted(set(fetch_bundle.provider_failures + probabilistic_failures))
    provider_failure_details = dict(fetch_bundle.provider_failure_details or {})
    provider_failure_details.update(probabilistic_failure_details or {})
    output: dict[str, EnsembleForecast] = {}
    for horizon_days, date_str in enumerate(date_strs):
        ensemble = build_ensemble_for_date(
            city,
            fetch_bundle.forecasts_by_model,
            date_str,
            probabilistic_by_date.get(date_str) or [],
            provider_failures=provider_failures,
            provider_failure_details=provider_failure_details,
            horizon_days=horizon_days,
        )
        if ensemble is not None:
            output[date_str] = ensemble
    return output


def fetch_all_city_ensembles(date_strs: list[str]) -> dict[str, dict[str, EnsembleForecast]]:
    output: dict[str, dict[str, EnsembleForecast]] = {}
    for city in CITY_CONFIGS:
        output[city.key] = fetch_city_ensembles(city, date_strs)
    return output
