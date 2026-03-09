from __future__ import annotations

import json
import os
import statistics
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from .degendoppler import CITY_CONFIGS, CityConfig


OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ENSEMBLE_BASE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
WEATHER_GOV_BASE_URL = "https://api.weather.gov"
TOMORROW_API_KEY = os.getenv("TOMORROW_API_KEY", "").strip()
WEATHERAPI_KEY = os.getenv("WEATHERAPI_KEY", "").strip()
VISUAL_CROSSING_API_KEY = os.getenv("VISUAL_CROSSING_API_KEY", "").strip()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "").strip()
WEATHER_CALIBRATION_PATH = os.getenv(
    "WEATHER_CALIBRATION_PATH",
    str(Path(__file__).resolve().parents[2] / "export" / "calibration" / "weather_model_calibration.json"),
)

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
    coverage_ok: bool = False
    degraded_reason: str | None = None
    provider_failures: list[str] | None = None
    effective_weights: dict[str, float] | None = None
    data_quality_score: float = 0.0


@dataclass
class ForecastFetchBundle:
    forecasts_by_model: dict[str, list[ModelForecast]]
    provider_failures: list[str]


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
        if name in {"gfs", "icon", "gem", "jma"}:
            return 0.85
        if name == "ecmwf":
            return 0.92
    if horizon_days == 1:
        if name in {"nws", "tomorrow", "weatherapi", "best_match"}:
            return 1.1
        if name in {"gfs", "icon", "gem", "jma"}:
            return 0.93
    return 1.0


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
    failure_penalty = min(0.45, 0.08 * len(provider_failures or []))
    probabilistic_score = min(1.0, probabilistic_member_count / 40.0) if probabilistic_member_count > 0 else 0.0
    score = (model_coverage_score * 0.6) + (probabilistic_score * 0.2) + 0.2
    if horizon_days is not None and horizon_days <= 1:
        near_term_support = sum(
            1
            for model_name in ("nws", "tomorrow", "weatherapi", "openweather", "visualcrossing", "best_match")
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


def _request_json(url: str, *, timeout: float = 20.0, headers: dict[str, str] | None = None) -> Any:
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
            return json.loads(payload)
    except urllib.error.HTTPError as err:
        raise RuntimeError(f"HTTP {err.code} fetching {url}") from err
    except urllib.error.URLError as err:
        raise RuntimeError(f"Network error fetching {url}: {err}") from err
    except json.JSONDecodeError as err:
        raise RuntimeError(f"Invalid JSON returned by {url}") from err


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
    data = _request_json(url)
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
    data = _request_json(url)
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
    points_url = f"{WEATHER_GOV_BASE_URL}/points/{city.lat},{city.lon}"
    points = _request_json(points_url)
    properties = points.get("properties") if isinstance(points, dict) else None
    if not isinstance(properties, dict):
        return []
    forecast_url = properties.get("forecast")
    if not forecast_url:
        return []

    forecast = _request_json(str(forecast_url))
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

    return [
        ModelForecast(model_name="nws", date=date, high=high, low=None, source="weather.gov")
        for date, high in sorted(by_date.items())
    ]


def _fetch_tomorrow_daily(city: CityConfig) -> list[ModelForecast]:
    if not TOMORROW_API_KEY:
        return []
    params = {
        "location": f"{city.lat},{city.lon}",
        "apikey": TOMORROW_API_KEY,
    }
    url = f"https://api.tomorrow.io/v4/weather/forecast?{urllib.parse.urlencode(params)}"
    data = _request_json(url)
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
    data = _request_json(url)
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
    data = _request_json(url)
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
    data = _request_json(url)
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
    fetchers: dict[str, Any] = {
        **{model_key: (lambda mk=model_key: _fetch_open_meteo_model(city, mk)) for model_key in OPEN_METEO_MODELS},
        "nws": lambda: _fetch_nws_daily(city),
    }
    optional_fetchers = {
        "tomorrow": _fetch_tomorrow_daily,
        "weatherapi": _fetch_weatherapi_daily,
        "visualcrossing": _fetch_visualcrossing_daily,
        "openweather": _fetch_openweather_daily,
    }
    for key, fetcher in optional_fetchers.items():
        fetchers[key] = lambda fn=fetcher: fn(city)

    with ThreadPoolExecutor(max_workers=min(8, len(fetchers))) as executor:
        future_map = {executor.submit(fetcher): key for key, fetcher in fetchers.items()}
        for future in as_completed(future_map):
            key = future_map[future]
            try:
                results[key] = future.result()
            except Exception:
                results[key] = []
                failures.append(key)
    return ForecastFetchBundle(forecasts_by_model=results, provider_failures=sorted(set(failures)))


def fetch_probabilistic_daily_highs(city: CityConfig, date_strs: list[str]) -> tuple[dict[str, dict[str, list[float]]], list[str]]:
    by_date: dict[str, dict[str, list[float]]] = {date_str: {} for date_str in date_strs}
    failures: list[str] = []
    for model_family, open_meteo_model in ENSEMBLE_MODELS.items():
        try:
            ensemble_rows = _fetch_open_meteo_ensemble_daily_highs(city, model_family, open_meteo_model)
        except Exception:
            failures.append(model_family)
            continue
        for date_str in date_strs:
            values = ensemble_rows.get(date_str) or []
            if values:
                by_date.setdefault(date_str, {})[model_family] = values
    return by_date, sorted(set(failures))


def build_ensemble_for_date(
    city: CityConfig,
    forecasts_by_model: dict[str, list[ModelForecast]],
    date_str: str,
    probabilistic_highs: dict[str, list[float]] | None = None,
    provider_failures: list[str] | None = None,
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
    min_models = int(os.getenv("WEATHER_MIN_VALID_MODELS", "5") or 5)
    coverage_ok = valid_model_count >= min_models and not provider_failures
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
        coverage_ok=coverage_ok,
        degraded_reason=";".join(degraded_reasons) if degraded_reasons else None,
        provider_failures=sorted(provider_failures or []),
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
    )


def fetch_city_ensembles(city: CityConfig, date_strs: list[str]) -> dict[str, EnsembleForecast]:
    fetch_bundle = fetch_all_model_forecasts(city)
    probabilistic_by_date, probabilistic_failures = fetch_probabilistic_daily_highs(city, date_strs)
    provider_failures = sorted(set(fetch_bundle.provider_failures + probabilistic_failures))
    output: dict[str, EnsembleForecast] = {}
    for horizon_days, date_str in enumerate(date_strs):
        ensemble = build_ensemble_for_date(
            city,
            fetch_bundle.forecasts_by_model,
            date_str,
            probabilistic_by_date.get(date_str) or [],
            provider_failures=provider_failures,
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
