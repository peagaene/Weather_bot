from __future__ import annotations

import json
import statistics
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from .degendoppler import CITY_CONFIGS, CityConfig


OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_GOV_BASE_URL = "https://api.weather.gov"

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
    "gfs": 1.0,
    "icon": 1.0,
    "gem": 0.9,
    "jma": 0.8,
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


def fetch_all_model_forecasts(city: CityConfig) -> dict[str, list[ModelForecast]]:
    results: dict[str, list[ModelForecast]] = {}
    for model_key in OPEN_METEO_MODELS:
        try:
            results[model_key] = _fetch_open_meteo_model(city, model_key)
        except Exception:
            results[model_key] = []
    try:
        results["nws"] = _fetch_nws_daily(city)
    except Exception:
        results["nws"] = []
    return results


def build_ensemble_for_date(
    city: CityConfig,
    forecasts_by_model: dict[str, list[ModelForecast]],
    date_str: str,
) -> EnsembleForecast | None:
    predictions: dict[str, float] = {}
    for model_name, entries in forecasts_by_model.items():
        match = next((entry for entry in entries if entry.date == date_str), None)
        if match is None:
            continue
        predictions[model_name] = match.high

    if not predictions:
        return None

    weighted_sum = 0.0
    total_weight = 0.0
    values = list(predictions.values())
    for model_name, value in predictions.items():
        weight = MODEL_WEIGHTS.get(model_name, 1.0)
        weighted_sum += value * weight
        total_weight += weight
    blended = weighted_sum / total_weight if total_weight else statistics.mean(values)
    spread = statistics.pstdev(values) if len(values) > 1 else 0.0
    sigma = max(1.5, 1.35 * spread, 2.0 if len(values) < 3 else 0.0)
    consensus = max(0.0, min(1.0, 1.0 - (spread / 6.0)))
    return EnsembleForecast(
        city_key=city.key,
        city_name=city.display_name,
        date=date_str,
        predictions={key: round(val, 2) for key, val in predictions.items()},
        blended_high=round(blended, 2),
        min_high=round(min(values), 2),
        max_high=round(max(values), 2),
        spread=round(spread, 4),
        sigma=round(sigma, 4),
        consensus_score=round(consensus, 4),
    )


def fetch_city_ensembles(city: CityConfig, date_strs: list[str]) -> dict[str, EnsembleForecast]:
    forecasts_by_model = fetch_all_model_forecasts(city)
    output: dict[str, EnsembleForecast] = {}
    for date_str in date_strs:
        ensemble = build_ensemble_for_date(city, forecasts_by_model, date_str)
        if ensemble is not None:
            output[date_str] = ensemble
    return output


def fetch_all_city_ensembles(date_strs: list[str]) -> dict[str, dict[str, EnsembleForecast]]:
    output: dict[str, dict[str, EnsembleForecast]] = {}
    for city in CITY_CONFIGS:
        output[city.key] = fetch_city_ensembles(city, date_strs)
    return output
