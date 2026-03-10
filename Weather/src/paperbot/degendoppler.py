from __future__ import annotations

import json
import math
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


DEGENDOPPLER_BASE_URL = "https://degendoppler.com"
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
MONTH_NAMES = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]
MARKET_FEE = 0.02


@dataclass(frozen=True)
class CityConfig:
    key: str
    display_name: str
    market_city: str
    lat: float
    lon: float
    timezone_name: str
    coastal: bool = False
    marine_influenced: bool = False
    urban_core: bool = False
    regime_tags: tuple[str, ...] = ()


CITY_CONFIGS: tuple[CityConfig, ...] = (
    CityConfig(
        "NYC",
        "New York City",
        "nyc",
        40.76,
        -73.86,
        "America/New_York",
        coastal=True,
        marine_influenced=True,
        urban_core=True,
        regime_tags=("coastal", "marine", "urban"),
    ),
    CityConfig(
        "CHI",
        "Chicago",
        "chicago",
        41.98,
        -87.91,
        "America/Chicago",
        coastal=True,
        marine_influenced=False,
        urban_core=True,
        regime_tags=("coastal_like_lake", "urban"),
    ),
    CityConfig(
        "ATL",
        "Atlanta",
        "atlanta",
        33.64,
        -84.41,
        "America/New_York",
        coastal=False,
        marine_influenced=False,
        urban_core=True,
        regime_tags=("inland", "urban"),
    ),
    CityConfig(
        "SEA",
        "Seattle",
        "seattle",
        47.44,
        -122.30,
        "America/Los_Angeles",
        coastal=True,
        marine_influenced=True,
        urban_core=True,
        regime_tags=("coastal", "marine", "urban"),
    ),
    CityConfig(
        "MIA",
        "Miami",
        "miami",
        25.85,
        -80.24,
        "America/New_York",
        coastal=True,
        marine_influenced=True,
        urban_core=True,
        regime_tags=("coastal", "marine", "urban"),
    ),
    CityConfig(
        "DAL",
        "Dallas",
        "dallas",
        32.85,
        -96.87,
        "America/Chicago",
        coastal=False,
        marine_influenced=False,
        urban_core=True,
        regime_tags=("inland", "urban"),
    ),
)


@dataclass
class ForecastPoint:
    date: str
    high: float
    low: float


@dataclass
class MarketBucket:
    label: str
    min_value: int | None
    max_value: int | None
    probability: float
    yes_price_cents: float
    no_price_cents: float
    question: str
    market_slug: str
    market_id: str
    token_id_yes: str | None
    token_id_no: str | None
    best_ask: float | None
    last_trade_price: float | None
    order_min_size: float | None
    yes_best_ask_cents: float | None = None
    no_best_ask_cents: float | None = None
    yes_best_bid_cents: float | None = None
    no_best_bid_cents: float | None = None
    yes_last_trade_cents: float | None = None
    no_last_trade_cents: float | None = None


@dataclass
class MarketScan:
    city_key: str
    date_str: str
    event_slug: str
    event_title: str
    buckets: list[MarketBucket]


@dataclass
class Opportunity:
    city_key: str
    city_name: str
    day_label: str
    date_str: str
    event_slug: str
    event_title: str
    bucket: str
    side: str
    edge: float
    ev_percent: float
    price_cents: float
    model_prob: float
    market_prob: float
    ensemble_prediction: float
    weighted_score: float
    token_id: str | None
    market_slug: str
    market_id: str
    min_value: int | None
    max_value: int | None

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["polymarket_url"] = f"https://polymarket.com/event/{self.event_slug}"
        return payload


def _request_json(url: str, *, timeout: float = 15.0) -> Any:
    request = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "bot-poly/1.0",
            "Accept": "application/json",
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


def _slug_for(city: CityConfig, target_date: datetime) -> str:
    month = MONTH_NAMES[target_date.month - 1]
    return f"highest-temperature-in-{city.market_city}-on-{month}-{target_date.day}-{target_date.year}"


def _parse_bucket_range(label: str) -> tuple[int | None, int | None]:
    cleaned = label.replace("°F", "").replace("F", "").strip().lower()
    if "or below" in cleaned or "or lower" in cleaned:
        number = int("".join(ch for ch in cleaned if ch.isdigit()) or "0")
        return None, number
    if "or above" in cleaned or "or higher" in cleaned:
        number = int("".join(ch for ch in cleaned if ch.isdigit()) or "0")
        return number, None
    if "-" in cleaned:
        left, right = cleaned.split("-", 1)
        left_num = int("".join(ch for ch in left if ch.isdigit()) or "0")
        right_num = int("".join(ch for ch in right if ch.isdigit()) or "0")
        return left_num, right_num
    number = int("".join(ch for ch in cleaned if ch.isdigit()) or "0")
    return number, number


def _parse_open_meteo_forecast(city: CityConfig) -> list[ForecastPoint]:
    params = urllib.parse.urlencode(
        {
            "latitude": str(city.lat),
            "longitude": str(city.lon),
            "daily": "temperature_2m_max,temperature_2m_min",
            "temperature_unit": "fahrenheit",
            "timezone": "auto",
            "forecast_days": "7",
        }
    )
    data = _request_json(f"{OPEN_METEO_BASE_URL}?{params}")
    daily = data.get("daily") if isinstance(data, dict) else None
    if not isinstance(daily, dict):
        return []
    times = daily.get("time") or []
    highs = daily.get("temperature_2m_max") or []
    lows = daily.get("temperature_2m_min") or []
    points: list[ForecastPoint] = []
    for idx, date_str in enumerate(times):
        try:
            points.append(
                ForecastPoint(
                    date=str(date_str),
                    high=float(highs[idx]),
                    low=float(lows[idx]),
                )
            )
        except (IndexError, TypeError, ValueError):
            continue
    return points


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_market_scan(city: CityConfig, target_date: datetime) -> MarketScan | None:
    slug = _slug_for(city, target_date)
    encoded = urllib.parse.quote(slug, safe="")
    data = _request_json(f"{DEGENDOPPLER_BASE_URL}/api/markets?slug={encoded}")
    if not isinstance(data, list) or not data:
        return None
    event = data[0]
    markets = event.get("markets") or []
    if not isinstance(markets, list) or not markets:
        return None

    buckets: list[MarketBucket] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        label = str(market.get("groupItemTitle") or market.get("question") or "").strip()
        if not label:
            continue
        min_value, max_value = _parse_bucket_range(label)
        try:
            prices = json.loads(market.get("outcomePrices") or "[\"0\",\"0\"]")
        except json.JSONDecodeError:
            prices = ["0", "0"]
        try:
            token_ids = json.loads(market.get("clobTokenIds") or "[]")
        except json.JSONDecodeError:
            token_ids = []

        yes_price = float(prices[0]) * 100 if len(prices) >= 1 else 0.0
        no_price = float(prices[1]) * 100 if len(prices) >= 2 else max(0.0, 100.0 - yes_price)
        token_id_yes = str(token_ids[0]) if len(token_ids) >= 1 else None
        token_id_no = str(token_ids[1]) if len(token_ids) >= 2 else None
        buckets.append(
            MarketBucket(
                label=label,
                min_value=min_value,
                max_value=max_value,
                probability=yes_price / 100.0,
                yes_price_cents=yes_price,
                no_price_cents=no_price,
                question=str(market.get("question") or ""),
                market_slug=str(market.get("slug") or ""),
                market_id=str(market.get("id") or ""),
                token_id_yes=token_id_yes,
                token_id_no=token_id_no,
                best_ask=_safe_float(market.get("bestAsk")),
                last_trade_price=_safe_float(market.get("lastTradePrice")),
                order_min_size=_safe_float(market.get("orderMinSize")),
            )
        )

    if not buckets:
        return None
    buckets.sort(key=lambda item: (-10_000 if item.min_value is None else item.min_value))
    return MarketScan(
        city_key=city.key,
        date_str=target_date.date().isoformat(),
        event_slug=str(event.get("slug") or slug),
        event_title=str(event.get("title") or slug),
        buckets=buckets,
    )


def _erf_prob(low: float, high: float, mean: float, sigma: float) -> float:
    z_low = (low - mean) / sigma
    z_high = (high - mean) / sigma
    prob_low = 0.5 * (1.0 + math.erf(z_low / math.sqrt(2.0)))
    prob_high = 0.5 * (1.0 + math.erf(z_high / math.sqrt(2.0)))
    return max(0.0, prob_high - prob_low)


def _fee_adjusted_price(price_cents: float) -> float:
    if price_cents <= 0 or price_cents >= 100:
        return price_cents
    return price_cents / (1 - MARKET_FEE * (1 - price_cents / 100.0))


def _calc_bucket_bounds(bucket: MarketBucket) -> tuple[float, float]:
    label = bucket.label.lower()
    if "below" in label or "lower" in label:
        upper = (bucket.max_value if bucket.max_value is not None else 20) + 0.5
        return -100.0, float(upper)
    if "above" in label or "higher" in label:
        lower = (bucket.min_value if bucket.min_value is not None else 50) - 0.5
        return float(lower), 200.0
    if bucket.min_value is not None and bucket.max_value is not None:
        return float(bucket.min_value) - 0.5, float(bucket.max_value) + 0.5
    return -100.0, 200.0


def _build_opportunity(
    *,
    city: CityConfig,
    day_label: str,
    scan: MarketScan,
    bucket: MarketBucket,
    side: str,
    edge: float,
    ev_percent: float,
    price_cents: float,
    model_prob: float,
    market_prob: float,
    prediction: float,
) -> Opportunity:
    token_id = bucket.token_id_yes if side == "YES" else bucket.token_id_no
    return Opportunity(
        city_key=city.key,
        city_name=city.display_name,
        day_label=day_label,
        date_str=scan.date_str,
        event_slug=scan.event_slug,
        event_title=scan.event_title,
        bucket=bucket.label,
        side=side,
        edge=round(edge, 4),
        ev_percent=round(ev_percent, 4),
        price_cents=round(price_cents, 4),
        model_prob=round(model_prob, 4),
        market_prob=round(market_prob, 4),
        ensemble_prediction=round(prediction, 2),
        weighted_score=round(edge * (model_prob / 100.0), 4),
        token_id=token_id,
        market_slug=bucket.market_slug,
        market_id=bucket.market_id,
        min_value=bucket.min_value,
        max_value=bucket.max_value,
    )


def calculate_quick_edge(
    city: CityConfig,
    day_label: str,
    forecast_points: list[ForecastPoint],
    scan: MarketScan,
    *,
    sigma: float = 2.0,
    min_alt_edge: float = 15.0,
) -> list[Opportunity]:
    forecast = next((item for item in forecast_points if item.date == scan.date_str), None)
    if forecast is None:
        return []

    prediction = forecast.high
    best_yes: tuple[MarketBucket, float, float, float] | None = None
    best_no: tuple[MarketBucket, float, float, float] | None = None

    for bucket in scan.buckets:
        low, high = _calc_bucket_bounds(bucket)
        model_prob_yes = _erf_prob(low, high, prediction, sigma) * 100.0
        market_prob_yes = bucket.probability * 100.0

        yes_break_even = _fee_adjusted_price(market_prob_yes)
        no_break_even = _fee_adjusted_price(100.0 - market_prob_yes)

        yes_edge = model_prob_yes - yes_break_even
        yes_ev = ((model_prob_yes / yes_break_even) - 1.0) * 100.0 if yes_break_even > 0 else 0.0

        model_prob_no = 100.0 - model_prob_yes
        no_edge = model_prob_no - no_break_even
        no_ev = ((model_prob_no / no_break_even) - 1.0) * 100.0 if no_break_even > 0 else 0.0

        if best_yes is None or yes_edge > best_yes[1]:
            best_yes = (bucket, yes_edge, yes_ev, model_prob_yes)
        if model_prob_yes < 50.0 and (best_no is None or no_edge > best_no[1]):
            best_no = (bucket, no_edge, no_ev, model_prob_no)

    if best_yes is None and best_no is None:
        return []

    output: list[Opportunity] = []
    if best_yes is not None and (best_no is None or best_yes[1] >= best_no[1]):
        output.append(
            _build_opportunity(
                city=city,
                day_label=day_label,
                scan=scan,
                bucket=best_yes[0],
                side="YES",
                edge=best_yes[1],
                ev_percent=best_yes[2],
                price_cents=best_yes[0].yes_price_cents,
                model_prob=best_yes[3],
                market_prob=best_yes[0].yes_price_cents,
                prediction=prediction,
            )
        )
        if best_no is not None and best_no[1] > min_alt_edge:
            output.append(
                _build_opportunity(
                    city=city,
                    day_label=day_label,
                    scan=scan,
                    bucket=best_no[0],
                    side="NO",
                    edge=best_no[1],
                    ev_percent=best_no[2],
                    price_cents=best_no[0].no_price_cents,
                    model_prob=best_no[3],
                    market_prob=best_no[0].no_price_cents,
                    prediction=prediction,
                )
            )
    elif best_no is not None:
        output.append(
            _build_opportunity(
                city=city,
                day_label=day_label,
                scan=scan,
                bucket=best_no[0],
                side="NO",
                edge=best_no[1],
                ev_percent=best_no[2],
                price_cents=best_no[0].no_price_cents,
                model_prob=best_no[3],
                market_prob=best_no[0].no_price_cents,
                prediction=prediction,
            )
        )
        if best_yes is not None and best_yes[1] > min_alt_edge:
            output.append(
                _build_opportunity(
                    city=city,
                    day_label=day_label,
                    scan=scan,
                    bucket=best_yes[0],
                    side="YES",
                    edge=best_yes[1],
                    ev_percent=best_yes[2],
                    price_cents=best_yes[0].yes_price_cents,
                    model_prob=best_yes[3],
                    market_prob=best_yes[0].yes_price_cents,
                    prediction=prediction,
                )
            )

    return output


def scan_degendoppler_opportunities(
    *,
    now: datetime | None = None,
    days_ahead: int = 3,
    min_edge: float = 0.0,
    min_model_prob: float = 15.0,
) -> list[Opportunity]:
    reference = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
    day_labels = ["today", "tomorrow", "day2"][: max(1, min(days_ahead, 3))]
    opportunities: list[Opportunity] = []

    for city in CITY_CONFIGS:
        forecasts = _parse_open_meteo_forecast(city)
        if not forecasts:
            continue
        city_now = reference.astimezone(ZoneInfo(city.timezone_name))
        for day_offset, day_label in enumerate(day_labels):
            target_date = city_now + timedelta(days=day_offset)
            scan = _parse_market_scan(city, target_date)
            if scan is None:
                continue
            for item in calculate_quick_edge(city, day_label, forecasts, scan):
                if item.edge < min_edge or item.model_prob < min_model_prob:
                    continue
                opportunities.append(item)

    opportunities.sort(key=lambda item: item.weighted_score, reverse=True)
    return opportunities
