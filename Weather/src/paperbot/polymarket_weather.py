from __future__ import annotations

import json
import math
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .degendoppler import CITY_CONFIGS, CityConfig, MONTH_NAMES, MarketBucket, _parse_bucket_range, _safe_float
from .weather_models import EnsembleForecast, fetch_city_ensembles


POLYMARKET_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
MARKET_FEE = 0.02


@dataclass
class WeatherOpportunity:
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
    consensus_score: float
    spread: float
    sigma: float
    token_id: str | None
    market_slug: str
    market_id: str
    best_ask: float | None
    last_trade_price: float | None
    order_min_size: float | None
    model_predictions: dict[str, float]

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["polymarket_url"] = f"https://polymarket.com/event/{self.event_slug}"
        return payload


@dataclass
class MarketScan:
    city_key: str
    date_str: str
    event_slug: str
    event_title: str
    buckets: list[MarketBucket]


def _request_json(url: str, timeout: float = 20.0) -> Any:
    request = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "weather-bot/1.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _slug_for(city: CityConfig, target_date: datetime) -> str:
    month = MONTH_NAMES[target_date.month - 1]
    return f"highest-temperature-in-{city.market_city}-on-{month}-{target_date.day}-{target_date.year}"


def fetch_market_scan(city: CityConfig, target_date: datetime) -> MarketScan | None:
    slug = _slug_for(city, target_date)
    url = f"{POLYMARKET_GAMMA_BASE_URL}/events?slug={urllib.parse.quote(slug, safe='')}"
    data = _request_json(url)
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
                token_id_yes=str(token_ids[0]) if len(token_ids) >= 1 else None,
                token_id_no=str(token_ids[1]) if len(token_ids) >= 2 else None,
                best_ask=_safe_float(market.get("bestAsk")),
                last_trade_price=_safe_float(market.get("lastTradePrice")),
                order_min_size=_safe_float(market.get("orderMinSize")),
            )
        )

    buckets.sort(key=lambda item: (-10_000 if item.min_value is None else item.min_value))
    return MarketScan(
        city_key=city.key,
        date_str=target_date.date().isoformat(),
        event_slug=str(event.get("slug") or slug),
        event_title=str(event.get("title") or slug),
        buckets=buckets,
    )


def _fee_adjusted_price(price_cents: float) -> float:
    if price_cents <= 0 or price_cents >= 100:
        return price_cents
    return price_cents / (1 - MARKET_FEE * (1 - price_cents / 100.0))


def _bucket_bounds(bucket: MarketBucket) -> tuple[float, float]:
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


def _gaussian_bucket_probability(low: float, high: float, mean: float, sigma: float) -> float:
    z_low = (low - mean) / sigma
    z_high = (high - mean) / sigma
    prob_low = 0.5 * (1 + math.erf(z_low / math.sqrt(2)))
    prob_high = 0.5 * (1 + math.erf(z_high / math.sqrt(2)))
    return max(0.0, prob_high - prob_low)


def _build_opportunity(
    city: CityConfig,
    day_label: str,
    scan: MarketScan,
    ensemble: EnsembleForecast,
    bucket: MarketBucket,
    side: str,
    edge: float,
    ev_percent: float,
    price_cents: float,
    model_prob: float,
    market_prob: float,
) -> WeatherOpportunity:
    return WeatherOpportunity(
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
        ensemble_prediction=ensemble.blended_high,
        weighted_score=round(edge * (model_prob / 100.0) * ensemble.consensus_score, 4),
        consensus_score=ensemble.consensus_score,
        spread=ensemble.spread,
        sigma=ensemble.sigma,
        token_id=bucket.token_id_yes if side == "YES" else bucket.token_id_no,
        market_slug=bucket.market_slug,
        market_id=bucket.market_id,
        best_ask=bucket.best_ask,
        last_trade_price=bucket.last_trade_price,
        order_min_size=bucket.order_min_size,
        model_predictions=ensemble.predictions,
    )


def score_market_scan(
    city: CityConfig,
    day_label: str,
    scan: MarketScan,
    ensemble: EnsembleForecast,
    *,
    min_alt_edge: float = 15.0,
) -> list[WeatherOpportunity]:
    best_yes: tuple[MarketBucket, float, float, float] | None = None
    best_no: tuple[MarketBucket, float, float, float] | None = None

    for bucket in scan.buckets:
        low, high = _bucket_bounds(bucket)
        model_prob_yes = _gaussian_bucket_probability(low, high, ensemble.blended_high, ensemble.sigma) * 100.0
        market_prob_yes = bucket.yes_price_cents

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

    opportunities: list[WeatherOpportunity] = []
    if best_yes is not None and (best_no is None or best_yes[1] >= best_no[1]):
        opportunities.append(
            _build_opportunity(
                city, day_label, scan, ensemble, best_yes[0], "YES", best_yes[1], best_yes[2], best_yes[0].yes_price_cents, best_yes[3], best_yes[0].yes_price_cents
            )
        )
        if best_no is not None and best_no[1] > min_alt_edge:
            opportunities.append(
                _build_opportunity(
                    city, day_label, scan, ensemble, best_no[0], "NO", best_no[1], best_no[2], best_no[0].no_price_cents, best_no[3], best_no[0].no_price_cents
                )
            )
    elif best_no is not None:
        opportunities.append(
            _build_opportunity(
                city, day_label, scan, ensemble, best_no[0], "NO", best_no[1], best_no[2], best_no[0].no_price_cents, best_no[3], best_no[0].no_price_cents
            )
        )
        if best_yes is not None and best_yes[1] > min_alt_edge:
            opportunities.append(
                _build_opportunity(
                    city, day_label, scan, ensemble, best_yes[0], "YES", best_yes[1], best_yes[2], best_yes[0].yes_price_cents, best_yes[3], best_yes[0].yes_price_cents
                )
            )
    return opportunities


def scan_weather_model_opportunities(
    *,
    now: datetime | None = None,
    days_ahead: int = 3,
    min_edge: float = 0.0,
    min_model_prob: float = 15.0,
    min_consensus: float = 0.35,
) -> list[WeatherOpportunity]:
    reference = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
    day_labels = ["today", "tomorrow", "day2"][: max(1, min(days_ahead, 3))]
    target_dates = [(reference + timedelta(days=offset)) for offset in range(len(day_labels))]
    target_date_strs = [item.date().isoformat() for item in target_dates]

    opportunities: list[WeatherOpportunity] = []
    for city in CITY_CONFIGS:
        ensembles = fetch_city_ensembles(city, target_date_strs)
        for idx, day_label in enumerate(day_labels):
            target_date = target_dates[idx]
            date_str = target_date.date().isoformat()
            ensemble = ensembles.get(date_str)
            if ensemble is None or ensemble.consensus_score < min_consensus:
                continue
            scan = fetch_market_scan(city, target_date)
            if scan is None:
                continue
            for item in score_market_scan(city, day_label, scan, ensemble):
                if item.edge < min_edge or item.model_prob < min_model_prob:
                    continue
                opportunities.append(item)

    opportunities.sort(key=lambda item: item.weighted_score, reverse=True)
    return opportunities
