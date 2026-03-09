from __future__ import annotations

import json
import math
import os
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .degendoppler import CITY_CONFIGS, CityConfig, MONTH_NAMES, MarketBucket, _parse_bucket_range, _safe_float
from .probability_calibration import apply_probability_calibration
from .weather_models import EnsembleForecast, fetch_city_ensembles


POLYMARKET_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_BASE_URL = os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")
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
    agreement_models: int = 0
    total_models: int = 0
    agreement_pct: float = 0.0
    agreement_summary: str = "--"
    agreeing_model_names: list[str] | None = None
    confidence_tier: str = "risky"
    coverage_ok: bool = False
    coverage_issue_type: str | None = None
    valid_model_count: int = 0
    required_model_count: int = 0
    degraded_reason: str | None = None
    provider_failures: list[str] | None = None
    provider_failure_details: dict[str, str] | None = None
    policy_allowed: bool = False
    policy_reason: str = ""
    price_source: str = "gamma_outcome_price"
    reference_price_cents: float | None = None
    best_bid_cents: float | None = None
    mean_agreeing_model_edge: float = 0.0
    min_agreeing_model_edge: float = 0.0
    agreeing_model_count: int = 0
    executable_quality_score: float = 0.0
    data_quality_score: float = 0.0
    adversarial_score: float = 0.0
    signal_tier: str = "C"
    signal_decision: str = "watch"

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


def _fetch_batch_prices(token_ids: list[str], side: str) -> dict[str, float | None]:
    if not token_ids:
        return {}
    request = urllib.request.Request(
        url=f"{POLYMARKET_CLOB_BASE_URL}/prices",
        data=json.dumps([{"token_id": token_id, "side": side} for token_id in token_ids]).encode("utf-8"),
        headers={
            "User-Agent": "weather-bot/1.0",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20.0) as response:
            data = json.loads(response.read().decode("utf-8"))
    except Exception:
        return {token_id: None for token_id in token_ids}

    output: dict[str, float | None] = {}
    if not isinstance(data, dict):
        return {token_id: None for token_id in token_ids}
    for token_id in token_ids:
        item = data.get(token_id) or {}
        value = _safe_float(item.get(side))
        output[token_id] = (value * 100.0 if value is not None and value <= 1.0 else value)
    return output


def _slug_for(city: CityConfig, target_date: datetime) -> str:
    month = MONTH_NAMES[target_date.month - 1]
    return f"highest-temperature-in-{city.market_city}-on-{month}-{target_date.day}-{target_date.year}"


def fetch_market_scan(city: CityConfig, target_date: datetime, *, book_cache: dict[str, dict[str, float | None]] | None = None) -> MarketScan | None:
    slug = _slug_for(city, target_date)
    url = f"{POLYMARKET_GAMMA_BASE_URL}/events?slug={urllib.parse.quote(slug, safe='')}"
    data = _request_json(url)
    if not isinstance(data, list) or not data:
        return None
    event = data[0]
    if event.get("closed") is True or event.get("active") is False:
        return None
    markets = event.get("markets") or []
    if not isinstance(markets, list) or not markets:
        return None

    parsed_markets: list[dict[str, Any]] = []
    yes_token_ids: list[str] = []
    no_token_ids: list[str] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        if market.get("closed") is True or market.get("active") is False:
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
        if token_id_yes:
            yes_token_ids.append(token_id_yes)
        if token_id_no:
            no_token_ids.append(token_id_no)
        parsed_markets.append(
            {
                "label": label,
                "min_value": min_value,
                "max_value": max_value,
                "yes_price": yes_price,
                "no_price": no_price,
                "question": str(market.get("question") or ""),
                "market_slug": str(market.get("slug") or ""),
                "market_id": str(market.get("id") or ""),
                "token_id_yes": token_id_yes,
                "token_id_no": token_id_no,
                "best_ask": _safe_float(market.get("bestAsk")),
                "last_trade_price": _safe_float(market.get("lastTradePrice")),
                "order_min_size": _safe_float(market.get("orderMinSize")),
            }
        )

    if book_cache is not None:
        missing_yes = [token_id for token_id in yes_token_ids if token_id not in book_cache]
        missing_no = [token_id for token_id in no_token_ids if token_id not in book_cache]
        sell_prices = _fetch_batch_prices(missing_yes + missing_no, "SELL")
        buy_prices = _fetch_batch_prices(missing_yes + missing_no, "BUY")
        for token_id in set(missing_yes + missing_no):
            book_cache[token_id] = {
                "best_ask_cents": sell_prices.get(token_id),
                "best_bid_cents": buy_prices.get(token_id),
            }
        token_cache = book_cache
    else:
        sell_prices = _fetch_batch_prices(yes_token_ids + no_token_ids, "SELL")
        buy_prices = _fetch_batch_prices(yes_token_ids + no_token_ids, "BUY")
        token_cache = {
            token_id: {
                "best_ask_cents": sell_prices.get(token_id),
                "best_bid_cents": buy_prices.get(token_id),
            }
            for token_id in set(yes_token_ids + no_token_ids)
        }

    buckets: list[MarketBucket] = []
    for item in parsed_markets:
        yes_book = token_cache.get(item["token_id_yes"], {}) if item["token_id_yes"] else {}
        no_book = token_cache.get(item["token_id_no"], {}) if item["token_id_no"] else {}
        buckets.append(
            MarketBucket(
                label=item["label"],
                min_value=item["min_value"],
                max_value=item["max_value"],
                probability=item["yes_price"] / 100.0,
                yes_price_cents=item["yes_price"],
                no_price_cents=item["no_price"],
                question=item["question"],
                market_slug=item["market_slug"],
                market_id=item["market_id"],
                token_id_yes=item["token_id_yes"],
                token_id_no=item["token_id_no"],
                best_ask=item["best_ask"],
                last_trade_price=item["last_trade_price"],
                order_min_size=item["order_min_size"],
                yes_best_ask_cents=yes_book.get("best_ask_cents"),
                no_best_ask_cents=no_book.get("best_ask_cents"),
                yes_best_bid_cents=yes_book.get("best_bid_cents"),
                no_best_bid_cents=no_book.get("best_bid_cents"),
                yes_last_trade_cents=None,
                no_last_trade_cents=None,
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


def _empirical_bucket_probability(samples: list[float], low: float, high: float) -> float | None:
    if not samples:
        return None
    hits = 0
    total = 0
    for value in samples:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        total += 1
        if low <= numeric <= high:
            hits += 1
    if total <= 0:
        return None
    return hits / total


def _ensemble_bucket_probability(ensemble: EnsembleForecast, low: float, high: float) -> float:
    gaussian_prob = _gaussian_bucket_probability(low, high, ensemble.blended_high, ensemble.sigma)

    deterministic_values: list[float] = []
    for value in ensemble.predictions.values():
        try:
            deterministic_values.append(float(value))
        except (TypeError, ValueError):
            continue
    deterministic_vote = _empirical_bucket_probability(deterministic_values, low, high)

    family_probabilities: list[float] = []
    for family_values in (getattr(ensemble, "probabilistic_family_highs", None) or {}).values():
        probability = _empirical_bucket_probability(list(family_values), low, high)
        if probability is not None:
            family_probabilities.append(probability)
    probabilistic_prob = (
        sum(family_probabilities) / len(family_probabilities)
        if family_probabilities
        else None
    )

    if probabilistic_prob is not None and deterministic_vote is not None:
        return max(0.0, min(1.0, (probabilistic_prob * 0.65) + (deterministic_vote * 0.25) + (gaussian_prob * 0.10)))
    if probabilistic_prob is not None:
        return max(0.0, min(1.0, (probabilistic_prob * 0.8) + (gaussian_prob * 0.2)))
    if deterministic_vote is not None:
        return max(0.0, min(1.0, (deterministic_vote * 0.7) + (gaussian_prob * 0.3)))
    return gaussian_prob


def _model_bucket_probability(low: float, high: float, prediction: float, sigma: float) -> float:
    return _gaussian_bucket_probability(low, high, prediction, max(1.0, sigma))


def _model_edge_statistics(
    ensemble: EnsembleForecast,
    *,
    low: float,
    high: float,
    side: str,
    break_even_price: float,
    horizon_days: int | None,
) -> tuple[float, float, int, int, float]:
    sigma = max(1.1, min(3.5, float(getattr(ensemble, "sigma", 2.0) or 2.0) * (0.9 if (horizon_days or 0) <= 1 else 1.0)))
    agreeing_edges: list[float] = []
    total_models = 0
    for value in ensemble.predictions.values():
        try:
            predicted_high = float(value)
        except (TypeError, ValueError):
            continue
        total_models += 1
        predicted_yes = low <= predicted_high <= high
        predicted_side = "YES" if predicted_yes else "NO"
        model_prob_yes = _model_bucket_probability(low, high, predicted_high, sigma) * 100.0
        model_prob_side = model_prob_yes if side == "YES" else (100.0 - model_prob_yes)
        if predicted_side == side:
            agreeing_edges.append(model_prob_side - break_even_price)
    if not agreeing_edges:
        return 0.0, 0.0, 0, total_models, 0.0
    agreeing_pct = (len(agreeing_edges) / total_models * 100.0) if total_models > 0 else 0.0
    return (
        round(sum(agreeing_edges) / len(agreeing_edges), 4),
        round(min(agreeing_edges), 4),
        len(agreeing_edges),
        total_models,
        round(agreeing_pct, 2),
    )


def _execution_quality_score(
    *,
    price_source: str,
    entry_price_cents: float,
    best_bid_cents: float | None,
    order_min_size: float | None,
) -> float:
    score = 1.0
    source = str(price_source or "").strip().lower()
    if source == "clob_best_ask":
        score = 1.0
    elif source == "clob_last_trade":
        score = 0.72
    else:
        score = 0.2
    if best_bid_cents is not None and entry_price_cents > 0:
        book_width = max(0.0, float(entry_price_cents) - float(best_bid_cents))
        if book_width > 6.0:
            score -= 0.35
        elif book_width > 3.0:
            score -= 0.2
        elif book_width > 1.5:
            score -= 0.08
    if order_min_size is not None and entry_price_cents > 0:
        default_min_stake = float(os.getenv("PAPERBOT_MIN_STAKE_USD", "5.0"))
        min_notional = float(order_min_size) * float(entry_price_cents) / 100.0
        if min_notional > default_min_stake * 1.5:
            score -= 0.25
        elif min_notional > default_min_stake:
            score -= 0.12
    return round(max(0.0, min(1.0, score)), 4)


def _signal_tier(
    *,
    model_prob: float,
    mean_agreeing_model_edge: float,
    min_agreeing_model_edge: float,
    agreement_pct: float,
    executable_quality_score: float,
    data_quality_score: float,
    consensus_score: float,
) -> tuple[str, str, float]:
    probability_score = max(0.0, min(1.0, model_prob / 100.0))
    edge_score = max(0.0, min(1.0, mean_agreeing_model_edge / 35.0))
    worst_case_score = max(0.0, min(1.0, min_agreeing_model_edge / 25.0))
    agreement_score = max(0.0, min(1.0, agreement_pct / 100.0))
    adversarial_score = (
        (probability_score * 0.2)
        + (edge_score * 0.2)
        + (worst_case_score * 0.25)
        + (agreement_score * 0.15)
        + (max(0.0, min(1.0, consensus_score)) * 0.1)
        + (executable_quality_score * 0.05)
        + (data_quality_score * 0.05)
    )
    if (
        min_agreeing_model_edge >= 20.0
        and agreement_pct >= 88.0
        and executable_quality_score >= 0.8
        and data_quality_score >= 0.75
        and consensus_score >= 0.7
    ):
        return "A+", "auto", round(adversarial_score * 100.0, 2)
    if (
        min_agreeing_model_edge >= 12.0
        and agreement_pct >= 75.0
        and executable_quality_score >= 0.65
        and data_quality_score >= 0.6
        and consensus_score >= 0.55
    ):
        return "A", "auto", round(adversarial_score * 100.0, 2)
    if (
        min_agreeing_model_edge >= 5.0
        and agreement_pct >= 60.0
        and executable_quality_score >= 0.45
        and data_quality_score >= 0.45
    ):
        return "B", "review", round(adversarial_score * 100.0, 2)
    return "C", "watch", round(adversarial_score * 100.0, 2)


def _model_side_agreement_pct(
    predictions: dict[str, float],
    low: float,
    high: float,
    side: str,
) -> tuple[int, int, float]:
    agree = 0
    total = 0
    for value in predictions.values():
        try:
            predicted_high = float(value)
        except (TypeError, ValueError):
            continue
        total += 1
        predicted_yes = low <= predicted_high <= high
        predicted_side = "YES" if predicted_yes else "NO"
        if predicted_side == side:
            agree += 1
    pct = (agree / total * 100.0) if total > 0 else 0.0
    return agree, total, pct


def _agreeing_model_names(
    predictions: dict[str, float],
    low: float,
    high: float,
    side: str,
) -> list[str]:
    agreeing: list[str] = []
    for model_name, value in predictions.items():
        try:
            predicted_high = float(value)
        except (TypeError, ValueError):
            continue
        predicted_yes = low <= predicted_high <= high
        predicted_side = "YES" if predicted_yes else "NO"
        if predicted_side == side:
            agreeing.append(str(model_name))
    return agreeing


def _confidence_tier_from_agreement(agreement_pct: float) -> str:
    return _confidence_tier_from_agreement_with_count(agreement_pct, 0)


def _confidence_tier_from_agreement_with_count(agreement_pct: float, total_models: int) -> str:
    if total_models < 3:
        return "risky"
    if total_models < 5 and agreement_pct >= 90.0:
        return "safe"
    if agreement_pct >= 99.999:
        return "lock"
    if agreement_pct >= 90.0:
        return "strong"
    if agreement_pct >= 80.0:
        return "safe"
    if agreement_pct >= 60.0:
        return "near-safe"
    return "risky"


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
    price_source: str,
    reference_price_cents: float,
    model_prob: float,
    market_prob: float,
    horizon_days: int | None,
) -> WeatherOpportunity:
    low, high = _bucket_bounds(bucket)
    agreement_models, total_models, agreement_pct = _model_side_agreement_pct(
        ensemble.predictions,
        low,
        high,
        side,
    )
    agreeing_model_names = _agreeing_model_names(ensemble.predictions, low, high, side)
    agreement_summary = (
        f"{agreement_models}/{total_models}"
        if total_models > 0
        else "--"
    )
    break_even_price = _fee_adjusted_price(price_cents)
    mean_agreeing_model_edge, min_agreeing_model_edge, agreeing_model_count, _, agreeing_pct = _model_edge_statistics(
        ensemble,
        low=low,
        high=high,
        side=side,
        break_even_price=break_even_price,
        horizon_days=horizon_days,
    )
    executable_quality_score = _execution_quality_score(
        price_source=price_source,
        entry_price_cents=price_cents,
        best_bid_cents=(bucket.yes_best_bid_cents if side == "YES" else bucket.no_best_bid_cents),
        order_min_size=bucket.order_min_size,
    )
    data_quality_score = float(getattr(ensemble, "data_quality_score", 0.0) or 0.0)
    signal_tier, signal_decision, adversarial_score = _signal_tier(
        model_prob=model_prob,
        mean_agreeing_model_edge=mean_agreeing_model_edge,
        min_agreeing_model_edge=min_agreeing_model_edge,
        agreement_pct=agreeing_pct,
        executable_quality_score=executable_quality_score,
        data_quality_score=data_quality_score,
        consensus_score=ensemble.consensus_score,
    )
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
        weighted_score=round(adversarial_score, 4),
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
        agreement_models=agreement_models,
        total_models=total_models,
        agreement_pct=round(agreement_pct, 2),
        agreement_summary=agreement_summary,
        agreeing_model_names=agreeing_model_names,
        confidence_tier=_confidence_tier_from_agreement_with_count(agreement_pct, total_models),
        coverage_ok=bool(getattr(ensemble, "coverage_ok", False)),
        coverage_issue_type=getattr(ensemble, "coverage_issue_type", None),
        valid_model_count=int(getattr(ensemble, "valid_model_count", 0) or 0),
        required_model_count=int(getattr(ensemble, "required_model_count", 0) or 0),
        degraded_reason=getattr(ensemble, "degraded_reason", None),
        provider_failures=list(getattr(ensemble, "provider_failures", None) or []),
        provider_failure_details=dict(getattr(ensemble, "provider_failure_details", None) or {}) or None,
        price_source=price_source,
        reference_price_cents=round(reference_price_cents, 4),
        best_bid_cents=(bucket.yes_best_bid_cents if side == "YES" else bucket.no_best_bid_cents),
        mean_agreeing_model_edge=mean_agreeing_model_edge,
        min_agreeing_model_edge=min_agreeing_model_edge,
        agreeing_model_count=agreeing_model_count,
        executable_quality_score=executable_quality_score,
        data_quality_score=data_quality_score,
        adversarial_score=adversarial_score,
        signal_tier=signal_tier,
        signal_decision=signal_decision,
    )


def _select_entry_price(bucket: MarketBucket, side: str) -> tuple[float, str, float]:
    if side == "YES":
        executable = bucket.yes_best_ask_cents
        fallback_trade = bucket.yes_last_trade_cents
        reference = bucket.yes_price_cents
    else:
        executable = bucket.no_best_ask_cents
        fallback_trade = bucket.no_last_trade_cents
        reference = bucket.no_price_cents

    if executable is not None and executable > 0:
        return float(executable), "clob_best_ask", float(reference)
    if fallback_trade is not None and fallback_trade > 0:
        return float(fallback_trade), "clob_last_trade", float(reference)
    return float(reference), "gamma_outcome_price", float(reference)


def _local_target_dates(city: CityConfig, reference: datetime, days_ahead: int) -> list[datetime]:
    city_now = reference.astimezone(ZoneInfo(city.timezone_name))
    return [city_now + timedelta(days=offset) for offset in range(days_ahead)]


def score_market_scan(
    city: CityConfig,
    day_label: str,
    scan: MarketScan,
    ensemble: EnsembleForecast,
    *,
    horizon_days: int | None = None,
    min_alt_edge: float = 15.0,
) -> list[WeatherOpportunity]:
    best_yes: tuple[MarketBucket, float, float, float, float, float, str] | None = None
    best_no: tuple[MarketBucket, float, float, float, float, float, str] | None = None

    for bucket in scan.buckets:
        low, high = _bucket_bounds(bucket)
        raw_model_prob_yes = _ensemble_bucket_probability(ensemble, low, high)
        calibrated_probability = apply_probability_calibration(
            raw_model_prob_yes,
            city_key=city.key,
            horizon_days=horizon_days,
        )
        model_prob_yes = calibrated_probability.calibrated_probability * 100.0
        yes_entry_price, yes_price_source, yes_reference = _select_entry_price(bucket, "YES")
        no_entry_price, no_price_source, no_reference = _select_entry_price(bucket, "NO")

        yes_break_even = _fee_adjusted_price(yes_entry_price)
        no_break_even = _fee_adjusted_price(no_entry_price)

        yes_edge = model_prob_yes - yes_break_even
        yes_ev = ((model_prob_yes / yes_break_even) - 1.0) * 100.0 if yes_break_even > 0 else 0.0

        model_prob_no = 100.0 - model_prob_yes
        no_edge = model_prob_no - no_break_even
        no_ev = ((model_prob_no / no_break_even) - 1.0) * 100.0 if no_break_even > 0 else 0.0

        if best_yes is None or yes_edge > best_yes[1]:
            best_yes = (bucket, yes_edge, yes_ev, model_prob_yes, yes_entry_price, yes_reference, yes_price_source)
        # A cheap NO can still be +EV even when the model gives YES > 50%.
        if best_no is None or no_edge > best_no[1]:
            best_no = (bucket, no_edge, no_ev, model_prob_no, no_entry_price, no_reference, no_price_source)

    opportunities: list[WeatherOpportunity] = []
    if best_yes is not None and (best_no is None or best_yes[1] >= best_no[1]):
        opportunities.append(
            _build_opportunity(
                city,
                day_label,
                scan,
                ensemble,
                best_yes[0],
                "YES",
                best_yes[1],
                best_yes[2],
                best_yes[4],
                best_yes[6],
                best_yes[5],
                best_yes[3],
                best_yes[4],
                horizon_days,
            )
        )
        if best_no is not None and best_no[1] > min_alt_edge:
            opportunities.append(
                _build_opportunity(
                    city,
                    day_label,
                    scan,
                    ensemble,
                    best_no[0],
                    "NO",
                    best_no[1],
                    best_no[2],
                    best_no[4],
                    best_no[6],
                    best_no[5],
                    best_no[3],
                    best_no[4],
                    horizon_days,
                )
            )
    elif best_no is not None:
        opportunities.append(
            _build_opportunity(
                city,
                day_label,
                scan,
                ensemble,
                best_no[0],
                "NO",
                best_no[1],
                best_no[2],
                best_no[4],
                best_no[6],
                best_no[5],
                best_no[3],
                best_no[4],
                horizon_days,
            )
        )
        if best_yes is not None and best_yes[1] > min_alt_edge:
            opportunities.append(
                _build_opportunity(
                    city,
                    day_label,
                    scan,
                    ensemble,
                    best_yes[0],
                    "YES",
                    best_yes[1],
                    best_yes[2],
                    best_yes[4],
                    best_yes[6],
                    best_yes[5],
                    best_yes[3],
                    best_yes[4],
                    horizon_days,
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

    opportunities: list[WeatherOpportunity] = []
    book_cache: dict[str, dict[str, float | None]] = {}
    for city in CITY_CONFIGS:
        target_dates = _local_target_dates(city, reference, len(day_labels))
        target_date_strs = [item.date().isoformat() for item in target_dates]
        ensembles = fetch_city_ensembles(city, target_date_strs)
        for idx, day_label in enumerate(day_labels):
            target_date = target_dates[idx]
            date_str = target_date.date().isoformat()
            ensemble = ensembles.get(date_str)
            if ensemble is None or ensemble.consensus_score < min_consensus:
                continue
            scan = fetch_market_scan(city, target_date, book_cache=book_cache)
            if scan is None:
                continue
            for item in score_market_scan(city, day_label, scan, ensemble, horizon_days=idx):
                if item.price_source == "gamma_outcome_price":
                    item.degraded_reason = "degraded_clob_price"
                if item.edge < min_edge or item.model_prob < min_model_prob:
                    continue
                opportunities.append(item)

    opportunities.sort(key=lambda item: item.weighted_score, reverse=True)
    return opportunities
