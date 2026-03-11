from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from .degendoppler import CITY_CONFIG_BY_KEY, parse_bucket_bounds as parse_market_bucket_bounds


@dataclass
class PolicyDecision:
    allowed: bool
    reason: str
    risk_label: str
    risk_score: float


def effective_price_bounds(
    opportunity: Any,
    *,
    min_price_cents: float,
    max_price_cents: float,
) -> tuple[float, float]:
    day_label = str(getattr(opportunity, "day_label", "") or "").strip().lower()
    city_key = str(getattr(opportunity, "city_key", "") or "").strip().upper()
    side = str(getattr(opportunity, "side", "") or "").strip().upper()
    agreement_pct = float(getattr(opportunity, "agreement_pct", 0.0) or 0.0)
    model_prob = float(getattr(opportunity, "model_prob", 0.0) or 0.0)
    confidence_tier = str(getattr(opportunity, "confidence_tier", "risky") or "risky").strip().lower()
    signal_tier = str(getattr(opportunity, "signal_tier", "C") or "C").strip().upper()
    tomorrow_price_override_enabled = str(
        os.getenv("WEATHER_POLICY_TOMORROW_PRICE_OVERRIDE_ENABLED", "1")
    ).strip().lower() not in {"0", "false", "no"}
    tomorrow_price_override_signal_tiers = {
        item.strip().upper()
        for item in str(os.getenv("WEATHER_POLICY_TOMORROW_PRICE_OVERRIDE_SIGNAL_TIERS", "A+,A,B")).split(",")
        if item.strip()
    }
    tomorrow_price_override_confidence = {
        item.strip().lower()
        for item in str(os.getenv("WEATHER_POLICY_TOMORROW_PRICE_OVERRIDE_CONFIDENCE", "safe,near-safe")).split(",")
        if item.strip()
    }
    tomorrow_price_override_max = float(
        os.getenv("WEATHER_POLICY_TOMORROW_MAX_PRICE_CENTS", str(max_price_cents))
    )
    effective_max_price = max_price_cents
    if (
        tomorrow_price_override_enabled
        and day_label == "tomorrow"
        and confidence_tier in tomorrow_price_override_confidence
        and signal_tier in tomorrow_price_override_signal_tiers
    ):
        effective_max_price = max(max_price_cents, tomorrow_price_override_max)
    return min_price_cents, effective_max_price


@lru_cache(maxsize=1)
def _load_policy_profile() -> dict[str, Any]:
    default_path = Path(__file__).resolve().parents[2] / "export" / "analysis" / "policy_profile.json"
    raw_path = str(os.getenv("WEATHER_POLICY_PROFILE_PATH", str(default_path))).strip()
    if not raw_path:
        return {}
    path = Path(raw_path)
    if not path.is_absolute():
        path = (Path(__file__).resolve().parents[2] / raw_path).resolve()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _profile_set(section: str) -> set[str]:
    payload = _load_policy_profile()
    values = payload.get(section)
    if not isinstance(values, list):
        return set()
    return {str(item).strip() for item in values if str(item).strip()}


def _env_csv_set(name: str, default: str = "") -> set[str]:
    return {
        item.strip()
        for item in str(os.getenv(name, default)).split(",")
        if item.strip()
    }


def _normalize_bucket_label(label: str) -> str:
    text = (label or "").upper()
    replacements = [
        "ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°F",
        "ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°F",
        "ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°F",
        "Ãƒâ€šÃ‚Â°F",
        "Ã‚Â°F",
        "Â°F",
        "ÂºF",
        "Ã‚ÂºF",
        "Ãƒâ€šÃ‚ÂºF",
    ]
    for token in replacements:
        text = text.replace(token, "F")
    return re.sub(r"\s+", " ", text).strip()


def _canonical_bucket_key(label: str) -> str:
    normalized = _normalize_bucket_label(label)
    normalized = normalized.replace("°", " ").replace("º", " ")
    normalized = re.sub(r"[^A-Z0-9]+", " ", normalized)
    normalized = re.sub(r"\bF\b", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _legacy_parse_bucket_bounds(label: str) -> tuple[float | None, float | None]:
    text = (label or "").upper()
    replacements = [
        "ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°F",
        "ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°F",
        "Ãƒâ€šÃ‚Â°F",
        "Ã‚Â°F",
        "Â°F",
        "°F",
        "ºF",
        "ÂºF",
        "Ã‚ÂºF",
    ]
    for token in replacements:
        text = text.replace(token, "F")
    text = re.sub(r"\s+", " ", text).strip()

    range_match = re.search(r"(\d+)\s*-\s*(\d+)\s*F?", text)
    if range_match:
        return float(range_match.group(1)), float(range_match.group(2))

    below_match = re.search(r"(\d+)\s*(?:F)?\s*OR\s*(LOWER|BELOW)", text)
    if below_match:
        return None, float(below_match.group(1))

    above_match = re.search(r"(\d+)\s*(?:F)?\s*OR\s*(HIGHER|ABOVE)", text)
    if above_match:
        return float(above_match.group(1)), None

    return None, None


def parse_bucket_bounds(label: str) -> tuple[float | None, float | None]:
    low, high, _ = parse_market_bucket_bounds(label)
    return low, high


def compute_risk_label(opportunity: Any, range_info: dict | None = None) -> tuple[str, float]:
    risk_points = 0.0

    consensus = float(getattr(opportunity, "consensus_score", 0.0) or 0.0)
    spread = float(getattr(opportunity, "spread", 0.0) or 0.0)
    sigma = float(getattr(opportunity, "sigma", 0.0) or 0.0)
    ensemble_prediction = float(getattr(opportunity, "ensemble_prediction", 0.0) or 0.0)
    bucket_label = str(getattr(opportunity, "bucket", "") or "")
    low, high = parse_bucket_bounds(bucket_label)

    if consensus < 0.45:
        risk_points += 3.0
    elif consensus < 0.55:
        risk_points += 2.0
    elif consensus < 0.65:
        risk_points += 1.0

    if spread > 4.5:
        risk_points += 2.0
    elif spread > 3.0:
        risk_points += 1.0

    if sigma > 3.5:
        risk_points += 1.5
    elif sigma > 2.5:
        risk_points += 0.75

    if low is not None and high is not None:
        width = high - low
        if width <= 1.0:
            risk_points += 2.0
        elif width <= 2.0:
            risk_points += 1.25
        midpoint = (low + high) / 2.0
        distance_to_edge = min(abs(ensemble_prediction - low), abs(ensemble_prediction - high))
        if distance_to_edge < 0.35:
            risk_points += 1.5
        elif distance_to_edge < 0.75:
            risk_points += 0.75
        if abs(ensemble_prediction - midpoint) < 0.25:
            risk_points -= 0.4
    else:
        risk_points += 0.35

    if range_info:
        try:
            min_price = float(range_info.get("min_price_cents") or 0.0)
            max_price = float(range_info.get("max_price_cents") or 0.0)
            samples = int(range_info.get("samples") or 0)
        except (TypeError, ValueError):
            min_price, max_price, samples = 0.0, 0.0, 0
        width = max(0.0, max_price - min_price)
        if width > 18.0:
            risk_points += 1.25
        elif width > 10.0:
            risk_points += 0.6
        if samples < 3:
            risk_points += 0.35

    risk_points = max(0.0, risk_points)
    if risk_points >= 4.5:
        return "Risky", risk_points
    if risk_points >= 2.25:
        return "Moderate", risk_points
    return "Safe", risk_points


def _baseline_thresholds(confidence_tier: str, policy_min_edge: float, policy_min_consensus: float) -> tuple[float, float]:
    if confidence_tier in {"lock", "strong", "safe"}:
        return policy_min_edge, policy_min_consensus
    if confidence_tier == "near-safe":
        return (
            float(os.getenv("WEATHER_POLICY_NEAR_SAFE_MIN_EDGE", "18")),
            float(os.getenv("WEATHER_POLICY_NEAR_SAFE_MIN_CONSENSUS", "0.5")),
        )
    return float("inf"), float("inf")


def _signal_adjustments(signal_tier: str) -> tuple[float, float]:
    if signal_tier in {"A+", "A"}:
        return 0.0, 0.0
    if signal_tier == "B":
        return 1.0, 0.02
    if signal_tier == "C":
        return 4.0, 0.08
    return 6.0, 0.12


def _risk_adjustments(risk_label: str) -> tuple[float, float]:
    if risk_label == "Safe":
        return 0.0, 0.0
    if risk_label == "Moderate":
        return 2.0, 0.05
    if risk_label == "Risky":
        return 6.0, 0.12
    return 0.0, 0.0


def _degraded_parts(degraded_reason: str) -> list[str]:
    return [item.strip() for item in str(degraded_reason or "").split(";") if item.strip()]


def _provider_failure_set_from_degraded(degraded_reason: str) -> set[str]:
    output: set[str] = set()
    for item in _degraded_parts(degraded_reason):
        if not item.startswith("provider_failures:"):
            continue
        _, _, raw_failures = item.partition(":")
        output |= {part.strip().lower() for part in raw_failures.split(",") if part.strip()}
    return output


def apply_trade_policy(opportunity: Any) -> PolicyDecision:
    # Execution is setup-driven, not city-driven. City/history only adjusts thresholds
    # or enables explicit observation-only mode when configured.
    risk_label, risk_score = compute_risk_label(opportunity)
    city_key = str(getattr(opportunity, "city_key", "") or "").strip().upper()
    day_label = str(getattr(opportunity, "day_label", "") or "").strip().lower()
    bucket_label = str(getattr(opportunity, "bucket", "") or "")
    normalized_bucket = _canonical_bucket_key(bucket_label)
    confidence_tier = str(getattr(opportunity, "confidence_tier", "risky") or "risky").strip().lower()
    signal_tier = str(getattr(opportunity, "signal_tier", "C") or "C").strip().upper()
    edge = float(getattr(opportunity, "edge", 0.0) or 0.0)
    min_agreeing_model_edge = float(getattr(opportunity, "min_agreeing_model_edge", 0.0) or 0.0)
    consensus = float(getattr(opportunity, "consensus_score", 0.0) or 0.0)
    spread = float(getattr(opportunity, "spread", 0.0) or 0.0)
    price = float(getattr(opportunity, "price_cents", 0.0) or 0.0)
    coverage_ok = bool(getattr(opportunity, "coverage_ok", False))
    coverage_score = float(getattr(opportunity, "coverage_score", 0.0) or 0.0)
    degraded_reason = str(getattr(opportunity, "degraded_reason", "") or "").strip()
    executable_quality_score = float(getattr(opportunity, "executable_quality_score", 0.0) or 0.0)
    data_quality_score = float(getattr(opportunity, "data_quality_score", 0.0) or 0.0)
    coverage_issue_type = str(getattr(opportunity, "coverage_issue_type", "") or "").strip().lower()
    side = str(getattr(opportunity, "side", "") or "").strip().upper()
    valid_model_count = int(getattr(opportunity, "valid_model_count", 0) or 0)
    required_model_count = int(getattr(opportunity, "required_model_count", 0) or 0)
    agreement_models = int(getattr(opportunity, "agreement_models", 0) or 0)
    total_models = int(getattr(opportunity, "total_models", 0) or 0)
    agreement_pct = float(getattr(opportunity, "agreement_pct", 0.0) or 0.0)
    provider_failures = {str(item).strip().lower() for item in (getattr(opportunity, "provider_failures", None) or []) if str(item).strip()}
    policy_min_edge = float(os.getenv("WEATHER_POLICY_MIN_EDGE", "10"))
    policy_min_consensus = float(os.getenv("WEATHER_POLICY_MIN_CONSENSUS", "0.42"))
    policy_min_price = float(os.getenv("WEATHER_POLICY_MIN_PRICE_CENTS", os.getenv("WEATHER_MIN_PRICE_CENTS", "10")))
    policy_max_price = float(os.getenv("WEATHER_POLICY_MAX_PRICE_CENTS", os.getenv("WEATHER_MAX_PRICE_CENTS", "60")))
    effective_min_price, effective_max_price = effective_price_bounds(
        opportunity,
        min_price_cents=policy_min_price,
        max_price_cents=policy_max_price,
    )
    policy_max_spread = float(os.getenv("WEATHER_POLICY_MAX_SPREAD", os.getenv("WEATHER_MAX_MODEL_SPREAD", "4.0")))
    min_worst_case_edge = float(os.getenv("WEATHER_POLICY_MIN_WORST_CASE_EDGE", "4"))
    min_execution_quality = float(os.getenv("WEATHER_POLICY_MIN_EXECUTION_QUALITY", "0.2"))
    min_data_quality = float(os.getenv("WEATHER_POLICY_MIN_DATA_QUALITY", "0.3"))
    min_coverage_score = float(os.getenv("WEATHER_POLICY_MIN_COVERAGE_SCORE", "0.45"))
    enforce_live_enabled = str(os.getenv("WEATHER_POLICY_ENFORCE_LIVE_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}
    enforce_blocked_city_keys = str(os.getenv("WEATHER_POLICY_ENFORCE_BLOCKED_CITY_KEYS", "0")).strip().lower() in {"1", "true", "yes", "on"}
    blocked_city_keys = {item.upper() for item in (_env_csv_set("WEATHER_POLICY_BLOCKED_CITY_KEYS", "") | _profile_set("blocked_city_keys"))}
    caution_city_keys = {item.upper() for item in (_env_csv_set("WEATHER_POLICY_CAUTION_CITY_KEYS", "") | _profile_set("caution_city_keys"))}
    caution_buckets = {_canonical_bucket_key(item) for item in _env_csv_set(
        "WEATHER_POLICY_CAUTION_BUCKETS",
        "64-65°F,82-83°F,66°F or higher,84-85°F",
    )}
    caution_buckets |= {_canonical_bucket_key(item) for item in _profile_set("caution_buckets")}
    today_min_edge = float(os.getenv("WEATHER_POLICY_TODAY_MIN_EDGE", "18"))
    today_min_consensus = float(os.getenv("WEATHER_POLICY_TODAY_MIN_CONSENSUS", "0.50"))
    today_allowed_confidence = {
        item.strip().lower()
        for item in str(os.getenv("WEATHER_POLICY_TODAY_ALLOWED_CONFIDENCE", "safe,strong,lock,near-safe")).split(",")
        if item.strip()
    }
    caution_edge_penalty = float(os.getenv("WEATHER_POLICY_CAUTION_EDGE_PENALTY", "4"))
    caution_consensus_penalty = float(os.getenv("WEATHER_POLICY_CAUTION_CONSENSUS_PENALTY", "0.05"))
    exceptional_risky_override_enabled = str(
        os.getenv("WEATHER_POLICY_GENERAL_RISKY_OVERRIDE_ENABLED", "1")
    ).strip().lower() not in {"0", "false", "no"}
    exceptional_risky_override_confidence = {
        item.strip().lower()
        for item in str(os.getenv("WEATHER_POLICY_GENERAL_RISKY_OVERRIDE_CONFIDENCE", "safe,strong,lock")).split(",")
        if item.strip()
    }
    exceptional_risky_override_signal_tiers = {
        item.strip().upper()
        for item in str(os.getenv("WEATHER_POLICY_GENERAL_RISKY_OVERRIDE_SIGNAL_TIERS", "A+,A,B")).split(",")
        if item.strip()
    }
    exceptional_risky_override_min_agreement_pct = float(
        os.getenv("WEATHER_POLICY_GENERAL_RISKY_OVERRIDE_MIN_AGREEMENT_PCT", "80")
    )
    exceptional_risky_override_min_edge = float(
        os.getenv("WEATHER_POLICY_GENERAL_RISKY_OVERRIDE_MIN_EDGE", "20")
    )
    exceptional_risky_override_min_consensus = float(
        os.getenv("WEATHER_POLICY_GENERAL_RISKY_OVERRIDE_MIN_CONSENSUS", "0.55")
    )
    fallback_enabled = str(os.getenv("WEATHER_POLICY_ALLOW_FALLBACK_COVERAGE", "1")).strip().lower() not in {"0", "false", "no"}
    fallback_min_models = int(os.getenv("WEATHER_POLICY_FALLBACK_MIN_VALID_MODELS", "4") or 4)
    fallback_min_worst_case_edge = float(os.getenv("WEATHER_POLICY_FALLBACK_MIN_WORST_CASE_EDGE", "8") or 8)
    fallback_min_execution_quality = float(os.getenv("WEATHER_POLICY_FALLBACK_MIN_EXECUTION_QUALITY", "0.4") or 0.4)
    tolerated_failures = {
        item.strip().lower()
        for item in str(
            os.getenv(
                "WEATHER_POLICY_FALLBACK_TOLERATED_PROVIDER_FAILURES",
                "best_match,ecmwf,gem,gfs,icon,jma,ecmwf_ens,gfs_ens,icon_ens,nws,tomorrow,weatherapi,visualcrossing,openweather",
            )
        ).split(",")
        if item.strip()
    }
    tolerated_degraded_provider_failures = {
        item.strip().lower()
        for item in str(
            os.getenv(
                "WEATHER_POLICY_TOLERATED_DEGRADED_PROVIDER_FAILURES",
                "ecmwf_ens,gfs_ens,icon_ens,weatherbit,meteosource,meteostat,noaa_isd,brightsky",
            )
        ).split(",")
        if item.strip()
    }

    fallback_coverage_ok = (
        fallback_enabled
        and coverage_issue_type in {"provider_failure", "mixed", "rate_limited", "mixed_rate_limited"}
        and valid_model_count >= max(fallback_min_models, required_model_count or 0)
        and min_agreeing_model_edge >= fallback_min_worst_case_edge
        and executable_quality_score >= fallback_min_execution_quality
        and provider_failures.issubset(tolerated_failures)
    )
    degraded_failure_names = _provider_failure_set_from_degraded(degraded_reason)
    degraded_parts = _degraded_parts(degraded_reason)
    degraded_has_non_provider_failure = any(
        not item.startswith("provider_failures:")
        for item in degraded_parts
    )
    tolerated_provider_degradation = (
        bool(degraded_failure_names)
        and not degraded_has_non_provider_failure
        and degraded_failure_names.issubset(tolerated_failures | tolerated_degraded_provider_failures)
        and valid_model_count >= max(4, fallback_min_models)
        and coverage_score >= max(min_coverage_score, 0.58)
        and data_quality_score >= max(min_data_quality, 0.4)
        and executable_quality_score >= max(min_execution_quality, 0.35)
    )

    effective_coverage_ok = coverage_ok or coverage_score >= min_coverage_score or fallback_coverage_ok
    effective_agreement_pct = agreement_pct
    if effective_agreement_pct <= 0.0 and total_models > 0 and agreement_models > 0:
        effective_agreement_pct = (agreement_models / total_models) * 100.0
    exceptional_risky_override = (
        exceptional_risky_override_enabled
        and confidence_tier in exceptional_risky_override_confidence
        and signal_tier in exceptional_risky_override_signal_tiers
        and effective_agreement_pct >= exceptional_risky_override_min_agreement_pct
        and edge >= exceptional_risky_override_min_edge
        and min_agreeing_model_edge >= max(min_worst_case_edge, exceptional_risky_override_min_edge * 0.6)
        and consensus >= exceptional_risky_override_min_consensus
    )
    if not effective_coverage_ok:
        return PolicyDecision(False, "coverage_not_ok", risk_label, risk_score)
    city_cfg = CITY_CONFIG_BY_KEY.get(city_key)
    if enforce_live_enabled and city_cfg is not None and not bool(getattr(city_cfg, "live_enabled", True)):
        return PolicyDecision(False, "city_observation_only", risk_label, risk_score)
    if enforce_blocked_city_keys and city_key and city_key in blocked_city_keys:
        return PolicyDecision(False, "city_blocked_historical_underperformance", risk_label, risk_score)
    if (
        degraded_reason
        and degraded_reason != "degraded_clob_price"
        and not fallback_coverage_ok
        and not tolerated_provider_degradation
    ):
        return PolicyDecision(False, f"degraded:{degraded_reason}", risk_label, risk_score)
    effective_signal_tier = signal_tier
    if fallback_coverage_ok and signal_tier == "C":
        effective_signal_tier = "B"
    if confidence_tier == "risky":
        return PolicyDecision(False, "confidence_risky", risk_label, risk_score)
    if min_agreeing_model_edge < min_worst_case_edge:
        return PolicyDecision(False, "worst_case_edge_too_low", risk_label, risk_score)
    if executable_quality_score < min_execution_quality:
        return PolicyDecision(False, "execution_quality_too_low", risk_label, risk_score)
    if data_quality_score < min_data_quality and not fallback_coverage_ok and coverage_score < 0.7:
        return PolicyDecision(False, "data_quality_too_low", risk_label, risk_score)
    if price < effective_min_price:
        return PolicyDecision(False, "price_below_policy_min", risk_label, risk_score)
    if price > effective_max_price:
        return PolicyDecision(False, "price_above_policy_max", risk_label, risk_score)
    if spread > policy_max_spread:
        return PolicyDecision(False, "spread_above_policy_max", risk_label, risk_score)
    if day_label == "today":
        if confidence_tier not in today_allowed_confidence:
            return PolicyDecision(False, "today_requires_higher_confidence", risk_label, risk_score)
        if edge < today_min_edge:
            return PolicyDecision(False, "today_edge_too_low", risk_label, risk_score)
        if consensus < today_min_consensus:
            return PolicyDecision(False, "today_consensus_too_low", risk_label, risk_score)
    required_edge, required_consensus = _baseline_thresholds(confidence_tier, policy_min_edge, policy_min_consensus)
    if required_edge == float("inf"):
        return PolicyDecision(False, "confidence_not_allowed", risk_label, risk_score)
    signal_edge_adj, signal_consensus_adj = _signal_adjustments(effective_signal_tier)
    risk_edge_adj, risk_consensus_adj = _risk_adjustments(risk_label)
    required_edge += signal_edge_adj + risk_edge_adj
    required_consensus += signal_consensus_adj + risk_consensus_adj
    if city_key and city_key in caution_city_keys:
        required_edge += caution_edge_penalty
        required_consensus += caution_consensus_penalty
    if normalized_bucket and normalized_bucket in caution_buckets:
        required_edge += caution_edge_penalty
        required_consensus += caution_consensus_penalty
    if risk_label == "Risky" and not exceptional_risky_override:
        return PolicyDecision(False, "risk_label_risky", risk_label, risk_score)
    if edge < required_edge:
        if confidence_tier == "near-safe":
            return PolicyDecision(False, "near_safe_edge_too_low", risk_label, risk_score)
        if city_key in caution_city_keys or normalized_bucket in caution_buckets:
            return PolicyDecision(False, "historical_segment_edge_too_low", risk_label, risk_score)
        return PolicyDecision(False, "edge_below_policy_min", risk_label, risk_score)
    if consensus < required_consensus:
        if confidence_tier == "near-safe":
            return PolicyDecision(False, "near_safe_consensus_too_low", risk_label, risk_score)
        if city_key in caution_city_keys or normalized_bucket in caution_buckets:
            return PolicyDecision(False, "historical_segment_consensus_too_low", risk_label, risk_score)
        return PolicyDecision(False, "consensus_below_policy_min", risk_label, risk_score)
    return PolicyDecision(True, "allowed", risk_label, risk_score)
