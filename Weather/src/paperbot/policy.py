from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any


@dataclass
class PolicyDecision:
    allowed: bool
    reason: str
    risk_label: str
    risk_score: float


def parse_bucket_bounds(label: str) -> tuple[float | None, float | None]:
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


def apply_trade_policy(opportunity: Any) -> PolicyDecision:
    risk_label, risk_score = compute_risk_label(opportunity)
    confidence_tier = str(getattr(opportunity, "confidence_tier", "risky") or "risky").strip().lower()
    signal_tier = str(getattr(opportunity, "signal_tier", "C") or "C").strip().upper()
    edge = float(getattr(opportunity, "edge", 0.0) or 0.0)
    min_agreeing_model_edge = float(getattr(opportunity, "min_agreeing_model_edge", 0.0) or 0.0)
    consensus = float(getattr(opportunity, "consensus_score", 0.0) or 0.0)
    spread = float(getattr(opportunity, "spread", 0.0) or 0.0)
    price = float(getattr(opportunity, "price_cents", 0.0) or 0.0)
    coverage_ok = bool(getattr(opportunity, "coverage_ok", False))
    degraded_reason = str(getattr(opportunity, "degraded_reason", "") or "").strip()
    executable_quality_score = float(getattr(opportunity, "executable_quality_score", 0.0) or 0.0)
    data_quality_score = float(getattr(opportunity, "data_quality_score", 0.0) or 0.0)
    allowed_signal_tiers = {
        item.strip().upper()
        for item in str(os.getenv("WEATHER_POLICY_ALLOWED_SIGNAL_TIERS", "A+,A")).split(",")
        if item.strip()
    }

    policy_min_edge = float(os.getenv("WEATHER_POLICY_MIN_EDGE", "18"))
    policy_min_consensus = float(os.getenv("WEATHER_POLICY_MIN_CONSENSUS", "0.55"))
    policy_min_price = float(os.getenv("WEATHER_POLICY_MIN_PRICE_CENTS", os.getenv("WEATHER_MIN_PRICE_CENTS", "10")))
    policy_max_price = float(os.getenv("WEATHER_POLICY_MAX_PRICE_CENTS", os.getenv("WEATHER_MAX_PRICE_CENTS", "55")))
    policy_max_spread = float(os.getenv("WEATHER_POLICY_MAX_SPREAD", os.getenv("WEATHER_MAX_MODEL_SPREAD", "3.0")))
    near_safe_min_edge = float(os.getenv("WEATHER_POLICY_NEAR_SAFE_MIN_EDGE", "25"))
    near_safe_min_consensus = float(os.getenv("WEATHER_POLICY_NEAR_SAFE_MIN_CONSENSUS", "0.65"))
    min_worst_case_edge = float(os.getenv("WEATHER_POLICY_MIN_WORST_CASE_EDGE", "10"))
    min_execution_quality = float(os.getenv("WEATHER_POLICY_MIN_EXECUTION_QUALITY", "0.6"))
    min_data_quality = float(os.getenv("WEATHER_POLICY_MIN_DATA_QUALITY", "0.55"))

    if not coverage_ok:
        return PolicyDecision(False, "coverage_not_ok", risk_label, risk_score)
    if degraded_reason:
        return PolicyDecision(False, f"degraded:{degraded_reason}", risk_label, risk_score)
    if signal_tier not in allowed_signal_tiers:
        return PolicyDecision(False, "signal_tier_not_actionable", risk_label, risk_score)
    if confidence_tier == "risky":
        return PolicyDecision(False, "confidence_risky", risk_label, risk_score)
    if risk_label == "Risky":
        return PolicyDecision(False, "risk_label_risky", risk_label, risk_score)
    if risk_label != "Safe":
        return PolicyDecision(False, "risk_label_not_safe", risk_label, risk_score)
    if min_agreeing_model_edge < min_worst_case_edge:
        return PolicyDecision(False, "worst_case_edge_too_low", risk_label, risk_score)
    if executable_quality_score < min_execution_quality:
        return PolicyDecision(False, "execution_quality_too_low", risk_label, risk_score)
    if data_quality_score < min_data_quality:
        return PolicyDecision(False, "data_quality_too_low", risk_label, risk_score)
    if price < policy_min_price:
        return PolicyDecision(False, "price_below_policy_min", risk_label, risk_score)
    if price > policy_max_price:
        return PolicyDecision(False, "price_above_policy_max", risk_label, risk_score)
    if spread > policy_max_spread:
        return PolicyDecision(False, "spread_above_policy_max", risk_label, risk_score)
    if confidence_tier in {"lock", "strong", "safe"}:
        if edge < policy_min_edge:
            return PolicyDecision(False, "edge_below_policy_min", risk_label, risk_score)
        if consensus < policy_min_consensus:
            return PolicyDecision(False, "consensus_below_policy_min", risk_label, risk_score)
        return PolicyDecision(True, "allowed", risk_label, risk_score)
    if confidence_tier == "near-safe":
        if edge < near_safe_min_edge:
            return PolicyDecision(False, "near_safe_edge_too_low", risk_label, risk_score)
        if consensus < near_safe_min_consensus:
            return PolicyDecision(False, "near_safe_consensus_too_low", risk_label, risk_score)
        if risk_label != "Safe":
            return PolicyDecision(False, "near_safe_requires_safe_risk", risk_label, risk_score)
        return PolicyDecision(True, "allowed", risk_label, risk_score)
    return PolicyDecision(False, "confidence_not_allowed", risk_label, risk_score)
