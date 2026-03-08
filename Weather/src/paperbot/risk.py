from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .config import RiskConfig
from .state import MarketState
from .types import Signal


@dataclass
class RiskDecision:
    ok: bool
    reason: str = ""


class RiskEngine:
    def __init__(self, config: RiskConfig):
        self.config = config

    def evaluate(self, state: MarketState, signal: Signal, estimated_price: float) -> RiskDecision:
        if signal.score < self.config.min_signal_score:
            return RiskDecision(False, "score below min")

        notional = signal.size * estimated_price
        if notional > state.equity * self.config.max_risk_per_trade:
            return RiskDecision(False, f"trade too large {notional:.2f}")

        if state.day_losses <= -state.starting_equity * self.config.max_daily_loss:
            return RiskDecision(False, "daily loss limit reached")

        current_exposure = state.total_exposure
        if notional + current_exposure > self.config.max_exposure_total:
            return RiskDecision(False, f"exposure cap: {notional + current_exposure:.2f} > {self.config.max_exposure_total:.2f}")

        if notional > self.config.max_exposure_per_symbol:
            return RiskDecision(False, f"symbol exposure cap: {notional:.2f}")

        return RiskDecision(True, "ok")

