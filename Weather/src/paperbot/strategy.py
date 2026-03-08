from __future__ import annotations

from collections import defaultdict, deque
from datetime import timedelta
from typing import Dict, Deque, List, Optional

from .config import StrategyConfig
from .types import MarketSnapshot, Signal, Side


class MomentumStrategy:
    def __init__(self, config: StrategyConfig) -> None:
        self.config = config
        self.history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=config.window * 2))

    def on_tick(self, tick: MarketSnapshot, bankroll: float, position_size: float) -> Optional[Signal]:
        prices: Deque[float] = self.history[tick.symbol]
        prices.append(tick.price)

        if len(prices) < self.config.window + 1:
            return None

        short = list(prices)[-self.config.window :]
        short_return = (short[-1] - short[0]) / short[0]
        volatility = max(1e-8, self._volatility(list(prices), period=8))
        score = min(1.0, abs(short_return) / (volatility * 1.8))
        if score < self.config.threshold:
            return None

        if score < self.config.min_score:
            return None

        size = max(0.05, bankroll * self.config.size_bps / 100)

        if short_return > self.config.threshold:
            side = Side.BUY
            reason = f"momentum_up_{short_return:.5f}"
        elif short_return < -self.config.threshold:
            side = Side.SELL
            reason = f"momentum_down_{short_return:.5f}"
        else:
            return None

        if position_size * side_to_sign(side) >= 0 and abs(position_size) > 0 and short_return * side_to_sign(side) > 0:
            size *= 0.55

        size = min(size, bankroll * 0.25)
        if size <= 0:
            return None

        return Signal(
            symbol=tick.symbol,
            side=side,
            size=round(size, 5),
            score=min(1.0, score),
            reason=reason,
            price=tick.ask if side == Side.BUY else tick.bid,
        )

    def _volatility(self, values: List[float], period: int = 8) -> float:
        if len(values) < period + 1:
            return 0.0
        sample = list(values)[-period - 1 :]
        returns = []
        for i in range(1, len(sample)):
            returns.append((sample[i] - sample[i - 1]) / max(1e-12, sample[i - 1]))
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        return max(0.0001, var ** 0.5)


def side_to_sign(side: Side) -> int:
    return 1 if side == Side.BUY else -1
