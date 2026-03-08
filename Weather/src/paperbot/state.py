from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict

from .types import Position


@dataclass
class MarketState:
    equity: float = 1_000.0
    cash: float = 1_000.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    starting_equity: float = 1_000.0
    tick_count: int = 0
    day_losses: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)

    @property
    def total_exposure(self) -> float:
        return sum(abs(p.notional) for p in self.positions.values())

    @property
    def drawdown(self) -> float:
        if self.starting_equity <= 0:
            return 0.0
        return max(0.0, (self.starting_equity - (self.equity)) / self.starting_equity)
