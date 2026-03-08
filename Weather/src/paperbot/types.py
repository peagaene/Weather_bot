from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class MarketSnapshot:
    ts: datetime
    symbol: str
    price: float
    bid: float
    ask: float
    volume: float = 0.0


@dataclass
class Signal:
    symbol: str
    side: Side
    size: float
    score: float
    reason: str
    price: float


@dataclass
class Position:
    symbol: str
    qty: float = 0.0
    avg_entry: float = 0.0
    realized_pnl: float = 0.0

    @property
    def notional(self) -> float:
        return abs(self.qty * self.avg_entry)
