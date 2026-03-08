from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict

from .state import MarketState
from .types import MarketSnapshot, Signal, Side


@dataclass
class FillResult:
    symbol: str
    side: Side
    size: float
    price: float
    ts: datetime
    pnl: float
    realized: bool = True


class PaperExecutionEngine:
    def __init__(self) -> None:
        self.last_prices: Dict[str, float] = {}

    def execute(self, state: MarketState, signal: Signal, tick: MarketSnapshot) -> FillResult:
        side = signal.side
        px = signal.price
        size = signal.size
        sym = signal.symbol
        pnl = 0.0

        pos = state.positions.get(sym, None)
        if pos is None:
            from .types import Position

            pos = Position(symbol=sym, qty=0.0, avg_entry=px)
            state.positions[sym] = pos

        if side == Side.BUY:
            if pos.qty < 0:
                close_qty = min(size, abs(pos.qty))
                pos.qty += close_qty
                pnl += close_qty * (pos.avg_entry - px)
                state.cash -= close_qty * px

                remaining = size - close_qty
                if remaining > 0:
                    pos.avg_entry = px
                    pos.qty = remaining
                    state.cash -= remaining * px
            elif pos.qty == 0:
                pos.avg_entry = px
                pos.qty = size
                state.cash -= size * px
            else:
                new_qty = pos.qty + size
                pos.avg_entry = (pos.avg_entry * pos.qty + size * px) / new_qty
                pos.qty = new_qty
                state.cash -= size * px
        else:
            if pos.qty > 0:
                close_qty = min(size, pos.qty)
                pos.qty -= close_qty
                pnl += close_qty * (px - pos.avg_entry)
                state.cash += close_qty * px

                remaining = size - close_qty
                if remaining > 0:
                    pos.avg_entry = px
                    pos.qty = -remaining
                    state.cash += remaining * px
            else:
                new_qty = pos.qty - size
                pos.avg_entry = (pos.avg_entry * abs(pos.qty) + size * px) / abs(new_qty) if new_qty else px
                pos.qty = new_qty
                state.cash += size * px

        if abs(pos.qty) < 1e-12:
            state.positions.pop(sym, None)

        state.realized_pnl += pnl
        state.day_losses = min(0.0, state.realized_pnl + state.unrealized_pnl)
        state.tick_count += 1
        return FillResult(symbol=sym, side=side, size=size, price=px, ts=tick.ts, pnl=pnl)
