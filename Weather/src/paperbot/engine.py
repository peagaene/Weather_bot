from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime

from .config import RuntimeConfig
from .execution import PaperExecutionEngine
from .feeds import MarketFeed
from .risk import RiskEngine
from .state import MarketState
from .strategy import MomentumStrategy
from .types import MarketSnapshot, Signal, Side


@dataclass
class RunResult:
    total_ticks: int
    final_equity: float
    realized_pnl: float
    unrealized_pnl: float
    total_exposure: float
    actions: List[dict]
    history: List[dict]


class TradingEngine:
    def __init__(
        self,
        config: RuntimeConfig,
        feed: MarketFeed,
        strategy: MomentumStrategy,
        risk: RiskEngine,
        execution: PaperExecutionEngine,
        state: MarketState | None = None,
    ) -> None:
        self.config = config
        self.feed = feed
        self.strategy = strategy
        self.risk = risk
        self.execution = execution
        self.state = state or MarketState()

    def update_unrealized(self, last_prices: Dict[str, float]) -> float:
        unrealized = 0.0
        for symbol, pos in self.state.positions.items():
            px = last_prices.get(symbol)
            if px is None:
                continue
            unrealized += pos.qty * (px - pos.avg_entry)
        self.state.unrealized_pnl = unrealized
        self.state.equity = self.state.cash + unrealized + self.state.realized_pnl
        return unrealized

    def run(self, ticks: int, close_positions: bool = False) -> RunResult:
        actions: List[dict] = []
        last_prices: Dict[str, float] = {}
        history: List[dict] = []
        step = 0
        last_tick_ts: datetime | None = None

        for i in range(ticks):
            for symbol in self.config.symbols:
                step += 1
                tick: MarketSnapshot = self.feed.next_tick(symbol)
                last_tick_ts = tick.ts
                last_prices[symbol] = tick.price
                self.update_unrealized(last_prices)

                position = self.state.positions.get(symbol)
                size = position.qty if position else 0.0
                signal = self.strategy.on_tick(tick, self.state.equity, size)
                action = "skip"
                action_data = {
                    "step": step,
                    "tick": i + 1,
                    "ts": tick.ts.isoformat(),
                    "symbol": symbol,
                    "price": round(tick.price, 6),
                    "action": action,
                    "reason": "",
                    "score": None,
                    "qty": round(size, 6),
                    "equity": round(self.state.equity, 4),
                    "cash": round(self.state.cash, 4),
                    "realized_pnl": round(self.state.realized_pnl, 4),
                    "unrealized_pnl": round(self.state.unrealized_pnl, 4),
                    "total_exposure": round(self.state.total_exposure, 4),
                }

                forced_signal = self._build_risk_exit_signal(tick)
                if forced_signal:
                    fill = self.execution.execute(self.state, forced_signal, tick)
                    self.update_unrealized(last_prices)

                    action_data.update(
                        {
                            "action": fill.side.value,
                            "reason": forced_signal.reason,
                            "score": round(forced_signal.score, 4),
                            "signal": forced_signal.side.value,
                            "signal_size": round(forced_signal.size, 6),
                            "fill_size": round(fill.size, 6),
                            "fill_price": round(fill.price, 6),
                            "fill_pnl": round(fill.pnl, 6),
                            "equity": round(self.state.equity, 4),
                            "cash": round(self.state.cash, 4),
                            "realized_pnl": round(self.state.realized_pnl, 4),
                            "unrealized_pnl": round(self.state.unrealized_pnl, 4),
                            "total_exposure": round(self.state.total_exposure, 4),
                        }
                    )
                    actions.append(
                        {
                            "ts": fill.ts.isoformat(),
                            "symbol": fill.symbol,
                            "action": fill.side.value,
                            "size": fill.size,
                            "price": fill.price,
                            "pnl": round(fill.pnl, 4),
                            "reason": forced_signal.reason,
                        }
                    )
                    history.append(action_data)
                    continue

                if signal is None:
                    history.append(action_data)
                    continue

                decision = self.risk.evaluate(self.state, signal, signal.price)
                if not decision.ok:
                    action = "reject"
                    action_data.update(
                        {
                            "action": action,
                            "reason": decision.reason,
                            "score": round(signal.score, 4),
                            "signal": signal.side.value,
                            "signal_size": round(signal.size, 6),
                        }
                    )
                    actions.append(
                        {
                            "ts": tick.ts.isoformat(),
                            "symbol": symbol,
                            "action": "reject",
                            "reason": decision.reason,
                            "score": round(signal.score, 4),
                        }
                    )
                    history.append(action_data)
                    continue

                fill = self.execution.execute(self.state, signal, tick)
                self.update_unrealized(last_prices)
                action = fill.side.value
                action_data.update(
                    {
                        "action": action,
                        "reason": "",
                        "score": round(signal.score, 4),
                        "signal": signal.side.value,
                        "signal_size": round(signal.size, 6),
                        "fill_size": round(fill.size, 6),
                        "fill_price": round(fill.price, 6),
                        "fill_pnl": round(fill.pnl, 6),
                    }
                )
                actions.append(
                    {
                        "ts": fill.ts.isoformat(),
                        "symbol": fill.symbol,
                        "action": fill.side.value,
                        "size": fill.size,
                        "price": fill.price,
                        "pnl": round(fill.pnl, 4),
                    }
                )
                history.append(
                    {
                        **action_data,
                        "equity": round(self.state.equity, 4),
                        "cash": round(self.state.cash, 4),
                        "realized_pnl": round(self.state.realized_pnl, 4),
                        "unrealized_pnl": round(self.state.unrealized_pnl, 4),
                        "total_exposure": round(self.state.total_exposure, 4),
                    }
                )

                if self.state.drawdown > self.config.risk.max_daily_loss:
                    actions.append(
                        {
                            "ts": tick.ts.isoformat(),
                            "symbol": symbol,
                            "action": "kill_switch",
                            "reason": "max drawdown reached",
                        }
                    )
                    return self._finalize(step, last_prices, actions, history, close_positions, last_tick_ts)

        return self._finalize(step, last_prices, actions, history, close_positions, last_tick_ts)

    def _build_risk_exit_signal(self, tick: MarketSnapshot) -> Signal | None:
        pos = self.state.positions.get(tick.symbol)
        if pos is None or pos.qty == 0:
            return None

        stop_loss = self.config.risk.stop_loss_pct
        take_profit = self.config.risk.take_profit_pct

        if pos.qty > 0:
            is_stop = tick.price <= pos.avg_entry * (1 - stop_loss)
            is_take = tick.price >= pos.avg_entry * (1 + take_profit)
            if is_stop or is_take:
                return Signal(
                    symbol=tick.symbol,
                    side=Side.SELL,
                    size=round(abs(pos.qty), 6),
                    score=1.0,
                    reason="risk_stop_loss" if is_stop else "risk_take_profit",
                    price=tick.bid,
                )
        elif pos.qty < 0:
            is_stop = tick.price >= pos.avg_entry * (1 + stop_loss)
            is_take = tick.price <= pos.avg_entry * (1 - take_profit)
            if is_stop or is_take:
                return Signal(
                    symbol=tick.symbol,
                    side=Side.BUY,
                    size=round(abs(pos.qty), 6),
                    score=1.0,
                    reason="risk_stop_loss" if is_stop else "risk_take_profit",
                    price=tick.ask,
                )

        return None

    def _finalize(
        self,
        total_ticks: int,
        last_prices: Dict[str, float],
        actions: List[dict],
        history: List[dict],
        close_positions: bool,
        last_tick_ts: datetime | None,
    ) -> RunResult:
        if close_positions and self.state.positions and last_tick_ts is not None:
            self._close_all_positions(total_ticks, last_prices, last_tick_ts, actions, history)
        self.update_unrealized(last_prices)
        return RunResult(
            total_ticks=total_ticks,
            final_equity=round(self.state.equity, 4),
            realized_pnl=round(self.state.realized_pnl, 4),
            unrealized_pnl=round(self.state.unrealized_pnl, 4),
            total_exposure=round(self.state.total_exposure, 4),
            actions=actions,
            history=history,
        )

    def _close_all_positions(
        self,
        total_ticks: int,
        last_prices: Dict[str, float],
        ts: datetime,
        actions: List[dict],
        history: List[dict],
    ) -> None:
        for symbol, pos in list(self.state.positions.items()):
            if pos.qty == 0:
                self.state.positions.pop(symbol, None)
                continue

            price = float(last_prices.get(symbol, pos.avg_entry))
            side = Side.BUY if pos.qty < 0 else Side.SELL
            signal = Signal(
                symbol=symbol,
                side=side,
                size=round(abs(pos.qty), 6),
                score=1.0,
                reason="end_of_simulation_close",
                price=price,
            )
            fill = self.execution.execute(self.state, signal, MarketSnapshot(ts=ts, symbol=symbol, price=price, bid=price, ask=price))
            self.update_unrealized(last_prices)

            actions.append(
                {
                    "ts": fill.ts.isoformat(),
                    "symbol": fill.symbol,
                    "action": fill.side.value,
                    "size": fill.size,
                    "price": fill.price,
                    "pnl": round(fill.pnl, 4),
                    "reason": "end_of_simulation_close",
                }
            )

            history.append(
                {
                    "step": total_ticks + 1,
                    "tick": total_ticks,
                    "ts": fill.ts.isoformat(),
                    "symbol": symbol,
                    "price": round(price, 6),
                    "action": fill.side.value,
                    "reason": "end_of_simulation_close",
                    "score": 1.0,
                    "signal": fill.side.value,
                    "signal_size": round(abs(pos.qty), 6),
                    "fill_size": round(fill.size, 6),
                    "fill_price": round(fill.price, 6),
                    "fill_pnl": round(fill.pnl, 6),
                    "equity": round(self.state.equity, 4),
                    "cash": round(self.state.cash, 4),
                    "realized_pnl": round(self.state.realized_pnl, 4),
                    "unrealized_pnl": round(self.state.unrealized_pnl, 4),
                    "total_exposure": round(self.state.total_exposure, 4),
                }
            )

            # execution engine already removes flat positions.
            # keep state clean for safety in next loop.
            self.state.positions.pop(symbol, None)
