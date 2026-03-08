from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class RiskConfig:
    max_risk_per_trade: float = 0.01
    max_daily_loss: float = 0.03
    max_exposure_per_symbol: float = 250.0
    max_exposure_total: float = 600.0
    stop_loss_pct: float = 0.08
    take_profit_pct: float = 0.12
    min_signal_score: float = 0.55


@dataclass
class StrategyConfig:
    window: int = 12
    threshold: float = 0.0025
    size_bps: float = 0.20
    min_score: float = 0.55


@dataclass
class RuntimeConfig:
    symbols: List[str]
    ticks: int = 300
    interval_seconds: float = 1.0
    risk: RiskConfig = field(default_factory=RiskConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    close_positions_on_end: bool = False
    feed_mode: str = "polymarket"
    polymarket_token_map: Dict[str, str] = field(default_factory=dict)
    polymarket_clob_base_url: str = "https://clob.polymarket.com"
    polymarket_gamma_base_url: str = "https://gamma-api.polymarket.com"
    polymarket_request_timeout: float = 4.0
