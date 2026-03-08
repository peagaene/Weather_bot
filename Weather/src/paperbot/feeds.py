from __future__ import annotations

import math
import json
import random
import urllib.error
import urllib.request
import re
from urllib import parse
from collections import deque
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, Iterator, Mapping

from .types import MarketSnapshot


class MarketFeed:
    def next_tick(self, symbol: str | None = None) -> MarketSnapshot | None:
        raise NotImplementedError


@dataclass
class SyntheticState:
    symbol: str
    price: float
    volatility: float = 0.004


class SyntheticFeed(MarketFeed):
    def __init__(
        self,
        symbols: Iterable[str],
        start_time: datetime | None = None,
        base_prices: Dict[str, float] | None = None,
        volatility: Dict[str, float] | None = None,
        seed: int = 7,
        spread_bps: float = 7.0,
    ) -> None:
        self.rng = random.Random(seed)
        self.symbols = list(symbols)
        self.state: Dict[str, SyntheticState] = {}
        self.spread_bps = spread_bps
        base_prices = base_prices or {}
        volatility = volatility or {}

        for symbol in self.symbols:
            self.state[symbol] = SyntheticState(
                symbol=symbol,
                price=float(base_prices.get(symbol, 100.0)),
                volatility=float(volatility.get(symbol, 0.004)),
            )

        self._ts = start_time or datetime.utcnow()
        self._tick = 0
        self._history: Dict[str, Deque[float]] = {s: deque(maxlen=128) for s in self.symbols}

    def next_tick(self, symbol: str | None = None) -> MarketSnapshot:
        if symbol is None:
            symbol = self.rng.choice(self.symbols)

        state = self.state[symbol]
        drift = self.rng.uniform(-0.0004, 0.0006)
        shock = self.rng.gauss(0.0, state.volatility)
        state.price = max(0.01, state.price * (1 + drift + shock))

        spread = max(0.001, state.price * (self.spread_bps / 10000))
        bid = round(state.price - spread / 2, 6)
        ask = round(state.price + spread / 2, 6)
        self._tick += 1
        self._ts = self._ts + timedelta(seconds=1)

        self._history[symbol].append(state.price)
        return MarketSnapshot(
            ts=self._ts,
            symbol=symbol,
            price=state.price,
            bid=bid,
            ask=ask,
            volume=math.fabs(shock) * 1_000 + 1.0,
        )

    def iter_ticks(self, ticks: int) -> Iterator[MarketSnapshot]:
        for _ in range(ticks):
            for symbol in self.symbols:
                yield self.next_tick(symbol)


class PolymarketAPIError(RuntimeError):
    pass


@dataclass
class _PolymarketState:
    symbol: str
    token_id: str | None
    last_bid: float | None = None
    last_ask: float | None = None
    last_price: float | None = None


class PolymarketFeed(MarketFeed):
    _TOKEN_ID_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

    def _normalize_token_id(self, token_id: str | None) -> str | None:
        if not token_id:
            return None
        token = token_id.strip()
        if not token:
            return None
        if token.lower().endswith("...") or "..." in token:
            return None
        if self._TOKEN_ID_RE.match(token):
            return token
        return None

    def __init__(
        self,
        symbols: Iterable[str],
        symbol_to_token_id: Mapping[str, str] | None = None,
        *,
        clob_base_url: str = "https://clob.polymarket.com",
        gamma_base_url: str = "https://gamma-api.polymarket.com",
        request_timeout: float = 4.0,
        user_agent: str = "paper-bot/1.0",
        spread_bps_fallback: float = 3.0,
    ) -> None:
        self.symbols = list(symbols)
        self.clob_base_url = clob_base_url.rstrip("/")
        self.gamma_base_url = gamma_base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.user_agent = user_agent
        self.spread_bps_fallback = spread_bps_fallback
        self._ts = datetime.utcnow()

        token_map = {
            symbol.strip(): token_id.strip()
            for symbol, token_id in (symbol_to_token_id or {}).items()
            if symbol.strip() and token_id and token_id.strip()
        }
        self._symbol_state: Dict[str, _PolymarketState] = {}
        for symbol in self.symbols:
            token_candidate = self._normalize_token_id(token_map.get(symbol))
            self._symbol_state[symbol] = _PolymarketState(
                symbol=symbol,
                token_id=token_candidate,
            )
            if not self._symbol_state[symbol].token_id:
                self._symbol_state[symbol].token_id = self._resolve_token_id(symbol)

    def _request_json(self, url: str, params: dict[str, str] | None = None) -> dict:
        if params:
            url = f"{url}?{parse.urlencode(params)}"
        req = urllib.request.Request(
            url=url,
            headers={"User-Agent": self.user_agent, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout) as response:
                payload = response.read().decode("utf-8")
                raw = json.loads(payload)
                if isinstance(raw, list):
                    return {"data": raw}
                if isinstance(raw, dict):
                    return raw
                return {}
        except urllib.error.HTTPError as err:
            raise PolymarketAPIError(f"HTTP {err.code} for {url}") from err
        except urllib.error.URLError as err:
            raise PolymarketAPIError(f"Network error fetching {url}: {err}") from err
        except json.JSONDecodeError as err:
            raise PolymarketAPIError(f"Invalid JSON from {url}") from err

    def _pick_first_list(self, value: object, fallback: str | None = None) -> str | None:
        if isinstance(value, str):
            return value.strip() or fallback
        if isinstance(value, (list, tuple)):
            for item in value:
                token = self._pick_first_list(item, fallback=None)
                if token:
                    return token
        return fallback

    def _resolve_token_id(self, symbol: str) -> str:
        normalized_candidates: list[str] = [symbol.strip(), symbol.strip().lower(), symbol.strip().upper()]
        compact = re.sub(r"\s+", "", symbol.strip())
        compact_no_sep = re.sub(r"[-_]", "", compact)
        for item in (compact, compact_no_sep):
            if item and item not in normalized_candidates:
                normalized_candidates.append(item)
                normalized_candidates.append(item.lower())
                normalized_candidates.append(item.upper())

        # common aliases with timeframe suffixes (ex: BTC5m, BTC_5m, BTC-15m)
        base_symbol = re.sub(r"\d+[a-zA-Z]+$", "", compact).strip()
        for item in (base_symbol, base_symbol.lower(), base_symbol.upper(), re.sub(r"[-_]", "", base_symbol)):
            if item and item not in normalized_candidates:
                normalized_candidates.append(item)
                normalized_candidates.append(item.lower())
                normalized_candidates.append(item.upper())

        market_fields = (
            ("clobTokenIds",),
            ("clob_token_ids",),
            ("tokenId",),
            ("token_id",),
        )
        candidate_urls = [
            (f"{self.gamma_base_url}/markets", {"slug": symbol}),
            (f"{self.gamma_base_url}/markets", {"query": symbol}),
            (f"{self.gamma_base_url}/markets", {"search": symbol}),
        ]
        last_error: Exception | None = None

        for base_url, params in candidate_urls:
            try:
                payload = self._request_json(base_url, params=params)
                markets = payload.get("data") or payload.get("markets") or payload.get("results")
                if not isinstance(markets, list):
                    continue

                # Match first by exact symbol/slugs, then by phrase.
                direct = None
                for market in markets:
                    if not isinstance(market, dict):
                        continue
                    token_id = None
                    for path in market_fields:
                        token_id = self._pick_first_list(self._nested_get(market, path), token_id)
                    if not token_id:
                        continue

                    slug = str(market.get("slug", "")).lower()
                    slug_l = slug.lower()
                    for candidate in normalized_candidates:
                        candidate_l = candidate.lower()
                        if slug_l == candidate_l:
                            direct = token_id
                            break
                        if candidate_l in slug_l:
                            direct = token_id
                            break
                        if slug_l in candidate_l:
                            direct = token_id
                            break
                    if direct:
                        break

                    question = str(market.get("question", "")).lower()
                    for candidate in normalized_candidates:
                        candidate_l = candidate.lower()
                        if candidate_l in question or question in candidate_l:
                            direct = token_id
                            break
                    if direct:
                        break
                if direct:
                    return direct
            except PolymarketAPIError as err:
                last_error = err
                continue

        if last_error:
            raise PolymarketAPIError(
                f"unable to resolve token id for symbol '{symbol}' from Polymarket API"
            ) from last_error
        raise PolymarketAPIError(f"unable to resolve token id for symbol '{symbol}' from Polymarket API")

    def _nested_get(self, data: dict, path: tuple[str, ...]) -> object:
        current: object = data
        for key in path:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
            if current is None:
                return None
        return current

    def _extract_book(self, token_id: str) -> dict:
        candidate_urls = [
            (f"{self.clob_base_url}/book", {"token_id": token_id}),
            (f"{self.clob_base_url}/orderbook", {"token_id": token_id}),
            (f"{self.clob_base_url}/book/{token_id}", None),
            (f"{self.clob_base_url}/orderbook/{token_id}", None),
        ]
        last_error: Exception | None = None
        for url, params in candidate_urls:
            try:
                payload = self._request_json(url, params=params)
            except PolymarketAPIError as err:
                last_error = err
                continue

            if not isinstance(payload, dict):
                continue
            if "data" in payload and isinstance(payload["data"], dict):
                payload = payload["data"]
            return payload

        if last_error:
            raise PolymarketAPIError(f"unable to fetch orderbook for token_id {token_id}") from last_error
        raise PolymarketAPIError(f"unable to fetch orderbook for token_id {token_id}")

    def _best_price(self, levels: object, side: str) -> float | None:
        if not isinstance(levels, list) or not levels:
            return None
        prices = []
        for level in levels:
            if not isinstance(level, dict):
                continue
            for field in ("price", "p", "price_with_precision", "priceWithPrecision"):
                if field in level:
                    value = level.get(field) or level.get("price")
                    try:
                        prices.append(float(value))
                    except (TypeError, ValueError):
                        pass
                    break
        if not prices:
            return None
        return max(prices) if side == "bid" else min(prices)

    def _to_book_volume(self, payload: dict) -> float:
        bids = payload.get("bids", [])
        asks = payload.get("asks", [])
        if isinstance(payload.get("volume"), (int, float)):
            return float(payload["volume"])
        total = 0.0
        for level in list(bids) + list(asks):
            if not isinstance(level, dict):
                continue
            if "size" in level:
                try:
                    total += float(level["size"])
                except (TypeError, ValueError):
                    continue
        return total

    def next_tick(self, symbol: str | None = None) -> MarketSnapshot:
        if symbol is None:
            raise ValueError("PolymarketFeed requires explicit symbol per tick")

        if symbol not in self._symbol_state:
            raise PolymarketAPIError(f"symbol '{symbol}' not configured for PolymarketFeed")
        state = self._symbol_state[symbol]

        if not state.token_id:
            raise PolymarketAPIError(f"symbol '{symbol}' without resolved token id")

        try:
            book = self._extract_book(state.token_id)
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            bid = self._best_price(bids, "bid")
            ask = self._best_price(asks, "ask")
            volume = self._to_book_volume(book)

            if bid is None and ask is None:
                raise PolymarketAPIError(f"orderbook without valid bid/ask for token_id {state.token_id}")

            if bid is None:
                bid = ask
            if ask is None:
                ask = bid

            spread = max(0.000001, ((ask - bid) if (ask is not None and bid is not None) else 0.0))
            fallback_spread = state.last_price if state.last_price is not None else 0.0
            if spread <= 0:
                spread = max(0.0001, fallback_spread * (self.spread_bps_fallback / 10000.0))
                bid = (bid or 0.0) - spread / 2
                ask = (ask or 0.0) + spread / 2
            price = float((bid + ask) / 2)

            state.last_bid = float(bid)
            state.last_ask = float(ask)
            state.last_price = float(price)
            self._ts = self._ts + timedelta(seconds=1)
            return MarketSnapshot(
                ts=self._ts,
                symbol=symbol,
                price=price,
                bid=float(bid),
                ask=float(ask),
                volume=float(volume) if volume else 1.0,
            )
        except Exception as error:
            if state.last_price is not None and state.last_bid is not None and state.last_ask is not None:
                self._ts = self._ts + timedelta(seconds=1)
                return MarketSnapshot(
                    ts=self._ts,
                    symbol=symbol,
                    price=float(state.last_price),
                    bid=float(state.last_bid),
                    ask=float(state.last_ask),
                    volume=0.0,
                )
            raise PolymarketAPIError(f"failed to fetch tick for symbol '{symbol}': {error}") from error

    def iter_ticks(self, ticks: int) -> Iterator[MarketSnapshot]:
        for _ in range(ticks):
            for symbol in self.symbols:
                yield self.next_tick(symbol)
