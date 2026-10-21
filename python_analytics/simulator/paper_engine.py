"""Local paper trading engine and asynchronous market-data feed.

The paper engine owns a C++ ``L2Book`` for market data and implements matching,
inventory and PnL accounting in pure Python. It exposes the subset of the native
``TradingEngine`` surface that ``AvellanedaStoikovStrategy`` relies on
(``bbo``, ``position``, ``risk_limits``, ``submit``, ``cancel``), so the same
strategy object can quote against the paper engine without modification.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from ..backtesting.metrics import max_drawdown, sharpe_ratio
from ..native import hft_native

logger = logging.getLogger(__name__)

BUY = "BUY"
SELL = "SELL"


def default_risk_limits() -> "hft_native.RiskLimits":
    """Wide risk limits so the paper engine does not clip strategy quotes."""
    limits = hft_native.RiskLimits()
    limits.max_order_notional = hft_native.Notional.from_string("1000000000")
    limits.max_order_quantity = hft_native.Quantity.from_string("100000")
    limits.max_position_notional = hft_native.Notional.from_string("1000000000")
    limits.max_open_orders = 1024
    return limits


def parse_depth_message(text: str) -> Optional["hft_native.DepthUpdate"]:
    """Parses a native ``depthUpdate`` diff or Binance partial-book message."""
    try:
        return hft_native.parse_depth_json(text)
    except RuntimeError:
        pass

    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict) or ("bids" not in data and "asks" not in data):
        return None

    bids = [(str(level[0]), str(level[1])) for level in data.get("bids", [])]
    asks = [(str(level[0]), str(level[1])) for level in data.get("asks", [])]
    exchange_ts = int(data.get("E", 0) or 0)
    final_id = int(data.get("lastUpdateId", 0) or data.get("u", 0) or 0)
    first_id = int(data.get("U", 0) or 0)
    return hft_native.DepthUpdate(
        bids=bids,
        asks=asks,
        exchange_ts=exchange_ts,
        first_update_id=first_id,
        final_update_id=final_id,
    )


def apply_depth_update(book: "hft_native.L2Book",
                       update: "hft_native.DepthUpdate") -> None:
    """Applies a parsed depth update to the C++ ``L2Book``."""
    for price_text, quantity_text in update.bids:
        try:
            price = hft_native.Price.from_string(price_text)
            quantity = hft_native.Quantity.from_string(quantity_text)
        except ValueError:
            continue
        book.apply_level(hft_native.Side.BUY, price, quantity, update.exchange_ts)
    for price_text, quantity_text in update.asks:
        try:
            price = hft_native.Price.from_string(price_text)
            quantity = hft_native.Quantity.from_string(quantity_text)
        except ValueError:
            continue
        book.apply_level(hft_native.Side.SELL, price, quantity, update.exchange_ts)
    book.set_sequence(update.final_update_id, update.exchange_ts, update.recv_ts)


def synthetic_depth_messages(mid: float = 30000.0, tick: float = 0.5,
                             spread_ticks: int = 2,
                             seed: Optional[int] = None) -> Iterable[str]:
    """Deterministic random-walk depth JSON generator for offline mock runs.

    Each tick emits an absolute top-of-book replacement: new levels plus a
    zero-quantity entry for any previous level that has moved away, keeping the
    C++ ladder coherent instead of accumulating stale, crossed levels.
    """
    rng = random.Random(seed)
    price = mid
    sequence = 1
    prev_bid_levels: List[List[str]] = []
    prev_ask_levels: List[List[str]] = []
    # Occasionally jump several ticks so the market trades through our resting
    # quotes, producing realistic adverse-selection fills.
    steps = (-3, -2, -1, 0, 1, 2, 3)
    while True:
        price += rng.choice(steps) * tick
        if price <= tick:
            price = tick
        bid = round(price - spread_ticks * tick, 8)
        ask = round(price + spread_ticks * tick, 8)
        bid_levels = [[f"{bid:.8f}", "1.5"], [f"{bid - tick:.8f}", "2.0"]]
        ask_levels = [[f"{ask:.8f}", "1.5"], [f"{ask + tick:.8f}", "2.0"]]

        bids = list(bid_levels)
        asks = list(ask_levels)
        new_bid_prices = {level[0] for level in bid_levels}
        new_ask_prices = {level[0] for level in ask_levels}
        for old_price, _ in prev_bid_levels:
            if old_price not in new_bid_prices:
                bids.append([old_price, "0"])
        for old_price, _ in prev_ask_levels:
            if old_price not in new_ask_prices:
                asks.append([old_price, "0"])

        message = {
            "e": "depthUpdate",
            "E": int(time.time() * 1000),
            "U": sequence,
            "u": sequence,
            "s": "BTCUSDT",
            "b": bids,
            "a": asks,
        }
        sequence += 1
        prev_bid_levels = bid_levels
        prev_ask_levels = ask_levels
        yield json.dumps(message)



@dataclass
class PaperOrder:
    order_id: int
    side: str
    price: float
    quantity: float
    remaining: float
    is_maker: bool = True


@dataclass
class PaperFill:
    order_id: int
    side: str
    price: float
    quantity: float
    fee: float
    is_maker: bool
    timestamp: float
    realized_pnl: float
    # Market-data provenance for the tick that crossed this quote.
    exchange_ts: int = 0
    sequence: int = 0


@dataclass
class PaperPosition:
    net_quantity: float
    average_entry_price: float = 0.0


class PaperOrderAck:
    def __init__(self, order_id: int = 0, accepted: bool = True,
                 reason: str = "NONE") -> None:
        self.order_id = order_id
        self._accepted = accepted
        self.reason = reason

    def accepted(self) -> bool:
        return self._accepted


class PaperMatchingEngine:
    """Matches resting maker quotes against incoming top-of-book ticks.

    Inventory, cash and PnL state are tracked here, separately from the C++
    ``L2Book`` which only maintains market data. The class also implements the
    minimal ``TradingEngine`` facade used by ``AvellanedaStoikovStrategy``.
    """

    def __init__(
        self,
        book: "hft_native.L2Book",
        *,
        maker_fee: float = -0.0001,
        taker_fee: float = 0.0005,
        initial_cash: float = 0.0,
        risk_limits: Optional["hft_native.RiskLimits"] = None,
        self_trade_prevention: str = "cancel_resting",
        risk_manager: Optional["hft_native.RiskManager"] = None,
        symbol: str = "",
    ) -> None:
        if self_trade_prevention not in ("none", "cancel_resting", "reject"):
            raise ValueError(
                "self_trade_prevention must be 'none', 'cancel_resting' or 'reject'"
            )
        self._book = book
        self._maker_fee = maker_fee
        self._taker_fee = taker_fee
        self._initial_cash = initial_cash
        self._self_trade_prevention = self_trade_prevention
        # Optional native pre-trade gate. When present every order is checked
        # against position, order-size, fat-finger and order-rate limits before
        # it is booked, mirroring what `TradingEngine::submit` does in C++.
        self._risk_manager = risk_manager
        self._symbol = symbol
        self._cash = initial_cash
        self._inventory = 0.0
        self._avg_entry_price = 0.0
        self._realized_pnl = 0.0
        self._unrealized_pnl = 0.0
        self._last_mark = 0.0
        self._risk_limits = (risk_limits if risk_limits is not None
                             else default_risk_limits())
        self._active_bid: Optional[PaperOrder] = None
        self._active_ask: Optional[PaperOrder] = None
        self._next_order_id = 1
        self._fills: List[PaperFill] = []
        self._trades_history: List[Dict[str, Any]] = []
        self._equity_curve: List[float] = []
        self._self_trade_prevented = 0
        self._last_market_ts = 0

    # ------------------------------------------------- strategy-facing facade
    def bbo(self) -> "hft_native.Bbo":
        return self._book.top()

    def position(self) -> PaperPosition:
        return PaperPosition(self._inventory, self._avg_entry_price)

    def risk_limits(self) -> "hft_native.RiskLimits":
        return self._risk_limits

    def risk_manager(self) -> Optional["hft_native.RiskManager"]:
        """The native pre-trade guard consulted by :meth:`submit`, if any."""
        return self._risk_manager

    def set_risk_manager(
        self,
        risk_manager: Optional["hft_native.RiskManager"],
        symbol: Optional[str] = None,
    ) -> None:
        self._risk_manager = risk_manager
        if symbol is not None:
            self._symbol = symbol

    def submit(self, request: "hft_native.OrderRequest") -> PaperOrderAck:
        side = BUY if request.side == hft_native.Side.BUY else SELL
        price = float(request.price)
        quantity = float(request.quantity)
        if price <= 0.0 or quantity <= 0.0:
            return PaperOrderAck(0, False, "INVALID_ORDER")

        # Native pre-trade gate: position, order size, fat-finger band and the
        # token-bucket order rate limit. It runs before any state changes so a
        # rejected order can never leave the book inconsistent.
        if self._risk_manager is not None:
            status = self._risk_manager.validate_order(
                self._symbol,
                price,
                quantity,
                hft_native.Side.BUY if side == BUY else hft_native.Side.SELL,
                self._inventory,
                self.mark_price(),
            )
            if not hft_native.is_approved(status):
                return PaperOrderAck(0, False, hft_native.risk_status_name(status))

        # Self-trade prevention: never rest a pair of crossing own quotes. A
        # post-only order that would lift our own opposite quote is either
        # rejected or replaces it, depending on the configured mode.
        opposite = self._active_ask if side == BUY else self._active_bid
        if opposite is not None and self._self_trade_prevention != "none":
            if side == BUY:
                crosses = price >= opposite.price
            else:
                crosses = price <= opposite.price
            if crosses:
                self._self_trade_prevented += 1
                if self._self_trade_prevention == "reject":
                    return PaperOrderAck(0, False, "SELF_TRADE_PREVENTED")
                # cancel_resting: retire the stale opposite quote, keep the new one.
                if side == BUY:
                    self._active_ask = None
                else:
                    self._active_bid = None

        order_id = self._next_order_id
        self._next_order_id += 1
        order = PaperOrder(order_id, side, price, quantity, quantity)
        if side == BUY:
            self._active_bid = order
        else:
            self._active_ask = order
        return PaperOrderAck(order_id, True, "NONE")

    def cancel(self, order_id: int) -> bool:
        if self._active_bid is not None and self._active_bid.order_id == order_id:
            self._active_bid = None
            return True
        if self._active_ask is not None and self._active_ask.order_id == order_id:
            self._active_ask = None
            return True
        return False

    def cancel_all(self) -> int:
        count = int(self._active_bid is not None) + int(self._active_ask is not None)
        self._active_bid = None
        self._active_ask = None
        return count

    # ------------------------------------------------------------ matching
    def mark_price(self) -> float:
        bbo = self._book.top()
        if bbo.is_valid():
            return (float(bbo.bid_price) + float(bbo.ask_price)) / 2.0
        return self._last_mark

    def on_market_tick(self) -> List[PaperFill]:
        """Matches resting quotes against the touch and records the equity mark."""
        fills = self.match_against_book()
        self._equity_curve.append(self.equity)
        return fills

    def match_against_book(self) -> List[PaperFill]:
        """Taker-cross matcher: external liquidity trades through our quotes.

        A resting bid is filled when the market's best ask drops to or below
        our bid price (``best_ask <= bid``); a resting ask is filled when the
        market's best bid rises to or above our ask price (``best_bid >= ask``).
        Fill size is capped by the visible size at the touch, so a thin book
        yields a partial fill rather than inventing liquidity.
        """
        fills: List[PaperFill] = []
        bbo = self._book.top()
        if not bbo.is_valid():
            self.mark_to_market(self._last_mark)
            return fills

        market_bid = float(bbo.bid_price)
        market_ask = float(bbo.ask_price)
        bid_size = float(bbo.bid_quantity)
        ask_size = float(bbo.ask_quantity)

        exchange_ts = int(getattr(bbo, "exchange_ts", 0) or 0)
        if exchange_ts <= 0:
            exchange_ts = int(self._book.last_update_ts() or 0)
        sequence = int(getattr(bbo, "sequence", 0) or 0)
        if exchange_ts > 0:
            self._last_market_ts = exchange_ts

        # Self-match guard: never let a crossed pair of our own quotes execute
        # against the same tick, which would print a fictitious round trip.
        self._uncross_own_quotes()

        bid = self._active_bid
        if bid is not None and market_ask <= bid.price:
            fill_qty = min(bid.remaining, ask_size)
            if fill_qty > 0.0:
                fills.append(self._execute(bid, fill_qty, bid.price,
                                           exchange_ts, sequence))

        ask = self._active_ask
        if ask is not None and market_bid >= ask.price:
            fill_qty = min(ask.remaining, bid_size)
            if fill_qty > 0.0:
                fills.append(self._execute(ask, fill_qty, ask.price,
                                           exchange_ts, sequence))

        self.mark_to_market(self.mark_price())
        return fills

    def _uncross_own_quotes(self) -> None:
        """Retires the older of two crossing own quotes (self-match guard)."""
        bid, ask = self._active_bid, self._active_ask
        if bid is None or ask is None or bid.price < ask.price:
            return
        if self._self_trade_prevention == "none":
            return
        if bid.order_id < ask.order_id:
            self._active_bid = None
        else:
            self._active_ask = None
        self._self_trade_prevented += 1
        logger.debug("self-match guard: retired a crossed own quote")

    def execute_taker(self, side: str, quantity: float) -> Optional[PaperFill]:
        """Crosses the spread immediately as a taker (for fee/PnL modelling)."""
        bbo = self._book.top()
        if not bbo.is_valid() or quantity <= 0.0:
            return None
        price = float(bbo.ask_price) if side == BUY else float(bbo.bid_price)
        exchange_ts = int(getattr(bbo, "exchange_ts", 0) or 0)
        if exchange_ts <= 0:
            exchange_ts = int(self._book.last_update_ts() or 0)
        sequence = int(getattr(bbo, "sequence", 0) or 0)
        return self._record_fill(side, price, quantity, is_maker=False, order_id=0,
                                 exchange_ts=exchange_ts, sequence=sequence)

    def mark_to_market(self, mark_price: float) -> None:
        self._last_mark = mark_price
        if self._inventory == 0.0 or self._avg_entry_price == 0.0:
            self._unrealized_pnl = 0.0
        else:
            self._unrealized_pnl = (mark_price - self._avg_entry_price) * self._inventory

    # ------------------------------------------------------------ internal
    def _execute(self, order: PaperOrder, fill_qty: float, fill_price: float,
                 exchange_ts: int = 0, sequence: int = 0) -> PaperFill:
        order.remaining -= fill_qty
        fill = self._record_fill(order.side, fill_price, fill_qty,
                                 order.is_maker, order.order_id,
                                 exchange_ts=exchange_ts, sequence=sequence)
        if order.remaining <= 1e-12:
            if order.side == BUY:
                self._active_bid = None
            else:
                self._active_ask = None
        return fill

    def _record_fill(self, side: str, price: float, quantity: float,
                     is_maker: bool, order_id: int, *,
                     exchange_ts: int = 0, sequence: int = 0) -> PaperFill:
        fee_rate = self._maker_fee if is_maker else self._taker_fee
        notional = price * quantity
        fee = notional * fee_rate  # negative => rebate received
        buy = side == BUY

        if buy:
            self._cash -= notional
        else:
            self._cash += notional
        self._cash -= fee  # a negative fee (rebate) increases cash

        existing = self._inventory
        signed_qty = quantity if buy else -quantity
        closing = min(abs(existing), quantity)

        realized_delta = 0.0
        if existing != 0.0 and (buy != (existing > 0.0)):
            if existing > 0.0:
                realized_delta += (price - self._avg_entry_price) * closing
            else:
                realized_delta += (self._avg_entry_price - price) * closing
        realized_delta -= fee

        new_inventory = existing + signed_qty
        if existing == 0.0:
            self._avg_entry_price = price
        elif (existing > 0.0) == (signed_qty > 0.0):
            # Adding to the position: volume-weighted average entry.
            total = abs(existing) + quantity
            self._avg_entry_price = (
                (abs(existing) * self._avg_entry_price + quantity * price) / total
                if total > 0.0 else 0.0
            )
        elif abs(new_inventory) <= 1e-12:
            # Flat: no residual cost basis.
            self._avg_entry_price = 0.0
        elif (new_inventory > 0.0) != (existing > 0.0):
            # Flipped through zero: the residual was opened at this price.
            self._avg_entry_price = price
        # else: partial reduction only - the remainder keeps its original basis.

        self._inventory = new_inventory
        self._realized_pnl += realized_delta

        # Prefer the exchange event time (epoch milliseconds) so the trade log
        # aligns with the market data; fall back to wall-clock seconds.
        timestamp = (exchange_ts / 1000.0) if exchange_ts > 0 else time.time()
        fill = PaperFill(
            order_id=order_id,
            side=side,
            price=price,
            quantity=quantity,
            fee=fee,
            is_maker=is_maker,
            timestamp=timestamp,
            realized_pnl=realized_delta,
            exchange_ts=exchange_ts,
            sequence=sequence,
        )
        self._fills.append(fill)
        self._trades_history.append({
            "timestamp": timestamp,
            "exchange_ts": exchange_ts,
            "sequence": sequence,
            "side": side,
            "price": price,
            "size": quantity,
            "quantity": quantity,
            "order_id": order_id,
            "is_maker": is_maker,
            "fee": fee,
            "realized_pnl": realized_delta,
            "inventory": self._inventory,
            "cash": self._cash,
        })
        return fill

    # ------------------------------------------------------------ accessors
    @property
    def inventory(self) -> float:
        return self._inventory

    @property
    def cash(self) -> float:
        return self._cash

    @property
    def realized_pnl(self) -> float:
        return self._realized_pnl

    @property
    def unrealized_pnl(self) -> float:
        return self._unrealized_pnl

    @property
    def total_pnl(self) -> float:
        return self._realized_pnl + self._unrealized_pnl

    @property
    def equity(self) -> float:
        return self._cash + self._last_mark * self._inventory

    @property
    def fills(self) -> List[PaperFill]:
        return list(self._fills)

    @property
    def equity_curve(self) -> List[float]:
        return list(self._equity_curve)

    @property
    def trades_history(self) -> List[Dict[str, Any]]:
        """Chronological execution log: ``timestamp``, ``side``, ``price``, ``size``."""
        return list(self._trades_history)

    @property
    def self_trade_prevented(self) -> int:
        """Count of quotes refused/retired by self-trade prevention."""
        return self._self_trade_prevented

    @property
    def last_market_ts(self) -> int:
        """Exchange timestamp (epoch ms) of the tick that last drove matching."""
        return self._last_market_ts

    @property
    def active_bid(self) -> Optional[PaperOrder]:
        return self._active_bid

    @property
    def active_ask(self) -> Optional[PaperOrder]:
        return self._active_ask

    def summary(self) -> Dict[str, Any]:
        trades = self._fills
        wins = sum(1 for fill in trades if fill.realized_pnl > 0.0)
        curve = self._equity_curve or [self.equity]
        returns = self._returns(curve)
        return {
            "inventory": self._inventory,
            "cash": self._cash,
            "realized_pnl": self._realized_pnl,
            "unrealized_pnl": self._unrealized_pnl,
            "total_pnl": self.total_pnl,
            "equity": self.equity,
            "total_trades": len(trades),
            "win_rate": (wins / len(trades)) if trades else 0.0,
            "sharpe": sharpe_ratio(returns),
            "max_drawdown": max_drawdown(curve),
            "max_drawdown_abs": self._absolute_drawdown(curve),
            "total_fees": sum(fill.fee for fill in trades),
            "self_trade_prevented": self._self_trade_prevented,
        }

    @staticmethod
    def _returns(curve: List[float]) -> List[float]:
        out: List[float] = []
        for index in range(1, len(curve)):
            if curve[index - 1] > 0.0:
                out.append((curve[index] - curve[index - 1]) / curve[index - 1])
        return out

    @staticmethod
    def _absolute_drawdown(curve: List[float]) -> float:
        """Peak-to-trough decline in currency units.

        The percentage variant divides by the running peak, which explodes when
        equity starts at zero (``--initial-cash 0``); this figure stays readable.
        """
        peak = float("-inf")
        worst = 0.0
        for value in curve:
            peak = max(peak, value)
            worst = max(worst, peak - value)
        return worst


class MarketDataFeed:
    """Asynchronous depth feed: WebSocket, NDJSON file, or synthetic mock."""

    def __init__(
        self,
        source: Union[str, Path],
        book: "hft_native.L2Book",
        on_depth: Optional[Callable[["hft_native.DepthUpdate"], Any]] = None,
        *,
        loop: bool = False,
        replay_delay: float = 0.1,
        reconnect_delay: float = 2.0,
        fallback: Optional[Union[str, Path]] = None,
    ) -> None:
        self._source = str(source)
        self._book = book
        self._on_depth = on_depth
        self._loop = loop
        self._replay_delay = replay_delay
        self._reconnect_delay = reconnect_delay
        self._fallback = str(fallback) if fallback is not None else None
        self._stopped = False
        self._messages = 0
        self._malformed = 0

    def stop(self) -> None:
        self._stopped = True

    @property
    def messages(self) -> int:
        return self._messages

    @property
    def malformed(self) -> int:
        return self._malformed

    async def run(self) -> None:
        if self._source in ("mock", "synthetic"):
            await self._run_synthetic()
        elif self._source.startswith(("ws://", "wss://")):
            await self._run_websocket()
        else:
            await self._run_file(self._source, loop=self._loop)

    async def _run_websocket(self) -> None:
        try:
            import websockets
        except ImportError:
            logger.warning("`websockets` is not installed; falling back to offline feed")
            await self._fallback_or_mock()
            return

        while not self._stopped:
            try:
                async with websockets.connect(self._source) as socket:
                    async for message in socket:
                        if self._stopped:
                            break
                        await self._handle_message(message)
            except Exception as exc:  # noqa: BLE001 - reconnect on any feed error
                logger.warning("WebSocket error (%s); reconnecting in %.1fs",
                               exc, self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)

    async def _run_file(self, path_text: str, loop: bool) -> None:
        path = Path(path_text)
        while not self._stopped:
            try:
                with path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if self._stopped:
                            break
                        stripped = line.strip()
                        if stripped:
                            await self._handle_message(stripped)
                        # Always yield: `sleep(0)` keeps the loop cooperative when
                        # replaying with zero delay, otherwise the duration timer
                        # and dashboard tasks are starved forever.
                        await asyncio.sleep(max(0.0, self._replay_delay))
            except OSError as exc:
                logger.error("cannot read feed file %s: %s", path, exc)
                return
            if not loop:
                break

    async def _run_synthetic(self) -> None:
        mid = float(self._book.center_price())
        tick = float(self._book.tick_size())
        for message in synthetic_depth_messages(mid=mid, tick=tick):
            if self._stopped:
                break
            await self._handle_message(message)
            # Always yield so the event loop can run the duration timer.
            await asyncio.sleep(max(0.0, self._replay_delay))

    async def _fallback_or_mock(self) -> None:
        fallback = self._fallback or self._default_fallback()
        if fallback:
            await self._run_file(fallback, loop=True)
        else:
            await self._run_synthetic()

    def _default_fallback(self) -> Optional[str]:
        candidate = Path(__file__).resolve().parents[2] / "data" / "sample_events.ndjson"
        return str(candidate) if candidate.exists() else None

    async def _handle_message(self, text: str) -> None:
        self._messages += 1
        try:
            kind = hft_native.classify_json(text)
        except RuntimeError:
            kind = hft_native.FeedMessageType.UNKNOWN
        if kind == hft_native.FeedMessageType.TRADE:
            return

        update = parse_depth_message(text)
        if update is None:
            self._malformed += 1
            return

        apply_depth_update(self._book, update)
        if self._on_depth is not None:
            result = self._on_depth(update)
            if inspect.isawaitable(result):
                await result



