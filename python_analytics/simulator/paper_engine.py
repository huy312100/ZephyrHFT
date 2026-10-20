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
    ) -> None:
        self._book = book
        self._maker_fee = maker_fee
        self._taker_fee = taker_fee
        self._initial_cash = initial_cash
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
        self._equity_curve: List[float] = []

    # ------------------------------------------------- strategy-facing facade
    def bbo(self) -> "hft_native.Bbo":
        return self._book.top()

    def position(self) -> PaperPosition:
        return PaperPosition(self._inventory, self._avg_entry_price)

    def risk_limits(self) -> "hft_native.RiskLimits":
        return self._risk_limits

    def submit(self, request: "hft_native.OrderRequest") -> PaperOrderAck:
        side = BUY if request.side == hft_native.Side.BUY else SELL
        price = float(request.price)
        quantity = float(request.quantity)
        if price <= 0.0 or quantity <= 0.0:
            return PaperOrderAck(0, False, "INVALID_ORDER")

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
        """Matches active orders against the current top-of-book and marks PnL."""
        fills: List[PaperFill] = []
        bbo = self._book.top()
        if bbo.is_valid():
            market_bid = float(bbo.bid_price)
            market_ask = float(bbo.ask_price)
            market_bid_qty = float(bbo.bid_quantity)
            market_ask_qty = float(bbo.ask_quantity)

            if self._active_bid is not None and market_ask <= self._active_bid.price:
                fill_qty = min(self._active_bid.remaining, market_ask_qty)
                if fill_qty > 0.0:
                    fills.append(self._execute(self._active_bid, fill_qty,
                                               self._active_bid.price))
            if self._active_ask is not None and market_bid >= self._active_ask.price:
                fill_qty = min(self._active_ask.remaining, market_bid_qty)
                if fill_qty > 0.0:
                    fills.append(self._execute(self._active_ask, fill_qty,
                                               self._active_ask.price))

        self.mark_to_market(self.mark_price())
        self._equity_curve.append(self.equity)
        return fills

    def execute_taker(self, side: str, quantity: float) -> Optional[PaperFill]:
        """Crosses the spread immediately as a taker (for fee/PnL modelling)."""
        bbo = self._book.top()
        if not bbo.is_valid() or quantity <= 0.0:
            return None
        price = float(bbo.ask_price) if side == BUY else float(bbo.bid_price)
        return self._record_fill(side, price, quantity, is_maker=False, order_id=0)

    def mark_to_market(self, mark_price: float) -> None:
        self._last_mark = mark_price
        if self._inventory == 0.0 or self._avg_entry_price == 0.0:
            self._unrealized_pnl = 0.0
        else:
            self._unrealized_pnl = (mark_price - self._avg_entry_price) * self._inventory

    # ------------------------------------------------------------ internal
    def _execute(self, order: PaperOrder, fill_qty: float,
                 fill_price: float) -> PaperFill:
        order.remaining -= fill_qty
        fill = self._record_fill(order.side, fill_price, fill_qty,
                                 order.is_maker, order.order_id)
        if order.remaining <= 1e-12:
            if order.side == BUY:
                self._active_bid = None
            else:
                self._active_ask = None
        return fill

    def _record_fill(self, side: str, price: float, quantity: float,
                     is_maker: bool, order_id: int) -> PaperFill:
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
        if existing == 0.0 or (existing > 0.0) == (signed_qty > 0.0):
            total = abs(existing) + quantity
            self._avg_entry_price = (
                (abs(existing) * self._avg_entry_price + quantity * price) / total
                if total > 0.0 else 0.0
            )
        elif new_inventory == 0.0:
            self._avg_entry_price = 0.0
        else:
            self._avg_entry_price = price

        self._inventory = new_inventory
        self._realized_pnl += realized_delta

        fill = PaperFill(
            order_id=order_id,
            side=side,
            price=price,
            quantity=quantity,
            fee=fee,
            is_maker=is_maker,
            timestamp=time.monotonic(),
            realized_pnl=realized_delta,
        )
        self._fills.append(fill)
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
            "total_fees": sum(fill.fee for fill in trades),
        }

    @staticmethod
    def _returns(curve: List[float]) -> List[float]:
        out: List[float] = []
        for index in range(1, len(curve)):
            if curve[index - 1] > 0.0:
                out.append((curve[index] - curve[index - 1]) / curve[index - 1])
        return out


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
                        if self._replay_delay > 0.0:
                            await asyncio.sleep(self._replay_delay)
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
            if self._replay_delay > 0.0:
                await asyncio.sleep(self._replay_delay)

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



