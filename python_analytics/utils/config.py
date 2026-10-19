"""Typed, validated access to ``config/settings.yaml``.

Decimal values stay as strings at this boundary; the native layer parses them
into fixed-point integers so no binary floating point ever reaches the engine.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"


class ConfigError(ValueError):
    """Raised when the configuration file is missing or invalid."""


@dataclass(frozen=True)
class RiskSettings:
    max_order_quantity: str = "10"
    max_order_notional: str = "250000"
    max_position_notional: str = "1000000"
    max_daily_loss: str = "25000"
    max_price_deviation: str = "500"
    max_open_orders: int = 64


@dataclass(frozen=True)
class EngineSettings:
    symbol: str = "BTCUSDT"
    tick_size: str = "0.50"
    center: str = "30000"
    stale_after_ns: int = 1_000_000_000
    risk: RiskSettings = field(default_factory=RiskSettings)


@dataclass(frozen=True)
class StrategySettings:
    name: str = "inventory_market_maker"
    half_spread_ticks: int = 2
    order_quantity: str = "0.05"
    max_inventory: str = "1.0"
    skew_ticks: int = 1


@dataclass(frozen=True)
class BacktestSettings:
    event_limit: int = 100_000
    record_fills: bool = True


@dataclass(frozen=True)
class AppSettings:
    engine: EngineSettings = field(default_factory=EngineSettings)
    strategy: StrategySettings = field(default_factory=StrategySettings)
    backtest: BacktestSettings = field(default_factory=BacktestSettings)


def _mapping(value: Any, section: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(f"'{section}' must be a mapping")
    return value


def _decimal(value: Any, section: str) -> str:
    text = str(value)
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise ConfigError(f"'{section}' is not a decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise ConfigError(f"'{section}' must be finite")
    if parsed != 0 and abs(parsed.adjusted()) > 50:
        raise ConfigError(f"'{section}' is out of range: {value!r}")
    # Normalise exponent notation (e.g. YAML's ``1e-05``) into plain decimal
    # text, which is what the native fixed-point parser accepts.
    normalized = format(parsed, "f")
    if len(normalized) > 64:
        raise ConfigError(f"'{section}' is too large: {value!r}")
    return normalized


def _positive_decimal(value: Any, section: str) -> str:
    text = _decimal(value, section)
    if Decimal(text) <= 0:
        raise ConfigError(f"'{section}' must be positive")
    return text


def _int(value: Any, section: str, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"'{section}' must be an integer: {value!r}") from exc
    if parsed < minimum:
        raise ConfigError(f"'{section}' must be >= {minimum}")
    return parsed


def _build_risk(raw: Mapping[str, Any]) -> RiskSettings:
    defaults = RiskSettings()
    return RiskSettings(
        max_order_quantity=_positive_decimal(
            raw.get("max_order_quantity", defaults.max_order_quantity),
            "engine.risk.max_order_quantity",
        ),
        max_order_notional=_positive_decimal(
            raw.get("max_order_notional", defaults.max_order_notional),
            "engine.risk.max_order_notional",
        ),
        max_position_notional=_positive_decimal(
            raw.get("max_position_notional", defaults.max_position_notional),
            "engine.risk.max_position_notional",
        ),
        max_daily_loss=_positive_decimal(
            raw.get("max_daily_loss", defaults.max_daily_loss),
            "engine.risk.max_daily_loss",
        ),
        max_price_deviation=_positive_decimal(
            raw.get("max_price_deviation", defaults.max_price_deviation),
            "engine.risk.max_price_deviation",
        ),
        max_open_orders=_int(
            raw.get("max_open_orders", defaults.max_open_orders),
            "engine.risk.max_open_orders",
            minimum=1,
        ),
    )


def load_settings(path: Optional[Path] = None) -> AppSettings:
    """Loads and validates a settings file, falling back to dataclass defaults."""
    config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise ConfigError(f"configuration file not found: {config_path}")
    try:
        document = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {config_path}: {exc}") from exc
    if not isinstance(document, Mapping):
        raise ConfigError("top-level configuration must be a mapping")

    engine_raw = _mapping(document.get("engine"), "engine")
    risk_raw = _mapping(engine_raw.get("risk"), "engine.risk")
    strategy_raw = _mapping(document.get("strategy"), "strategy")
    backtest_raw = _mapping(document.get("backtest"), "backtest")

    defaults = EngineSettings()
    engine = EngineSettings(
        symbol=str(engine_raw.get("symbol", defaults.symbol)),
        tick_size=_positive_decimal(
            engine_raw.get("tick_size", defaults.tick_size), "engine.tick_size"
        ),
        center=_positive_decimal(engine_raw.get("center", defaults.center),
                                 "engine.center"),
        stale_after_ns=_int(
            engine_raw.get("stale_after_ns", defaults.stale_after_ns),
            "engine.stale_after_ns",
            minimum=1,
        ),
        risk=_build_risk(risk_raw),
    )

    strategy_defaults = StrategySettings()
    strategy = StrategySettings(
        name=str(strategy_raw.get("name", strategy_defaults.name)),
        half_spread_ticks=_int(
            strategy_raw.get("half_spread_ticks", strategy_defaults.half_spread_ticks),
            "strategy.half_spread_ticks",
            minimum=1,
        ),
        order_quantity=_positive_decimal(
            strategy_raw.get("order_quantity", strategy_defaults.order_quantity),
            "strategy.order_quantity",
        ),
        max_inventory=_positive_decimal(
            strategy_raw.get("max_inventory", strategy_defaults.max_inventory),
            "strategy.max_inventory",
        ),
        skew_ticks=_int(
            strategy_raw.get("skew_ticks", strategy_defaults.skew_ticks),
            "strategy.skew_ticks",
        ),
    )

    backtest_defaults = BacktestSettings()
    backtest = BacktestSettings(
        event_limit=_int(
            backtest_raw.get("event_limit", backtest_defaults.event_limit),
            "backtest.event_limit",
            minimum=1,
        ),
        record_fills=bool(
            backtest_raw.get("record_fills", backtest_defaults.record_fills)
        ),
    )
    return AppSettings(engine=engine, strategy=strategy, backtest=backtest)


def as_dict(settings: AppSettings) -> Dict[str, Any]:
    """Serialises settings for logging or report artefacts."""
    return {
        "engine": {
            "symbol": settings.engine.symbol,
            "tick_size": settings.engine.tick_size,
            "center": settings.engine.center,
            "stale_after_ns": settings.engine.stale_after_ns,
            "risk": settings.engine.risk.__dict__,
        },
        "strategy": settings.strategy.__dict__,
        "backtest": settings.backtest.__dict__,
    }
