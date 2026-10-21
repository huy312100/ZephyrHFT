"""Builds native engine objects from the YAML-backed settings model."""
from __future__ import annotations

from typing import Union

from ..utils.config import AppSettings, EngineSettings, KillSwitchSettings
from . import hft_native


def _price(text: str) -> "hft_native.Price":
    value = hft_native.Price.from_string(text)
    if value is None:
        raise ValueError(f"invalid price: {text!r}")
    return value


def _quantity(text: str) -> "hft_native.Quantity":
    value = hft_native.Quantity.from_string(text)
    if value is None:
        raise ValueError(f"invalid quantity: {text!r}")
    return value


def _notional(text: str) -> "hft_native.Notional":
    value = hft_native.Notional.from_string(text)
    if value is None:
        raise ValueError(f"invalid notional: {text!r}")
    return value


def pretrade_config(
    settings: Union[AppSettings, EngineSettings],
) -> "hft_native.RiskManagerConfig":
    """Builds the native pre-trade risk configuration from settings."""
    engine_settings = settings.engine if isinstance(settings, AppSettings) else settings
    pretrade = engine_settings.pretrade
    config = hft_native.RiskManagerConfig()
    config.enabled = pretrade.enabled
    config.max_position = float(pretrade.max_position)
    config.max_order_size = float(pretrade.max_order_size)
    config.max_price_deviation_pct = float(pretrade.max_price_deviation_pct)
    config.order_rate_limit = float(pretrade.order_rate_limit)
    config.order_burst = float(pretrade.order_burst)
    return config


def build_risk_manager(
    settings: Union[AppSettings, EngineSettings],
) -> "hft_native.RiskManager":
    """Creates a standalone pre-trade gate for the Python paper engine."""
    return hft_native.RiskManager(pretrade_config(settings))


def kill_switch_config(
    settings: Union[AppSettings, KillSwitchSettings],
) -> "hft_native.KillSwitchConfig":
    """Builds the native emergency-halt configuration from settings."""
    block = settings.kill_switch if isinstance(settings, AppSettings) else settings
    config = hft_native.KillSwitchConfig()
    config.enabled = block.enabled
    config.max_drawdown_limit = float(block.max_drawdown_limit)
    config.max_consecutive_rejects = int(block.max_consecutive_rejects)
    return config


def build_kill_switch(
    settings: Union[AppSettings, KillSwitchSettings],
) -> "hft_native.KillSwitch":
    """Creates a standalone kill switch for the Python paper engine."""
    return hft_native.KillSwitch(kill_switch_config(settings))


def engine_config(settings: Union[AppSettings, EngineSettings]) -> "hft_native.EngineConfig":
    engine_settings = settings.engine if isinstance(settings, AppSettings) else settings
    config = hft_native.EngineConfig()
    config.tick_size = _price(engine_settings.tick_size)
    config.center = _price(engine_settings.center)
    config.stale_after_ns = engine_settings.stale_after_ns
    config.risk_manager = pretrade_config(settings)
    return config


def risk_limits(settings: AppSettings) -> "hft_native.RiskLimits":
    risk = settings.engine.risk
    limits = hft_native.RiskLimits()
    limits.max_order_quantity = _quantity(risk.max_order_quantity)
    limits.max_order_notional = _notional(risk.max_order_notional)
    limits.max_position_notional = _notional(risk.max_position_notional)
    limits.max_daily_loss = _notional(risk.max_daily_loss)
    limits.max_price_deviation = _price(risk.max_price_deviation)
    limits.max_open_orders = risk.max_open_orders
    return limits


def build_engine(settings: AppSettings) -> "hft_native.TradingEngine":
    """Creates a fully configured engine, including risk limits."""
    engine = hft_native.TradingEngine(engine_config(settings))
    engine.set_risk_limits(risk_limits(settings))
    return engine
