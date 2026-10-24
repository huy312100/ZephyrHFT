"""Exchange connectivity for the Python layer.

Currently this package exposes the Binance USDⓈ-M Futures testnet client used by
``scripts/run_testnet_pilot.py``. Nothing here reads credentials from disk:
keys come from ``BINANCE_API_KEY`` / ``BINANCE_API_SECRET`` only.
"""
from .binance_futures import (
    API_KEY_VAR,
    API_SECRET_VAR,
    DEFAULT_ENVIRONMENT,
    BinanceAPIError,
    BinanceConfigError,
    BinanceCredentials,
    BinanceError,
    BinanceFuturesClient,
    BinanceFuturesConfig,
    BinanceTransportError,
    DepthTick,
    LimitOrderPlan,
    MissingCredentialsError,
    OrderValidationError,
    SymbolFilters,
    UnknownSymbolError,
    build_limit_order,
    decimal_text,
    parse_depth_tick,
)

__all__ = [
    "API_KEY_VAR",
    "API_SECRET_VAR",
    "DEFAULT_ENVIRONMENT",
    "BinanceAPIError",
    "BinanceConfigError",
    "BinanceCredentials",
    "BinanceError",
    "BinanceFuturesClient",
    "BinanceFuturesConfig",
    "BinanceTransportError",
    "DepthTick",
    "LimitOrderPlan",
    "MissingCredentialsError",
    "OrderValidationError",
    "SymbolFilters",
    "UnknownSymbolError",
    "build_limit_order",
    "decimal_text",
    "parse_depth_tick",
]
