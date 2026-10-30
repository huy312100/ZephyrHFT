"""Typed, validated access to ``config/settings.yaml``.

Decimal values stay as strings at this boundary; the native layer parses them
into fixed-point integers so no binary floating point ever reaches the engine.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "settings.yaml"

# Public market-data WebSocket endpoints keyed by the ``data_ingestion.exchange``
# value. The stream URL is built as ``<endpoint>/<symbol>@<stream_type>`` for a
# single stream, or ``<endpoint>/stream?streams=a/b`` when auxiliary streams are
# configured (``data_ingestion.aux_streams``), so any Binance-compatible venue
# that follows the same naming scheme can be added here (or overridden per run
# with ``data_ingestion.ws_base_url``).
EXCHANGE_ENDPOINTS: Dict[str, str] = {
    "binance_futures": "wss://fstream.binance.com/ws",
    "binance_futures_testnet": "wss://stream.binancefuture.com/ws",
    "binance_spot": "wss://stream.binance.com:9443/ws",
}

# Binance USDⓈ-M Futures environments selected by ``BINANCE_ENV``. Only public
# endpoint metadata lives here: API keys and secrets are read from the
# ``BINANCE_API_KEY`` / ``BINANCE_API_SECRET`` environment variables so they are
# never written to (or committed with) the YAML configuration.
BINANCE_ENV_VAR = "BINANCE_ENV"
BINANCE_DEFAULT_ENVIRONMENT = "testnet"
BINANCE_ENDPOINTS: Dict[str, Dict[str, str]] = {
    "testnet": {
        "rest": "https://testnet.binancefuture.com",
        "ws": "wss://stream.binancefuture.com/ws",
    },
    "production": {
        "rest": "https://fapi.binance.com",
        "ws": "wss://fstream.binance.com/ws",
    },
}

COMPRESSION_CHOICES: Tuple[str, ...] = ("gzip", "none")


class ConfigError(ValueError):
    """Raised when the configuration file is missing or invalid."""


def resolve_binance_environment(
    value: Optional[str] = None,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    """Resolves ``testnet`` / ``production``, defaulting to the testnet.

    An explicit ``value`` wins over ``BINANCE_ENV``; an empty variable falls
    back to ``testnet`` so a bare shell cannot accidentally point a pilot at
    production funds.
    """

    env = os.environ if environ is None else environ
    candidate = value if value is not None else env.get(BINANCE_ENV_VAR)
    resolved = str(candidate or BINANCE_DEFAULT_ENVIRONMENT).strip().lower()
    if resolved not in BINANCE_ENDPOINTS:
        allowed = ", ".join(sorted(BINANCE_ENDPOINTS))
        raise ConfigError(f"'{BINANCE_ENV_VAR}' must be one of {allowed}: {candidate!r}")
    return resolved


def binance_endpoints(
    environment: Optional[str] = None,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> Dict[str, str]:
    """Returns the REST/WebSocket base URLs for a Binance Futures environment."""

    resolved = resolve_binance_environment(environment, environ=environ)
    return dict(BINANCE_ENDPOINTS[resolved])


@dataclass(frozen=True)
class RiskSettings:
    max_order_quantity: str = "10"
    max_order_notional: str = "250000"
    max_position_notional: str = "1000000"
    max_daily_loss: str = "25000"
    max_price_deviation: str = "500"
    max_open_orders: int = 64


@dataclass(frozen=True)
class PreTradeSettings:
    """Order-flow and fat-finger guards implemented by ``RiskManager``.

    ``max_position`` and ``max_order_size`` are base-asset quantities,
    ``max_price_deviation_pct`` is a fraction of mid (``"0.01"`` is 1%) and the
    rate fields are orders per second / burst depth. Set ``enabled`` to false to
    bypass the gate entirely (replay, backtests, and the default paper session).
    """

    enabled: bool = False
    max_position: str = "10"
    max_order_size: str = "1"
    max_price_deviation_pct: str = "0.01"
    order_rate_limit: int = 50
    order_burst: int = 20


@dataclass(frozen=True)
class KillSwitchSettings:
    """Emergency halt implemented by the native ``KillSwitch``.

    ``max_drawdown_limit`` is an absolute drawdown in quote currency (``"0"``
    disables the drawdown guard) and ``max_consecutive_rejects`` trips after N
    rejected orders in a row (``0`` disables the streak guard). ``enabled``
    only arms those automatic guards; a manual ``trip()`` always blocks.
    """

    enabled: bool = True
    max_drawdown_limit: str = "25000"
    max_consecutive_rejects: int = 10


@dataclass(frozen=True)
class DataIngestionSettings:
    """Real-time L2 capture settings used by ``scripts/ingest_l2.py``.

    ``exchange`` selects the WebSocket endpoint (see ``EXCHANGE_ENDPOINTS``),
    ``symbol`` and ``stream_type`` are joined into ``<endpoint>/<symbol>@<type>``.
    ``aux_streams`` adds more streams on the *same* socket (the combined-stream
    form), so prints land in the archive interleaved with the depth frames they
    moved: without the tape a backtest cannot tell a fill from a book crossing.
    ``storage_path`` is resolved against the repository root, so a relative value
    keeps captures inside the project; ``compression`` is ``gzip`` or ``none``.
    ``flush_interval_ms`` bounds how long a record sits in the write buffer, and
    ``max_file_mb`` / ``rotate_seconds`` (``0`` disables either rule) control
    roll-file rotation. ``max_reconnect_attempts`` of ``0`` means retry forever.
    """

    enabled: bool = True
    exchange: str = "binance_futures"
    symbol: str = "btcusdt"
    stream_type: str = "depth@100ms"
    aux_streams: Tuple[str, ...] = ()
    storage_path: str = "data/raw_l2"
    compression: str = "gzip"
    compression_level: int = 1
    flush_interval_ms: int = 250
    max_file_mb: int = 256
    rotate_seconds: int = 900
    telemetry_interval_s: float = 10.0
    latency_warn_ms: float = 250.0
    reconnect_base_delay_s: float = 0.5
    reconnect_max_delay_s: float = 30.0
    max_reconnect_attempts: int = 0
    ws_base_url: str = ""

    @property
    def endpoint(self) -> str:
        """Resolved WebSocket base URL (config override wins over the default)."""
        if self.ws_base_url:
            return self.ws_base_url.rstrip("/")
        return EXCHANGE_ENDPOINTS[self.exchange]

    @property
    def stream_tags(self) -> Tuple[str, ...]:
        """Stream suffixes on the subscription, depth first."""
        return (self.stream_type, *self.aux_streams)

    @property
    def combined_stream_url(self) -> Optional[str]:
        """Combined-stream URL for ``stream_tags``, or ``None`` for one stream.

        Binance multiplexes several ``<symbol>@<type>`` streams on a single
        socket only through ``<endpoint>/stream?streams=a/b``; each frame then
        arrives wrapped as ``{"stream": .., "data": ..}``.
        """
        tags = self.stream_tags
        if len(tags) < 2:
            return None
        base = self.endpoint
        # ``/ws`` is the single-stream endpoint; the multiplexed one is its
        # sibling ``/stream``. Measured on this venue (``docs/phase5`` §11.6.1):
        # ``<sym>@depth@100ms`` and ``<sym>@trade`` share that socket and arrive
        # interleaved, which is exactly what the tape capture needs. ``@aggTrade``
        # is accepted there and then silent, so it is flagged at ingest startup
        # (``silent_aux_streams``) instead of being routed to another path.
        base = base[: -len("/ws")] if base.endswith("/ws") else base
        streams = "/".join(f"{self.symbol}@{tag}" for tag in tags)
        return f"{base}/stream?streams={streams}"

    @property
    def stream_url(self) -> str:
        """Full subscription URL, e.g. ``wss://fstream.binance.com/ws/btcusdt@depth@100ms``."""
        combined = self.combined_stream_url
        if combined is not None:
            return combined
        return f"{self.endpoint}/{self.symbol}@{self.stream_type}"

    @property
    def storage_dir(self) -> Path:
        """Absolute capture directory (created lazily by the ingest client)."""
        path = Path(self.storage_path).expanduser()
        return path if path.is_absolute() else PROJECT_ROOT / path

    @property
    def file_suffix(self) -> str:
        return ".ndjson.gz" if self.compression == "gzip" else ".ndjson"


@dataclass(frozen=True)
class EngineSettings:
    symbol: str = "BTCUSDT"
    tick_size: str = "0.50"
    center: str = "30000"
    stale_after_ns: int = 1_000_000_000
    risk: RiskSettings = field(default_factory=RiskSettings)
    pretrade: PreTradeSettings = field(default_factory=PreTradeSettings)


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
    kill_switch: KillSwitchSettings = field(default_factory=KillSwitchSettings)
    data_ingestion: DataIngestionSettings = field(default_factory=DataIngestionSettings)


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


def _non_negative_decimal(value: Any, section: str) -> str:
    text = _decimal(value, section)
    if Decimal(text) < 0:
        raise ConfigError(f"'{section}' must be >= 0")
    return text


def _int(value: Any, section: str, minimum: int = 0,
         maximum: Optional[int] = None) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"'{section}' must be an integer: {value!r}") from exc
    if parsed < minimum:
        raise ConfigError(f"'{section}' must be >= {minimum}")
    if maximum is not None and parsed > maximum:
        raise ConfigError(f"'{section}' must be <= {maximum}")
    return parsed


def _float(
    value: Any,
    section: str,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> float:
    """Parses a finite float and range-checks it (inclusive bounds)."""
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"'{section}' must be a number: {value!r}") from exc
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        raise ConfigError(f"'{section}' must be finite: {value!r}")
    if minimum is not None and parsed < minimum:
        raise ConfigError(f"'{section}' must be >= {minimum}")
    if maximum is not None and parsed > maximum:
        raise ConfigError(f"'{section}' must be <= {maximum}")
    return parsed


def _choice(value: Any, section: str, choices: Sequence[str]) -> str:
    """Lower-cases ``value`` and asserts it is one of ``choices``."""
    text = str(value).strip().lower()
    if text not in choices:
        raise ConfigError(f"'{section}' must be one of {sorted(choices)}, got {value!r}")
    return text


def _pattern(value: Any, section: str, pattern: str, hint: str) -> str:
    """Lower-cases ``value`` and asserts it matches the given regex."""
    text = str(value).strip().lower()
    if not re.fullmatch(pattern, text):
        raise ConfigError(f"'{section}' {hint}: {value!r}")
    return text


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


def _build_pretrade(raw: Mapping[str, Any]) -> PreTradeSettings:
    defaults = PreTradeSettings()
    return PreTradeSettings(
        enabled=bool(raw.get("enabled", defaults.enabled)),
        max_position=_positive_decimal(
            raw.get("max_position", defaults.max_position),
            "engine.pretrade.max_position",
        ),
        max_order_size=_positive_decimal(
            raw.get("max_order_size", defaults.max_order_size),
            "engine.pretrade.max_order_size",
        ),
        max_price_deviation_pct=_positive_decimal(
            raw.get("max_price_deviation_pct", defaults.max_price_deviation_pct),
            "engine.pretrade.max_price_deviation_pct",
        ),
        order_rate_limit=_int(
            raw.get("order_rate_limit", defaults.order_rate_limit),
            "engine.pretrade.order_rate_limit",
            minimum=1,
        ),
        order_burst=_int(
            raw.get("order_burst", defaults.order_burst),
            "engine.pretrade.order_burst",
            minimum=1,
        ),
    )


def _build_kill_switch(raw: Mapping[str, Any]) -> KillSwitchSettings:
    defaults = KillSwitchSettings()
    return KillSwitchSettings(
        enabled=bool(raw.get("enabled", defaults.enabled)),
        max_drawdown_limit=_non_negative_decimal(
            raw.get("max_drawdown_limit", defaults.max_drawdown_limit),
            "kill_switch.max_drawdown_limit",
        ),
        max_consecutive_rejects=_int(
            raw.get("max_consecutive_rejects", defaults.max_consecutive_rejects),
            "kill_switch.max_consecutive_rejects",
            minimum=0,
        ),
    )


def parse_stream_names(
    value: Any, section: str = "data_ingestion.aux_streams"
) -> Tuple[str, ...]:
    """Validates auxiliary stream names, preserving their case.

    Binance stream names are case-sensitive (``aggTrade``, ``markPrice@1s``),
    so unlike ``_pattern`` this must not lower-case the value. Order is
    preserved and duplicates are dropped.
    """
    if value is None:
        return ()
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ConfigError(f"'{section}' must be a list of stream names")
    names: list = []
    for item in value:
        text = str(item).strip()
        if not re.fullmatch(r"[A-Za-z]{1,16}(@[A-Za-z0-9]{1,12})?", text):
            raise ConfigError(
                f"'{section}' entries must look like 'aggTrade' or 'markPrice@1s': "
                f"{item!r}"
            )
        if text not in names:
            names.append(text)
    return tuple(names)


def require_aligned_center(center: Any, tick_size: Any) -> None:
    """Rejects a ladder centre that is not a whole number of ticks.

    The native ladder indexes a price by ``(price - centre) / tick`` and refuses
    any price whose delta does not divide exactly (``LadderSide::slot_for``), so
    a centre off the grid rejects every frame, leaves resting orders unable to
    match the depth in the book, and reads as a quiet market rather than as a
    misconfiguration. Quotes are snapped *from zero* onto the tick grid
    (``MarketMakerStrategy.adjusted_quotes``), so no quoting choice can fix an
    unaligned centre: it has to be rejected before the run starts.
    """
    try:
        tick = Decimal(str(tick_size))
        price = Decimal(str(center))
    except (InvalidOperation, ValueError) as exc:  # pragma: no cover - defensive
        raise ValueError(
            f"center {center!r} / tick size {tick_size!r} is not a decimal price"
        ) from exc
    # The tick is checked before the division: ``Decimal`` raises on a zero
    # divisor, and a NaN comparison raises too, so neither can be left to the
    # ``steps`` test below.
    if not tick.is_finite() or tick <= 0:
        raise ValueError(f"tick size must be positive, got {tick_size!r}")
    if not price.is_finite():
        raise ValueError(f"center {center!r} is not a finite price")
    steps = price / tick
    if steps != steps.to_integral_value():
        raise ValueError(
            f"center {center!r} is not a whole number of ticks ({tick_size!r}): "
            f"{steps} steps, so every price would be off the ladder's grid"
        )


#: Aux streams the venue accepts and then never delivers (measured in
#: ``docs/phase5_production_readiness.md`` §11.6.1: ``@aggTrade`` carries 0
#: frames on the configured endpoint while ``@trade`` delivers ~40/s).
SILENT_AUX_STREAMS: Tuple[str, ...] = ("aggTrade",)


def silent_aux_streams(aux_streams: Sequence[str]) -> Tuple[str, ...]:
    """The configured aux streams that this venue accepts but never delivers."""
    return tuple(
        tag for tag in aux_streams if tag.split("@", 1)[0] in SILENT_AUX_STREAMS
    )


def _build_data_ingestion(raw: Mapping[str, Any]) -> DataIngestionSettings:
    defaults = DataIngestionSettings()
    exchange = _choice(
        raw.get("exchange", defaults.exchange),
        "data_ingestion.exchange",
        tuple(EXCHANGE_ENDPOINTS),
    )
    base_url = str(raw.get("ws_base_url", defaults.ws_base_url) or "").strip()
    if base_url and not base_url.lower().startswith(("ws://", "wss://")):
        raise ConfigError(
            "'data_ingestion.ws_base_url' must start with ws:// or wss://: "
            f"{base_url!r}"
        )
    storage_path = str(raw.get("storage_path", defaults.storage_path)).strip()
    if not storage_path:
        raise ConfigError("'data_ingestion.storage_path' must not be empty")
    base_delay = _float(
        raw.get("reconnect_base_delay_s", defaults.reconnect_base_delay_s),
        "data_ingestion.reconnect_base_delay_s",
        minimum=0.001,
    )
    max_delay = _float(
        raw.get("reconnect_max_delay_s", defaults.reconnect_max_delay_s),
        "data_ingestion.reconnect_max_delay_s",
        minimum=base_delay,
    )
    return DataIngestionSettings(
        enabled=bool(raw.get("enabled", defaults.enabled)),
        exchange=exchange,
        symbol=_pattern(
            raw.get("symbol", defaults.symbol),
            "data_ingestion.symbol",
            r"[a-z0-9]{2,20}",
            "must be 2-20 alphanumerics (e.g. btcusdt)",
        ),
        stream_type=_pattern(
            raw.get("stream_type", defaults.stream_type),
            "data_ingestion.stream_type",
            r"[a-z0-9]{1,20}(@[a-z0-9]{1,12})?",
            "must look like 'depth@100ms'",
        ),
        aux_streams=parse_stream_names(
            raw.get("aux_streams", defaults.aux_streams),
            "data_ingestion.aux_streams",
        ),
        storage_path=storage_path,
        compression=_choice(
            raw.get("compression", defaults.compression),
            "data_ingestion.compression",
            COMPRESSION_CHOICES,
        ),
        compression_level=_int(
            raw.get("compression_level", defaults.compression_level),
            "data_ingestion.compression_level",
            minimum=1,
            maximum=9,
        ),
        flush_interval_ms=_int(
            raw.get("flush_interval_ms", defaults.flush_interval_ms),
            "data_ingestion.flush_interval_ms",
            minimum=1,
        ),
        max_file_mb=_int(
            raw.get("max_file_mb", defaults.max_file_mb),
            "data_ingestion.max_file_mb",
            minimum=0,
        ),
        rotate_seconds=_int(
            raw.get("rotate_seconds", defaults.rotate_seconds),
            "data_ingestion.rotate_seconds",
            minimum=0,
        ),
        telemetry_interval_s=_float(
            raw.get("telemetry_interval_s", defaults.telemetry_interval_s),
            "data_ingestion.telemetry_interval_s",
            minimum=0.5,
        ),
        latency_warn_ms=_float(
            raw.get("latency_warn_ms", defaults.latency_warn_ms),
            "data_ingestion.latency_warn_ms",
            minimum=0.001,
        ),
        reconnect_base_delay_s=base_delay,
        reconnect_max_delay_s=max_delay,
        max_reconnect_attempts=_int(
            raw.get("max_reconnect_attempts", defaults.max_reconnect_attempts),
            "data_ingestion.max_reconnect_attempts",
            minimum=0,
        ),
        ws_base_url=base_url,
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
    pretrade_raw = _mapping(engine_raw.get("pretrade"), "engine.pretrade")
    strategy_raw = _mapping(document.get("strategy"), "strategy")
    backtest_raw = _mapping(document.get("backtest"), "backtest")
    kill_switch_raw = _mapping(document.get("kill_switch"), "kill_switch")
    ingestion_raw = _mapping(document.get("data_ingestion"), "data_ingestion")

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
        pretrade=_build_pretrade(pretrade_raw),
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
    return AppSettings(engine=engine, strategy=strategy, backtest=backtest,
                       kill_switch=_build_kill_switch(kill_switch_raw),
                       data_ingestion=_build_data_ingestion(ingestion_raw))


def as_dict(settings: AppSettings) -> Dict[str, Any]:
    """Serialises settings for logging or report artefacts."""
    return {
        "engine": {
            "symbol": settings.engine.symbol,
            "tick_size": settings.engine.tick_size,
            "center": settings.engine.center,
            "stale_after_ns": settings.engine.stale_after_ns,
            "risk": settings.engine.risk.__dict__,
            "pretrade": settings.engine.pretrade.__dict__,
        },
        "strategy": settings.strategy.__dict__,
        "backtest": settings.backtest.__dict__,
        "kill_switch": settings.kill_switch.__dict__,
        "data_ingestion": {
            **settings.data_ingestion.__dict__,
            "stream_url": settings.data_ingestion.stream_url,
            "storage_dir": str(settings.data_ingestion.storage_dir),
        },
    }
