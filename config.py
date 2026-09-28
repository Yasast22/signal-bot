"""
Configuration Module for Binance 24/7 Trading Signal Generator Bot.

Loads environment variables from .env file, provides type validation,
and sets sensible defaults for technical indicators, rate limits, and alerting.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

# Base directory of the project
BASE_DIR = Path(__file__).resolve().parent

# Load environment variables from .env file
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Sri Lankan Standard Time (UTC+5:30)
SRI_LANKA_TZ = timezone(timedelta(hours=5, minutes=30), name="Asia/Colombo")


@dataclass(frozen=True)
class BotConfig:
    """Strongly typed application configuration."""

    # ---------------------------------------------------------
    # Authentication & API Keys
    # ---------------------------------------------------------
    binance_api_key: str = field(
        default_factory=lambda: os.getenv("BINANCE_API_KEY", "").strip()
    )
    binance_secret_key: str = field(
        default_factory=lambda: os.getenv("BINANCE_SECRET_KEY", "").strip()
    )

    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "").strip()
    )

    # ---------------------------------------------------------
    # Market & Watchlist Scanning Options
    # ---------------------------------------------------------
    enable_spot: bool = field(
        default_factory=lambda: os.getenv("ENABLE_SPOT", "true").lower() in ("true", "1", "yes")
    )
    enable_futures: bool = field(
        default_factory=lambda: os.getenv("ENABLE_FUTURES", "true").lower() in ("true", "1", "yes")
    )

    # Specific watchlist (e.g. "BTCUSDT,ETHUSDT,SOLUSDT"). If empty, scans all active pairs
    watchlist: list[str] = field(
        default_factory=lambda: [
            s.strip().upper()
            for s in os.getenv("WATCHLIST", "").split(",")
            if s.strip()
        ]
    )

    # Quote asset to filter trading pairs (default USDT)
    quote_asset: str = field(
        default_factory=lambda: os.getenv("QUOTE_ASSET", "USDT").strip().upper()
    )

    # Filter out dead / illiquid coins with 24h volume below this threshold (in USDT)
    # Set to 0.0 to scan every active trading pair without volume filtering
    min_24h_volume_usdt: float = field(
        default_factory=lambda: float(os.getenv("MIN_24H_VOLUME_USDT", "500000"))
    )

    # ---------------------------------------------------------
    # Technical Indicator Parameters
    # ---------------------------------------------------------
    kline_timeframe: str = field(
        default_factory=lambda: os.getenv("KLINE_TIMEFRAME", "1h").strip()
    )
    # Number of historical candles to fetch per pair. Must be >= 250 for EMA(200) precision
    kline_limit: int = field(
        default_factory=lambda: int(os.getenv("KLINE_LIMIT", "300"))
    )

    # RSI Settings
    rsi_period: int = field(
        default_factory=lambda: int(os.getenv("RSI_PERIOD", "14"))
    )
    rsi_oversold: float = field(
        default_factory=lambda: float(os.getenv("RSI_OVERSOLD", "30.0"))
    )
    rsi_overbought: float = field(
        default_factory=lambda: float(os.getenv("RSI_OVERBOUGHT", "70.0"))
    )

    # Trend Filter (EMA)
    ema_period: int = field(
        default_factory=lambda: int(os.getenv("EMA_PERIOD", "200"))
    )

    # Volume Moving Average Filter
    volume_ma_period: int = field(
        default_factory=lambda: int(os.getenv("VOLUME_MA_PERIOD", "20"))
    )

    # Analyze only closed candles to prevent repainting (True = candle[-2], False = live candle[-1])
    use_closed_candles_only: bool = field(
        default_factory=lambda: os.getenv("USE_CLOSED_CANDLES_ONLY", "true").lower() in ("true", "1", "yes")
    )

    # ---------------------------------------------------------
    # Signal Cooldown & Concurrency
    # ---------------------------------------------------------
    # Cooldown in hours before repeating the same signal for a coin
    cooldown_hours: float = field(
        default_factory=lambda: float(os.getenv("COOLDOWN_HOURS", "4.0"))
    )

    # Scanning loop interval in minutes (how often to run a complete market scan)
    scan_interval_minutes: int = field(
        default_factory=lambda: int(os.getenv("SCAN_INTERVAL_MINUTES", "15"))
    )

    # Max concurrent HTTP requests to avoid Binance IP rate limiting
    max_concurrent_requests: int = field(
        default_factory=lambda: int(os.getenv("MAX_CONCURRENT_REQUESTS", "20"))
    )

    # Request timeout in seconds
    request_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
    )

    # ---------------------------------------------------------
    # Operational & Storage
    # ---------------------------------------------------------
    # Path to SQLite database for cooldowns and signal history
    database_path: str = field(
        default_factory=lambda: os.getenv("DATABASE_PATH", str(BASE_DIR / "data" / "signals.db"))
    )

    # Heartbeat interval in hours (sends bot health status to Telegram)
    heartbeat_interval_hours: int = field(
        default_factory=lambda: int(os.getenv("HEARTBEAT_INTERVAL_HOURS", "12"))
    )

    # Logging level: DEBUG, INFO, WARNING, ERROR
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper()
    )

    def validate(self) -> list[str]:
        """Validate critical configuration settings and return list of warnings/errors."""
        errors: list[str] = []
        if not self.telegram_bot_token:
            errors.append("TELEGRAM_BOT_TOKEN is missing. Telegram alerts will be disabled.")
        if not self.telegram_chat_id:
            errors.append("TELEGRAM_CHAT_ID is missing. Telegram alerts will be disabled.")
        if not self.enable_spot and not self.enable_futures:
            errors.append("Both ENABLE_SPOT and ENABLE_FUTURES are disabled. Bot has no markets to scan.")
        if self.kline_limit < self.ema_period + 30:
            errors.append(
                f"KLINE_LIMIT ({self.kline_limit}) is too small for EMA_PERIOD ({self.ema_period}). "
                f"Set at least {self.ema_period + 50} for numerical convergence."
            )
        return errors


# Singleton instance helper
def get_config() -> BotConfig:
    """Return the instantiated BotConfig."""
    return BotConfig()
