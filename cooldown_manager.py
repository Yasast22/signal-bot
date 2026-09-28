"""
Persistent SQLite Cooldown and Signal History Manager.

Prevents alert spamming by tracking per-symbol cooldowns across bot restarts.
Maintains a full audit trail of past signals for quantitative performance analysis.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from analyzer import TradingSignal

logger = logging.getLogger(__name__)


class CooldownManager:
    """
    Thread-safe SQLite-backed signal cooldown and history manager.
    """

    def __init__(self, db_path: str = "data/signals.db") -> None:
        self.db_path = Path(db_path)
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_database()

    def _get_connection(self) -> sqlite3.Connection:
        """Create a connection with timeout and WAL mode enabled for concurrency."""
        conn = sqlite3.connect(str(self.db_path), timeout=20.0)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for high-concurrency read/write operations
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        return conn

    def _init_database(self) -> None:
        """Initialize database schema with tables and indexes."""
        with self._lock, self._get_connection() as conn:
            cursor = conn.cursor()

            # Active cooldowns table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS cooldowns (
                    symbol TEXT NOT NULL,
                    market TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    last_triggered_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (symbol, market, signal_type)
                );
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_cooldown_expires ON cooldowns (expires_at);"
            )

            # Registered bot subscribers table (for multi-user support)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id TEXT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    joined_at TEXT NOT NULL,
                    is_active INTEGER DEFAULT 1
                );
                """
            )

            # Signal history log table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    market TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    price REAL NOT NULL,
                    rsi REAL NOT NULL,
                    ema_200 REAL NOT NULL,
                    volume REAL NOT NULL,
                    volume_ma REAL NOT NULL,
                    volume_ratio REAL NOT NULL,
                    distance_to_ema_pct REAL NOT NULL,
                    timeframe TEXT NOT NULL,
                    candle_time TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_symbol ON signal_history (symbol, market);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_created ON signal_history (created_at);"
            )
            conn.commit()

        logger.info("Initialized Cooldown SQLite database at %s", self.db_path)

    def is_on_cooldown(self, symbol: str, market: str, signal_type: str) -> bool:
        """
        Check if an active cooldown exists for the specified symbol, market, and signal type.

        Args:
            symbol: e.g. 'BTCUSDT'
            market: e.g. 'SPOT' or 'FUTURES'
            signal_type: e.g. 'SPOT_BUY' or 'FUTURES_LONG'

        Returns:
            True if currently in cooldown, False otherwise.
        """
        now_ts = datetime.now(timezone.utc).timestamp()
        with self._lock, self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT expires_at FROM cooldowns
                WHERE symbol = ? AND market = ? AND signal_type = ?
                """,
                (symbol, market, signal_type),
            )
            row = cursor.fetchone()
            if row and row["expires_at"] > now_ts:
                return True
        return False

    def record_signal(self, signal: TradingSignal, cooldown_hours: float) -> None:
        """
        Record a triggered signal, update its cooldown expiration, and log to history.

        Args:
            signal: Validated TradingSignal object.
            cooldown_hours: Cooldown window in hours.
        """
        now = datetime.now(timezone.utc)
        now_ts = now.timestamp()
        expires_at = now_ts + (cooldown_hours * 3600.0)

        with self._lock, self._get_connection() as conn:
            cursor = conn.cursor()

            # Upsert cooldown record
            cursor.execute(
                """
                INSERT INTO cooldowns (symbol, market, signal_type, last_triggered_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol, market, signal_type) DO UPDATE SET
                    last_triggered_at = excluded.last_triggered_at,
                    expires_at = excluded.expires_at
                """,
                (
                    signal.symbol,
                    signal.market.value,
                    signal.signal_type.value,
                    now_ts,
                    expires_at,
                ),
            )

            # Insert history audit record
            cursor.execute(
                """
                INSERT INTO signal_history (
                    symbol, market, signal_type, price, rsi, ema_200,
                    volume, volume_ma, volume_ratio, distance_to_ema_pct,
                    timeframe, candle_time, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.symbol,
                    signal.market.value,
                    signal.signal_type.value,
                    signal.price,
                    signal.rsi,
                    signal.ema_200,
                    signal.volume,
                    signal.volume_ma,
                    signal.volume_ratio,
                    signal.distance_to_ema_pct,
                    signal.timeframe,
                    signal.candle_time.isoformat(),
                    now.isoformat(),
                ),
            )
            conn.commit()

        logger.debug(
            "Registered cooldown for %s [%s] until %s",
            signal.symbol,
            signal.signal_type.value,
            datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(),
        )

    def get_signal_stats(self) -> dict:
        """Retrieve total signal counts and active cooldown counts."""
        now_ts = datetime.now(timezone.utc).timestamp()
        with self._lock, self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) AS total_history FROM signal_history"
            )
            total_history = cursor.fetchone()["total_history"]

            cursor.execute(
                "SELECT COUNT(*) AS active_cooldowns FROM cooldowns WHERE expires_at > ?",
                (now_ts,),
            )
            active_cooldowns = cursor.fetchone()["active_cooldowns"]

            # Today's signal count
            today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            cursor.execute(
                "SELECT COUNT(*) AS today_count FROM signal_history WHERE created_at LIKE ?",
                (f"{today_prefix}%",),
            )
            today_count = cursor.fetchone()["today_count"]

        return {
            "total_signals_recorded": total_history,
            "active_cooldowns": active_cooldowns,
            "today_signals": today_count,
        }

    def cleanup_old_records(self, days: int = 7) -> int:
        """
        Delete expired cooldowns and prune old history beyond retention period.

        Args:
            days: Retention period in days.

        Returns:
            Number of rows cleaned.
        """
        now_ts = datetime.now(timezone.utc).timestamp()
        cutoff_ts = now_ts - (days * 86400.0)
        cutoff_iso = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).isoformat()

        with self._lock, self._get_connection() as conn:
            cursor = conn.cursor()
            # Clean expired cooldowns
            cursor.execute("DELETE FROM cooldowns WHERE expires_at < ?", (now_ts,))
            cleaned_cooldowns = cursor.rowcount

            # Clean old history records
            cursor.execute("DELETE FROM signal_history WHERE created_at < ?", (cutoff_iso,))
            cleaned_history = cursor.rowcount

            conn.commit()

        total = cleaned_cooldowns + cleaned_history
        if total > 0:
            logger.info("Cleaned up %d expired cooldowns and old signal records", total)
        return total

    def add_subscriber(self, chat_id: str, username: str = "", first_name: str = "") -> None:
        """Register or reactivate a Telegram subscriber."""
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock, self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO subscribers (chat_id, username, first_name, joined_at, is_active)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(chat_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    is_active = 1
                """,
                (chat_id, username, first_name, now_iso),
            )
            conn.commit()

    def get_active_subscribers(self) -> list[str]:
        """Return all active subscriber chat IDs."""
        with self._lock, self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id FROM subscribers WHERE is_active = 1")
            rows = cursor.fetchall()
            return [str(row["chat_id"]) for row in rows]

    def unsubscribe(self, chat_id: str) -> None:
        """Deactivate automated broadcast alerts for a subscriber."""
        with self._lock, self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE subscribers SET is_active = 0 WHERE chat_id = ?", (chat_id,))
            conn.commit()
