"""
Main 24/7 Cryptocurrency Trading Signal Generator Worker Bot.

Orchestrates:
1. Dynamic pair discovery from Binance Spot & USDⓈ-M Futures
2. High-speed concurrent market data fetching via AsyncIO
3. Multi-factor quantitative technical analysis (200 EMA + 14 RSI + Volume Spike)
4. Cooldown duplicate prevention with SQLite persistence
5. Rich Telegram notifications with distinct Spot vs Futures alerts
6. Graceful shutdown handling (SIGINT/SIGTERM) for cloud container deployments
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from aiohttp import web

from analyzer import MarketAnalyzer, MarketType, TradingSignal
from binance_client import BinanceAsyncClient
from config import BotConfig, get_config
from cooldown_manager import CooldownManager
from telegram_notifier import TelegramNotifier

# Ensure UTF-8 output encoding across Windows and POSIX terminals
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("CryptoSignalBot")


class CryptoSignalBot:
    """
    Production-grade 24/7 automated cryptocurrency signal generator.
    """

    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.client = BinanceAsyncClient(config)
        self.analyzer = MarketAnalyzer(config)
        self.cooldown_mgr = CooldownManager(config.database_path)
        self.notifier = TelegramNotifier(config)

        self._shutdown_event = asyncio.Event()
        self._start_time = datetime.now(timezone.utc)
        self._last_heartbeat = time.time()
        self._last_cleanup = time.time()

        # Cache symbol lists across cycles to avoid redundant exchangeInfo requests
        self._cached_spot_symbols: list[str] = []
        self._cached_futures_symbols: list[str] = []
        self._symbols_last_updated = 0.0

        # Lightweight HTTP server for Render and UptimeRobot keep-alive
        self._http_runner: Optional[web.AppRunner] = None
        self._http_site: Optional[web.TCPSite] = None

    async def initialize(self) -> None:
        """Initialize components and background workers."""
        logger.info("Initializing CryptoSignalBot...")
        validation_errors = self.config.validate()
        for err in validation_errors:
            logger.warning("Config Notice: %s", err)

        # Wire up interactive Telegram listener & multi-user subscriber store
        self.notifier.on_user_query = self.handle_user_query
        self.notifier.on_user_registered = self.cooldown_mgr.add_subscriber
        self.notifier.get_subscribers = self.cooldown_mgr.get_active_subscribers
        if self.config.telegram_chat_id:
            self.cooldown_mgr.add_subscriber(self.config.telegram_chat_id, "admin", "Admin")

        await self.notifier.start()
        await self._start_health_server()
        logger.info("Bot configuration loaded. Timeframe: %s", self.config.kline_timeframe)

    async def _start_health_server(self) -> None:
        """
        Start lightweight HTTP health server for Render Free Web Service & UptimeRobot keep-alive.
        Render injects $PORT (default 8080).
        """
        port_str = os.environ.get("PORT", "8080")
        try:
            port = int(port_str)
        except ValueError:
            port = 8080

        app = web.Application()

        async def handle_index(request: web.Request) -> web.Response:
            uptime = int(time.time() - self._start_time.timestamp())
            data = {
                "status": "online",
                "bot": "Binance 24/7 Quantitative Signal Bot",
                "version": "2.0.0",
                "uptime_seconds": uptime,
                "monitored_pairs": {
                    "spot": len(self._cached_spot_symbols),
                    "futures": len(self._cached_futures_symbols),
                },
                "subscribers": len(self.cooldown_mgr.get_active_subscribers()),
                "time_utc": datetime.now(timezone.utc).isoformat(),
            }
            return web.json_response(data)

        async def handle_health(request: web.Request) -> web.Response:
            return web.Response(text="OK - 24/7 Signal Bot Active", status=200, content_type="text/plain")

        app.router.add_get("/", handle_index)
        app.router.add_get("/health", handle_health)
        app.router.add_get("/ping", handle_health)

        self._http_runner = web.AppRunner(app)
        await self._http_runner.setup()
        self._http_site = web.TCPSite(self._http_runner, "0.0.0.0", port)
        try:
            await self._http_site.start()
            logger.info("Health check HTTP server started on 0.0.0.0:%d (Ready for Render & UptimeRobot)", port)
        except OSError as err:
            logger.warning("Could not bind HTTP server to port %d (%s). Continuing...", port, err)

    async def handle_user_query(
        self,
        chat_id: str,
        message_id: int,
        raw_symbol: str,
        market: MarketType,
        timeframe: str,
        callback_query_id: Optional[str] = None,
    ) -> None:
        """
        Handle interactive on-demand coin analysis requested by user via Telegram.
        """
        symbol = self.client.normalize_symbol(raw_symbol)
        logger.info(
            "Processing on-demand query: %s [%s] on %s timeframe (chat_id: %s)",
            symbol,
            market.value,
            timeframe,
            chat_id,
        )

        try:
            # 1. Fetch live real-time ticker price
            live_price = await self.client.get_live_price(symbol, market)

            # 2. Fetch klines for requested timeframe
            klines_df = await self.client.fetch_klines(
                symbol=symbol,
                market=market,
                interval=timeframe,
                limit=self.config.kline_limit,
            )

            if klines_df is None or klines_df.empty:
                error_msg = (
                    f"⚠️ <b>{html.escape(symbol)} is not available on Binance {market.value}</b>\n"
                    f"<i>(Note: This coin might only exist on the other market).</i>"
                )
                fallback_markup = self.notifier.build_timeframe_keyboard(symbol, market, timeframe)
                if callback_query_id:
                    await self.notifier.answer_callback_query(
                        callback_query_id, text=f"⚠️ {symbol} not available on {market.value}!"
                    )
                if message_id > 0:
                    await self.notifier.edit_message_text(chat_id, message_id, error_msg, reply_markup=fallback_markup)
                else:
                    await self.notifier._send_raw_message(error_msg, chat_id=chat_id, reply_markup=fallback_markup)
                return

            # 3. Perform detailed on-demand analysis in Sri Lankan Time
            analysis_result = self.analyzer.analyze_on_demand(
                symbol=symbol,
                market=market,
                timeframe=timeframe,
                klines_df=klines_df,
                live_price=live_price,
            )

            if analysis_result is None:
                error_msg = (
                    f"⚠️ <b>Insufficient candle history on {html.escape(timeframe)} for {html.escape(symbol)}</b>\n"
                    f"Need at least 220 candles to compute 200 EMA."
                )
                fallback_markup = self.notifier.build_timeframe_keyboard(symbol, market, timeframe)
                if message_id > 0:
                    await self.notifier.edit_message_text(chat_id, message_id, error_msg, reply_markup=fallback_markup)
                else:
                    await self.notifier._send_raw_message(error_msg, chat_id=chat_id, reply_markup=fallback_markup)
                return

            # 4. Format rich HTML card and inline buttons
            msg_text, markup = self.notifier.format_on_demand_message(analysis_result)

            # 5. If it was an inline button click, edit existing message; else send new message
            if message_id > 0:
                await self.notifier.edit_message_text(chat_id, message_id, msg_text, reply_markup=markup)
            else:
                await self.notifier._send_raw_message(msg_text, chat_id=chat_id, reply_markup=markup)

        except Exception as e:
            logger.error("Error processing user query for %s: %s", symbol, e, exc_info=True)
            await self.notifier._send_raw_message(
                f"❌ Error analyzing {html.escape(symbol)}: {html.escape(str(e))}", chat_id=chat_id
            )

    async def shutdown(self, reason: str = "User termination") -> None:
        """Trigger graceful shutdown across all modules."""
        logger.info("Initiating graceful shutdown (%s)...", reason)
        self._shutdown_event.set()

        try:
            self.notifier.send_shutdown_alert(reason=reason)
            await self.notifier.stop()
        except Exception as e:
            logger.error("Error shutting down notifier: %s", e)

        try:
            await self.client.close()
        except Exception as e:
            logger.error("Error closing Binance client: %s", e)

        if self._http_runner:
            try:
                await self._http_runner.cleanup()
                logger.info("Health check HTTP server stopped")
            except Exception as e:
                logger.debug("Error stopping HTTP server: %s", e)

        logger.info("CryptoSignalBot shutdown complete. Goodbye!")

    async def _update_trading_pairs(self) -> tuple[list[str], list[str]]:
        """
        Refresh active USDT pairs from Binance or apply custom watchlist.
        """
        # If user configured a specific watchlist in .env, honor it directly
        if self.config.watchlist:
            spot_list = [self.client.normalize_symbol(s) for s in self.config.watchlist] if self.config.enable_spot else []
            futures_list = [self.client.normalize_symbol(s) for s in self.config.watchlist] if self.config.enable_futures else []
            return spot_list, futures_list

        now = time.time()
        # Refresh pair list every 6 hours (21600 seconds)
        if self._cached_spot_symbols and self._cached_futures_symbols and (now - self._symbols_last_updated < 21600):
            return self._cached_spot_symbols, self._cached_futures_symbols

        spot_symbols: list[str] = []
        futures_symbols: list[str] = []

        if self.config.enable_spot:
            raw_spot = await self.client.get_spot_symbols()
            spot_symbols = await self.client.filter_by_24h_volume(
                raw_spot, MarketType.SPOT, self.config.min_24h_volume_usdt
            )

        if self.config.enable_futures:
            raw_futures = await self.client.get_futures_symbols()
            futures_symbols = await self.client.filter_by_24h_volume(
                raw_futures, MarketType.FUTURES, self.config.min_24h_volume_usdt
            )

        self._cached_spot_symbols = spot_symbols
        self._cached_futures_symbols = futures_symbols
        self._symbols_last_updated = now

        return spot_symbols, futures_symbols

    async def _scan_single_pair(
        self,
        symbol: str,
        market: MarketType,
    ) -> Optional[TradingSignal]:
        """
        Fetch klines and evaluate technical rules for a single pair.
        """
        try:
            klines_df = await self.client.fetch_klines(
                symbol=symbol,
                market=market,
                interval=self.config.kline_timeframe,
                limit=self.config.kline_limit,
            )
            if klines_df is None or klines_df.empty:
                return None

            return self.analyzer.analyze_pair(symbol, market, klines_df)
        except Exception as e:
            logger.debug("Error scanning pair %s [%s]: %s", symbol, market.value, e)
            return None

    async def run_scan_cycle(self) -> int:
        """
        Execute a full concurrent market scan cycle across Spot and Futures.

        Returns:
            Number of new signals triggered and alerted.
        """
        cycle_start = time.perf_counter()
        spot_symbols, futures_symbols = await self._update_trading_pairs()
        total_targets = len(spot_symbols) + len(futures_symbols)

        if total_targets == 0:
            logger.warning("No candidate pairs found to scan. Check network or volume thresholds.")
            return 0

        logger.info(
            "Starting market scan cycle across %d pairs (%d Spot, %d Futures)...",
            total_targets,
            len(spot_symbols),
            len(futures_symbols),
        )

        # Build async task list
        tasks = []
        for sym in spot_symbols:
            tasks.append(self._scan_single_pair(sym, MarketType.SPOT))
        for sym in futures_symbols:
            tasks.append(self._scan_single_pair(sym, MarketType.FUTURES))

        # Run concurrent analysis bounded by semaphore
        results = await asyncio.gather(*tasks, return_exceptions=True)

        new_signals: list[TradingSignal] = []
        suppressed_signals = 0

        for res in results:
            if isinstance(res, TradingSignal):
                # Verify cooldown status in persistent SQLite
                if self.cooldown_mgr.is_on_cooldown(
                    res.symbol, res.market.value, res.signal_type.value
                ):
                    suppressed_signals += 1
                    logger.debug(
                        "Signal %s [%s] is on cooldown; suppressed.",
                        res.symbol,
                        res.display_action,
                    )
                else:
                    new_signals.append(res)
                    # Register cooldown and log to database
                    self.cooldown_mgr.record_signal(res, self.config.cooldown_hours)
                    # Dispatch Telegram alert
                    self.notifier.send_signal_alert(res)

        elapsed = time.perf_counter() - cycle_start
        logger.info(
            "Scan completed in %.2fs. Analyzed: %d pairs | New Signals: %d | Cooldown Suppressed: %d",
            elapsed,
            total_targets,
            len(new_signals),
            suppressed_signals,
        )

        return len(new_signals)

    async def _check_heartbeat(self) -> None:
        """Send periodic Telegram heartbeat and health status."""
        now = time.time()
        interval_secs = self.config.heartbeat_interval_hours * 3600
        if now - self._last_heartbeat >= interval_secs:
            uptime_delta = datetime.now(timezone.utc) - self._start_time
            hours, remainder = divmod(int(uptime_delta.total_seconds()), 3600)
            minutes, _ = divmod(remainder, 60)
            uptime_str = f"{hours}h {minutes}m"

            stats = self.cooldown_mgr.get_signal_stats()
            self.notifier.send_heartbeat_alert(
                uptime_str=uptime_str,
                spot_count=len(self._cached_spot_symbols),
                futures_count=len(self._cached_futures_symbols),
                today_signals=stats["today_signals"],
                active_cooldowns=stats["active_cooldowns"],
            )
            self._last_heartbeat = now

    async def _check_db_cleanup(self) -> None:
        """Daily purge of expired database records."""
        now = time.time()
        if now - self._last_cleanup >= 86400:  # 24 hours
            self.cooldown_mgr.cleanup_old_records(days=7)
            self._last_cleanup = now

    async def run(self, once: bool = False) -> None:
        """
        Main 24/7 event loop.

        Args:
            once: If True, execute a single scan pass and exit (useful for testing or cron).
        """
        await self.initialize()

        # Initial pair discovery
        spot_syms, futures_syms = await self._update_trading_pairs()
        logger.info(
            "Bot started successfully! Monitored pairs: %d Spot, %d Futures.",
            len(spot_syms),
            len(futures_syms),
        )

        # Notify Telegram on startup
        self.notifier.send_startup_alert(len(spot_syms), len(futures_syms))

        # Single-run mode
        if once:
            logger.info("Executing single scan pass (--once mode)...")
            await self.run_scan_cycle()
            await self.shutdown(reason="Single pass completed")
            return

        # Continuous 24/7 loop
        scan_interval_secs = self.config.scan_interval_minutes * 60
        logger.info(
            "Entering 24/7 background worker loop. Interval: %d minutes.",
            self.config.scan_interval_minutes,
        )

        while not self._shutdown_event.is_set():
            loop_start = time.perf_counter()
            try:
                await self.run_scan_cycle()
                await self._check_heartbeat()
                await self._check_db_cleanup()
            except Exception as e:
                logger.error("Unexpected error in scan cycle: %s", e, exc_info=True)

            loop_elapsed = time.perf_counter() - loop_start
            sleep_time = max(1.0, scan_interval_secs - loop_elapsed)

            logger.info("Next market scan in %d seconds. Sleeping...", int(sleep_time))

            try:
                # Interruptible sleep allowing immediate shutdown on SIGINT/SIGTERM
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=sleep_time)
            except asyncio.TimeoutError:
                pass


async def main() -> None:
    """CLI entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="24/7 Production Crypto Trading Signal Generator Bot"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single market scan cycle and exit immediately (useful for testing)",
    )
    parser.add_argument(
        "--spot-only",
        action="store_true",
        help="Scan only Binance Spot market",
    )
    parser.add_argument(
        "--futures-only",
        action="store_true",
        help="Scan only Binance USDⓈ-M Futures market",
    )
    args = parser.parse_args()

    # Load configuration
    cfg = get_config()

    # Apply CLI overrides if present
    if args.spot_only:
        object.__setattr__(cfg, "enable_spot", True)
        object.__setattr__(cfg, "enable_futures", False)
    elif args.futures_only:
        object.__setattr__(cfg, "enable_spot", False)
        object.__setattr__(cfg, "enable_futures", True)

    bot = CryptoSignalBot(cfg)

    # Cross-platform signal handling (Windows + POSIX)
    loop = asyncio.get_running_loop()

    def handle_signal():
        asyncio.create_task(bot.shutdown(reason="System signal received"))

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except (NotImplementedError, AttributeError):
            # Fallback for OS environments where add_signal_handler is unsupported (e.g. Windows)
            signal.signal(sig, lambda s, f: asyncio.create_task(bot.shutdown("Signal caught")))

    try:
        await bot.run(once=args.once)
    except (asyncio.CancelledError, KeyboardInterrupt):
        await bot.shutdown(reason="Interrupted by user")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot terminated by user.")
