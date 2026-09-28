"""
Telegram Notifier & Interactive Bot Module for Real-Time Crypto Signals.

Features:
- Premium HTML formatting with distinct badges for SPOT vs FUTURES
- All timestamps presented in Sri Lankan Standard Time (Asia/Colombo, UTC+5:30)
- Multi-factor technical indicator breakdown (Price, EMA-200, RSI-14, Volume Ratio)
- Interactive on-demand analysis for any requested coin and timeframe (1m, 5m, 10m, 1h)
- Telegram Inline Keyboard buttons to quickly toggle timeframes and markets
- Long-polling command listener for real-time user requests
- Rate-limited message dispatching (prevents Telegram 429 errors)
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import html
import logging
from typing import Any, Callable, Coroutine, Optional

import aiohttp

from analyzer import MarketType, OnDemandResult, SignalType, TradingSignal
from config import BotConfig, SRI_LANKA_TZ

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """
    High-reliability Telegram notifier and interactive command listener.
    """

    TELEGRAM_API_BASE = "https://api.telegram.org/bot"

    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._queue: asyncio.Queue[tuple[str, Optional[dict], Optional[str]]] = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._last_update_id = 0

        # Multi-user subscriber hooks
        self.on_user_registered: Optional[Callable[[str, str, str], None]] = None
        self.get_subscribers: Optional[Callable[[], list[str]]] = None

        # Callback for handling user interactive queries (symbol, market, timeframe)
        self.on_user_query: Optional[
            Callable[[str, int, str, MarketType, str, Optional[str]], Coroutine[Any, Any, None]]
        ] = None

    @property
    def is_configured(self) -> bool:
        """Check if Telegram credentials are present."""
        return bool(self.config.telegram_bot_token and self.config.telegram_chat_id)

    async def start(self) -> None:
        """Start the background rate-limited message consumer and Telegram command listener."""
        if not self.is_configured:
            logger.warning("Telegram credentials not configured. Telegram alerts disabled.")
            return

        self._is_running = True
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout_seconds)
        )
        self._worker_task = asyncio.create_task(self._process_queue(), name="telegram_worker")
        self._listener_task = asyncio.create_task(self._poll_telegram_updates(), name="telegram_listener")
        logger.info("Telegram notifier and interactive listener started")

    async def stop(self) -> None:
        """Flush remaining messages and shut down consumer cleanly."""
        self._is_running = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        if self._worker_task:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        if self._session and not self._session.closed:
            await self._session.close()
            await asyncio.sleep(0.2)
            logger.info("Telegram notifier HTTP session closed")

    async def _send_raw_message(
        self,
        text: str,
        chat_id: Optional[str] = None,
        reply_markup: Optional[dict] = None,
        parse_mode: str = "HTML",
    ) -> bool:
        """Send a raw message to Telegram via HTTP POST."""
        if not self.is_configured or not self._session or self._session.closed:
            return False

        target_chat_id = chat_id or self.config.telegram_chat_id
        url = f"{self.TELEGRAM_API_BASE}{self.config.telegram_bot_token}/sendMessage"
        payload: dict[str, Any] = {
            "chat_id": target_chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        try:
            async with self._session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return True

                body = await resp.text()
                if resp.status == 429:
                    logger.warning("Telegram 429 rate limit hit: %s", body)
                    await asyncio.sleep(2.0)
                else:
                    logger.error("Telegram API error (status %d): %s", resp.status, body)
                    if parse_mode == "HTML" and "can't parse entities" in body.lower():
                        return await self._send_raw_message(
                            text, chat_id=target_chat_id, reply_markup=reply_markup, parse_mode=""
                        )
        except Exception as e:
            logger.error("Network error sending Telegram message: %s", e)

        return False

    async def edit_message_text(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        reply_markup: Optional[dict] = None,
        parse_mode: str = "HTML",
    ) -> bool:
        """Edit an existing Telegram message in-place (for seamless button clicking)."""
        if not self.is_configured or not self._session or self._session.closed:
            return False

        url = f"{self.TELEGRAM_API_BASE}{self.config.telegram_bot_token}/editMessageText"
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        try:
            async with self._session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return True
                body = await resp.text()
                # Ignore if content hasn't changed
                if "message is not modified" in body:
                    return True
                if parse_mode == "HTML" and "can't parse entities" in body.lower():
                    logger.info("Retrying editMessageText with plain text fallback...")
                    return await self.edit_message_text(
                        chat_id, message_id, text, reply_markup=reply_markup, parse_mode=""
                    )
                logger.warning("Failed to edit Telegram message: %s", body)
        except Exception as e:
            logger.debug("Error editing Telegram message: %s", e)
        return False

    async def answer_callback_query(self, callback_query_id: str, text: str = "") -> None:
        """Acknowledge Telegram callback query from inline button."""
        if not self.is_configured or not self._session or self._session.closed:
            return
        url = f"{self.TELEGRAM_API_BASE}{self.config.telegram_bot_token}/answerCallbackQuery"
        try:
            await self._session.post(
                url, json={"callback_query_id": callback_query_id, "text": text}
            )
        except Exception:
            pass

    async def _process_queue(self) -> None:
        """Continuous consumer task enforcing rate limits."""
        while self._is_running:
            try:
                item = await self._queue.get()
                text, markup, target_chat_id = item
                await self._send_raw_message(text, chat_id=target_chat_id, reply_markup=markup)
                self._queue.task_done()
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in Telegram message consumer: %s", e)
                await asyncio.sleep(0.5)

    def enqueue_message(
        self, text: str, reply_markup: Optional[dict] = None, chat_id: Optional[str] = None
    ) -> None:
        """Enqueue an alert for rate-limited dispatch."""
        if self.is_configured:
            self._queue.put_nowait((text, reply_markup, chat_id))

    # -----------------------------------------------------------------
    # Interactive Telegram Polling Listener
    # -----------------------------------------------------------------
    async def _poll_telegram_updates(self) -> None:
        """
        Long-polling loop listening for user messages and inline button clicks.
        """
        while self._is_running:
            try:
                if not self._session or self._session.closed:
                    await asyncio.sleep(1.0)
                    continue

                url = f"{self.TELEGRAM_API_BASE}{self.config.telegram_bot_token}/getUpdates"
                params = {
                    "offset": self._last_update_id + 1,
                    "timeout": 20,
                    "allowed_updates": ["message", "callback_query"],
                }

                async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(3.0)
                        continue

                    data = await resp.json()
                    if not data.get("ok") or not data.get("result"):
                        continue

                    for update in data["result"]:
                        self._last_update_id = update["update_id"]
                        await self._handle_single_update(update)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Telegram polling exception: %s", e)
                await asyncio.sleep(2.0)

    async def _handle_single_update(self, update: dict) -> None:
        """Route user text command or inline button callback."""
        if "callback_query" in update:
            cb = update["callback_query"]
            user_info = cb.get("from", {})
            cb_id = cb["id"]
            data = cb.get("data", "")
            message = cb.get("message", {})
            chat_id = str(message.get("chat", {}).get("id", ""))
            message_id = message.get("message_id", 0)

            # Auto-register subscriber
            if self.on_user_registered and chat_id and user_info:
                self.on_user_registered(
                    chat_id, user_info.get("username", ""), user_info.get("first_name", "")
                )

            # Parse callback data: e.g. "q:BTCUSDT:FUTURES:5m"
            if data.startswith("q:") and self.on_user_query:
                parts = data.split(":")
                if len(parts) >= 4:
                    sym = parts[1]
                    mkt = MarketType(parts[2])
                    tf = parts[3]
                    await self.answer_callback_query(cb_id, text=f"Analyzing {sym} ({tf})...")
                    await self.on_user_query(chat_id, message_id, sym, mkt, tf, cb_id)
            return

        if "message" in update:
            msg = update["message"]
            user_info = msg.get("from", {})
            chat_id = str(msg.get("chat", {}).get("id", ""))
            text = msg.get("text", "").strip()

            if not text:
                return

            # Auto-register subscriber
            if self.on_user_registered and chat_id and user_info:
                self.on_user_registered(
                    chat_id, user_info.get("username", ""), user_info.get("first_name", "")
                )

            if text.startswith("/start") or text.startswith("/help") or text.startswith("/menu"):
                await self._send_welcome_menu(chat_id)
                return

            # Parse user input like "BTC", "ETH 5m", "SOL 10m", "/signal DOGE 1h spot"
            clean_text = text.replace("/signal", "").replace("/analyze", "").strip()
            parts = clean_text.split()
            if not parts:
                return

            raw_sym = parts[0]
            tf = "5m"  # default on-demand timeframe
            mkt = MarketType.FUTURES  # default market

            for part in parts[1:]:
                p_lower = part.lower()
                if p_lower in ("1m", "5m", "10m", "15m", "1h", "4h"):
                    tf = p_lower
                elif p_lower in ("spot", "s"):
                    mkt = MarketType.SPOT
                elif p_lower in ("futures", "future", "f"):
                    mkt = MarketType.FUTURES

            if self.on_user_query:
                await self.on_user_query(chat_id, 0, raw_sym, mkt, tf, None)

    async def _send_welcome_menu(self, chat_id: str) -> None:
        """Send interactive menu with quick-select buttons."""
        msg = (
            "🤖 <b>BINANCE QUANTITATIVE TRADING BOT</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🇱🇰 <b>Sri Lankan Standard Time:</b> Active (UTC+5:30)\n"
            "📊 <b>Supported Timeframes:</b> <code>1m</code>, <code>5m</code>, <code>10m</code>, <code>1h</code>\n"
            "🎯 <b>Markets:</b> Spot & USDⓈ-M Perpetual Futures\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "<b>💡 How to Query Any Coin:</b>\n"
            "• Type just the coin name: <code>BTC</code> or <code>SOL</code>\n"
            "• Specify timeframe: <code>ETH 5m</code> or <code>BNB 10m</code>\n"
            "• Specify market: <code>DOGE 1m spot</code> or <code>BTC 1h futures</code>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "👇 <b>Select a Quick Coin to Analyze:</b>"
        )
        markup = {
            "inline_keyboard": [
                [
                    {"text": "🪙 Bitcoin (BTC)", "callback_data": "q:BTCUSDT:FUTURES:5m"},
                    {"text": "🪙 Ethereum (ETH)", "callback_data": "q:ETHUSDT:FUTURES:5m"},
                ],
                [
                    {"text": "🪙 Solana (SOL)", "callback_data": "q:SOLUSDT:FUTURES:5m"},
                    {"text": "🪙 Binance Coin (BNB)", "callback_data": "q:BNBUSDT:FUTURES:5m"},
                ],
                [
                    {"text": "🪙 Ripple (XRP)", "callback_data": "q:XRPUSDT:FUTURES:5m"},
                    {"text": "🪙 Dogecoin (DOGE)", "callback_data": "q:DOGEUSDT:FUTURES:5m"},
                ],
            ]
        }
        await self._send_raw_message(msg, chat_id=chat_id, reply_markup=markup)

    # -----------------------------------------------------------------
    # Signal Alert Formatter (Sri Lankan Standard Time)
    # -----------------------------------------------------------------
    @staticmethod
    def _format_price(price: float) -> str:
        """Format price with appropriate decimal precision."""
        if price >= 1000.0:
            return f"${price:,.2f}"
        elif price >= 1.0:
            return f"${price:,.4f}"
        elif price >= 0.01:
            return f"${price:,.5f}"
        elif price >= 0.0001:
            return f"${price:,.6f}"
        else:
            return f"${price:,.8f}"

    def build_timeframe_keyboard(self, symbol: str, current_market: MarketType, current_tf: str) -> dict:
        """Build interactive inline keyboard for toggling timeframe & market."""
        other_market = MarketType.SPOT if current_market == MarketType.FUTURES else MarketType.FUTURES
        market_label = "🟢 Switch to SPOT" if current_market == MarketType.FUTURES else "🚀 Switch to FUTURES"

        tfs = ["1m", "5m", "10m", "1h"]
        tf_buttons = []
        for tf in tfs:
            btn_text = f"✅ {tf}" if tf == current_tf else tf
            tf_buttons.append({
                "text": btn_text,
                "callback_data": f"q:{symbol}:{current_market.value}:{tf}",
            })

        return {
            "inline_keyboard": [
                tf_buttons,
                [
                    {
                        "text": market_label,
                        "callback_data": f"q:{symbol}:{other_market.value}:{current_tf}",
                    },
                    {
                        "text": "🔄 Refresh Price",
                        "callback_data": f"q:{symbol}:{current_market.value}:{current_tf}",
                    },
                ],
            ]
        }

    def format_on_demand_message(self, res: OnDemandResult) -> tuple[str, dict]:
        """
        Build rich HTML card for on-demand coin analysis in Sri Lankan Time.
        """
        clean_symbol = res.symbol.upper()
        if res.market == MarketType.FUTURES:
            trade_url = f"https://www.binance.com/en/futures/{clean_symbol}"
            mkt_badge = "🚀 [USDⓈ-M FUTURES]"
        else:
            base_coin = clean_symbol.replace(self.config.quote_asset, "")
            trade_url = f"https://www.binance.com/en/trade/{base_coin}_{self.config.quote_asset}?type=spot"
            mkt_badge = "🟢 [BINANCE SPOT]"

        formatted_price = self._format_price(res.current_price)
        formatted_entry = self._format_price(res.entry_price)
        formatted_tp1 = self._format_price(res.tp1_price)
        formatted_tp2 = self._format_price(res.tp2_price)
        formatted_tp3 = self._format_price(res.tp3_price)
        formatted_sl = self._format_price(res.stop_loss_price)
        formatted_ema = self._format_price(res.ema_200)
        formatted_sup = self._format_price(res.support_price)
        formatted_res = self._format_price(res.resistance_price)

        dist_sign = "+" if res.distance_to_ema_pct >= 0 else ""
        dist_str = f"{dist_sign}{res.distance_to_ema_pct:.2f}%"

        # EMA trend description
        if res.ema_9 > 0 and res.ema_21 > 0:
            ema_trend = "🟢 Bullish (EMA 9 ≥ 21)" if res.ema_9 >= res.ema_21 else "🔴 Bearish (EMA 9 ≤ 21)"
        else:
            ema_trend = f"200 EMA: {formatted_ema} ({dist_str})"

        # MACD description
        if res.macd_hist != 0.0:
            macd_desc = f"🟢 Bullish ({res.macd_hist:+.4f})" if res.macd_hist > 0 else f"🔴 Bearish ({res.macd_hist:+.4f})"
        else:
            macd_desc = "Neutral / Flat"

        # RSI analysis
        if res.rsi <= 25.0:
            rsi_desc = f"Extreme Oversold ({res.rsi:.1f} ≤ 25)"
        elif res.rsi <= 35.0:
            rsi_desc = f"Oversold Dip ({res.rsi:.1f} ≤ 35)"
        elif res.rsi >= 75.0:
            rsi_desc = f"Extreme Overbought ({res.rsi:.1f} ≥ 75)"
        elif res.rsi >= 65.0:
            rsi_desc = f"Overbought Peak ({res.rsi:.1f} ≥ 65)"
        elif res.rsi >= 50.0:
            rsi_desc = f"Bullish Zone ({res.rsi:.1f})"
        else:
            rsi_desc = f"Bearish Zone ({res.rsi:.1f})"

        vol_status = "Confirmed (High)" if res.volume_ratio >= 1.0 else "Normal/Low"

        clean_action_badge = html.escape(res.action_badge)
        clean_verdict_title = html.escape(res.verdict_title).replace("&amp;", "&")

        msg = (
            f"🎯 <b>{mkt_badge} SIGNAL SETUP</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>Coin:</b> #{html.escape(clean_symbol)}\n"
            f"⚡ <b>ACTION:</b> <b>{clean_action_badge}</b>\n"
            f"💵 <b>Current Live Price:</b> <code>{formatted_price}</code>\n"
            f"⏱ <b>Timeframe:</b> <code>{html.escape(res.timeframe)}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>TRADE SETUP (Targets &amp; Stop Loss):</b>\n"
            f"• 📍 <b>Entry Price:</b> <code>{formatted_entry}</code>\n"
            f"• 🎯 <b>Take-Profit 1:</b> <code>{formatted_tp1}</code> (+{res.tp1_pct:.2f}%)\n"
            f"• 🎯 <b>Take-Profit 2:</b> <code>{formatted_tp2}</code> (+{res.tp2_pct:.2f}%)\n"
            f"• 🎯 <b>Take-Profit 3:</b> <code>{formatted_tp3}</code> (+{res.tp3_pct:.2f}%)\n"
            f"• 🛑 <b>Stop-Loss:</b> <code>{formatted_sl}</code> (-{res.sl_pct:.2f}%)\n"
            f"• ⚖️ <b>Risk / Reward:</b> <code>{res.risk_reward_ratio}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Technical Indicators Confluence:</b>\n"
            f"• 📈 <b>Momentum EMAs:</b> {ema_trend}\n"
            f"• 🌊 <b>MACD (12,26,9):</b> {macd_desc}\n"
            f"• ⚡ <b>14 RSI:</b> <code>{res.rsi:.1f}</code> — {rsi_desc}\n"
            f"• 🔊 <b>Volume Ratio:</b> <code>{res.volume_ratio:.2f}x</code> ({vol_status})\n"
            f"• 🚦 <b>Condition:</b> {clean_verdict_title}\n"
            f"• 🛡 <b>Key Levels:</b> Support: <code>{formatted_sup}</code> | Res: <code>{formatted_res}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🇱🇰 <b>Sri Lankan Time:</b> <code>{res.sri_lanka_time_str}</code>\n"
            f"🔗 <a href=\"{trade_url}\"><b>Open {res.market.value} Chart on Binance</b></a>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>👇 Tap a button below to change timeframe or market:</i>"
        )

        markup = self.build_timeframe_keyboard(res.symbol, res.market, res.timeframe)
        return msg, markup

    def format_signal_message(self, signal: TradingSignal) -> str:
        """
        Build rich HTML formatted alert with Sri Lankan Standard Time.
        """
        if signal.signal_type == SignalType.FUTURES_LONG:
            header_badge = "🚀 <b>[USDⓈ-M FUTURES]</b> 🚀"
            action_badge = "🟢 <b>ACTION: FUTURES LONG (BUY)</b>"
            trend_desc = "Uptrend Confirmation"
        elif signal.signal_type == SignalType.FUTURES_SHORT:
            header_badge = "🔻 <b>[USDⓈ-M FUTURES]</b> 🔻"
            action_badge = "🔴 <b>ACTION: FUTURES SHORT (SELL)</b>"
            trend_desc = "Downtrend Confirmation"
        elif signal.signal_type == SignalType.SPOT_BUY:
            header_badge = "🟢 <b>[BINANCE SPOT]</b> 🟢"
            action_badge = "💎 <b>ACTION: SPOT BUY (ACCUMULATE)</b>"
            trend_desc = "Uptrend Confirmation"
        else:  # SPOT_SELL
            header_badge = "🔴 <b>[BINANCE SPOT]</b> 🔴"
            action_badge = "⚠️ <b>ACTION: SPOT EXIT / TAKE-PROFIT</b>"
            trend_desc = "Downtrend Warning"

        clean_symbol = signal.symbol.upper()
        if signal.market == MarketType.FUTURES:
            trade_url = f"https://www.binance.com/en/futures/{clean_symbol}"
        else:
            base_coin = clean_symbol.replace(self.config.quote_asset, "")
            trade_url = f"https://www.binance.com/en/trade/{base_coin}_{self.config.quote_asset}?type=spot"

        formatted_price = self._format_price(signal.price)
        formatted_ema = self._format_price(signal.ema_200)

        dist_sign = "+" if signal.distance_to_ema_pct >= 0 else ""
        dist_str = f"{dist_sign}{signal.distance_to_ema_pct:.2f}%"

        if signal.rsi <= self.config.rsi_oversold:
            rsi_status = f"Oversold Dip ({signal.rsi:.1f} ≤ {self.config.rsi_oversold})"
        elif signal.rsi >= self.config.rsi_overbought:
            rsi_status = f"Overbought Peak ({signal.rsi:.1f} ≥ {self.config.rsi_overbought})"
        else:
            rsi_status = f"Neutral ({signal.rsi:.1f})"

        # Sri Lankan Time (Asia/Colombo UTC+5:30)
        candle_time_slt = signal.candle_time.astimezone(SRI_LANKA_TZ)
        slt_str = candle_time_slt.strftime("%Y-%m-%d %I:%M %p SLST")

        msg = (
            f"{header_badge}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>Pair:</b> #{html.escape(clean_symbol)}\n"
            f"⚡ {action_badge}\n"
            f"💵 <b>Current Price:</b> <code>{formatted_price}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Technical Analysis ({signal.timeframe}):</b>\n"
            f"• <b>200 EMA:</b> <code>{formatted_ema}</code> ({dist_str} | {trend_desc})\n"
            f"• <b>14 RSI:</b> <code>{signal.rsi:.1f}</code> — {rsi_status}\n"
            f"• <b>Volume Spike:</b> <code>{signal.volume_ratio:.2f}x</code> 20-period MA\n"
            f"• 🇱🇰 <b>Time (Sri Lanka):</b> <code>{slt_str}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 <a href=\"{trade_url}\"><b>Open {signal.market.value} Chart on Binance</b></a>\n"
            f"🤖 <i>Automated 24/7 Quantitative Signal Bot</i>"
        )
        return msg

    def send_signal_alert(self, signal: TradingSignal) -> None:
        """Format and enqueue a trading signal alert to all active subscribers."""
        message = self.format_signal_message(signal)
        recipients = self.get_subscribers() if self.get_subscribers else []
        if not recipients and self.config.telegram_chat_id:
            recipients = [self.config.telegram_chat_id]

        for cid in recipients:
            self.enqueue_message(message, chat_id=cid)

    def send_startup_alert(self, spot_count: int, futures_count: int) -> None:
        """Notify Telegram chat that the bot has started."""
        now_slt = datetime.now(SRI_LANKA_TZ).strftime("%Y-%m-%d %I:%M %p SLST")
        msg = (
            "🤖 <b>CRYPTO SIGNAL BOT ONLINE (24/7)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "✅ <b>Status:</b> Active & Scanning\n"
            f"📈 <b>Spot Pairs Monitored:</b> {spot_count}\n"
            f"🚀 <b>Futures Pairs Monitored:</b> {futures_count}\n"
            f"⏱ <b>Candle Timeframe:</b> {self.config.kline_timeframe}\n"
            f"⏳ <b>Scan Frequency:</b> Every {self.config.scan_interval_minutes}m\n"
            f"🛡 <b>Duplicate Cooldown:</b> {self.config.cooldown_hours}h\n"
            f"🇱🇰 <b>System Time:</b> <code>{now_slt}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "💬 <i>Tip: Send any coin name (e.g. <code>BTC</code> or <code>SOL 5m</code>) anytime to get an instant real-time analysis!</i>"
        )
        self.enqueue_message(msg)

    def send_shutdown_alert(self, reason: str = "Graceful shutdown") -> None:
        """Notify Telegram chat that the bot is stopping."""
        now_slt = datetime.now(SRI_LANKA_TZ).strftime("%Y-%m-%d %I:%M %p SLST")
        msg = (
            "🛑 <b>CRYPTO SIGNAL BOT STOPPED</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Reason:</b> {html.escape(reason)}\n"
            f"🇱🇰 <b>Time:</b> <code>{now_slt}</code>\n"
            "<i>Market scanning has ceased.</i>"
        )
        self.enqueue_message(msg)

    def send_heartbeat_alert(
        self,
        uptime_str: str,
        spot_count: int,
        futures_count: int,
        today_signals: int,
        active_cooldowns: int,
    ) -> None:
        """Send periodic health and performance statistics in Sri Lankan Time."""
        now_slt = datetime.now(SRI_LANKA_TZ).strftime("%Y-%m-%d %I:%M %p SLST")
        msg = (
            "💓 <b>SYSTEM HEARTBEAT & HEALTH REPORT</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱ <b>Uptime:</b> {uptime_str}\n"
            f"📡 <b>Monitored Pairs:</b> {spot_count} Spot / {futures_count} Futures\n"
            f"🎯 <b>Signals Generated Today:</b> {today_signals}\n"
            f"⏳ <b>Active Cooldowns:</b> {active_cooldowns}\n"
            f"🇱🇰 <b>Sri Lankan Time:</b> <code>{now_slt}</code>\n"
            "🟢 <b>Status:</b> All systems operational\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "<i>24/7 Quantitative Engine Running Smoothly</i>"
        )
        self.enqueue_message(msg)
