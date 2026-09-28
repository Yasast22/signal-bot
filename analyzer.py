"""
Multi-Factor Signal Analyzer Module.

Applies quantitative trading rules across 1-hour candles:
1. Trend Filter: Price vs 200 EMA (Uptrend if Price > EMA200, Downtrend if Price < EMA200)
2. Momentum Filter: RSI(14) Oversold (< 30) for Long/Buy, Overbought (> 70) for Short/Sell
3. Volume Confirmation: Current Volume > 20-period Volume MA (Volume Ratio > 1.0)
4. Market Categorization: Explicitly distinguishes SPOT and FUTURES signals
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import pandas as pd

from config import BotConfig, SRI_LANKA_TZ
from indicators import enrich_klines

logger = logging.getLogger(__name__)


class MarketType(str, Enum):
    """Trading market category."""
    SPOT = "SPOT"
    FUTURES = "FUTURES"


class SignalType(str, Enum):
    """Trading signal action."""
    # Spot Actions
    SPOT_BUY = "SPOT_BUY"
    SPOT_SELL = "SPOT_SELL"
    # Futures Actions
    FUTURES_LONG = "FUTURES_LONG"
    FUTURES_SHORT = "FUTURES_SHORT"


@dataclass
class OnDemandResult:
    """Detailed technical analysis result for on-demand query of any coin."""
    symbol: str
    market: MarketType
    timeframe: str
    current_price: float
    ema_200: float
    rsi: float
    volume: float
    volume_ma: float
    volume_ratio: float
    distance_to_ema_pct: float
    action: str  # "LONG", "SHORT", or "WAIT"
    action_badge: str  # "🟢 LONG (BUY)" or "🔴 SHORT (SELL)"
    verdict: str  # "LONG", "SHORT", "BULLISH_BIAS", "BEARISH_BIAS", "NEUTRAL"
    verdict_title: str
    entry_price: float
    tp1_price: float
    tp2_price: float
    tp3_price: float
    stop_loss_price: float
    tp1_pct: float
    tp2_pct: float
    tp3_pct: float
    sl_pct: float
    risk_reward_ratio: str
    support_price: float
    resistance_price: float
    sri_lanka_time_str: str
    ema_9: float = 0.0
    ema_21: float = 0.0
    ema_50: float = 0.0
    macd: float = 0.0
    macd_signal: float = 0.0
    macd_hist: float = 0.0
    confluence_score: float = 0.0
    confluence_summary: str = ""
    entry_zone: str = ""


@dataclass
class TradingSignal:
    """Represents a validated multi-factor trading signal."""
    symbol: str
    market: MarketType
    signal_type: SignalType
    price: float
    rsi: float
    ema_200: float
    volume: float
    volume_ma: float
    volume_ratio: float
    distance_to_ema_pct: float
    timeframe: str
    candle_time: datetime
    created_at: datetime

    @property
    def cooldown_key(self) -> str:
        """Unique key used for duplicate suppression in cooldown manager."""
        return f"{self.symbol}:{self.market.value}:{self.signal_type.value}"

    @property
    def is_bullish(self) -> bool:
        """Return True if this is a bullish (Buy/Long) signal."""
        return self.signal_type in (SignalType.SPOT_BUY, SignalType.FUTURES_LONG)

    @property
    def display_action(self) -> str:
        """Formatted human-readable action."""
        if self.signal_type == SignalType.SPOT_BUY:
            return "SPOT BUY"
        elif self.signal_type == SignalType.SPOT_SELL:
            return "SPOT EXIT / SELL"
        elif self.signal_type == SignalType.FUTURES_LONG:
            return "FUTURES LONG"
        elif self.signal_type == SignalType.FUTURES_SHORT:
            return "FUTURES SHORT"
        return self.signal_type.value

    def to_dict(self) -> dict:
        """Convert signal to a dictionary for persistence or logging."""
        return {
            "symbol": self.symbol,
            "market": self.market.value,
            "signal_type": self.signal_type.value,
            "price": self.price,
            "rsi": self.rsi,
            "ema_200": self.ema_200,
            "volume": self.volume,
            "volume_ma": self.volume_ma,
            "volume_ratio": self.volume_ratio,
            "distance_to_ema_pct": self.distance_to_ema_pct,
            "timeframe": self.timeframe,
            "candle_time": self.candle_time.isoformat(),
            "created_at": self.created_at.isoformat(),
        }


class MarketAnalyzer:
    """
    Evaluates market candles against quantitative multi-factor rules.
    """

    def __init__(self, config: BotConfig) -> None:
        self.config = config

    def analyze_pair(
        self,
        symbol: str,
        market: MarketType,
        klines_df: pd.DataFrame,
    ) -> Optional[TradingSignal]:
        """
        Analyze historical klines for a single cryptocurrency trading pair.

        Args:
            symbol: Trading pair symbol (e.g. 'BTCUSDT').
            market: MarketType.SPOT or MarketType.FUTURES.
            klines_df: Raw klines DataFrame with columns ['timestamp', 'open', 'high', 'low', 'close', 'volume'].

        Returns:
            TradingSignal if all criteria are satisfied, otherwise None.
        """
        min_required_candles = self.config.ema_period + 20
        if klines_df.empty or len(klines_df) < min_required_candles:
            logger.debug(
                "Skipping %s: insufficient candles (%d < %d)",
                symbol,
                len(klines_df) if not klines_df.empty else 0,
                min_required_candles,
            )
            return None

        # Compute technical indicators
        df = enrich_klines(
            klines_df,
            rsi_period=self.config.rsi_period,
            ema_period=self.config.ema_period,
            volume_ma_period=self.config.volume_ma_period,
        )

        # Select target candle:
        # iloc[-2] is the last completed candle (prevents repainting on open 1h candle)
        # iloc[-1] is the ongoing live candle
        target_idx = -2 if self.config.use_closed_candles_only else -1

        try:
            candle = df.iloc[target_idx]
        except IndexError:
            return None

        # Extract values
        close_price = float(candle["close"])
        ema_val = float(candle["ema"])
        rsi_val = float(candle["rsi"])
        volume_val = float(candle["volume"])
        volume_ma_val = float(candle["volume_ma"])
        volume_ratio = float(candle["volume_ratio"])
        ema_dist_pct = float(candle["ema_dist_pct"])
        raw_timestamp = candle["timestamp"]

        # Parse candle timestamp
        if isinstance(raw_timestamp, (int, float)):
            # Binance timestamp is in milliseconds
            candle_time = datetime.fromtimestamp(raw_timestamp / 1000.0, tz=timezone.utc)
        elif isinstance(raw_timestamp, pd.Timestamp):
            candle_time = raw_timestamp.to_pydatetime()
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
        else:
            candle_time = datetime.now(timezone.utc)

        # Validate that indicators are not NaN
        if pd.isna(ema_val) or pd.isna(rsi_val) or pd.isna(volume_ma_val):
            return None

        # -------------------------------------------------------------
        # Factor 1: Volume Confirmation
        # Current volume must exceed the volume moving average
        # -------------------------------------------------------------
        has_volume_confirmation = volume_val > volume_ma_val and volume_ratio > 1.0
        if not has_volume_confirmation:
            return None

        # -------------------------------------------------------------
        # Factor 2 & 3: Multi-Factor Bullish Check
        # - Macro Trend: Price > 200 EMA (Uptrend)
        # - Momentum: RSI < 30 (Oversold pullback in uptrend)
        # -------------------------------------------------------------
        is_bullish_signal = (
            close_price > ema_val and rsi_val <= self.config.rsi_oversold
        )

        # -------------------------------------------------------------
        # Factor 2 & 3: Multi-Factor Bearish Check
        # - Macro Trend: Price < 200 EMA (Downtrend)
        # - Momentum: RSI > 70 (Overbought rally in downtrend)
        # -------------------------------------------------------------
        is_bearish_signal = (
            close_price < ema_val and rsi_val >= self.config.rsi_overbought
        )

        signal_type: Optional[SignalType] = None

        if is_bullish_signal:
            if market == MarketType.FUTURES:
                signal_type = SignalType.FUTURES_LONG
            else:
                signal_type = SignalType.SPOT_BUY
        elif is_bearish_signal:
            if market == MarketType.FUTURES:
                signal_type = SignalType.FUTURES_SHORT
            else:
                signal_type = SignalType.SPOT_SELL

        if signal_type is None:
            return None

        signal = TradingSignal(
            symbol=symbol,
            market=market,
            signal_type=signal_type,
            price=close_price,
            rsi=rsi_val,
            ema_200=ema_val,
            volume=volume_val,
            volume_ma=volume_ma_val,
            volume_ratio=volume_ratio,
            distance_to_ema_pct=ema_dist_pct,
            timeframe=self.config.kline_timeframe,
            candle_time=candle_time,
            created_at=datetime.now(timezone.utc),
        )

        logger.info(
            "Found %s signal for %s [%s]: Price=%.4f, RSI=%.2f, EMA200=%.4f, VolRatio=%.2fx",
            signal.display_action,
            signal.symbol,
            signal.market.value,
            signal.price,
            signal.rsi,
            signal.ema_200,
            signal.volume_ratio,
        )

        return signal

    def analyze_on_demand(
        self,
        symbol: str,
        market: MarketType,
        timeframe: str,
        klines_df: pd.DataFrame,
        live_price: Optional[float] = None,
    ) -> Optional[OnDemandResult]:
        """
        Perform detailed technical analysis on any requested coin and timeframe.
        Provides live real-time values, multi-factor indicator confluence breakdown,
        timeframe-adaptive dynamic TP/SL levels, and Sri Lankan Standard Time.
        """
        min_required = self.config.ema_period + 20
        if klines_df.empty or len(klines_df) < min_required:
            logger.warning(
                "Insufficient candles for %s on %s: %d < %d",
                symbol,
                timeframe,
                len(klines_df) if not klines_df.empty else 0,
                min_required,
            )
            return None

        df = enrich_klines(
            klines_df,
            rsi_period=self.config.rsi_period,
            ema_period=self.config.ema_period,
            volume_ma_period=self.config.volume_ma_period,
        )

        last_row = df.iloc[-1]
        prev_row = df.iloc[-2] if len(df) >= 2 else last_row

        close_price = live_price if (live_price is not None and live_price > 0) else float(last_row["close"])
        open_price = float(last_row["open"])
        high_price = float(last_row["high"])
        low_price = float(last_row["low"])

        ema_9 = float(last_row.get("ema_9", close_price))
        ema_21 = float(last_row.get("ema_21", close_price))
        ema_50 = float(last_row.get("ema_50", close_price))
        ema_200 = float(last_row.get("ema_200", last_row.get("ema", close_price)))

        rsi_val = float(last_row["rsi"])
        prev_rsi = float(prev_row["rsi"])

        macd_val = float(last_row.get("macd", 0.0))
        macd_sig = float(last_row.get("macd_signal", 0.0))
        macd_hist = float(last_row.get("macd_hist", 0.0))
        prev_macd_hist = float(prev_row.get("macd_hist", 0.0))

        vol_val = float(last_row["volume"])
        vol_ma_val = float(last_row["volume_ma"])
        vol_ratio = float(last_row["volume_ratio"])
        ema_dist_pct = ((close_price - ema_200) / ema_200) * 100.0 if ema_200 > 0 else 0.0

        atr_val = float(last_row["atr"]) if "atr" in last_row and not pd.isna(last_row["atr"]) else close_price * 0.015

        # -------------------------------------------------------------
        # Multi-Factor Confluence Scoring Algorithm (-10.0 to +10.0)
        # -------------------------------------------------------------
        score = 0.0
        reasons: list[str] = []

        # 1. Short-Term Momentum: EMA 9 vs EMA 21
        if ema_9 > ema_21:
            score += 2.0
            reasons.append("EMA 9>21 Bullish")
        else:
            score -= 2.0
            reasons.append("EMA 9<21 Bearish")

        # 2. Macro Trend Alignment: Price vs EMA 50 & EMA 200
        if close_price > ema_50:
            score += 1.0
        else:
            score -= 1.0

        if close_price > ema_200:
            score += 1.5
            reasons.append("Above 200 EMA")
        else:
            score -= 1.5
            reasons.append("Below 200 EMA")

        # EMA Ribbon alignment
        if ema_9 > ema_21 > ema_50:
            score += 1.0
            reasons.append("Bullish Trend Ribbon")
        elif ema_9 < ema_21 < ema_50:
            score -= 1.0
            reasons.append("Bearish Trend Ribbon")

        # 3. MACD Momentum (12, 26, 9)
        if macd_val > macd_sig:
            score += 1.5
            if macd_hist > 0 and macd_hist > prev_macd_hist:
                score += 0.5
                reasons.append("MACD Bullish Expansion")
        else:
            score -= 1.5
            if macd_hist < 0 and macd_hist < prev_macd_hist:
                score -= 0.5
                reasons.append("MACD Bearish Expansion")

        # 4. RSI Overbought / Oversold Protection & Zones
        if rsi_val >= 75.0:
            score -= 3.0
            reasons.append(f"RSI {rsi_val:.1f} Extreme Overbought")
        elif rsi_val >= 68.0:
            score -= 1.5
            reasons.append(f"RSI {rsi_val:.1f} Near Overbought")
        elif 52.0 <= rsi_val < 68.0:
            score += 1.5
            reasons.append("RSI Bullish Momentum")
        elif 48.0 <= rsi_val < 52.0:
            pass  # Neutral RSI
        elif 32.0 <= rsi_val < 48.0:
            score -= 1.5
            reasons.append("RSI Bearish Momentum")
        elif 25.0 < rsi_val <= 32.0:
            score += 1.5
            reasons.append(f"RSI {rsi_val:.1f} Near Oversold")
        elif rsi_val <= 25.0:
            score += 3.0
            reasons.append(f"RSI {rsi_val:.1f} Extreme Oversold")

        # RSI Slope
        if rsi_val > prev_rsi:
            score += 0.5
        else:
            score -= 0.5

        # 5. Candlestick Price Action
        if close_price > open_price:
            score += 0.5
        elif close_price < open_price:
            score -= 0.5

        candle_range = high_price - low_price
        if candle_range > 0:
            lower_wick = min(open_price, close_price) - low_price
            upper_wick = high_price - max(open_price, close_price)
            if lower_wick / candle_range > 0.4:
                score += 0.5
                reasons.append("Buyers Defending Lows")
            elif upper_wick / candle_range > 0.4:
                score -= 0.5
                reasons.append("Sellers Rejecting Highs")

        # 6. Volume Confirmation
        if vol_ratio >= 1.2:
            if score > 0:
                score += 1.0
                reasons.append(f"High Vol {vol_ratio:.1f}x")
            elif score < 0:
                score -= 1.0
                reasons.append(f"High Vol {vol_ratio:.1f}x")

        # -------------------------------------------------------------
        # Action, Badge, and Verdict Decision
        # -------------------------------------------------------------
        is_futures = market == MarketType.FUTURES

        if score >= 3.0:
            action = "LONG"
            action_badge = "🟢 LONG (BUY)" if is_futures else "💎 SPOT BUY (ACCUMULATE)"
            verdict = "LONG"
            verdict_title = "🚀 STRONG LONG (High Probability Trend)" if is_futures else "💎 STRONG SPOT BUY SIGNAL"
        elif 0.5 <= score < 3.0:
            action = "LONG"
            action_badge = "🟢 LONG (BUY PULLBACK)" if is_futures else "🟢 SPOT BUY (DIP)"
            verdict = "BULLISH_BIAS"
            verdict_title = "📈 BULLISH BIAS (Buy Dips / Support)"
        elif -0.5 < score < 0.5:
            action = "WAIT"
            action_badge = "⚖️ NEUTRAL / WAIT"
            verdict = "NEUTRAL"
            verdict_title = "⚖️ NEUTRAL / CONSOLIDATION (Wait for Breakout)"
        elif -3.0 < score <= -0.5:
            action = "SHORT"
            action_badge = "🔴 SHORT (SELL RALLY)" if is_futures else "⚠️ SPOT CAUTION / TRIM"
            verdict = "BEARISH_BIAS"
            verdict_title = "📉 BEARISH BIAS (Sell Resistance / Rallies)"
        else:  # score <= -3.0
            action = "SHORT"
            action_badge = "🔴 SHORT (SELL)" if is_futures else "🛑 SPOT EXIT / TAKE-PROFIT"
            verdict = "SHORT"
            verdict_title = "🔻 STRONG SHORT (High Probability Breakdown)" if is_futures else "🛑 SPOT EXIT / SELL SIGNAL"

        # Dynamic support and resistance (recent 20-period swing high/low)
        recent_window = df.tail(20)
        recent_low = float(recent_window["low"].min())
        recent_high = float(recent_window["high"].max())
        support_price = min(ema_200, recent_low) if close_price > ema_200 else recent_low
        resistance_price = max(ema_200, recent_high) if close_price < ema_200 else recent_high

        # -------------------------------------------------------------
        # Timeframe-Adaptive Volatility & Dynamic TP / SL Sizing
        # -------------------------------------------------------------
        tf_clean = timeframe.lower().strip()
        if tf_clean == "1m":
            atr_mult = 1.2
            min_risk_pct = 0.0015  # 0.15% min SL for 1m scalp
            max_risk_pct = 0.0080  # 0.80% max SL
        elif tf_clean == "5m":
            atr_mult = 1.4
            min_risk_pct = 0.0030  # 0.30% min SL for 5m intraday
            max_risk_pct = 0.0150  # 1.50% max SL
        elif tf_clean in ("10m", "15m"):
            atr_mult = 1.5
            min_risk_pct = 0.0050  # 0.50% min SL
            max_risk_pct = 0.0250  # 2.50% max SL
        elif tf_clean in ("1h", "60m"):
            atr_mult = 1.8
            min_risk_pct = 0.0100  # 1.00% min SL for 1h swing
            max_risk_pct = 0.0500  # 5.00% max SL
        else:
            atr_mult = 1.5
            min_risk_pct = 0.0050
            max_risk_pct = 0.0300

        calc_risk = atr_val * atr_mult
        risk_dist = max(close_price * min_risk_pct, min(close_price * max_risk_pct, calc_risk))

        entry_price = close_price

        if action in ("LONG", "WAIT"):
            stop_loss = max(0.0, entry_price - risk_dist)
            tp1 = entry_price + (1.0 * risk_dist)
            tp2 = entry_price + (2.0 * risk_dist)
            tp3 = entry_price + (3.5 * risk_dist)
            entry_low = entry_price - (0.2 * risk_dist)
            entry_high = entry_price + (0.1 * risk_dist)
        else:  # SHORT
            stop_loss = entry_price + risk_dist
            tp1 = max(0.0, entry_price - (1.0 * risk_dist))
            tp2 = max(0.0, entry_price - (2.0 * risk_dist))
            tp3 = max(0.0, entry_price - (3.5 * risk_dist))
            entry_low = entry_price - (0.1 * risk_dist)
            entry_high = entry_price + (0.2 * risk_dist)

        tp1_pct = abs((tp1 - entry_price) / entry_price) * 100.0
        tp2_pct = abs((tp2 - entry_price) / entry_price) * 100.0
        tp3_pct = abs((tp3 - entry_price) / entry_price) * 100.0
        sl_pct = abs((stop_loss - entry_price) / entry_price) * 100.0
        rr_ratio = "1 : 2.0"

        entry_zone = f"${entry_low:,.4f} - ${entry_high:,.4f}" if entry_price < 1000 else f"${entry_low:,.2f} - ${entry_high:,.2f}"
        confluence_summary = " • ".join(reasons[:4]) if reasons else "Multi-factor confluence"

        # Current Sri Lankan Standard Time (UTC+5:30)
        now_slt = datetime.now(SRI_LANKA_TZ)
        slt_str = now_slt.strftime("%Y-%m-%d %I:%M:%S %p SLST")

        return OnDemandResult(
            symbol=symbol,
            market=market,
            timeframe=timeframe,
            current_price=close_price,
            ema_200=ema_200,
            rsi=rsi_val,
            volume=vol_val,
            volume_ma=vol_ma_val,
            volume_ratio=vol_ratio,
            distance_to_ema_pct=ema_dist_pct,
            action=action,
            action_badge=action_badge,
            verdict=verdict,
            verdict_title=verdict_title,
            entry_price=entry_price,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_price=tp3,
            stop_loss_price=stop_loss,
            tp1_pct=tp1_pct,
            tp2_pct=tp2_pct,
            tp3_pct=tp3_pct,
            sl_pct=sl_pct,
            risk_reward_ratio=rr_ratio,
            support_price=support_price,
            resistance_price=resistance_price,
            sri_lanka_time_str=slt_str,
            ema_9=ema_9,
            ema_21=ema_21,
            ema_50=ema_50,
            macd=macd_val,
            macd_signal=macd_sig,
            macd_hist=macd_hist,
            confluence_score=score,
            confluence_summary=confluence_summary,
            entry_zone=entry_zone,
        )
