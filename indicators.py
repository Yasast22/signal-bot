"""
Technical Indicators Module for Quantitative Signal Generation.

Implements high-performance vectorized calculations for:
- Relative Strength Index (RSI - 14) with standard Wilder's smoothing (identical to TradingView/Binance)
- Exponential Moving Average (EMA - 200)
- Volume Moving Average (SMA - 20) and Volume Spike Ratio
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate the Relative Strength Index (RSI) using Wilder's Exponential Smoothing.
    
    This matches TradingView's and Binance's standard RSI formula.
    
    Args:
        series: Price series (typically close prices).
        period: Lookback window (default: 14).

    Returns:
        pd.Series containing RSI values scaled between 0 and 100.
    """
    if len(series) < period:
        return pd.Series(index=series.index, data=np.nan)

    delta = series.diff()

    # Separate positive and negative price changes
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    # Wilder's smoothing uses alpha = 1 / period
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    # Calculate Relative Strength (RS)
    # Avoid zero-division errors when avg_loss is 0
    rs = avg_gain / avg_loss.replace(0.0, np.nan)

    rsi = 100.0 - (100.0 / (1.0 + rs))

    # In case avg_loss is 0: if avg_gain > 0, RSI is 100. If avg_gain is 0, RSI is 0.
    fallback = pd.Series(
        np.where(avg_gain > 0, 100.0, np.where(avg_loss > 0, 0.0, 50.0)),
        index=series.index,
    )
    rsi = rsi.combine_first(fallback)
    return rsi


def calculate_ema(series: pd.Series, period: int = 200) -> pd.Series:
    """
    Calculate the Exponential Moving Average (EMA).
    
    Uses standard span smoothing factor alpha = 2 / (period + 1).

    Args:
        series: Price series (typically close prices).
        period: EMA span (default: 200).

    Returns:
        pd.Series containing EMA values.
    """
    return series.ewm(span=period, adjust=False).mean()


def calculate_sma(series: pd.Series, period: int = 20) -> pd.Series:
    """
    Calculate the Simple Moving Average (SMA).
    
    Used primarily for volume confirmation.

    Args:
        series: Data series (e.g. candle volume).
        period: Rolling window size (default: 20).

    Returns:
        pd.Series containing SMA values.
    """
    return series.rolling(window=period, min_periods=period).mean()


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate the Average True Range (ATR) for volatility and stop-loss/take-profit sizing.
    """
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()


def calculate_macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Calculate the Moving Average Convergence Divergence (MACD).

    Args:
        series: Price series (typically close prices).
        fast: Fast EMA period (default: 12).
        slow: Slow EMA period (default: 26).
        signal: Signal line EMA period (default: 9).

    Returns:
        tuple of (macd_line, signal_line, macd_histogram)
    """
    fast_ema = series.ewm(span=fast, adjust=False).mean()
    slow_ema = series.ewm(span=slow, adjust=False).mean()
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    macd_hist = macd_line - signal_line
    return macd_line, signal_line, macd_hist


def enrich_klines(
    df: pd.DataFrame,
    rsi_period: int = 14,
    ema_period: int = 200,
    volume_ma_period: int = 20,
) -> pd.DataFrame:
    """
    Compute and append all required technical indicators to a klines DataFrame.

    Expected input columns in df:
        ['timestamp', 'open', 'high', 'low', 'close', 'volume']

    Enriched columns added:
        - ema_9, ema_21, ema_50, ema_200: Multi-period EMAs for trend ribbon
        - ema: Alias for ema_200
        - rsi: Relative Strength Index (default 14)
        - macd, macd_signal, macd_hist: Standard MACD (12, 26, 9)
        - volume_ma: Volume Moving Average (default 20)
        - volume_ratio: Current volume / volume_ma
        - ema_dist_pct: ((close - ema_200) / ema_200) * 100
        - atr: Average True Range (default 14)
    """
    if df.empty or len(df) < max(ema_period, rsi_period, volume_ma_period):
        return df

    # Ensure numeric columns are float
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns and not np.issubdtype(df[col].dtype, np.floating):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop any NaNs in primary fields
    df = df.dropna(subset=["close", "volume"]).copy()

    # Calculate multi-period Exponential Moving Averages
    df["ema_9"] = calculate_ema(df["close"], period=9)
    df["ema_21"] = calculate_ema(df["close"], period=21)
    df["ema_50"] = calculate_ema(df["close"], period=50)
    df["ema_200"] = calculate_ema(df["close"], period=ema_period)
    df["ema"] = df["ema_200"]  # Backwards compatibility alias

    # Calculate Momentum & Oscillator indicators
    df["rsi"] = calculate_rsi(df["close"], period=rsi_period)
    macd_line, sig_line, hist_line = calculate_macd(df["close"], fast=12, slow=26, signal=9)
    df["macd"] = macd_line
    df["macd_signal"] = sig_line
    df["macd_hist"] = hist_line

    # Volume & Volatility
    df["volume_ma"] = calculate_sma(df["volume"], period=volume_ma_period)
    df["atr"] = calculate_atr(df, period=rsi_period)

    # Volume ratio (handle potential zero in volume_ma safely)
    df["volume_ratio"] = np.where(
        df["volume_ma"] > 0,
        df["volume"] / df["volume_ma"],
        1.0,
    )

    # Percentage distance from price to 200 EMA
    df["ema_dist_pct"] = np.where(
        df["ema_200"] > 0,
        ((df["close"] - df["ema_200"]) / df["ema_200"]) * 100.0,
        0.0,
    )

    return df
