"""
Asynchronous Binance REST Client for Spot and USDⓈ-M Futures.

Features:
- Dynamic discovery of all active USDT trading pairs
- High-concurrency kline fetching via asyncio & aiohttp connection pooling
- Intelligent rate-limit monitoring via Binance response headers (x-mbx-used-weight-1m)
- 24h volume filtering to skip dead/illiquid markets
- Exponential backoff and retry handling for 429/5xx status codes
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import aiohttp
import pandas as pd

from analyzer import MarketType
from config import BotConfig

logger = logging.getLogger(__name__)


class BinanceAsyncClient:
    """
    Asynchronous client interfacing with Binance Spot and USDⓈ-M Futures REST APIs.
    """

    SPOT_BASE_URL = "https://api.binance.com"
    FUTURES_BASE_URL = "https://fapi.binance.com"

    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.max_concurrent_requests)

        # Track used request weights reported by Binance
        self._spot_used_weight_1m = 0
        self._futures_used_weight_1m = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create reusable aiohttp client session with connection pooling."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=30,
                ttl_dns_cache=300,
                keepalive_timeout=60,
                enable_cleanup_closed=True,
            )
            timeout = aiohttp.ClientTimeout(total=self.config.request_timeout_seconds)
            headers = {"User-Agent": "CryptoSignalBot/1.0"}
            if self.config.binance_api_key:
                headers["X-MBX-APIKEY"] = self.config.binance_api_key

            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=headers,
            )
        return self._session

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            # Allow underlying SSL transports to close cleanly
            await asyncio.sleep(0.25)
            logger.info("Binance HTTP session closed")

    async def _request(
        self,
        method: str,
        url: str,
        params: Optional[dict[str, Any]] = None,
        is_futures: bool = False,
        retries: int = 3,
    ) -> Any:
        """
        Execute an HTTP request with concurrency control and rate-limit guard.
        """
        session = await self._get_session()

        for attempt in range(1, retries + 1):
            async with self._semaphore:
                # Proactive rate-limit throttle if weight is near ceiling
                weight = self._futures_used_weight_1m if is_futures else self._spot_used_weight_1m
                limit_threshold = 2100 if is_futures else 1050
                if weight > limit_threshold:
                    wait_seconds = 5.0
                    logger.warning(
                        "Rate limit weight near capacity (%d). Throttling for %.1fs...",
                        weight,
                        wait_seconds,
                    )
                    await asyncio.sleep(wait_seconds)

                try:
                    async with session.request(method, url, params=params) as resp:
                        # Extract used weight from headers
                        weight_header = resp.headers.get("x-mbx-used-weight-1m")
                        if weight_header and weight_header.isdigit():
                            if is_futures:
                                self._futures_used_weight_1m = int(weight_header)
                            else:
                                self._spot_used_weight_1m = int(weight_header)

                        # Handle rate limiting (429) or IP bans (418)
                        if resp.status in (429, 418):
                            retry_after = int(resp.headers.get("Retry-After", "10"))
                            logger.error(
                                "Binance rate limit hit (status %d)! Backing off for %ds (attempt %d/%d)",
                                resp.status,
                                retry_after,
                                attempt,
                                retries,
                            )
                            await asyncio.sleep(retry_after)
                            continue

                        # Handle server errors (5xx)
                        if resp.status >= 500:
                            logger.warning(
                                "Binance 5xx error (%d) for %s. Attempt %d/%d",
                                resp.status,
                                url,
                                attempt,
                                retries,
                            )
                            await asyncio.sleep(attempt * 1.5)
                            continue

                        resp.raise_for_status()
                        return await resp.json()

                except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                    if attempt == retries:
                        logger.error("Request failed after %d retries for %s: %s", retries, url, err)
                        return None
                    await asyncio.sleep(attempt * 1.0)

        return None

    # -----------------------------------------------------------------
    # Pair Discovery & Filtering
    # -----------------------------------------------------------------
    async def get_spot_symbols(self) -> list[str]:
        """
        Dynamically fetch all active Spot trading pairs quoted in USDT.
        Excludes leveraged tokens (UP/DOWN/BEAR/BULL) and non-trading pairs.
        """
        url = f"{self.SPOT_BASE_URL}/api/v3/exchangeInfo"
        data = await self._request("GET", url, is_futures=False)
        if not data or "symbols" not in data:
            logger.error("Failed to fetch Spot exchangeInfo")
            return []

        active_symbols: list[str] = []
        target_quote = self.config.quote_asset

        for item in data["symbols"]:
            symbol = item.get("symbol", "")
            quote_asset = item.get("quoteAsset", "")
            status = item.get("status", "")
            is_spot_allowed = item.get("isSpotTradingAllowed", False)

            if quote_asset == target_quote and status == "TRADING" and is_spot_allowed:
                # Filter out leveraged token symbols
                if any(symbol.endswith(suffix) for suffix in ("UPUSDT", "DOWNUSDT", "BEARUSDT", "BULLUSDT")):
                    continue
                active_symbols.append(symbol)

        logger.info("Found %d active Spot %s pairs", len(active_symbols), target_quote)
        return active_symbols

    async def get_futures_symbols(self) -> list[str]:
        """
        Dynamically fetch all active USDⓈ-M Perpetual Futures pairs quoted in USDT.
        """
        url = f"{self.FUTURES_BASE_URL}/fapi/v1/exchangeInfo"
        data = await self._request("GET", url, is_futures=True)
        if not data or "symbols" not in data:
            logger.error("Failed to fetch Futures exchangeInfo")
            return []

        active_symbols: list[str] = []
        target_quote = self.config.quote_asset

        for item in data["symbols"]:
            symbol = item.get("symbol", "")
            quote_asset = item.get("quoteAsset", "")
            status = item.get("status", "")
            contract_type = item.get("contractType", "")

            if quote_asset == target_quote and status == "TRADING" and contract_type == "PERPETUAL":
                active_symbols.append(symbol)

        logger.info("Found %d active USD(S)-M Futures %s perpetual pairs", len(active_symbols), target_quote)
        return active_symbols

    async def filter_by_24h_volume(
        self,
        symbols: list[str],
        market: MarketType,
        min_volume_usdt: float,
    ) -> list[str]:
        """
        Filter symbols by 24h USDT trading volume to prune illiquid markets.

        Args:
            symbols: List of trading pair symbols.
            market: MarketType.SPOT or MarketType.FUTURES.
            min_volume_usdt: Minimum 24h volume in USDT.

        Returns:
            Filtered list of symbols meeting the volume threshold.
        """
        if min_volume_usdt <= 0 or not symbols:
            return symbols

        is_futures = market == MarketType.FUTURES
        base_url = self.FUTURES_BASE_URL if is_futures else self.SPOT_BASE_URL
        endpoint = "/fapi/v1/ticker/24hr" if is_futures else "/api/v3/ticker/24hr"

        data = await self._request("GET", f"{base_url}{endpoint}", is_futures=is_futures)
        if not data or not isinstance(data, list):
            logger.warning("Could not fetch 24h tickers; proceeding with unfiltered symbols")
            return symbols

        # Map symbol -> 24h quoteVolume (USDT volume)
        volume_map: dict[str, float] = {}
        for ticker in data:
            sym = ticker.get("symbol")
            try:
                volume_map[sym] = float(ticker.get("quoteVolume", 0.0))
            except (ValueError, TypeError):
                continue

        filtered = [s for s in symbols if volume_map.get(s, 0.0) >= min_volume_usdt]
        logger.info(
            "Volume filter (>= $%.0f USDT) retained %d / %d %s symbols",
            min_volume_usdt,
            len(filtered),
            len(symbols),
            market.value,
        )
        return filtered

    # -----------------------------------------------------------------
    # Symbol Normalization & Ticker Price
    # -----------------------------------------------------------------
    def normalize_symbol(self, raw_symbol: str) -> str:
        """
        Normalize user input symbol (e.g. 'BTC', 'eth', 'solusdt') into a standard trading pair (e.g. 'BTCUSDT').
        """
        clean = raw_symbol.strip().upper().replace("/", "").replace("-", "").replace("_", "")
        if not clean.endswith(self.config.quote_asset):
            clean = f"{clean}{self.config.quote_asset}"
        return clean

    async def get_live_price(self, symbol: str, market: MarketType) -> Optional[float]:
        """
        Fetch the exact current real-time market price for a symbol.
        """
        is_futures = market == MarketType.FUTURES
        base_url = self.FUTURES_BASE_URL if is_futures else self.SPOT_BASE_URL
        endpoint = "/fapi/v1/ticker/price" if is_futures else "/api/v3/ticker/price"

        data = await self._request("GET", f"{base_url}{endpoint}", params={"symbol": symbol}, is_futures=is_futures)
        if data and "price" in data:
            try:
                return float(data["price"])
            except (ValueError, TypeError):
                pass
        return None

    # -----------------------------------------------------------------
    # Kline Historical Data Fetching (Supports 1m, 5m, 10m, 15m, 1h)
    # -----------------------------------------------------------------
    async def fetch_klines(
        self,
        symbol: str,
        market: MarketType,
        interval: str = "1h",
        limit: int = 300,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical candlestick (kline) data for a symbol.
        Supports native intervals ('1m', '5m', '15m', '1h') and synthetic '10m' (via 5m resampling).

        Args:
            symbol: Trading pair symbol (e.g. 'BTCUSDT').
            market: MarketType.SPOT or MarketType.FUTURES.
            interval: Candlestick timeframe ('1m', '5m', '10m', '15m', '1h').
            limit: Number of candles (default 300).

        Returns:
            DataFrame with columns ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            or None on failure.
        """
        is_futures = market == MarketType.FUTURES
        base_url = self.FUTURES_BASE_URL if is_futures else self.SPOT_BASE_URL
        endpoint = "/fapi/v1/klines" if is_futures else "/api/v3/klines"

        # Binance native intervals: 1m, 3m, 5m, 15m, 30m, 1h
        # If user requests 10m, fetch 5m candles and resample
        needs_10m_resample = interval.lower() in ("10m", "10min")
        api_interval = "5m" if needs_10m_resample else interval
        api_limit = min(1000, limit * 2) if needs_10m_resample else limit

        params = {
            "symbol": symbol,
            "interval": api_interval,
            "limit": api_limit,
        }

        raw_data = await self._request(
            "GET",
            f"{base_url}{endpoint}",
            params=params,
            is_futures=is_futures,
        )

        if not raw_data or not isinstance(raw_data, list):
            return None

        try:
            # Binance klines format:
            # 0: Open time (ms), 1: Open, 2: High, 3: Low, 4: Close, 5: Volume
            columns = ["timestamp", "open", "high", "low", "close", "volume"]
            parsed_rows = [row[0:6] for row in raw_data]

            df = pd.DataFrame(parsed_rows, columns=columns)
            # Fast numeric conversion
            for col in ("open", "high", "low", "close", "volume"):
                df[col] = df[col].astype(float)
            df["timestamp"] = pd.to_numeric(df["timestamp"])

            # If 10m was requested, resample 5m bars to 10m bars
            if needs_10m_resample and not df.empty:
                df["dt"] = pd.to_datetime(df["timestamp"], unit="ms")
                df_resampled = (
                    df.set_index("dt")
                    .resample("10min")
                    .agg({
                        "timestamp": "first",
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "volume": "sum",
                    })
                    .dropna()
                    .reset_index(drop=True)
                )
                return df_resampled.tail(limit)

            return df
        except Exception as e:
            logger.debug("Error parsing klines for %s [%s]: %s", symbol, market.value, e)
            return None
