# 🤖 24/7 Quantitative Crypto Trading Signal Generator Bot

A production-ready, highly optimized, 24/7 automated cryptocurrency trading signal generator bot that concurrently scans both **Binance Spot** and **Binance USDⓈ-M Perpetual Futures** markets, evaluates multi-factor technical criteria on 1-hour candles, prevents duplicate alert spam via persistent SQLite cooldown tracking, and dispatches rich visual alerts to Telegram.

---

## ⚡ Key Highlights & Architecture

- **Dual Market Coverage (Spot & Futures):** Dynamically discovers all active USDT trading pairs on Binance Spot (e.g. `BTCUSDT`, `ETHUSDT`) and USDⓈ-M Perpetual Futures.
- **Clear Differentiation:** Telegram alerts clearly label and color-code `🟢 [BINANCE SPOT] BUY`, `🔴 [BINANCE SPOT] EXIT`, `🚀 [USDⓈ-M FUTURES] LONG`, and `🔻 [USDⓈ-M FUTURES] SHORT` with direct links to Binance trading charts.
- **Multi-Factor Quantitative Filter:**
  1. **Macro Trend (200 EMA):** Price > 200 EMA for bullish entries; Price < 200 EMA for bearish entries.
  2. **Momentum (14 RSI - Wilder's Smoothing):** Identifies oversold pullbacks (RSI $\le$ 30) in uptrends, and overbought exhaustion rallies (RSI $\ge$ 70) in downtrends.
  3. **Volume Confirmation (20 SMA):** Filters out low-liquidity fakeouts by requiring current candle volume > 20-period moving average volume.
  4. **Zero-Repaint Execution:** Analyzes closed candles (`iloc[-2]`) by default, ensuring signals never repaint or disappear.
- **High-Speed Concurrency with Rate Limiting:**
  - Built on `asyncio` and `aiohttp` connection pooling.
  - Automatically reads Binance `x-mbx-used-weight-1m` headers and throttles if weight exceeds safe thresholds.
  - Scans **300+ pairs in under 5 seconds** without triggering HTTP 429 rate limits.
- **Intelligent Cooldown Mechanism (SQLite Persistence):**
  - Configurable duplicate suppression window (e.g. 4.0 hours per coin/signal).
  - Stored in a local WAL-mode SQLite database (`data/signals.db`). Cooldowns survive bot restarts and cloud worker redeployments.
  - Full signal audit history with prices, RSI values, EMA distances, and volume ratios for performance tracking.
- **Cloud Deployment Ready:**
  - Pre-configured for **Render** (`render.yaml`), **Railway** (`Procfile`), and **Docker** (`Dockerfile`, `docker-compose.yml`).
  - Includes graceful shutdown handlers (`SIGINT`/`SIGTERM`) and periodic Telegram health heartbeats.

---

## 📐 Quantitative Signal Logic

| Market | Condition | Trend Filter (200 EMA) | Momentum (14 RSI) | Volume Filter (20 MA) | Action Triggered |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **USDⓈ-M Futures** | Bullish Pullback | $\text{Price} > \text{EMA}_{200}$ | $\text{RSI}_{14} \le 30.0$ | $\text{Vol} > \text{SMA}_{\text{Vol}}$ | `🚀 FUTURES LONG` |
| **USDⓈ-M Futures** | Bearish Rejection | $\text{Price} < \text{EMA}_{200}$ | $\text{RSI}_{14} \ge 70.0$ | $\text{Vol} > \text{SMA}_{\text{Vol}}$ | `🔻 FUTURES SHORT` |
| **Binance Spot** | Bullish Accumulation | $\text{Price} > \text{EMA}_{200}$ | $\text{RSI}_{14} \le 30.0$ | $\text{Vol} > \text{SMA}_{\text{Vol}}$ | `🟢 SPOT BUY` |
| **Binance Spot** | Bearish Warning | $\text{Price} < \text{EMA}_{200}$ | $\text{RSI}_{14} \ge 70.0$ | $\text{Vol} > \text{SMA}_{\text{Vol}}$ | `🔴 SPOT EXIT / SELL` |

---

## 📂 Project Structure

```
binance-signal-bot/
├── config.py                 # Typed configuration & .env validation
├── binance_client.py         # Async REST client for Spot & Futures with rate limit guard
├── indicators.py             # Vectorized indicators (RSI-14, EMA-200, Volume MA)
├── analyzer.py               # Quantitative multi-factor evaluation engine
├── cooldown_manager.py       # Thread-safe SQLite cooldown & history tracker
├── telegram_notifier.py      # Rich HTML alerts, auto-queueing & rate limiting
├── bot.py                    # Main 24/7 async daemon, scheduler, heartbeat & shutdown
├── .env.example              # Environment variables template
├── requirements.txt          # Production dependencies
├── Dockerfile                # Production-grade multi-stage container
├── docker-compose.yml        # Docker compose configuration
├── Procfile                  # Worker declaration for Render / Railway
├── render.yaml               # 1-Click Render blueprint
├── data/
│   └── signals.db            # Persistent SQLite database (auto-created)
└── tests/
    ├── test_indicators.py    # Unit tests for RSI, EMA, Volume SMA
    ├── test_cooldown.py      # Unit tests for SQLite cooldown persistence
    ├── test_analyzer.py      # Unit tests for multi-factor rules
    └── test_notifier.py      # Unit tests for Telegram alert formatting
```

---

## 🚀 Quickstart & Setup Guide

### 1. Prerequisites
- Python 3.10+ (tested up to Python 3.14)
- A Telegram account to create a bot and receive alerts

### 2. Create Telegram Bot & Obtain Chat ID
1. Open Telegram and search for [@BotFather](https://t.me/BotFather).
2. Send `/newbot`, choose a name and username. BotFather will provide your **`BOT_TOKEN`** (e.g. `123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ`).
3. Start a chat with your new bot by clicking `Start`.
4. To find your **`CHAT_ID`**, message [@userinfobot](https://t.me/userinfobot) or [@RawDataBot](https://t.me/RawDataBot). It will display your numeric user ID (e.g. `987654321`). For groups or channels, add your bot as an admin and use the channel ID (e.g. `-100123456789`).

### 3. Installation
```bash
# Clone or navigate to the project directory
cd binance-signal-bot

# Create and activate virtual environment
python -m venv .venv

# On Linux / macOS:
source .venv/bin/activate
# On Windows (PowerShell):
.\.venv\Scripts\Activate.ps1

# Install production dependencies
pip install -r requirements.txt
```

### 4. Configure Environment (`.env`)
Copy `.env.example` to `.env` and fill in your Telegram credentials:
```bash
cp .env.example .env
```
Edit `.env`:
```ini
TELEGRAM_BOT_TOKEN=123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ
TELEGRAM_CHAT_ID=987654321

# Optional Binance API keys (klines are public, but keys give higher rate limits)
BINANCE_API_KEY=
BINANCE_SECRET_KEY=

# Markets to monitor
ENABLE_SPOT=true
ENABLE_FUTURES=true
QUOTE_ASSET=USDT

# Minimum 24h volume in USDT to filter out dead coins (0 to scan all)
MIN_24H_VOLUME_USDT=5000000

# Strategy settings
KLINE_TIMEFRAME=1h
KLINE_LIMIT=300
RSI_PERIOD=14
RSI_OVERSOLD=30.0
RSI_OVERBOUGHT=70.0
EMA_PERIOD=200
VOLUME_MA_PERIOD=20
COOLDOWN_HOURS=4.0
SCAN_INTERVAL_MINUTES=15
```

### 5. Run the Bot
- **Single Test Pass (Verifies API connectivity and logic without looping):**
  ```bash
  python bot.py --once
  ```
- **24/7 Background Daemon:**
  ```bash
  python bot.py
  ```
- **Market Filters:**
  ```bash
  python bot.py --spot-only      # Monitor Binance Spot only
  python bot.py --futures-only   # Monitor USDⓈ-M Futures only
  ```

### 6. Run Unit Tests
Verify all 17 unit tests pass:
```bash
pytest -v
```

---

## 📱 Telegram Alert Preview

```
🚀 [USDⓈ-M FUTURES] 🚀
━━━━━━━━━━━━━━━━━━━━
🪙 Pair: #FILUSDT
⚡ 🟢 ACTION: FUTURES LONG (BUY)
💵 Current Price: $1.0610
━━━━━━━━━━━━━━━━━━━━
📊 Technical Analysis (1-Hour):
• 200 EMA: $1.0285 (+3.16% | Uptrend Confirmation)
• 14 RSI: 29.9 — Oversold Dip (29.9 ≤ 30.0)
• Volume Spike: 1.09x 20-period MA
• Candle Close: 2026-09-28 08:00 UTC
━━━━━━━━━━━━━━━━━━━━
🔗 Open FUTURES Chart on Binance
🤖 Automated 24/7 Quantitative Signal Bot
```

---

## ☁️ Cloud Deployment Options

### Option A: Railway (Recommended)
1. Fork or push this repository to GitHub.
2. Sign in to [Railway.app](https://railway.app/).
3. Click **New Project** $\rightarrow$ **Deploy from GitHub repo**.
4. Railway will automatically detect the `Procfile` (`worker: python bot.py`).
5. In Railway project settings, go to **Variables** and add:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - (Optionally other `.env` variables)
6. To persist cooldowns across worker redeploys, add a persistent Volume mounted at `/app/data`.

### Option B: Render Background Worker
1. Push code to GitHub.
2. Sign in to [Render.com](https://render.com/).
3. Click **New** $\rightarrow$ **Blueprint** and connect your repository (Render automatically reads `render.yaml`).
   *Alternatively*, create a **Background Worker**:
   - Environment: `Python 3`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python bot.py`
4. Add environment variables in the Render Dashboard (`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`).
5. Attach a Persistent Disk at `/app/data` (1GB is more than enough).

### Option C: Docker / VPS (Self-Hosted)
Run 24/7 with auto-restart on any Linux VPS (Ubuntu, Debian, AWS EC2, DigitalOcean):
```bash
# Build and run in background with persistent volume
docker compose up -d --build

# View real-time logs
docker compose logs -f

# Stop container
docker compose down
```

---

## ⚙️ Configuration Reference

| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `TELEGRAM_BOT_TOKEN` | *None* | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | *None* | Target user or group chat ID |
| `BINANCE_API_KEY` | `""` | Binance API Key (Optional) |
| `BINANCE_SECRET_KEY` | `""` | Binance Secret Key (Optional) |
| `ENABLE_SPOT` | `true` | Enable Binance Spot market scanning |
| `ENABLE_FUTURES` | `true` | Enable Binance USDⓈ-M Futures scanning |
| `QUOTE_ASSET` | `USDT` | Target quote currency pair filter |
| `MIN_24H_VOLUME_USDT` | `500000` | Minimum 24h USDT volume to filter out illiquid pairs |
| `KLINE_TIMEFRAME` | `1h` | Candlestick interval (1h required by strategy) |
| `KLINE_LIMIT` | `300` | Lookback candles fetched (must be $\ge 250$ for 200 EMA) |
| `RSI_PERIOD` | `14` | Period for RSI calculation |
| `RSI_OVERSOLD` | `30.0` | Oversold threshold for Long/Buy |
| `RSI_OVERBOUGHT` | `70.0` | Overbought threshold for Short/Sell |
| `EMA_PERIOD` | `200` | Period for Exponential Moving Average trend filter |
| `VOLUME_MA_PERIOD` | `20` | Lookback period for volume simple moving average |
| `USE_CLOSED_CANDLES_ONLY` | `true` | Analyze closed candles (`iloc[-2]`) to prevent repainting |
| `COOLDOWN_HOURS` | `4.0` | Cooldown period before alerting same coin & signal type |
| `SCAN_INTERVAL_MINUTES` | `15` | Delay between consecutive full market scan cycles |
| `MAX_CONCURRENT_REQUESTS` | `20` | Max simultaneous HTTP requests to Binance REST API |
| `DATABASE_PATH` | `data/signals.db` | Path to persistent SQLite database |
| `HEARTBEAT_INTERVAL_HOURS` | `12` | Frequency of health reports sent to Telegram |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

---

## 🛡️ License & Disclaimer

This project is licensed under the MIT License.

**Disclaimer:** *This software is for educational, analytical, and research purposes only. Cryptocurrency trading carries substantial risk of financial loss. Past performance of technical indicators is not indicative of future returns. Always conduct your own due diligence before trading.*
