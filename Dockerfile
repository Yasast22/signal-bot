# =================================================================
# Production Dockerfile for Binance 24/7 Trading Signal Bot
# =================================================================

FROM python:3.11-slim AS base

# Prevent Python from writing .pyc files and enable immediate unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install security updates and curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first for optimal Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Create directory for persistent SQLite database and set ownership
RUN mkdir -p /app/data && chmod 777 /app/data

# Run as non-privileged system user for cloud security
RUN useradd -m -u 1001 botuser && chown -R botuser:botuser /app
USER botuser

# Default command: run the 24/7 worker
CMD ["python", "bot.py"]
