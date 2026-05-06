# ─────────────────────────────────────────────────────────────────────────────
# Multi-Tier Decision Engine – Docker image
# Base: python:3.11-slim  (Debian Bookworm)
# Includes: GLPK LP solver (glpsol) for Pyomo
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS base

# System dependencies: GLPK solver + build tools for pvlib/numpy wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
        glpk-utils \
        gcc \
        g++ \
        libgmp-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Python dependencies ───────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Application source ────────────────────────────────────────────────────────
COPY config/ ./config/
COPY src/    ./src/

# ── Runtime defaults ──────────────────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USER=guest \
    RABBITMQ_PASS=guest \
    CONTROL_INTERVAL_SEC=30 \
    PREDICTION_HORIZON=24 \
    SOLVER_NAME=glpk

ENTRYPOINT ["python", "-m", "src.main"]
