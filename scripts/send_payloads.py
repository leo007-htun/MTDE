"""
AMOS MTDE – Continuous Payload Sender
======================================
Simulates all four partner data streams by POSTing realistic payloads
to the ingest-api endpoints on a rolling schedule.

Usage:
    python scripts/send_payloads.py
    python scripts/send_payloads.py --url https://mtde-production.up.railway.app

Intervals (configurable via env vars):
    SENSOR_INTERVAL_SEC   default 30   – sensor + IDB telemetry
    MARKET_INTERVAL_SEC   default 300  – market price signal
    TTA_INTERVAL_SEC      default 3600 – TTA 48-hour forecast
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
import random
from datetime import datetime, timedelta, timezone

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sender")

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_URL            = os.getenv("INGEST_API_URL", "https://mtde-production.up.railway.app")
SENSOR_INTERVAL     = int(os.getenv("SENSOR_INTERVAL_SEC",  "30"))
MARKET_INTERVAL     = int(os.getenv("MARKET_INTERVAL_SEC",  "300"))
TTA_INTERVAL        = int(os.getenv("TTA_INTERVAL_SEC",     "30"))

N_FARMS  = 3
N_PANELS = 10
FARMS    = [f"farm_{i:02d}" for i in range(N_FARMS)]
PANELS   = [f"panel_{j:02d}" for j in range(N_PANELS)]


# ── Helpers ────────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def solar_factor() -> float:
    """0–1 bell curve peaking at solar noon (UTC 12:00)."""
    hour = datetime.now(tz=timezone.utc).hour + datetime.now(tz=timezone.utc).minute / 60
    angle = math.pi * max(0.0, (hour - 6) / 12)
    return max(0.0, math.sin(angle)) if 6 <= hour <= 18 else 0.0


def jitter(value: float, pct: float = 0.05) -> float:
    return value * (1 + random.uniform(-pct, pct))


# ── Payload builders ───────────────────────────────────────────────────────────

def build_sensor(farm_id: str) -> dict:
    sf  = solar_factor()
    ts  = now_iso()
    irr = jitter(800 * sf)
    return {
        "farm_id":   farm_id,
        "timestamp": ts,
        "panels": [
            {
                "panel_id":        f"{farm_id}_{p}",
                "timestamp":       ts,
                "power_kw":        round(jitter(max(0.0, sf * 45.0)), 3),
                "irradiance_wm2":  round(irr, 1),
                "inverter_temp_c": round(jitter(35.0 + sf * 20.0), 1),
                "ambient_temp_c":  round(jitter(18.0 + sf * 7.0),  1),
            }
            for p in PANELS
        ],
    }


def build_idb() -> dict:
    sf  = solar_factor()
    soc = jitter(600.0)
    return {
        "timestamp":             now_iso(),
        "battery_soc_kwh":       round(soc, 1),
        "battery_soc_max_kwh":   1000.0,
        "battery_temp_c":        round(jitter(28.0 + sf * 5.0), 1),
        "battery_power_kw":      round(jitter(sf * 80.0 - 20.0), 1),   # + charging daytime
        "solar_power_kw":        round(max(0.0, jitter(sf * 480.0)), 1),
        "grid_exchange_kw":      round(jitter(-30.0), 1),               # slight export
        "compressor_vibration_g": round(random.uniform(0.05, 0.28), 3),
        "compressor_load_pct":   round(jitter(45.0), 1),
    }


def build_market() -> dict:
    # UK day-ahead spot price: higher peak hours, lower off-peak
    hour  = datetime.now(tz=timezone.utc).hour
    base  = 0.28 if 7 <= hour <= 21 else 0.12
    return {
        "timestamp":                now_iso(),
        "price_per_kwh":            round(jitter(base, 0.15), 4),
        "carbon_intensity_gco2_kwh": round(jitter(220.0, 0.20), 1),
    }


def build_tta() -> dict:
    base      = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    steps     = 48
    forecasts = []
    timestamps = []
    for h in range(steps):
        t    = base + timedelta(hours=h)
        hour = t.hour
        sf   = max(0.0, math.sin(math.pi * max(0.0, (hour - 6) / 12))) if 6 <= hour <= 18 else 0.0
        forecasts.append(round(jitter(sf * 420.0, 0.10), 2))
        timestamps.append(t.isoformat())
    return {
        "data_id":                       "solar1_meter_1",
        "timestamp":                     now_iso(),
        "adapted_predictions_denorm":    forecasts,
        "prediction_timestamps":         timestamps,
        "adaptation_gap":                round(random.uniform(0.01, 0.25), 4),
        "original_predictions_denorm":   [round(v * jitter(1.0, 0.08), 2) for v in forecasts],
    }


# ── Sender ─────────────────────────────────────────────────────────────────────

async def post(client: httpx.AsyncClient, path: str, payload: dict) -> bool:
    url = f"{BASE_URL}{path}"
    try:
        r = await client.post(url, json=payload, timeout=10.0)
        r.raise_for_status()
        log.info("POST %-22s → %d  %s", path, r.status_code, r.json().get("status", ""))
        return True
    except httpx.HTTPStatusError as e:
        log.error("POST %-22s → %d  %s", path, e.response.status_code, e.response.text[:120])
    except Exception as e:
        log.error("POST %-22s → FAILED  %s", path, e)
    return False


# ── Loop tasks ─────────────────────────────────────────────────────────────────

async def sensor_loop(client: httpx.AsyncClient) -> None:
    while True:
        for farm in FARMS:
            await post(client, "/ingest/sensor", build_sensor(farm))
        await post(client, "/ingest/telemetries", build_idb())
        await asyncio.sleep(SENSOR_INTERVAL)


async def market_loop(client: httpx.AsyncClient) -> None:
    while True:
        await post(client, "/ingest/market", build_market())
        await asyncio.sleep(MARKET_INTERVAL)


async def tta_loop(client: httpx.AsyncClient) -> None:
    while True:
        await post(client, "/ingest/tta", build_tta())
        await asyncio.sleep(TTA_INTERVAL)


# ── Entry point ────────────────────────────────────────────────────────────────

async def main(url: str) -> None:
    global BASE_URL
    BASE_URL = url.rstrip("/")

    log.info("Target: %s", BASE_URL)
    log.info("Intervals — sensor/IDB: %ds | market: %ds | TTA: %ds",
             SENSOR_INTERVAL, MARKET_INTERVAL, TTA_INTERVAL)

    async with httpx.AsyncClient() as client:
        # Health check first
        try:
            r = await client.get(f"{BASE_URL}/health", timeout=10.0)
            log.info("Health: %s", r.json())
        except Exception as e:
            log.warning("Health check failed: %s — continuing anyway", e)

        await asyncio.gather(
            sensor_loop(client),
            market_loop(client),
            tta_loop(client),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AMOS MTDE payload sender")
    parser.add_argument(
        "--url",
        default=os.getenv("INGEST_API_URL", "https://mtde-production.up.railway.app"),
        help="Base URL of the ingest-api (default: https://mtde-production.up.railway.app)",
    )
    args = parser.parse_args()
    try:
        asyncio.run(main(args.url))
    except KeyboardInterrupt:
        log.info("Stopped.")
