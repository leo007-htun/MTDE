"""
AMOS – Enverse CSV Replay
=========================
Reads a 6707ba_*.csv file from Enverse and feeds real inverter data
into the AMOS ingest-api. Also sends synthetic IDB telemetry and market
signals so all four dashboard cards populate from a single script.

Usage:
    python scripts/enverse_replay.py
    python scripts/enverse_replay.py Enverse_data/6707ba_2026-05-15-17-00-14_24h.csv
    python scripts/enverse_replay.py --url https://mtde-production.up.railway.app
    python scripts/enverse_replay.py --delay 2.0          # seconds between rows (default 1.0)
    python scripts/enverse_replay.py --realtime           # sleep to match real 1-min intervals
    python scripts/enverse_replay.py --skip-night         # skip rows where irradiance == 0

Columns used:
    timestamp           → SensorDataMessage.timestamp
    {inv}-Active Power  → power_kw  (per inverter)
    bLTGSH-Irradiance   → irradiance_wm2 (shared across all inverters)
    inverter_temp_c     → 45.0 constant (not in CSV)

Side-car data (synthetic, sent periodically):
    /ingest/tta          every 60 rows
    /ingest/telemetries  every 10 rows  (IDB battery hardware telemetry)
    /ingest/market       every 20 rows  (electricity market price signal)
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import math
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("enverse-replay")

# ── Constants ──────────────────────────────────────────────────────────────────

FARM_ID   = "6707ba"
INVERTERS = ["ELDCwM", "PQMwDB", "QUwKPC", "ethTug", "mnsfgX", "qfpgEw"]
INVERTER_TEMP_C  = 45.0
AMBIENT_TEMP_C   = 25.0

TTA_EVERY_N_ROWS    = 60   # ~1 per hour of data
IDB_EVERY_N_ROWS    = 10   # battery telemetry
MARKET_EVERY_N_ROWS = 20   # market price signal

DEFAULT_URL = os.getenv("INGEST_API_URL", "https://mtde-production.up.railway.app")


# ── CSV helpers ────────────────────────────────────────────────────────────────

def find_latest_csv() -> Path:
    candidates = sorted(Path("Enverse_data").glob("6707ba_*.csv"), reverse=True)
    if not candidates:
        candidates = sorted(Path(".").glob("6707ba_*.csv"), reverse=True)
    if not candidates:
        raise FileNotFoundError("No 6707ba_*.csv found in Enverse_data/ or current directory")
    return candidates[0]


def _f(row: dict, col: str) -> float:
    v = row.get(col, "")
    try:
        return float(v) if v not in ("", None) else 0.0
    except ValueError:
        return 0.0


# ── Payload builders ───────────────────────────────────────────────────────────

def build_sensor_payload(row: dict) -> dict:
    ts = datetime.fromisoformat(row["timestamp"]).astimezone(timezone.utc).isoformat()
    irr = max(0.0, _f(row, "bLTGSH-Irradiance"))
    return {
        "farm_id":   FARM_ID,
        "timestamp": ts,
        "panels": [
            {
                "panel_id":        f"{FARM_ID}_{inv}",
                "timestamp":       ts,
                "power_kw":        max(0.0, _f(row, f"{inv}-Active Power")),
                "irradiance_wm2":  irr,
                "inverter_temp_c": INVERTER_TEMP_C,
                "ambient_temp_c":  AMBIENT_TEMP_C,
            }
            for inv in INVERTERS
        ],
    }


def build_tta_payload(reference_ts: datetime) -> dict:
    base = reference_ts.replace(minute=0, second=0, microsecond=0)
    forecasts, timestamps = [], []
    for h in range(48):
        t    = base + timedelta(hours=h)
        hour = t.hour
        sf   = max(0.0, math.sin(math.pi * max(0.0, (hour - 6) / 12))) if 6 <= hour <= 18 else 0.0
        forecasts.append(round(sf * 200.0 * (1 + random.uniform(-0.08, 0.08)), 2))
        timestamps.append(t.isoformat())
    return {
        "data_id":                     f"{FARM_ID}_meter_1",
        "timestamp":                   reference_ts.isoformat(),
        "adapted_predictions_denorm":  forecasts,
        "prediction_timestamps":       timestamps,
        "adaptation_gap":              round(random.uniform(0.05, 0.25), 4),
        "original_predictions_denorm": [round(v * (1 + random.uniform(-0.06, 0.06)), 2)
                                        for v in forecasts],
    }


def build_idb_payload(solar_kw: float) -> dict:
    soc = round(random.uniform(400.0, 800.0), 1)
    return {
        "timestamp":              datetime.now(tz=timezone.utc).isoformat(),
        "battery_soc_kwh":        soc,
        "battery_soc_max_kwh":    1000.0,
        "battery_temp_c":         round(random.uniform(26.0, 34.0), 1),
        "battery_power_kw":       round(solar_kw * 0.15 - 10.0, 1),  # charge when sunny
        "solar_power_kw":         round(solar_kw, 1),
        "grid_exchange_kw":       round(random.uniform(-40.0, 10.0), 1),
        "compressor_vibration_g": round(random.uniform(0.05, 0.22), 3),
        "compressor_load_pct":    round(random.uniform(30.0, 60.0), 1),
    }


def build_market_payload() -> dict:
    hour = datetime.now(tz=timezone.utc).hour
    base = 0.28 if 7 <= hour <= 21 else 0.12
    return {
        "timestamp":                 datetime.now(tz=timezone.utc).isoformat(),
        "price_per_kwh":             round(base * (1 + random.uniform(-0.15, 0.15)), 4),
        "carbon_intensity_gco2_kwh": round(random.uniform(180.0, 260.0), 1),
    }


# ── HTTP helper ────────────────────────────────────────────────────────────────

async def post(client: httpx.AsyncClient, base_url: str, path: str, payload: dict) -> bool:
    try:
        r = await client.post(f"{base_url}{path}", json=payload, timeout=15.0)
        r.raise_for_status()
        return True
    except httpx.HTTPStatusError as e:
        log.error("POST %s → %d  %s", path, e.response.status_code, e.response.text[:120])
    except Exception as e:
        log.error("POST %s → FAILED  %s", path, e)
    return False


# ── Main replay loop ───────────────────────────────────────────────────────────

async def replay(csv_path: Path, base_url: str, delay: float, realtime: bool, skip_night: bool) -> None:
    log.info("CSV:    %s", csv_path)
    log.info("Target: %s", base_url)
    log.info("Mode:   %s | delay=%.1fs | skip_night=%s",
             "realtime" if realtime else "fast", delay, skip_night)

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{base_url}/health", timeout=10.0)
            log.info("Health: %s", r.json())
        except Exception as e:
            log.warning("Health check failed (%s) — continuing anyway", e)

        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        log.info("Rows to replay: %d", len(rows))

        prev_ts: datetime | None = None
        tta_counter = idb_counter = market_counter = 0

        for i, row in enumerate(rows):
            irr = _f(row, "bLTGSH-Irradiance")
            if skip_night and irr == 0.0:
                continue

            # ── Real sensor data ──────────────────────────────────────────────
            ok = await post(client, base_url, "/ingest/sensor", build_sensor_payload(row))
            cur_ts   = datetime.fromisoformat(row["timestamp"]).astimezone(timezone.utc)
            total_kw = sum(max(0.0, _f(row, f"{inv}-Active Power")) for inv in INVERTERS)

            if ok:
                log.info("Row %4d/%d | %s | irr=%.0f W/m² | total=%.1f kW",
                         i + 1, len(rows), cur_ts.strftime("%H:%M UTC"), irr, total_kw)

            # ── IDB hardware telemetry ────────────────────────────────────────
            idb_counter += 1
            if idb_counter >= IDB_EVERY_N_ROWS:
                idb_counter = 0
                await post(client, base_url, "/ingest/telemetries", build_idb_payload(total_kw))

            # ── Market signal ─────────────────────────────────────────────────
            market_counter += 1
            if market_counter >= MARKET_EVERY_N_ROWS:
                market_counter = 0
                await post(client, base_url, "/ingest/market", build_market_payload())

            # ── TTA forecast ──────────────────────────────────────────────────
            tta_counter += 1
            if tta_counter >= TTA_EVERY_N_ROWS:
                tta_counter = 0
                await post(client, base_url, "/ingest/tta", build_tta_payload(cur_ts))
                log.info("TTA sent for %s", cur_ts.strftime("%Y-%m-%d %H:%M UTC"))

            # ── Pacing ────────────────────────────────────────────────────────
            if realtime and prev_ts is not None:
                gap = (cur_ts - prev_ts).total_seconds()
                if gap > 0:
                    await asyncio.sleep(gap)
            elif delay > 0:
                await asyncio.sleep(delay)

            prev_ts = cur_ts

        log.info("Replay complete — %d rows sent", len(rows))


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enverse CSV → AMOS ingest replay")
    parser.add_argument("csv", nargs="?", help="Path to 6707ba_*.csv (auto-detects latest if omitted)")
    parser.add_argument("--url",        default=DEFAULT_URL, help="Base URL of ingest-api")
    parser.add_argument("--delay",      type=float, default=1.0, help="Seconds between rows in fast mode (default 1.0)")
    parser.add_argument("--realtime",   action="store_true", help="Sleep to match real 1-minute row intervals")
    parser.add_argument("--skip-night", action="store_true", help="Skip rows where irradiance == 0")
    args = parser.parse_args()

    csv_path = Path(args.csv) if args.csv else find_latest_csv()
    if not csv_path.exists():
        raise SystemExit(f"File not found: {csv_path}")

    try:
        asyncio.run(replay(
            csv_path   = csv_path,
            base_url   = args.url.rstrip("/"),
            delay      = args.delay,
            realtime   = args.realtime,
            skip_night = args.skip_night,
        ))
    except KeyboardInterrupt:
        log.info("Stopped.")
