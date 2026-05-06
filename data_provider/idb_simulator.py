"""
IDB Protect GO – Synthetic Telemetry Publisher
===============================================
Simulates the IDB Protect GO edge hardware publishing real-time telemetry
to the AMOS Ingest API every PUBLISH_INTERVAL_SEC seconds.

Realistic behaviour modelled:
  - Battery SoC charges during solar peak, discharges overnight
  - Battery temperature rises with charge/discharge rate, cools at rest
  - Solar output follows diurnal curve with cloud scatter
  - Grid exchange = demand − (solar + battery discharge)
  - Compressor load peaks mid-morning and late afternoon
  - Compressor vibration baseline ~0.15g with random degradation events

Matches IDBTelemetry Pydantic schema in src/models/data_models.py.

Run:
    pip install httpx
    python data_provider/idb_simulator.py

    # Fast demo (5-second publish interval):
    PUBLISH_INTERVAL_SEC=5 python data_provider/idb_simulator.py

    # Simulate a degrading compressor (vibration drifts upward):
    DEGRADE_MODE=1 python data_provider/idb_simulator.py

Environment variables:
    INGEST_API_URL         (default: http://localhost:8000)
    PUBLISH_INTERVAL_SEC   seconds between publishes  (default: 30)
    BATT_MAX_KWH           battery capacity           (default: 1000.0)
    P_SOLAR_MAX_KW         peak solar capacity        (default: 500.0)
    P_DEMAND_KW            flat base demand           (default: 280.0)
    DEGRADE_MODE           1 = slowly increasing vibration (default: 0)
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import random
from datetime import datetime, timezone

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("idb.simulator")

# ── Config ─────────────────────────────────────────────────────────────────────
INGEST_API_URL   = os.getenv("INGEST_API_URL",       "http://localhost:8000")
ENDPOINT         = f"{INGEST_API_URL}/ingest/telemetries"
PUBLISH_INTERVAL = int(os.getenv("PUBLISH_INTERVAL_SEC", "30"))
BATT_MAX_KWH     = float(os.getenv("BATT_MAX_KWH",      "1000.0"))
P_SOLAR_MAX_KW   = float(os.getenv("P_SOLAR_MAX_KW",    "500.0"))
P_DEMAND_KW      = float(os.getenv("P_DEMAND_KW",       "280.0"))
DEGRADE_MODE     = int(os.getenv("DEGRADE_MODE",        "0"))


# ── Physical model ─────────────────────────────────────────────────────────────

def _solar_kw(now: datetime) -> float:
    """Simple diurnal solar curve (London noon ≈ 12:00 UTC in summer)."""
    hour = now.hour + now.minute / 60.0
    angle = math.pi * (hour - 6) / 12          # zero at 06:00 and 18:00
    clearsky = max(0.0, math.sin(angle))
    cloud    = random.uniform(0.70, 1.05)       # light cloud scatter
    return round(P_SOLAR_MAX_KW * clearsky * cloud, 2)


def _compressor_load_pct(now: datetime) -> float:
    """Compressor load peaks mid-morning and late afternoon."""
    hour = now.hour + now.minute / 60.0
    # two humps: ~09:00 and ~16:00
    peak1 = 60 * math.exp(-0.5 * ((hour - 9) / 2) ** 2)
    peak2 = 55 * math.exp(-0.5 * ((hour - 16) / 2) ** 2)
    base  = 20.0
    noise = random.uniform(-3.0, 3.0)
    return round(min(100.0, max(0.0, base + peak1 + peak2 + noise)), 1)


class BatteryModel:
    """
    Simple state-of-charge integrator.
    Charges when solar > demand, discharges otherwise.
    Respects SoC bounds [0.10, 0.95] × BATT_MAX_KWH.
    """

    SOC_MIN = 0.10
    SOC_MAX = 0.95
    CHARGE_EFF  = 0.95
    DISCHARGE_EFF = 0.92
    MAX_RATE_KW = 200.0   # max charge/discharge rate

    def __init__(self) -> None:
        self.soc_kwh   = BATT_MAX_KWH * 0.50   # start at 50 %
        self.temp_c    = 25.0                    # ambient start

    def step(self, solar_kw: float, demand_kw: float, dt_hours: float) -> tuple[float, float, float]:
        """
        Returns (battery_power_kw, grid_exchange_kw, battery_temp_c).
        battery_power_kw: positive = charging, negative = discharging
        grid_exchange_kw: positive = importing from grid
        """
        net = solar_kw - demand_kw              # surplus (+ ) or deficit (−)

        if net >= 0:
            # Surplus → charge battery
            charge_kw  = min(net, self.MAX_RATE_KW,
                             (BATT_MAX_KWH * self.SOC_MAX - self.soc_kwh) / (dt_hours + 1e-9))
            charge_kw  = max(0.0, charge_kw)
            self.soc_kwh += charge_kw * dt_hours * self.CHARGE_EFF
            grid_kw      = -(net - charge_kw)  # export surplus
            batt_kw      = charge_kw
        else:
            # Deficit → discharge battery
            needed       = -net
            discharge_kw = min(needed, self.MAX_RATE_KW,
                               (self.soc_kwh - BATT_MAX_KWH * self.SOC_MIN) / (dt_hours + 1e-9) * self.DISCHARGE_EFF)
            discharge_kw = max(0.0, discharge_kw)
            self.soc_kwh -= discharge_kw * dt_hours / self.DISCHARGE_EFF
            grid_kw       = needed - discharge_kw   # import shortfall
            batt_kw       = -discharge_kw

        # Clamp SoC
        self.soc_kwh = max(BATT_MAX_KWH * self.SOC_MIN,
                           min(BATT_MAX_KWH * self.SOC_MAX, self.soc_kwh))

        # Battery temperature: rises with |power|, decays toward ambient (25°C)
        power_heat   = abs(batt_kw) / self.MAX_RATE_KW * 18.0   # up to +18°C at full rate
        ambient_c    = 25.0 + 5.0 * math.sin(math.pi * datetime.now(tz=timezone.utc).hour / 12)
        self.temp_c += (power_heat - (self.temp_c - ambient_c) * 0.15) * dt_hours
        self.temp_c  = round(max(15.0, min(65.0, self.temp_c)), 1)

        return round(batt_kw, 2), round(grid_kw, 2), self.temp_c


class CompressorModel:
    """
    Vibration baseline ~0.12–0.18g.
    DEGRADE_MODE=1: baseline drifts up by ~0.002g/publish toward 0.60g.
    Occasional random spikes regardless of mode.
    """

    def __init__(self) -> None:
        self._baseline  = 0.15
        self._spike_ttl = 0          # spike counter (steps remaining)

    def vibration_g(self) -> float:
        # Gradual degradation drift
        if DEGRADE_MODE:
            self._baseline = min(0.60, self._baseline + random.uniform(0.001, 0.003))

        # Random spike event (5 % chance each step, lasts 2–5 steps)
        if self._spike_ttl > 0:
            self._spike_ttl -= 1
            spike = random.uniform(0.20, 0.45)
        elif random.random() < 0.05:
            self._spike_ttl = random.randint(2, 5)
            spike = random.uniform(0.20, 0.45)
        else:
            spike = 0.0

        noise = random.gauss(0, 0.008)
        return round(max(0.0, self._baseline + spike + noise), 4)


# ── Main publish loop ──────────────────────────────────────────────────────────

async def main() -> None:
    logger.info(
        "IDB simulator → %s | interval=%ds | batt=%.0f kWh | degrade=%s",
        ENDPOINT, PUBLISH_INTERVAL, BATT_MAX_KWH, "ON" if DEGRADE_MODE else "off",
    )

    battery    = BatteryModel()
    compressor = CompressorModel()
    dt_hours   = PUBLISH_INTERVAL / 3600.0
    cycle      = 0

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                now       = datetime.now(tz=timezone.utc)
                solar_kw  = _solar_kw(now)
                demand_kw = P_DEMAND_KW + random.uniform(-20, 20)

                batt_kw, grid_kw, batt_temp = battery.step(solar_kw, demand_kw, dt_hours)
                vibration  = compressor.vibration_g()
                comp_load  = _compressor_load_pct(now)
                cycle     += 1

                payload = {
                    "timestamp":              now.isoformat(),
                    "battery_soc_kwh":        round(battery.soc_kwh, 2),
                    "battery_soc_max_kwh":    BATT_MAX_KWH,
                    "battery_temp_c":         batt_temp,
                    "battery_power_kw":       batt_kw,
                    "solar_power_kw":         solar_kw,
                    "grid_exchange_kw":       grid_kw,
                    "compressor_vibration_g": vibration,
                    "compressor_load_pct":    comp_load,
                }

                resp = await client.post(ENDPOINT, json=payload)
                resp.raise_for_status()

                soc_pct = battery.soc_kwh / BATT_MAX_KWH * 100
                logger.info(
                    "[%4d] POST → %d | SoC=%5.1f%% (%6.1f kWh) | batt=%+6.1f kW "
                    "| solar=%5.1f kW | grid=%+6.1f kW | vib=%.3fg | comp=%4.1f%%",
                    cycle, resp.status_code, soc_pct, battery.soc_kwh,
                    batt_kw, solar_kw, grid_kw, vibration, comp_load,
                )

            except httpx.HTTPStatusError as exc:
                logger.error("Ingest API rejected IDB payload: %s – %s", exc.response.status_code, exc.response.text)
            except Exception as exc:
                logger.warning("Failed to POST IDB telemetry: %s", exc)

            await asyncio.sleep(PUBLISH_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
