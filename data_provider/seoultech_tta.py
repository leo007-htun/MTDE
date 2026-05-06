"""
Seoultech TTA -- live synthetic tta_predictions stream
=======================================================
Simulates a real solar farm publishing 48-hour TTA forecasts every hour.
POSTs to the AMOS Ingest API so Regional Edge Layer picks up
adapted_predictions_denorm and uses it instead of pvlib.

Realistic behaviour modelled:
  - Diurnal solar curve (Singapore solar noon ~13:00)
  - Seasonal day-length shift (month-aware sunrise/sunset)
  - Random cloud events (0-3 per day, reduce output 30-70%)
  - Morning ramp-up / evening ramp-down with scatter
  - TTA-adapted values tighter than base model (±3% vs ±12%)
  - Zero generation during night hours

Run:
    pip install httpx
    python data_provider/seoultech_tta.py            # publish every hour
    PUBLISH_INTERVAL_SEC=5 python data_provider/seoultech_tta.py   # fast demo

Environment variables:
    INGEST_API_URL        (default: http://localhost:8000)
    PUBLISH_INTERVAL_SEC  (default: 3600)
"""
import asyncio
import logging
import math
import os
import random
from datetime import datetime, timedelta, timezone

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seoultech.tta")

# ── Config ─────────────────────────────────────────────────────────────────────
DATA_ID          = os.getenv("SEOULTECH_DATA_ID",        "solar1_meter_1")
P_MAX_KW         = float(os.getenv("P_MAX_KW",           "900.0"))
FORECAST_HOURS   = int(os.getenv("FORECAST_HOURS",       "48"))
LOOKBACK_HOURS   = int(os.getenv("LOOKBACK_HOURS",       "96"))
PUBLISH_INTERVAL = int(os.getenv("PUBLISH_INTERVAL_SEC", "3600"))

INGEST_API_URL = os.getenv("INGEST_API_URL", "http://localhost:8000")
ENDPOINT       = f"{INGEST_API_URL}/ingest/tta"

# ── Solar physics ──────────────────────────────────────────────────────────────

def _daylight_fraction(month: int) -> float:
    """Approximate Singapore day-length fraction (equatorial, small swing)."""
    # Singapore daylight varies roughly 11.8h – 12.2h
    return 0.495 + 0.01 * math.cos(2 * math.pi * (month - 1) / 12)

def _solar_clearsky(hour_utc: float, month: int) -> float:
    """
    Clear-sky fraction [0..1] for a given UTC hour.
    Singapore is UTC+8, solar noon local ~13:00 = 05:00 UTC.
    """
    solar_noon_utc = 5.0                            # 13:00 SGT = 05:00 UTC
    dl = _daylight_fraction(month) * 24             # daylight hours (~12h)
    half = dl / 2
    angle = math.pi * (hour_utc - solar_noon_utc) / half
    return max(0.0, math.sin(angle))

def _build_cloud_events(n_hours: int, now: datetime) -> dict[int, float]:
    """
    Randomly place 0-3 cloud events in the forecast window.
    Returns {hour_offset: attenuation_factor}.
    """
    events: dict[int, float] = {}
    n_clouds = random.randint(0, 3)
    for _ in range(n_clouds):
        offset    = random.randint(1, n_hours - 1)
        duration  = random.randint(1, 4)            # cloud lasts 1-4 hours
        intensity = random.uniform(0.30, 0.70)      # 30-70% reduction
        for d in range(duration):
            if offset + d < n_hours:
                events[offset + d] = 1.0 - intensity
    return events

# ── Message builder ────────────────────────────────────────────────────────────

def build_tta_message(now: datetime) -> dict:
    cloud_events = _build_cloud_events(FORECAST_HOURS, now)

    timestamps, original_preds, adapted_preds = [], [], []

    for h in range(1, FORECAST_HOURS + 1):
        ts    = now + timedelta(hours=h)
        frac  = _solar_clearsky(ts.hour + ts.minute / 60, ts.month)
        cloud = cloud_events.get(h, 1.0)

        # base model: wider scatter (±12%)
        base  = P_MAX_KW * frac * cloud * random.uniform(0.88, 1.12)
        # TTA-adapted: tighter (±3%), slight bias correction toward clearsky
        adapt = P_MAX_KW * frac * cloud * random.uniform(0.97, 1.03)

        timestamps.append(ts.isoformat())
        original_preds.append(round(base,  4))
        adapted_preds.append(round(adapt, 4))

    # adaptation_gap: mean absolute difference between base and TTA-adapted forecasts,
    # normalised by P_MAX_KW.  This is the health stress indicator used by the
    # ConsensusLayer — a larger gap means the base model needed more correction,
    # indicating equipment operating outside its training distribution.
    adaptation_gap = sum(
        abs(a - b) for a, b in zip(adapted_preds, original_preds)
    ) / (len(adapted_preds) * P_MAX_KW + 1e-9)

    return {
        "data_id":                     DATA_ID,
        "timestamp":                   now.isoformat(),
        "original_predictions_denorm": original_preds,
        "adapted_predictions_denorm":  adapted_preds,
        "prediction_timestamps":       timestamps,
        "adaptation_gap":              round(adaptation_gap, 6),   # health indicator for ConsensusLayer
        "model_info": {
            "model_type":       "DLinear",
            "checkpoint":       f"{DATA_ID}_model.pth",
            "lookback_hours":   LOOKBACK_HOURS,
            "forecast_horizon": FORECAST_HOURS,
            "training_columns": 30,
            "target_column":    "Active Power",
        },
        "adaptation_info": {
            "adapter":        "TAFAS",
            "adapted_at":     now.isoformat(),
            "n_ground_truth": LOOKBACK_HOURS,
            "tta_enabled":    True,
        },
    }

# ── Main loop ──────────────────────────────────────────────────────────────────

async def main() -> None:
    logger.info("Seoultech TTA provider → %s (every %ds)", ENDPOINT, PUBLISH_INTERVAL)

    cycle = 0
    async with httpx.AsyncClient(timeout=15.0) as client:
        while True:
            try:
                now = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
                msg = build_tta_message(now)
                cycle += 1

                peak     = max(msg["adapted_predictions_denorm"])
                nonzero  = sum(1 for v in msg["adapted_predictions_denorm"] if v > 0)
                n_clouds = sum(1 for v in msg["adapted_predictions_denorm"]
                               if 0 < v < P_MAX_KW * 0.5)

                resp = await client.post(
                    ENDPOINT,
                    json=msg,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
                logger.info(
                    "[cycle %d] POST %s → %d | peak=%.1f kW | daylight=%d/48 | cloudy=%d",
                    cycle, ENDPOINT, resp.status_code, peak, nonzero, n_clouds,
                )

            except httpx.HTTPStatusError as exc:
                logger.error("Ingest API rejected TTA payload: %s – %s", exc.response.status_code, exc.response.text)
            except Exception as exc:
                logger.warning("Failed to POST TTA forecast: %s", exc)

            await asyncio.sleep(PUBLISH_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
