"""
Data Provider – Sensor Publisher
=================================
Run this on YOUR side (on-prem SCADA, edge server, laptop).
Reads real sensor data and pushes it to the cloud MTDE engine via RabbitMQ.

Usage:
    pip install aio-pika
    python publisher.py

Environment variables (or edit CONFIG below):
    RABBITMQ_HOST   cloud server IP or hostname  (default: localhost)
    RABBITMQ_PORT   AMQP port                    (default: 5672)
    RABBITMQ_USER   username                      (default: guest)
    RABBITMQ_PASS   password                      (default: guest)
    PUBLISH_INTERVAL_SEC  seconds between cycles  (default: 30)
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import aio_pika

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG = {
    "host":     os.getenv("RABBITMQ_HOST", "localhost"),
    "port":     int(os.getenv("RABBITMQ_PORT", "5672")),
    "user":     os.getenv("RABBITMQ_USER", "guest"),
    "password": os.getenv("RABBITMQ_PASS", "guest"),
    "interval": int(os.getenv("PUBLISH_INTERVAL_SEC", "30")),
}

EXCHANGE    = "mtde.topic"
ROUTING_KEY = "iot.sensor"          # must match engine's ROUTING_SENSOR_DATA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("provider.publisher")


# ─────────────────────────────────────────────────────────────────────────────
# Replace this function with your real data source
# ─────────────────────────────────────────────────────────────────────────────

def read_farm_sensors(farm_id: str) -> dict:
    """
    REPLACE THIS with your actual sensor reading logic.

    Examples:
        - Call a Modbus/OPC-UA client
        - Query a SCADA REST API
        - Read from a database
        - Parse a CSV/stream from an inverter

    Must return a dict matching SensorDataMessage schema:
    {
        "farm_id":   str,
        "timestamp": ISO-8601 string (UTC),
        "panels": [
            {
                "panel_id":        str,
                "timestamp":       ISO-8601 string (UTC),
                "power_kw":        float,   # DC power output
                "irradiance_wm2":  float,   # plane-of-array irradiance
                "inverter_temp_c": float,   # inverter temperature
            },
            ...
        ]
    }
    """
    # ── STUB: farm-specific degradation profiles ─────────────────────────────
    import math
    import random

    # Each farm has distinct characteristics so maintenance priority differentiates
    FARM_PROFILES = {
        "farm_00": dict(eff_mean=0.20, eff_std=0.005, temp_mean=35.0, temp_std=3.0),   # healthy
        "farm_01": dict(eff_mean=0.14, eff_std=0.015, temp_mean=72.0, temp_std=5.0),   # degrading – low efficiency, hot
        "farm_02": dict(eff_mean=0.17, eff_std=0.010, temp_mean=55.0, temp_std=4.0),   # stable – moderate wear
    }
    profile = FARM_PROFILES.get(farm_id, FARM_PROFILES["farm_00"])

    hour     = datetime.now(tz=timezone.utc).hour
    irr_base = max(0.0, 900 * math.sin(math.pi * (hour - 6) / 12))

    panels = []
    for i in range(10):
        irr   = irr_base * random.uniform(0.90, 1.10)
        eff   = max(0.05, random.gauss(profile["eff_mean"], profile["eff_std"]))
        power = irr * 1.96 * eff / 1_000
        temp  = max(20.0, min(90.0, random.gauss(profile["temp_mean"], profile["temp_std"])))
        panels.append({
            "panel_id":        f"{farm_id}_P{i:03d}",
            "timestamp":       datetime.now(tz=timezone.utc).isoformat(),
            "power_kw":        round(power, 4),
            "irradiance_wm2":  round(irr, 2),
            "inverter_temp_c": round(temp, 1),
        })

    return {
        "farm_id":   farm_id,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "panels":    panels,
    }
    # ── End stub ──────────────────────────────────────────────────────────────


FARM_IDS = ["farm_00", "farm_01", "farm_02"]   # replace with your farm IDs


# ─────────────────────────────────────────────────────────────────────────────
# Publisher – no changes needed below this line
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    url = (
        f"amqp://{CONFIG['user']}:{CONFIG['password']}"
        f"@{CONFIG['host']}:{CONFIG['port']}/"
    )
    logger.info("Connecting to RabbitMQ at %s:%s …", CONFIG["host"], CONFIG["port"])
    connection = await aio_pika.connect_robust(url)
    channel    = await connection.channel()
    exchange   = await channel.declare_exchange(
        EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    logger.info("Connected. Publishing every %ds for farms: %s",
                CONFIG["interval"], FARM_IDS)

    try:
        while True:
            for farm_id in FARM_IDS:
                try:
                    payload = read_farm_sensors(farm_id)
                    body    = json.dumps(payload).encode()
                    await exchange.publish(
                        aio_pika.Message(
                            body=body,
                            content_type="application/json",
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=ROUTING_KEY,
                    )
                    logger.info("Published %d panels for %s", len(payload["panels"]), farm_id)
                except Exception:
                    logger.exception("Failed to read/publish farm %s", farm_id)

            await asyncio.sleep(CONFIG["interval"])

    except asyncio.CancelledError:
        pass
    finally:
        await connection.close()
        logger.info("Publisher disconnected.")


if __name__ == "__main__":
    asyncio.run(main())
