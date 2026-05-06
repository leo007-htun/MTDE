"""
Data Provider – Fleet Schedule Subscriber
==========================================
Run this on YOUR side (control room, SCADA, dashboard server).
Receives optimised dispatch targets from the cloud MTDE engine via RabbitMQ.

Usage:
    pip install aio-pika
    python subscriber.py

Environment variables (or edit CONFIG below):
    RABBITMQ_HOST   cloud server IP or hostname  (default: localhost)
    RABBITMQ_PORT   AMQP port                    (default: 5672)
    RABBITMQ_USER   username                      (default: guest)
    RABBITMQ_PASS   password                      (default: guest)
"""
import asyncio
import json
import logging
import os
from datetime import datetime

import aio_pika

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG = {
    "host":     os.getenv("RABBITMQ_HOST", "localhost"),
    "port":     int(os.getenv("RABBITMQ_PORT", "5672")),
    "user":     os.getenv("RABBITMQ_USER", "guest"),
    "password": os.getenv("RABBITMQ_PASS", "guest"),
}

EXCHANGE    = "mtde.topic"
QUEUE       = "central.fleet_schedule"     # durable queue, messages wait for you
ROUTING_KEY = "central.fleet"              # must match engine's ROUTING_FLEET_SCHEDULE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("provider.subscriber")


# ─────────────────────────────────────────────────────────────────────────────
# Replace this function with your real dispatch logic
# ─────────────────────────────────────────────────────────────────────────────

def apply_dispatch(fleet: dict) -> None:
    """
    REPLACE THIS with your actual control logic.

    Called every time the engine publishes a new fleet schedule (~every 30s).

    `fleet` matches FleetScheduleMessage schema:
    {
        "timestamp":          ISO-8601 string,
        "horizon_hours":      int,
        "total_generation_kw": float,
        "total_carbon_kg":    float,
        "solver_status":      "optimal" | "infeasible" | ...,
        "farm_targets": [
            {
                "farm_id":               str,
                "generation_target_kw":  float,   # set this on your inverter
                "storage_allocation_kwh": float,  # set this on your BMS
                "maintenance_priority":  float,   # 0=fine, 1=urgent
            },
            ...
        ]
    }

    Examples of what you can do here:
        - Send setpoints to inverters via Modbus/OPC-UA
        - Write to your SCADA historian
        - POST to a dashboard API
        - Trigger maintenance alerts when priority > 0.8
        - Log to a database for reporting
    """
    # ── STUB: just print the received targets ─────────────────────────────────
    solver = fleet.get("solver_status", "unknown")
    total  = fleet.get("total_generation_kw", 0)
    co2    = fleet.get("total_carbon_kg", 0)

    logger.info("─" * 60)
    logger.info("Fleet schedule received | solver=%-10s gen=%.1f kW  CO2=%.2f kg",
                solver, total, co2)

    for target in fleet.get("farm_targets", []):
        farm    = target["farm_id"]
        gen_kw  = target["generation_target_kw"]
        storage = target["storage_allocation_kwh"]
        maint   = target["maintenance_priority"]

        logger.info("  Farm %-15s | gen=%.1f kW | storage=%.1f kWh | maint=%.2f%s",
                    farm, gen_kw, storage, maint,
                    "  ⚠ MAINTENANCE NEEDED" if maint > 0.8 else "")

        # Example: call your inverter API here
        # set_inverter_setpoint(farm_id=farm, power_kw=gen_kw)
        # set_bms_allocation(farm_id=farm, kwh=storage)

    logger.info("─" * 60)
    # ── End stub ──────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Subscriber – no changes needed below this line
# ─────────────────────────────────────────────────────────────────────────────

async def on_message(message: aio_pika.IncomingMessage) -> None:
    async with message.process():
        try:
            fleet = json.loads(message.body)
            apply_dispatch(fleet)
        except Exception:
            logger.exception("Failed to process fleet schedule")


async def main() -> None:
    url = (
        f"amqp://{CONFIG['user']}:{CONFIG['password']}"
        f"@{CONFIG['host']}:{CONFIG['port']}/"
    )
    logger.info("Connecting to RabbitMQ at %s:%s …", CONFIG["host"], CONFIG["port"])
    connection = await aio_pika.connect_robust(url)
    channel    = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    exchange = await channel.declare_exchange(
        EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    queue = await channel.declare_queue(QUEUE, durable=True)
    await queue.bind(exchange, routing_key=ROUTING_KEY)

    logger.info("Subscribed to '%s'. Waiting for fleet schedules …", QUEUE)
    await queue.consume(on_message)

    try:
        await asyncio.Future()   # block forever
    except asyncio.CancelledError:
        pass
    finally:
        await connection.close()
        logger.info("Subscriber disconnected.")


if __name__ == "__main__":
    asyncio.run(main())
