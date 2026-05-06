"""
Multi-Tier Decision Engine – main entry point
=============================================

Starts three async workers concurrently inside a single Docker container
(or each in its own container via docker-compose scaling):

    Tier-1  IoTAssetLayerWorker
    Tier-2  RegionalEdgeLayerWorker
    Tier-3  CentralOptimizationLayerWorker
    Exec    ControlExecutorWorker   (consumes fleet schedule, applies control)

Additionally spawns a SensorSimulator that continuously injects synthetic
sensor readings so the pipeline can run end-to-end without real hardware.

Message flow
------------
  [Simulator]
      │  iot.sensor_data  (routing: iot.sensor)
      ▼
  [IoT Layer]
      │  iot.panel_health  (routing: iot.health)
      ▼
  [Regional Edge Layer]  ← Seoultech TTA + Pyomo MPC
      │  ← strategy.profile  (from Agent Strategic Layer, 5-hour cycle)
      │  regional.schedule  (routing: regional.schedule)
      ▼
  [Central Layer]  ← fleet LP
      │  central.fleet_schedule  (routing: central.fleet)
      ▼
  [Control Executor]  ← logs / applies setpoints

  [Seoultech TTA] ──► [Consensus Layer]  ← 5-hour rolling buffer
                           │  consensus.metrics
                           ▼
                  [Agent Strategic Layer]  ← Bytez Qwen3-0.6B LLM
                           │  strategy.profile  (→ Regional Edge Layer)
                           ▼
                  [Enverse UI / Dashboard]  ← NL rationale + health class
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
from datetime import datetime, timezone

import httpx

from config.settings import CONTROL_INTERVAL_SEC, INGEST_API_URL
from src.iot_layer                import IoTAssetLayerWorker
from src.regional_edge_layer      import RegionalEdgeLayerWorker
from src.central_layer            import CentralOptimizationLayerWorker
from src.consensus_layer          import ConsensusLayerWorker
from src.agent_strategic_layer    import AgentStrategicLayerWorker
from src.models.data_models       import PanelSensorReading, SensorDataMessage

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("mtde.main")


# ─────────────────────────────────────────────────────────────────────────────
# Control Executor
# ─────────────────────────────────────────────────────────────────────────────

class ControlExecutorWorker:
    """
    Tier-4 subscriber: consumes fleet schedule and 'applies' control.
    In production this calls SCADA / inverter APIs.
    """

    def __init__(self) -> None:
        import aio_pika
        from config.settings import (
            EXCHANGE_MAIN, QUEUE_FLEET_SCHEDULE, ROUTING_FLEET_SCHEDULE
        )
        self._aio_pika             = aio_pika
        self._exchange_name        = EXCHANGE_MAIN
        self._queue_name           = QUEUE_FLEET_SCHEDULE
        self._routing_key          = ROUTING_FLEET_SCHEDULE
        self._connection           = None

    async def connect(self) -> None:
        import json
        import aio_pika
        from config.settings import RABBITMQ_URL, EXCHANGE_MAIN, QUEUE_FLEET_SCHEDULE, ROUTING_FLEET_SCHEDULE
        from src.models.data_models import FleetScheduleMessage

        self._connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel  = await self._connection.channel()
        exchange = await channel.declare_exchange(EXCHANGE_MAIN, aio_pika.ExchangeType.TOPIC, durable=True)
        queue    = await channel.declare_queue(QUEUE_FLEET_SCHEDULE, durable=True)
        await queue.bind(exchange, routing_key=ROUTING_FLEET_SCHEDULE)

        async def _on_fleet(msg: aio_pika.IncomingMessage) -> None:
            async with msg.process():
                try:
                    fleet = FleetScheduleMessage.model_validate_json(msg.body)
                    logger.info(
                        "CONTROL EXEC | farms=%d | gen=%.1f kW | CO2=%.2f kg | solver=%s",
                        len(fleet.farm_targets),
                        fleet.total_generation_kw,
                        fleet.total_carbon_kg,
                        fleet.solver_status,
                    )
                    for target in fleet.farm_targets:
                        logger.info(
                            "  → Farm %-15s | gen_target=%.1f kW | storage=%.1f kWh | maint=%.2f",
                            target.farm_id,
                            target.generation_target_kw,
                            target.storage_allocation_kwh,
                            target.maintenance_priority,
                        )
                except Exception:
                    logger.exception("Control Executor failed")

        await queue.consume(_on_fleet)
        logger.info("Control Executor connected, listening on %s", QUEUE_FLEET_SCHEDULE)

    async def run(self) -> None:
        await self.connect()
        await asyncio.Future()

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()


# ─────────────────────────────────────────────────────────────────────────────
# Sensor Simulator  (synthetic data injector)
# ─────────────────────────────────────────────────────────────────────────────

class SensorSimulator:
    """
    POSTs synthetic sensor readings to the Ingest API every CONTROL_INTERVAL_SEC
    seconds so the pipeline runs without physical hardware.
    Partners send data the same way — POST to /ingest/sensor.
    """

    N_PANELS  = int(os.getenv("SIM_N_PANELS", "10"))
    N_FARMS   = int(os.getenv("SIM_N_FARMS",  "3"))
    _ENDPOINT = f"{INGEST_API_URL}/ingest/sensor"

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def run(self) -> None:
        if self.N_FARMS == 0:
            logger.info("Sensor Simulator disabled (SIM_N_FARMS=0) – waiting for real data")
            await asyncio.Future()
            return
        self._client = httpx.AsyncClient(timeout=10.0)
        logger.info("Sensor Simulator started: %d farms × %d panels → %s",
                    self.N_FARMS, self.N_PANELS, self._ENDPOINT)
        await asyncio.sleep(STARTUP_DELAY_SEC)
        while True:
            for farm_idx in range(self.N_FARMS):
                await self._emit_farm(f"farm_{farm_idx:02d}")
            await asyncio.sleep(CONTROL_INTERVAL_SEC)

    async def _emit_farm(self, farm_id: str) -> None:
        import math
        hour = datetime.now(tz=timezone.utc).hour
        angle = math.pi * (hour - 6) / 12
        irr_base = max(0.0, 900 * math.sin(angle))

        panels = []
        for i in range(self.N_PANELS):
            irr   = irr_base * random.uniform(0.90, 1.10)
            power = irr * 1.96 * random.uniform(0.18, 0.21) / 1_000
            temp  = random.uniform(30.0, 75.0)
            panels.append(PanelSensorReading(
                panel_id=f"{farm_id}_P{i:03d}",
                timestamp=datetime.now(tz=timezone.utc),
                power_kw=power,
                irradiance_wm2=irr,
                inverter_temp_c=temp,
            ))

        msg = SensorDataMessage(
            farm_id=farm_id,
            timestamp=datetime.now(tz=timezone.utc),
            panels=panels,
        )
        try:
            resp = await self._client.post(
                self._ENDPOINT,
                content=msg.model_dump_json(),
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            logger.debug("Simulator → /ingest/sensor | farm=%s panels=%d", farm_id, len(panels))
        except Exception as exc:
            logger.warning("Simulator failed to POST sensor data for %s: %s", farm_id, exc)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()


# ─────────────────────────────────────────────────────────────────────────────
# Startup / shutdown orchestration
# ─────────────────────────────────────────────────────────────────────────────

STARTUP_DELAY_SEC = int(os.getenv("STARTUP_DELAY_SEC", "5"))


async def _wait_for_rabbitmq(max_attempts: int = 20, delay: float = 3.0) -> None:
    """Poll until RabbitMQ is reachable."""
    import aio_pika
    from config.settings import RABBITMQ_URL
    for attempt in range(1, max_attempts + 1):
        try:
            conn = await aio_pika.connect_robust(RABBITMQ_URL)
            await conn.close()
            logger.info("RabbitMQ ready (attempt %d)", attempt)
            return
        except Exception as exc:
            logger.warning("Waiting for RabbitMQ … (attempt %d/%d): %s",
                           attempt, max_attempts, exc)
            await asyncio.sleep(delay)
    raise RuntimeError("RabbitMQ not reachable after %d attempts" % max_attempts)


async def main() -> None:
    logger.info("Multi-Tier Decision Engine starting …")
    await _wait_for_rabbitmq()
    await asyncio.sleep(STARTUP_DELAY_SEC)   # let exchange declarations settle

    workers = [
        IoTAssetLayerWorker(),
        RegionalEdgeLayerWorker(),
        CentralOptimizationLayerWorker(),
        ControlExecutorWorker(),
        SensorSimulator(),
        ConsensusLayerWorker(),
        AgentStrategicLayerWorker(),
    ]

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler(*_):
        logger.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    tasks = [asyncio.create_task(w.run(), name=type(w).__name__) for w in workers]

    logger.info("All %d workers started", len(tasks))
    await shutdown_event.wait()

    logger.info("Shutting down …")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    for w in workers:
        try:
            await w.close()
        except Exception:
            pass

    logger.info("Engine stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
