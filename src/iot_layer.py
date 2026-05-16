"""
Tier-1 – IoT Asset Layer
========================
Consumes raw sensor readings from RabbitMQ (queue: iot.sensor_data),
computes per-panel Health Index (HI) and degradation flags, then
publishes PanelHealthMessage to queue: iot.panel_health.

Health Index formula (from pseudo-code):
    HI = 0.5 * (η / η_expected) + 0.5 * (1 − T_inv / T_max_safe)

Decision published downstream per panel:
    • health_index  ∈ [0, 1]
    • degradation_flag  →  True when η < η_expected − tolerance
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import aio_pika

from config.settings import (
    EXPECTED_EFFICIENCY,
    EFFICIENCY_TOLERANCE,
    MAX_SAFE_TEMP_C,
    PANEL_AREA_M2,
    RABBITMQ_URL,
    EXCHANGE_MAIN,
    QUEUE_SENSOR_DATA,
    QUEUE_PANEL_HEALTH,
    ROUTING_SENSOR_DATA,
    ROUTING_PANEL_HEALTH,
)
from src.models.data_models import (
    SensorDataMessage,
    PanelHealth,
    PanelHealthMessage,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Core computation  (pure, testable, no I/O)
# ─────────────────────────────────────────────────────────────────────────────

def compute_panel_health(
    panel_id: str,
    power_kw: float,
    irradiance_wm2: float,
    inverter_temp_c: float,
    panel_area_m2: float = PANEL_AREA_M2,
    expected_efficiency: float = EXPECTED_EFFICIENCY,
    efficiency_tolerance: float = EFFICIENCY_TOLERANCE,
    max_safe_temp: float = MAX_SAFE_TEMP_C,
) -> PanelHealth:
    """
    Compute Health Index for a single panel.

    Parameters
    ----------
    power_kw         : measured DC power output (kW)
    irradiance_wm2   : plane-of-array irradiance (W/m²)
    inverter_temp_c  : current inverter temperature (°C)

    Returns
    -------
    PanelHealth dataclass with hi, degradation_flag, efficiency
    """
    # Guard against zero irradiance (night / sensor error)
    # Efficiency can't be measured without irradiance; use temperature component only
    if irradiance_wm2 <= 0:
        temp_ratio = inverter_temp_c / max_safe_temp
        temp_component = max(0.0, 1.0 - temp_ratio)
        hi = max(0.0, min(0.5 * 1.0 + 0.5 * temp_component, 1.0))
        return PanelHealth(
            panel_id=panel_id,
            health_index=hi,
            efficiency=0.0,
            degradation_flag=False,
            inverter_temp_c=inverter_temp_c,
            power_kw=power_kw,
            irradiance_wm2=irradiance_wm2,
        )

    # η  = P_out / (G × A)
    power_w = power_kw * 1_000.0
    irradiance_input_w = irradiance_wm2 * panel_area_m2
    efficiency = power_w / irradiance_input_w

    # Clamp efficiency to [0, 1] – sensor noise may produce small negatives
    efficiency = max(0.0, min(efficiency, 1.0))

    # Temperature component: normalise relative to max safe threshold
    temp_ratio = inverter_temp_c / max_safe_temp
    temp_component = max(0.0, 1.0 - temp_ratio)   # 1 = cool, 0 = at limit

    # Composite Health Index
    hi = 0.5 * (efficiency / expected_efficiency) + 0.5 * temp_component
    hi = max(0.0, min(hi, 1.0))                    # clamp to [0, 1]

    degradation_flag = efficiency < (expected_efficiency - efficiency_tolerance)

    logger.debug(
        "Panel %s | η=%.4f | HI=%.4f | degrad=%s",
        panel_id, efficiency, hi, degradation_flag,
    )

    return PanelHealth(
        panel_id=panel_id,
        health_index=hi,
        efficiency=efficiency,
        degradation_flag=degradation_flag,
        inverter_temp_c=inverter_temp_c,
        power_kw=power_kw,
        irradiance_wm2=irradiance_wm2,
    )


def process_sensor_data(msg: SensorDataMessage) -> PanelHealthMessage:
    """
    Run compute_panel_health for every panel in the message, aggregate
    the average HI, and return a PanelHealthMessage ready to publish.
    """
    results: list[PanelHealth] = []
    for reading in msg.panels:
        ph = compute_panel_health(
            panel_id=reading.panel_id,
            power_kw=reading.power_kw,
            irradiance_wm2=reading.irradiance_wm2,
            inverter_temp_c=reading.inverter_temp_c,
        )
        results.append(ph)

    avg_hi = (
        sum(p.health_index for p in results) / len(results) if results else 0.0
    )

    degraded_count = sum(1 for p in results if p.degradation_flag)
    if degraded_count:
        logger.warning(
            "Farm %s: %d/%d panels flagged degraded (avg HI=%.3f)",
            msg.farm_id, degraded_count, len(results), avg_hi,
        )

    return PanelHealthMessage(
        farm_id=msg.farm_id,
        timestamp=msg.timestamp,
        panel_health=results,
        avg_health_index=avg_hi,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Async RabbitMQ consumer / producer
# ─────────────────────────────────────────────────────────────────────────────

class IoTAssetLayerWorker:
    """
    Async worker that:
      1. Subscribes to  QUEUE_SENSOR_DATA
      2. Calls process_sensor_data()
      3. Publishes result to QUEUE_PANEL_HEALTH
    """

    def __init__(self) -> None:
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(RABBITMQ_URL)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=10)

        self._exchange = await self._channel.declare_exchange(
            EXCHANGE_MAIN, aio_pika.ExchangeType.TOPIC, durable=True
        )

        # Declare & bind input queue
        in_queue = await self._channel.declare_queue(QUEUE_SENSOR_DATA, durable=True)
        await in_queue.bind(self._exchange, routing_key=ROUTING_SENSOR_DATA)

        # Declare output queue (so it exists before first publish)
        out_queue = await self._channel.declare_queue(QUEUE_PANEL_HEALTH, durable=True)
        await out_queue.bind(self._exchange, routing_key=ROUTING_PANEL_HEALTH)

        logger.info("IoT Layer connected to RabbitMQ, listening on %s", QUEUE_SENSOR_DATA)
        await in_queue.consume(self._on_message)

    async def _on_message(self, message: aio_pika.IncomingMessage) -> None:
        async with message.process(requeue=True):
            try:
                payload = json.loads(message.body)
                sensor_msg = SensorDataMessage.model_validate(payload)
                health_msg = process_sensor_data(sensor_msg)
                await self._publish_health(health_msg)
            except Exception:
                logger.exception("IoT Layer failed to process message")
                raise

    async def _publish_health(self, health_msg: PanelHealthMessage) -> None:
        body = health_msg.model_dump_json().encode()
        await self._exchange.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=ROUTING_PANEL_HEALTH,
        )
        logger.debug(
            "Published panel health for farm %s (avg HI=%.3f)",
            health_msg.farm_id, health_msg.avg_health_index,
        )

    async def run(self) -> None:
        await self.connect()
        logger.info("IoT Asset Layer running …")
        await asyncio.Future()   # block forever; cancelled on shutdown

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
