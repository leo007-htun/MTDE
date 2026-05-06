"""
CDAH-MPC – Consensus Layer  (ljmu-consensus container)
=======================================================
Maintains a 5-hour rolling buffer of SeoulTech TTA predictions.
Once CONSENSUS_BUFFER_SIZE samples are accumulated, computes:

    consensus_forecast  – element-wise mean of 5 × 48-step power arrays
    health_stress       – mean adaptation gap across buffer (0=healthy, 1=critical)
    degradation_trend   – linear slope of adaptation gaps per hour
    forecast_confidence – 1.0 − health_stress

Publishes ConsensusMetrics to consensus.metrics queue on every new sample
(once the buffer is full). The buffer advances by dropping the oldest entry
(deque with maxlen).
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Deque, List, Optional

import aio_pika

from config.settings import (
    CONSENSUS_BUFFER_SIZE,
    EXCHANGE_MAIN,
    QUEUE_CONSENSUS_METRICS,
    QUEUE_TTA_PREDICTIONS_CONSENSUS,
    RABBITMQ_URL,
    ROUTING_CONSENSUS_METRICS,
    ROUTING_TTA_PREDICTIONS,
)
from src.models.data_models import ConsensusMetrics, TTAPredictionSample

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Consensus computation ─────────────────────────────────────────────────────

def _linear_slope(values: List[float]) -> float:
    """Least-squares slope through (0,v0),(1,v1),...,  units = per sample."""
    n = len(values)
    if n < 2:
        return 0.0
    x_bar = (n - 1) / 2.0
    y_bar = sum(values) / n
    num = sum((i - x_bar) * (v - y_bar) for i, v in enumerate(values))
    den = sum((i - x_bar) ** 2 for i in range(n))
    return num / den if den else 0.0


def compute_consensus(samples: List[TTAPredictionSample]) -> ConsensusMetrics:
    n = len(samples)
    forecast_len = len(samples[0].adapted_predictions_denorm)

    consensus_forecast = [
        sum(s.adapted_predictions_denorm[i] for s in samples) / n
        for i in range(forecast_len)
    ]

    gaps = [s.adaptation_gap for s in samples]
    health_stress       = sum(gaps) / n
    degradation_trend   = _linear_slope(gaps)          # per sample ≈ per hour
    forecast_confidence = max(0.0, min(1.0, 1.0 - health_stress))

    return ConsensusMetrics(
        timestamp=datetime.now(tz=timezone.utc),
        window_start=samples[0].timestamp,
        window_end=samples[-1].timestamp,
        n_samples=n,
        consensus_forecast=consensus_forecast,
        health_stress=health_stress,
        degradation_trend=degradation_trend,
        forecast_confidence=forecast_confidence,
    )


# ── Async worker ──────────────────────────────────────────────────────────────

class ConsensusLayerWorker:
    """
    Subscribes to tta_predictions (own durable queue so regional layer
    keeps its copy), fills rolling buffer, and publishes ConsensusMetrics.
    """

    def __init__(self) -> None:
        self._buffer: Deque[TTAPredictionSample] = deque(maxlen=CONSENSUS_BUFFER_SIZE)
        self._connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self._channel:    Optional[aio_pika.abc.AbstractChannel]          = None
        self._exchange:   Optional[aio_pika.abc.AbstractExchange]          = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(RABBITMQ_URL)
        self._channel    = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=5)

        self._exchange = await self._channel.declare_exchange(
            EXCHANGE_MAIN, aio_pika.ExchangeType.TOPIC, durable=True
        )

        # Own queue so regional layer still gets every TTA message
        tta_queue = await self._channel.declare_queue(
            QUEUE_TTA_PREDICTIONS_CONSENSUS, durable=True
        )
        await tta_queue.bind(self._exchange, routing_key=ROUTING_TTA_PREDICTIONS)

        out_queue = await self._channel.declare_queue(QUEUE_CONSENSUS_METRICS, durable=True)
        await out_queue.bind(self._exchange, routing_key=ROUTING_CONSENSUS_METRICS)

        await tta_queue.consume(self._on_tta)
        logger.info(
            "Consensus Layer connected | buffer_capacity=%d | in=%s | out=%s",
            CONSENSUS_BUFFER_SIZE, QUEUE_TTA_PREDICTIONS_CONSENSUS, QUEUE_CONSENSUS_METRICS,
        )

    async def _on_tta(self, message: aio_pika.IncomingMessage) -> None:
        async with message.process(requeue=True):
            try:
                raw = json.loads(message.body)
                ts_raw = raw.get("timestamp")
                ts = (datetime.fromisoformat(ts_raw)
                      if ts_raw else datetime.now(tz=timezone.utc))

                sample = TTAPredictionSample(
                    timestamp=ts,
                    data_id=raw.get("data_id", "unknown"),
                    adapted_predictions_denorm=raw["adapted_predictions_denorm"],
                    adaptation_gap=float(raw.get("adaptation_gap", 0.0)),
                )
                self._buffer.append(sample)
                logger.debug(
                    "TTA buffered | id=%-12s | gap=%.4f | buffer=%d/%d",
                    sample.data_id, sample.adaptation_gap,
                    len(self._buffer), CONSENSUS_BUFFER_SIZE,
                )

                if len(self._buffer) == CONSENSUS_BUFFER_SIZE:
                    metrics = compute_consensus(list(self._buffer))
                    await self._publish(metrics)

            except Exception:
                logger.exception("Consensus Layer: failed to process TTA message")

    async def _publish(self, metrics: ConsensusMetrics) -> None:
        body = metrics.model_dump_json().encode()
        await self._exchange.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=ROUTING_CONSENSUS_METRICS,
        )
        logger.info(
            "Consensus published | stress=%.3f | trend=%+.4f | confidence=%.3f",
            metrics.health_stress, metrics.degradation_trend, metrics.forecast_confidence,
        )

    async def _wait_for_rabbitmq(self, max_attempts: int = 20, delay: float = 3.0) -> None:
        for attempt in range(1, max_attempts + 1):
            try:
                conn = await aio_pika.connect_robust(RABBITMQ_URL)
                await conn.close()
                logger.info("RabbitMQ ready (attempt %d)", attempt)
                return
            except Exception as exc:
                logger.warning("Waiting for RabbitMQ … (attempt %d/%d): %s", attempt, max_attempts, exc)
                await asyncio.sleep(delay)
        raise RuntimeError("RabbitMQ not reachable after %d attempts" % max_attempts)

    async def run(self) -> None:
        await self._wait_for_rabbitmq()
        await self.connect()
        logger.info("Consensus Layer running …")
        await asyncio.Future()

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
