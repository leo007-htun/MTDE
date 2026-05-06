"""
CDAH-MPC – AI Agent Strategic Layer  (ljmu-agent-strategic container)
======================================================================
Triggered every 5 hours by a ConsensusMetrics message.
Optionally enriched by the latest IDB Protect GO telemetry
(consumed from a RabbitMQ fanout exchange).

Calls Bytez Qwen3-0.6B via llm_agent.generate_strategy_profile()
and publishes the resulting StrategyProfile to strategy.profile queue.
The Regional Edge Layer (MPC) subscribes to this queue to update its
operating constraints before the next 15-minute solve cycle.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import aio_pika

from config.settings import (
    EXCHANGE_IDB_FANOUT,
    EXCHANGE_MAIN,
    QUEUE_CONSENSUS_METRICS,
    QUEUE_IDB_TELEMETRY,
    QUEUE_MARKET_SIGNAL_AGENT,
    QUEUE_STRATEGY_PROFILE,
    RABBITMQ_URL,
    ROUTING_CONSENSUS_METRICS,
    ROUTING_MARKET_SIGNAL,
    ROUTING_STRATEGY_PROFILE,
)
from src.models.data_models import ConsensusMetrics, IDBTelemetry, MarketSignal, StrategyProfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


class AgentStrategicLayerWorker:
    """
    Consumes ConsensusMetrics (5-hour trigger) + IDB telemetry (cached).
    Calls Bytez LLM and publishes StrategyProfile for the MPC layer.
    """

    def __init__(self) -> None:
        self._connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self._channel:    Optional[aio_pika.abc.AbstractChannel]          = None
        self._exchange:   Optional[aio_pika.abc.AbstractExchange]          = None
        self._latest_telemetry: Optional[IDBTelemetry]                    = None
        self._latest_market:    Optional[MarketSignal]                     = None
        self._llm_semaphore = asyncio.Semaphore(1)

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(RABBITMQ_URL)
        self._channel    = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=5)

        self._exchange = await self._channel.declare_exchange(
            EXCHANGE_MAIN, aio_pika.ExchangeType.TOPIC, durable=True
        )

        # ── Inbound: ConsensusMetrics (5-hour trigger) ─────────────────────────
        consensus_q = await self._channel.declare_queue(QUEUE_CONSENSUS_METRICS, durable=True)
        await consensus_q.bind(self._exchange, routing_key=ROUTING_CONSENSUS_METRICS)
        await consensus_q.consume(self._on_consensus)

        # ── Inbound: IDB telemetry (cache only; degrade gracefully if absent) ──
        try:
            idb_exchange = await self._channel.declare_exchange(
                EXCHANGE_IDB_FANOUT, aio_pika.ExchangeType.FANOUT, durable=True
            )
            idb_q = await self._channel.declare_queue(QUEUE_IDB_TELEMETRY, durable=True)
            await idb_q.bind(idb_exchange)
            await idb_q.consume(self._on_idb_telemetry)
            logger.info("Agent subscribed to IDB fanout: %s", EXCHANGE_IDB_FANOUT)
        except Exception as exc:
            logger.warning(
                "IDB fanout exchange '%s' unavailable (%s) — running without live telemetry",
                EXCHANGE_IDB_FANOUT, exc,
            )

        # ── Inbound: Market signal (cached; used to enrich LLM prompt) ────────
        market_q = await self._channel.declare_queue(QUEUE_MARKET_SIGNAL_AGENT, durable=True)
        await market_q.bind(self._exchange, routing_key=ROUTING_MARKET_SIGNAL)
        await market_q.consume(self._on_market_signal)

        # ── Outbound: StrategyProfile → MPC layer ─────────────────────────────
        strategy_q = await self._channel.declare_queue(QUEUE_STRATEGY_PROFILE, durable=True)
        await strategy_q.bind(self._exchange, routing_key=ROUTING_STRATEGY_PROFILE)

        logger.info(
            "Agent Strategic Layer connected | triggers=%s | out=%s",
            QUEUE_CONSENSUS_METRICS, QUEUE_STRATEGY_PROFILE,
        )

    # ── Message handlers ──────────────────────────────────────────────────────

    async def _on_idb_telemetry(self, message: aio_pika.IncomingMessage) -> None:
        """Cache the latest IDB hardware reading — used on next strategy cycle."""
        async with message.process(requeue=False):
            try:
                self._latest_telemetry = IDBTelemetry.model_validate_json(message.body)
            except Exception:
                logger.debug("Failed to parse IDB telemetry message")

    async def _on_market_signal(self, message: aio_pika.IncomingMessage) -> None:
        """Cache the latest market signal — included in LLM prompt on next cycle."""
        async with message.process(requeue=True):
            try:
                self._latest_market = MarketSignal.model_validate_json(message.body)
                logger.info(
                    "Market signal cached | price=£%.4f/kWh | carbon=%.0f gCO2/kWh",
                    self._latest_market.price_per_kwh,
                    self._latest_market.carbon_intensity_gco2_kwh,
                )
            except Exception:
                logger.debug("Failed to parse MarketSignal message")

    async def _on_consensus(self, message: aio_pika.IncomingMessage) -> None:
        """5-hour trigger: run LLM agent and publish StrategyProfile."""
        async with message.process(requeue=True):
            try:
                consensus = ConsensusMetrics.model_validate_json(message.body)
                logger.info(
                    "Agent triggered | stress=%.3f | trend=%+.4f | confidence=%.3f",
                    consensus.health_stress, consensus.degradation_trend, consensus.forecast_confidence,
                )
                async with self._llm_semaphore:
                    profile = await asyncio.get_running_loop().run_in_executor(
                        None, self._call_llm, consensus
                    )
                await self._publish_profile(profile)
            except Exception:
                logger.exception("Agent Strategic Layer: failed to process ConsensusMetrics")

    def _call_llm(self, consensus: ConsensusMetrics) -> StrategyProfile:
        from src.utils.llm_agent import generate_strategy_profile
        return generate_strategy_profile(
            consensus=consensus,
            telemetry=self._latest_telemetry,
            market=self._latest_market,
        )

    async def _publish_profile(self, profile: StrategyProfile) -> None:
        body = profile.model_dump_json().encode()
        await self._exchange.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=ROUTING_STRATEGY_PROFILE,
        )
        logger.info(
            "Strategy published | class=%-10s | charge_limit=%.0f kW "
            "| thermal=%.0f°C | maint=%dh | %s",
            profile.health_class,
            profile.max_charge_rate_kw,
            profile.thermal_limit_c,
            profile.maintenance_window_hours,
            profile.rationale[:80],
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

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
        logger.info("Agent Strategic Layer running …")
        await asyncio.Future()

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
