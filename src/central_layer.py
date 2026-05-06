"""
Tier-3 – Central Optimization Layer
=====================================
Consumes RegionalScheduleMessage(s) from queue: regional.schedule.
Aggregates farm-level schedules, applies fleet-level LP optimization
(carbon, demand, maintenance priorities), then publishes
FleetScheduleMessage to queue: central.fleet_schedule.

Decision flow (matching pseudo-code):
    1. Aggregate farm schedules
    2. Optimise {Gen_f, Maint_f, Store_f} across fleet
    3. Publish fleet schedule to control executor
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List

import aio_pika

from config.settings import (
    BATT_MAX_KWH,
    LAMBDA_CARBON,
    LAMBDA_FAILURE,
    LAMBDA_MAINTENANCE,
    P_MAX_KW,
    RABBITMQ_URL,
    EXCHANGE_MAIN,
    QUEUE_REGIONAL_SCHEDULE,
    QUEUE_FLEET_SCHEDULE,
    QUEUE_MARKET_SIGNAL_CENTRAL,
    ROUTING_REGIONAL_SCHEDULE,
    ROUTING_FLEET_SCHEDULE,
    ROUTING_MARKET_SIGNAL,
)
from src.models.data_models import (
    FarmFleetTarget,
    FleetConstraints,
    FleetScheduleMessage,
    MarketSignal,
    RegionalScheduleMessage,
)
from src.utils.optimization import fleet_optimize

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Stub data providers  (replace with real market / grid API clients)
# ─────────────────────────────────────────────────────────────────────────────

def _stub_fleet_constraints() -> FleetConstraints:
    return FleetConstraints(
        grid_demand_kw=P_MAX_KW * 0.55,
        max_grid_export_kw=P_MAX_KW * 1.10,
        maintenance_windows={},
    )


def _stub_market_signal() -> MarketSignal:
    return MarketSignal(
        timestamp=datetime.now(tz=timezone.utc),
        price_per_kwh=0.08,
        carbon_intensity_gco2_kwh=220.0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Aggregation & normalisation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _aggregate_schedule(schedule: RegionalScheduleMessage) -> float:
    """Peak generation capacity (kW) from a farm's setpoints."""
    if not schedule.setpoints:
        return 0.0
    return max(sp.p_inverter_kw for sp in schedule.setpoints)


def _aggregate_failure_risk(schedule: RegionalScheduleMessage) -> float:
    """Average failure probability across the horizon."""
    if not schedule.setpoints:
        return 0.0
    return sum(sp.failure_probability for sp in schedule.setpoints) / len(schedule.setpoints)


# ─────────────────────────────────────────────────────────────────────────────
# Core computation
# ─────────────────────────────────────────────────────────────────────────────

def run_central_optimization(
    farm_schedules: List[RegionalScheduleMessage],
    constraints: FleetConstraints | None = None,
    market: MarketSignal | None = None,
) -> FleetScheduleMessage:
    """
    Execute fleet-level LP and return a FleetScheduleMessage.
    """
    constraints = constraints or _stub_fleet_constraints()
    market      = market      or _stub_market_signal()

    farm_ids = [s.farm_id for s in farm_schedules]

    # Per-farm aggregated metrics
    generation_capacity: Dict[str, float] = {}
    failure_risk: Dict[str, float]        = {}
    maintenance_cost: Dict[str, float]    = {}
    carbon_intensity: Dict[str, float]    = {}
    batt_max: Dict[str, float]            = {}

    for sched in farm_schedules:
        fid = sched.farm_id
        generation_capacity[fid] = _aggregate_schedule(sched)
        failure_risk[fid]        = _aggregate_failure_risk(sched)
        # Maintenance cost proxy: higher HI → lower maintenance urgency
        maintenance_cost[fid]    = max(0.0, 1.0 - sched.avg_health_index) * 1_000.0
        # Carbon intensity from market signal (global for now; per-farm in prod)
        carbon_intensity[fid]    = market.carbon_intensity_gco2_kwh / 1_000.0  # kg/kWh
        batt_max[fid]            = BATT_MAX_KWH

    gen_targets, _, _, solver_status = fleet_optimize(
        farm_ids=farm_ids,
        generation_capacity=generation_capacity,
        failure_risk=failure_risk,
        maintenance_cost=maintenance_cost,
        carbon_intensity=carbon_intensity,
        grid_demand_kw=constraints.grid_demand_kw,
        max_grid_export_kw=constraints.max_grid_export_kw,
        batt_max_per_farm=batt_max,
        price_per_kwh=market.price_per_kwh,
    )

    # Maintenance priority and storage allocation are health-derived, not LP outputs.
    # The LP has no incentive to assign non-zero values to these, so compute directly:
    #   maintenance_priority = 1 − avg_HI  (0=healthy, 1=critical)
    #   storage_allocation   = failure_risk × battery_capacity  (higher risk → more buffer)
    maint_prio: Dict[str, float] = {
        s.farm_id: round(max(0.0, 1.0 - s.avg_health_index), 3)
        for s in farm_schedules
    }
    storage_alloc: Dict[str, float] = {
        fid: round(failure_risk[fid] * batt_max[fid], 1)
        for fid in farm_ids
    }

    farm_targets: List[FarmFleetTarget] = []
    total_gen = 0.0
    total_carbon = 0.0
    for fid in farm_ids:
        gen = gen_targets.get(fid, 0.0)
        total_gen   += gen
        total_carbon += gen * carbon_intensity.get(fid, 0.0)
        farm_targets.append(FarmFleetTarget(
            farm_id=fid,
            generation_target_kw=gen,
            storage_allocation_kwh=storage_alloc.get(fid, 0.0),
            maintenance_priority=maint_prio.get(fid, 0.0),
        ))

    horizon = max((s.horizon_hours for s in farm_schedules), default=0)

    logger.info(
        "Central Opt solver=%s farms=%d total_gen=%.1f kW total_CO2=%.2f kg",
        solver_status, len(farm_ids), total_gen, total_carbon,
    )

    return FleetScheduleMessage(
        timestamp=datetime.now(tz=timezone.utc),
        horizon_hours=horizon,
        farm_targets=farm_targets,
        total_generation_kw=total_gen,
        total_carbon_kg=total_carbon,
        solver_status=solver_status,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Async RabbitMQ worker
# ─────────────────────────────────────────────────────────────────────────────

class CentralOptimizationLayerWorker:
    """
    Collects RegionalScheduleMessages (one per farm per cycle),
    buffers them, runs central optimization, publishes fleet schedule.

    Simple strategy: flush when a message arrives for every known farm,
    or after a configurable timeout (default 10 s).
    """

    FLUSH_TIMEOUT_SEC = 10.0

    def __init__(self) -> None:
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None

        # Buffer: farm_id → latest schedule
        self._buffer: Dict[str, RegionalScheduleMessage] = {}
        self._flush_task: asyncio.Task | None = None
        self._latest_market: MarketSignal | None = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(RABBITMQ_URL)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=20)

        self._exchange = await self._channel.declare_exchange(
            EXCHANGE_MAIN, aio_pika.ExchangeType.TOPIC, durable=True
        )

        in_queue = await self._channel.declare_queue(QUEUE_REGIONAL_SCHEDULE, durable=True)
        await in_queue.bind(self._exchange, routing_key=ROUTING_REGIONAL_SCHEDULE)

        market_queue = await self._channel.declare_queue(QUEUE_MARKET_SIGNAL_CENTRAL, durable=True)
        await market_queue.bind(self._exchange, routing_key=ROUTING_MARKET_SIGNAL)

        out_queue = await self._channel.declare_queue(QUEUE_FLEET_SCHEDULE, durable=True)
        await out_queue.bind(self._exchange, routing_key=ROUTING_FLEET_SCHEDULE)

        logger.info("Central Layer connected, listening on %s", QUEUE_REGIONAL_SCHEDULE)
        await market_queue.consume(self._on_market_message)
        await in_queue.consume(self._on_message)

    async def _on_market_message(self, message: aio_pika.IncomingMessage) -> None:
        async with message.process(requeue=True):
            try:
                self._latest_market = MarketSignal.model_validate_json(message.body)
                logger.info(
                    "Market signal received | price=£%.4f/kWh | carbon=%.0f gCO2/kWh",
                    self._latest_market.price_per_kwh,
                    self._latest_market.carbon_intensity_gco2_kwh,
                )
            except Exception:
                logger.exception("Central Layer failed to parse MarketSignal")

    async def _on_message(self, message: aio_pika.IncomingMessage) -> None:
        async with message.process(requeue=True):
            try:
                payload = json.loads(message.body)
                sched = RegionalScheduleMessage.model_validate(payload)
                self._buffer[sched.farm_id] = sched
                logger.debug("Buffered schedule from farm %s (%d farms buffered)",
                             sched.farm_id, len(self._buffer))
                await self._schedule_flush()
            except Exception:
                logger.exception("Central Layer failed to process message")
                raise

    async def _schedule_flush(self) -> None:
        """Cancel any pending flush timer and restart it."""
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
        self._flush_task = asyncio.create_task(self._delayed_flush())

    async def _delayed_flush(self) -> None:
        await asyncio.sleep(self.FLUSH_TIMEOUT_SEC)
        await self._flush()

    async def _flush(self) -> None:
        if not self._buffer:
            return
        schedules = list(self._buffer.values())
        self._buffer.clear()

        fleet_msg = await asyncio.get_running_loop().run_in_executor(
            None, run_central_optimization, schedules, None, self._latest_market
        )
        await self._publish_fleet(fleet_msg)

    async def _publish_fleet(self, fleet_msg: FleetScheduleMessage) -> None:
        body = fleet_msg.model_dump_json().encode()
        await self._exchange.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=ROUTING_FLEET_SCHEDULE,
        )
        logger.info(
            "Published fleet schedule: %d farms, %.1f kW, %.2f kg CO2, solver=%s",
            len(fleet_msg.farm_targets),
            fleet_msg.total_generation_kw,
            fleet_msg.total_carbon_kg,
            fleet_msg.solver_status,
        )

    async def run(self) -> None:
        await self.connect()
        logger.info("Central Optimization Layer running …")
        await asyncio.Future()

    async def close(self) -> None:
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
        if self._connection:
            await self._connection.close()
