"""
Tier-2 – Regional Edge Layer (MPC / Optimisation)
==================================================
Consumes PanelHealthMessage from queue: iot.panel_health.
Fetches or receives weather & demand forecasts.
Runs pvlib + Pyomo MPC to produce hourly inverter / battery / curtailment
setpoints, then publishes RegionalScheduleMessage to queue: regional.schedule.

Decision flow (matching pseudo-code):
    1. predicted_generation = PV_Forecast(weather_forecast)
    2. Optimise {P_inv(t), P_batt(t), C(t)} over prediction_horizon
    3. Publish optimised schedule
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List

import aio_pika

from config.settings import (
    CONTROL_INTERVAL_SEC,
    PREDICTION_HORIZON,
    P_MAX_KW,
    BATT_MAX_KWH,
    RABBITMQ_URL,
    EXCHANGE_MAIN,
    QUEUE_PANEL_HEALTH,
    QUEUE_REGIONAL_SCHEDULE,
    QUEUE_STRATEGY_PROFILE,
    QUEUE_TTA_PREDICTIONS_REGIONAL,
    ROUTING_PANEL_HEALTH,
    ROUTING_REGIONAL_SCHEDULE,
    ROUTING_STRATEGY_PROFILE,
    ROUTING_TTA_PREDICTIONS,
)
from src.models.data_models import (
    PanelHealthMessage,
    StrategyProfile,
    WeatherForecast,
    DemandForecast,
    StorageStatus,
    HourlySetpoint,
    RegionalScheduleMessage,
)
from src.utils.forecasting import pv_forecast
from src.utils.optimization import regional_optimize

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Stub data providers  (replace with real API clients in production)
# ─────────────────────────────────────────────────────────────────────────────

def _stub_weather_forecast(horizon: int = PREDICTION_HORIZON) -> WeatherForecast:
    """
    Stub: returns a sinusoidal irradiance profile centred on solar noon.
    In production, call a NWP / weather API here.
    """
    import math
    now = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    timestamps, irr, temps = [], [], []
    for h in range(horizon):
        ts = now + timedelta(hours=h)
        hour_of_day = ts.hour
        # Simple solar noon at hour 12, zero at night
        angle = math.pi * (hour_of_day - 6) / 12
        g = max(0.0, 900 * math.sin(angle))
        timestamps.append(ts)
        irr.append(g)
        temps.append(20.0 + 5 * math.sin(angle))
    return WeatherForecast(
        timestamps=timestamps,
        irradiance_wm2=irr,
        ambient_temp_c=temps,
        wind_speed_ms=[2.0] * horizon,
    )


def _stub_demand_forecast(horizon: int = PREDICTION_HORIZON) -> DemandForecast:
    """Stub flat demand profile at 60 % of P_MAX."""
    now = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    return DemandForecast(
        timestamps=[now + timedelta(hours=h) for h in range(horizon)],
        demand_kw=[P_MAX_KW * 0.60] * horizon,
    )


def _stub_storage_status() -> StorageStatus:
    return StorageStatus(
        battery_soc_kwh=BATT_MAX_KWH * 0.50,
        battery_max_kwh=BATT_MAX_KWH,
        is_charging=False,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Core computation
# ─────────────────────────────────────────────────────────────────────────────

def run_regional_mpc(
    health_msg: PanelHealthMessage,
    weather: WeatherForecast | None = None,
    demand: DemandForecast | None = None,
    storage: StorageStatus | None = None,
    gen_pred_override: list[float] | None = None,
    strategy_profile: "StrategyProfile | None" = None,
) -> RegionalScheduleMessage:
    """
    Execute the full Regional Edge pipeline and return a schedule message.

    gen_pred_override: if provided (Seoultech adapted_predictions_denorm),
    skips pvlib entirely and uses these kW values directly.
    """
    weather  = weather  or _stub_weather_forecast()
    demand   = demand   or _stub_demand_forecast()
    storage  = storage  or _stub_storage_status()

    hi_avg = health_msg.avg_health_index

    # Step 1: PV forecast — always Seoultech TTA (caller guarantees gen_pred_override is set)
    gen_pred = gen_pred_override[:PREDICTION_HORIZON]
    logger.info("Regional MPC [%s] using Seoultech TTA forecast (%d steps)",
                health_msg.farm_id, len(gen_pred))

    # Align length to demand forecast (shorter wins)
    horizon = min(len(gen_pred), len(demand.demand_kw))
    gen_pred     = gen_pred[:horizon]
    demand_list  = demand.demand_kw[:horizon]
    timestamps   = weather.timestamps[:horizon]

    # Step 2: MPC optimisation – apply StrategyProfile constraints when available
    p_max = strategy_profile.max_charge_rate_kw if strategy_profile else P_MAX_KW
    if strategy_profile:
        logger.info(
            "Regional MPC [%s] strategy=%s charge_limit=%.0f kW thermal=%.0f°C maint=%dh",
            health_msg.farm_id, strategy_profile.health_class,
            p_max, strategy_profile.thermal_limit_c,
            strategy_profile.maintenance_window_hours,
        )

    p_inv, p_batt, curt, solver_status = regional_optimize(
        predicted_generation=gen_pred,
        demand_forecast=demand_list,
        hi_avg=hi_avg,
        battery_soc_kwh=storage.battery_soc_kwh,
        p_max_kw=p_max,
    )

    # Step 3: Failure probability proxy per step
    fail_probs = [max(0.0, (1 - hi_avg) * 0.3 + c * 0.1) for c in curt]

    setpoints: List[HourlySetpoint] = []
    for t in range(horizon):
        setpoints.append(HourlySetpoint(
            timestamp=timestamps[t],
            p_inverter_kw=p_inv[t],
            p_battery_kw=p_batt[t],
            curtailment_fraction=curt[t],
            failure_probability=fail_probs[t],
        ))

    logger.info(
        "Regional MPC [%s] solver=%s horizon=%dh avg_HI=%.3f",
        health_msg.farm_id, solver_status, horizon, hi_avg,
    )

    return RegionalScheduleMessage(
        farm_id=health_msg.farm_id,
        timestamp=health_msg.timestamp,
        horizon_hours=horizon,
        setpoints=setpoints,
        avg_health_index=hi_avg,
        solver_status=solver_status,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Async RabbitMQ worker
# ─────────────────────────────────────────────────────────────────────────────

class RegionalEdgeLayerWorker:
    """
    Subscribes to iot.panel_health + tta_predictions, runs MPC, publishes to regional.schedule.
    Uses Seoultech TTA forecast when available; falls back to pvlib otherwise.
    """

    def __init__(self) -> None:
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None
        self._latest_gen_pred: list[float] | None = None       # cached Seoultech TTA forecast
        self._strategy_profile: StrategyProfile | None = None  # cached AI Agent strategy

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(RABBITMQ_URL)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=5)

        self._exchange = await self._channel.declare_exchange(
            EXCHANGE_MAIN, aio_pika.ExchangeType.TOPIC, durable=True
        )

        # Inbound: panel health from IoT layer
        in_queue = await self._channel.declare_queue(QUEUE_PANEL_HEALTH, durable=True)
        await in_queue.bind(self._exchange, routing_key=ROUTING_PANEL_HEALTH)

        # Inbound: Seoultech TTA forecast — own queue so consensus layer also gets every message
        tta_queue = await self._channel.declare_queue(QUEUE_TTA_PREDICTIONS_REGIONAL, durable=True)
        await tta_queue.bind(self._exchange, routing_key=ROUTING_TTA_PREDICTIONS)

        # Inbound: AI Agent strategy profile (5-hour cycle, cached between updates)
        strategy_queue = await self._channel.declare_queue(QUEUE_STRATEGY_PROFILE, durable=True)
        await strategy_queue.bind(self._exchange, routing_key=ROUTING_STRATEGY_PROFILE)

        # Outbound: regional schedule to Central layer
        out_queue = await self._channel.declare_queue(QUEUE_REGIONAL_SCHEDULE, durable=True)
        await out_queue.bind(self._exchange, routing_key=ROUTING_REGIONAL_SCHEDULE)

        logger.info("Regional Edge Layer connected — health=%s  tta=%s  strategy=%s",
                    QUEUE_PANEL_HEALTH, QUEUE_TTA_PREDICTIONS_REGIONAL, QUEUE_STRATEGY_PROFILE)

        await tta_queue.consume(self._on_tta_message)
        await strategy_queue.consume(self._on_strategy_message)
        await in_queue.consume(self._on_message)

    async def _on_tta_message(self, message: aio_pika.IncomingMessage) -> None:
        """Cache the latest Seoultech 48h forecast as soon as it arrives."""
        async with message.process(requeue=True):
            try:
                tta = json.loads(message.body)
                self._latest_gen_pred = tta["adapted_predictions_denorm"]
                logger.info(
                    "TTA forecast received | data_id=%s | steps=%d | peak=%.1f kW",
                    tta.get("data_id", "?"),
                    len(self._latest_gen_pred),
                    max(self._latest_gen_pred),
                )
            except Exception:
                logger.exception("Failed to parse TTA forecast message")

    async def _on_strategy_message(self, message: aio_pika.IncomingMessage) -> None:
        """Cache the latest AI Agent StrategyProfile for use in the next MPC cycle."""
        async with message.process(requeue=True):
            try:
                self._strategy_profile = StrategyProfile.model_validate_json(message.body)
                logger.info(
                    "Strategy profile received | class=%s | charge_limit=%.0f kW | maint=%dh",
                    self._strategy_profile.health_class,
                    self._strategy_profile.max_charge_rate_kw,
                    self._strategy_profile.maintenance_window_hours,
                )
            except Exception:
                logger.exception("Failed to parse StrategyProfile message")

    async def _on_message(self, message: aio_pika.IncomingMessage) -> None:
        async with message.process(requeue=True):
            try:
                payload = json.loads(message.body)
                health_msg = PanelHealthMessage.model_validate(payload)

                if self._latest_gen_pred is None:
                    logger.info("Regional MPC [%s] waiting for first TTA forecast — skipping cycle",
                                health_msg.farm_id)
                    return

                loop = asyncio.get_running_loop()
                schedule = await asyncio.wait_for(
                    loop.run_in_executor(
                        None, run_regional_mpc, health_msg,
                        None, None, None, self._latest_gen_pred, self._strategy_profile,
                    ),
                    timeout=60.0,
                )
                await self._publish_schedule(schedule)
            except Exception:
                logger.exception("Regional Edge Layer failed to process message")
                raise

    async def _publish_schedule(self, schedule: RegionalScheduleMessage) -> None:
        body = schedule.model_dump_json().encode()
        await self._exchange.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=ROUTING_REGIONAL_SCHEDULE,
        )
        logger.debug("Published regional schedule for farm %s", schedule.farm_id)

    async def run(self) -> None:
        await self.connect()
        logger.info("Regional Edge Layer running …")
        await asyncio.Future()

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
