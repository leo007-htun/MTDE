"""
REST Ingest API
===============
FastAPI application that accepts telemetry from partner systems via HTTP POST
and forwards each payload into the RabbitMQ engine pipeline.

Endpoints
---------
POST /ingest/sensor   → SensorDataMessage    → routing key: iot.sensor
POST /ingest/tta      → TTA forecast         → routing key: tta_predictions
POST /ingest/idb      → IDBTelemetry         → exchange:    idb.fanout

Run standalone:
    uvicorn src.api.ingest:app --host 0.0.0.0 --port 8000 --reload
"""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional

import aio_pika
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from config.settings import (
    EXCHANGE_IDB_FANOUT,
    EXCHANGE_MAIN,
    RABBITMQ_URL,
    ROUTING_MARKET_SIGNAL,
    ROUTING_SENSOR_DATA,
    ROUTING_TTA_PREDICTIONS,
)
from src.models.data_models import IDBTelemetry, MarketSignal, PanelSensorReading, SensorDataMessage

logger = logging.getLogger(__name__)

# ── RabbitMQ state (shared across requests) ────────────────────────────────────

_connection: aio_pika.abc.AbstractRobustConnection | None = None
_topic_exchange: aio_pika.abc.AbstractExchange | None = None
_fanout_exchange: aio_pika.abc.AbstractExchange | None = None


async def _get_topic_exchange() -> aio_pika.abc.AbstractExchange:
    if _topic_exchange is None:
        raise HTTPException(status_code=503, detail="RabbitMQ not connected")
    return _topic_exchange


async def _get_fanout_exchange() -> aio_pika.abc.AbstractExchange:
    if _fanout_exchange is None:
        raise HTTPException(status_code=503, detail="RabbitMQ not connected")
    return _fanout_exchange


async def _publish(exchange: aio_pika.abc.AbstractExchange, body: bytes, routing_key: str = "") -> None:
    await exchange.publish(
        aio_pika.Message(
            body=body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key,
    )


# ── Lifespan: connect / disconnect RabbitMQ ───────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _connection, _topic_exchange, _fanout_exchange

    logger.info("Ingest API connecting to RabbitMQ …")
    _connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await _connection.channel()

    _topic_exchange = await channel.declare_exchange(
        EXCHANGE_MAIN, aio_pika.ExchangeType.TOPIC, durable=True
    )
    _fanout_exchange = await channel.declare_exchange(
        EXCHANGE_IDB_FANOUT, aio_pika.ExchangeType.FANOUT, durable=True
    )
    logger.info("Ingest API ready")

    yield

    await _connection.close()
    logger.info("Ingest API disconnected")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="AMOS Ingest API",
    description="REST endpoints for partner telemetry ingestion into the MTDE pipeline",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Request models ────────────────────────────────────────────────────────────

class TTAIngestRequest(BaseModel):
    """Subset of SeoulTech TTA message that the engine requires."""
    data_id: str                                  = Field(..., description="Meter / site identifier")
    timestamp: datetime                           = Field(..., description="Forecast generation time (ISO 8601)")
    adapted_predictions_denorm: List[float]       = Field(..., description="48-step power forecast (kW)")
    prediction_timestamps: List[datetime]         = Field(..., description="Timestamp per forecast step")
    adaptation_gap: float                         = Field(default=0.0, ge=0.0,
                                                          description="Normalised base-to-TTA delta [0–1]")
    original_predictions_denorm: Optional[List[float]] = Field(default=None)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.post(
    "/ingest/sensor",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Panel sensor readings from a solar farm",
)
async def ingest_sensor(payload: SensorDataMessage):
    """
    Accepts raw panel sensor readings and forwards them to the IoT Asset Layer.

    - `farm_id`: unique farm identifier
    - `panels`: list of per-panel readings (`power_kw`, `irradiance_wm2`, `inverter_temp_c`)
    """
    exchange = await _get_topic_exchange()
    try:
        await _publish(exchange, payload.model_dump_json().encode(), routing_key=ROUTING_SENSOR_DATA)
        logger.info("Ingest /sensor | farm=%s panels=%d", payload.farm_id, len(payload.panels))
        return {"status": "accepted", "farm_id": payload.farm_id, "panels": len(payload.panels)}
    except Exception as exc:
        logger.exception("Failed to forward sensor payload")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/ingest/tta",
    status_code=status.HTTP_202_ACCEPTED,
    summary="SeoulTech TTA 48-hour solar power forecast",
)
async def ingest_tta(payload: TTAIngestRequest):
    """
    Accepts a 48-step TTA-adapted power forecast and forwards it to the
    Regional Edge Layer (MPC) and Consensus Layer.

    - `adapted_predictions_denorm`: **required** — 48 float values in kW
    - `adaptation_gap`: health stress indicator (0 = healthy, 1 = critical)
    """
    if len(payload.adapted_predictions_denorm) < 1:
        raise HTTPException(status_code=422, detail="adapted_predictions_denorm must not be empty")

    exchange = await _get_topic_exchange()
    try:
        body = payload.model_dump_json().encode()
        await _publish(exchange, body, routing_key=ROUTING_TTA_PREDICTIONS)
        logger.info(
            "Ingest /tta | id=%s steps=%d peak=%.1f kW gap=%.4f",
            payload.data_id,
            len(payload.adapted_predictions_denorm),
            max(payload.adapted_predictions_denorm),
            payload.adaptation_gap,
        )
        return {
            "status": "accepted",
            "data_id": payload.data_id,
            "steps": len(payload.adapted_predictions_denorm),
        }
    except Exception as exc:
        logger.exception("Failed to forward TTA payload")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/ingest/telemetries",
    status_code=status.HTTP_202_ACCEPTED,
    summary="IDB Protect GO real-time battery and compressor telemetry",
)
async def ingest_idb(payload: IDBTelemetry):
    """
    Accepts IDB Protect GO hardware telemetry and forwards it to the
    AI Agent Strategic Layer.

    - `battery_power_kw`: positive = charging, negative = discharging
    - `grid_exchange_kw`: positive = importing, negative = exporting
    - `compressor_vibration_g`: normal < 0.3 g
    """
    exchange = await _get_fanout_exchange()
    try:
        await _publish(exchange, payload.model_dump_json().encode(), routing_key="")
        logger.info(
            "Ingest /idb | soc=%.1f kWh batt=%+.1f kW solar=%.1f kW vib=%.3fg",
            payload.battery_soc_kwh,
            payload.battery_power_kw,
            payload.solar_power_kw,
            payload.compressor_vibration_g,
        )
        return {"status": "accepted", "battery_soc_kwh": payload.battery_soc_kwh}
    except Exception as exc:
        logger.exception("Failed to forward IDB payload")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post(
    "/ingest/market",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Electricity market price and carbon intensity signal",
)
async def ingest_market(payload: MarketSignal):
    """
    Accepts a market signal and forwards it to the Central Optimization Layer
    (fleet LP revenue objective) and the AI Agent Strategic Layer (LLM prompt).

    - `price_per_kwh`: spot or day-ahead electricity price (£/kWh)
    - `carbon_intensity_gco2_kwh`: grid carbon intensity (gCO2/kWh)
    """
    exchange = await _get_topic_exchange()
    try:
        await _publish(exchange, payload.model_dump_json().encode(), routing_key=ROUTING_MARKET_SIGNAL)
        logger.info(
            "Ingest /market | price=£%.4f/kWh | carbon=%.0f gCO2/kWh",
            payload.price_per_kwh,
            payload.carbon_intensity_gco2_kwh,
        )
        return {
            "status": "accepted",
            "price_per_kwh": payload.price_per_kwh,
            "carbon_intensity_gco2_kwh": payload.carbon_intensity_gco2_kwh,
        }
    except Exception as exc:
        logger.exception("Failed to forward market signal")
        raise HTTPException(status_code=500, detail=str(exc))


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health", summary="API and RabbitMQ connectivity check")
async def health():
    connected = _connection is not None and not _connection.is_closed
    if not connected:
        return JSONResponse(status_code=503, content={"status": "degraded", "rabbitmq": False})
    return {"status": "ok", "rabbitmq": True}
