"""
AMOS MTDE – Live Dashboard Backend
====================================
Subscribes to all output queues (using its own durable queues so it
never competes with engine workers) and pushes updates to browser
clients via WebSocket.

REST:
    GET  /          → dashboard HTML
    GET  /api/state → latest snapshot of all queues (JSON)

WebSocket:
    WS /ws          → real-time push on every new message
                      payload: {"type": "fleet"|"strategy"|"consensus"|"idb", "data": {...}}
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import aio_pika
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("dashboard")

# ── Config ────────────────────────────────────────────────────────────────────
RABBITMQ_URL = os.getenv("RABBITMQ_URL") or (
    f"amqp://{os.getenv('RABBITMQ_USER', 'guest')}:{os.getenv('RABBITMQ_PASS', 'guest')}"
    f"@{os.getenv('RABBITMQ_HOST', 'rabbitmq')}:{os.getenv('RABBITMQ_PORT', '5672')}/"
)
EXCHANGE_MAIN      = "mtde.topic"
EXCHANGE_IDB_FANOUT = os.getenv("EXCHANGE_IDB_FANOUT", "idb.fanout")

# ── In-memory state cache ─────────────────────────────────────────────────────
_state: dict = {
    "fleet":      None,
    "strategy":   None,
    "consensus":  None,
    "idb":        None,
    "last_updated": None,
}

# ── Connected WebSocket clients ───────────────────────────────────────────────
_clients: list[WebSocket] = []


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _wait_for_rabbitmq(max_attempts: int = 20, delay: float = 3.0) -> None:
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


async def _broadcast(msg_type: str, data: dict) -> None:
    if not _clients:
        return
    payload = json.dumps({"type": msg_type, "data": data})
    dead = []
    for ws in _clients:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            _clients.remove(ws)
        except ValueError:
            pass


def _touch() -> None:
    _state["last_updated"] = datetime.now(tz=timezone.utc).isoformat()


# ── Message handlers ──────────────────────────────────────────────────────────

async def _on_fleet(msg: aio_pika.IncomingMessage) -> None:
    async with msg.process():
        try:
            data = json.loads(msg.body)
            _state["fleet"] = data
            _touch()
            await _broadcast("fleet", data)
            logger.info("Fleet update | farms=%d | gen=%.1f kW | CO2=%.2f kg",
                        len(data.get("farm_targets", [])),
                        data.get("total_generation_kw", 0),
                        data.get("total_carbon_kg", 0))
        except Exception:
            logger.exception("Failed to process fleet message")


async def _on_strategy(msg: aio_pika.IncomingMessage) -> None:
    async with msg.process():
        try:
            data = json.loads(msg.body)
            _state["strategy"] = data
            _touch()
            await _broadcast("strategy", data)
            logger.info("Strategy update | class=%s | charge=%.0f kW",
                        data.get("health_class"), data.get("max_charge_rate_kw"))
        except Exception:
            logger.exception("Failed to process strategy message")


async def _on_consensus(msg: aio_pika.IncomingMessage) -> None:
    async with msg.process():
        try:
            data = json.loads(msg.body)
            _state["consensus"] = data
            _touch()
            await _broadcast("consensus", data)
            logger.info("Consensus update | stress=%.3f | confidence=%.3f",
                        data.get("health_stress", 0), data.get("forecast_confidence", 0))
        except Exception:
            logger.exception("Failed to process consensus message")


async def _on_idb(msg: aio_pika.IncomingMessage) -> None:
    async with msg.process(requeue=False):
        try:
            data = json.loads(msg.body)
            _state["idb"] = data
            _touch()
            await _broadcast("idb", data)
        except Exception:
            logger.exception("Failed to process IDB message")


# ── RabbitMQ consumer task ────────────────────────────────────────────────────

async def _consume() -> None:
    await _wait_for_rabbitmq()
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel    = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    exchange = await channel.declare_exchange(
        EXCHANGE_MAIN, aio_pika.ExchangeType.TOPIC, durable=True
    )

    # Own queues – separate from engine queues so we get a full copy
    fleet_q = await channel.declare_queue("dashboard.fleet_schedule", durable=True)
    await fleet_q.bind(exchange, routing_key="central.fleet")
    await fleet_q.consume(_on_fleet)

    strategy_q = await channel.declare_queue("dashboard.strategy_profile", durable=True)
    await strategy_q.bind(exchange, routing_key="strategy.profile")
    await strategy_q.consume(_on_strategy)

    consensus_q = await channel.declare_queue("dashboard.consensus_metrics", durable=True)
    await consensus_q.bind(exchange, routing_key="consensus.metrics")
    await consensus_q.consume(_on_consensus)

    try:
        idb_exchange = await channel.declare_exchange(
            EXCHANGE_IDB_FANOUT, aio_pika.ExchangeType.FANOUT, durable=True
        )
        idb_q = await channel.declare_queue("dashboard.idb_telemetry", durable=True)
        await idb_q.bind(idb_exchange)
        await idb_q.consume(_on_idb)
        logger.info("Subscribed to IDB fanout exchange")
    except Exception as exc:
        logger.warning("IDB fanout unavailable (%s) — skipping", exc)

    logger.info("Dashboard consumer ready — listening on 4 queues")
    await asyncio.Future()


# ── FastAPI app ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_consume())
    yield
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


app = FastAPI(title="AMOS MTDE Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("static/index.html") as f:
        return f.read()


@app.get("/api/state")
async def get_state():
    return _state


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    _clients.append(websocket)
    # Send current cached state immediately on connect
    await websocket.send_text(json.dumps({"type": "state", "data": _state}))
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        try:
            _clients.remove(websocket)
        except ValueError:
            pass
