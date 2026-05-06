"""
Global configuration for the Multi-Tier Decision Engine (MTDE).
Values can be overridden via environment variables.
"""
import os

# ── RabbitMQ ──────────────────────────────────────────────────────────────────
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

RABBITMQ_URL = os.getenv("RABBITMQ_URL") or (
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/{RABBITMQ_VHOST}"
)

# ── Queue / Exchange names ────────────────────────────────────────────────────
EXCHANGE_MAIN = "mtde.topic"          # single topic exchange for all tiers

QUEUE_SENSOR_DATA       = "iot.sensor_data"        # raw sensor → IoT layer
QUEUE_PANEL_HEALTH      = "iot.panel_health"        # IoT output → Edge layer
QUEUE_REGIONAL_SCHEDULE = "regional.schedule"       # Edge output → Central layer
QUEUE_FLEET_SCHEDULE    = "central.fleet_schedule"  # Central output → executor
QUEUE_TTA_PREDICTIONS   = "tta_predictions"         # Seoultech TTA → Regional Edge

ROUTING_SENSOR_DATA       = "iot.sensor"
ROUTING_PANEL_HEALTH      = "iot.health"
ROUTING_REGIONAL_SCHEDULE = "regional.schedule"
ROUTING_FLEET_SCHEDULE    = "central.fleet"
ROUTING_TTA_PREDICTIONS   = "tta_predictions"

# ── Control loop ──────────────────────────────────────────────────────────────
CONTROL_INTERVAL_SEC  = int(os.getenv("CONTROL_INTERVAL_SEC", "30"))
PREDICTION_HORIZON    = int(os.getenv("PREDICTION_HORIZON", "24"))   # hours

# ── Asset physics ─────────────────────────────────────────────────────────────
PANEL_AREA_M2          = float(os.getenv("PANEL_AREA_M2", "1.96"))
EXPECTED_EFFICIENCY    = float(os.getenv("EXPECTED_EFFICIENCY", "0.20"))
EFFICIENCY_TOLERANCE   = float(os.getenv("EFFICIENCY_TOLERANCE", "0.02"))
MAX_SAFE_TEMP_C        = float(os.getenv("MAX_SAFE_TEMP_C", "85.0"))
P_MAX_KW               = float(os.getenv("P_MAX_KW", "500.0"))
BATT_MAX_KWH           = float(os.getenv("BATT_MAX_KWH", "1000.0"))

# ── Optimisation weights ──────────────────────────────────────────────────────
LAMBDA_FAILURE         = float(os.getenv("LAMBDA_FAILURE", "0.3"))
LAMBDA_CURTAILED       = float(os.getenv("LAMBDA_CURTAILED", "0.1"))
LAMBDA_MAINTENANCE     = float(os.getenv("LAMBDA_MAINTENANCE", "0.2"))
LAMBDA_CARBON          = float(os.getenv("LAMBDA_CARBON", "0.15"))

# ── Location (used by pvlib) ──────────────────────────────────────────────────
SITE_LATITUDE   = float(os.getenv("SITE_LATITUDE", "51.5074"))
SITE_LONGITUDE  = float(os.getenv("SITE_LONGITUDE", "-0.1278"))
SITE_ALTITUDE_M = float(os.getenv("SITE_ALTITUDE_M", "11.0"))
SITE_TIMEZONE   = os.getenv("SITE_TIMEZONE", "Europe/London")

# ── Solver ────────────────────────────────────────────────────────────────────
SOLVER_NAME = os.getenv("SOLVER_NAME", "glpk")  # glpk | ipopt | cbc

# ── Ingest API ───────────────────────────────────────────────────────────────
INGEST_API_URL = os.getenv("INGEST_API_URL", "http://localhost:8000")

# ── CDAH-MPC: OpenAI (AI Agent strategic layer) ──────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL_ID = os.getenv("OPENAI_MODEL_ID", "gpt-5-mini")

# ── CDAH-MPC: New queues / routing ────────────────────────────────────────────
# TTA predictions: each subscriber gets its own durable queue bound to the same
# routing key so both regional MPC and consensus layer see every sample.
QUEUE_TTA_PREDICTIONS_REGIONAL  = "tta_predictions.regional"
QUEUE_TTA_PREDICTIONS_CONSENSUS = "tta_predictions.consensus"

QUEUE_IDB_TELEMETRY    = "idb.telemetry"
QUEUE_CONSENSUS_METRICS = "consensus.metrics"
QUEUE_STRATEGY_PROFILE  = "strategy.profile"

# Market signal: separate durable queues per consumer so both Central and Agent
# receive every price update independently (same pattern as TTA predictions).
QUEUE_MARKET_SIGNAL_CENTRAL = "market.signal.central"
QUEUE_MARKET_SIGNAL_AGENT   = "market.signal.agent"

ROUTING_IDB_TELEMETRY     = "idb.telemetry"
ROUTING_CONSENSUS_METRICS  = "consensus.metrics"
ROUTING_STRATEGY_PROFILE   = "strategy.profile"
ROUTING_MARKET_SIGNAL      = "market.signal"

# IDB Protect GO publishes to its own fanout exchange
EXCHANGE_IDB_FANOUT = os.getenv("EXCHANGE_IDB_FANOUT", "idb.fanout")

# ── CDAH-MPC: Consensus engine ────────────────────────────────────────────────
CONSENSUS_BUFFER_SIZE = int(os.getenv("CONSENSUS_BUFFER_SIZE", "5"))  # 5 hourly samples = 5-hour window

# Health stress thresholds (from CDAH-MPC spec)
HEALTH_STRESS_HEALTHY   = float(os.getenv("HEALTH_STRESS_HEALTHY",   "0.30"))
HEALTH_STRESS_STABLE    = float(os.getenv("HEALTH_STRESS_STABLE",    "0.50"))
HEALTH_STRESS_DEGRADING = float(os.getenv("HEALTH_STRESS_DEGRADING", "0.70"))
TREND_THRESHOLD         = float(os.getenv("TREND_THRESHOLD",         "0.01"))
