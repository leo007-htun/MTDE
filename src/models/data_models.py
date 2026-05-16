"""
Pydantic data-transfer objects shared across all three decision tiers.
Every message published to / consumed from RabbitMQ is serialised from
one of these models (model.model_dump_json() / Model.model_validate_json()).
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────────────────────
# Tier-0  Raw sensor data
# ─────────────────────────────────────────────────────────────────────────────

class PanelSensorReading(BaseModel):
    panel_id: str
    timestamp: datetime
    power_kw: float          = Field(..., description="Measured DC power output (kW)")
    irradiance_wm2: float    = Field(..., description="Plane-of-array irradiance (W/m²)")
    inverter_temp_c: float   = Field(..., description="Inverter temperature (°C)")
    ambient_temp_c: float    = Field(default=25.0)


class SensorDataMessage(BaseModel):
    """Published to queue: iot.sensor_data"""
    farm_id: str
    timestamp: datetime
    panels: List[PanelSensorReading]


# ─────────────────────────────────────────────────────────────────────────────
# Tier-1  IoT Asset Layer output
# ─────────────────────────────────────────────────────────────────────────────

class PanelHealth(BaseModel):
    panel_id: str
    health_index: float       = Field(..., ge=0.0, le=1.0, description="Composite Health Index (0–1)")
    efficiency: float         = Field(..., description="Measured efficiency (0–1)")
    degradation_flag: bool    = Field(default=False)
    inverter_temp_c: float
    power_kw: float           = Field(default=0.0, description="Measured active power (kW)")
    irradiance_wm2: float     = Field(default=0.0, description="Plane-of-array irradiance (W/m²)")


class PanelHealthMessage(BaseModel):
    """Published to queue: iot.panel_health"""
    farm_id: str
    timestamp: datetime
    panel_health: List[PanelHealth]
    avg_health_index: float   = Field(default=0.0)


# ─────────────────────────────────────────────────────────────────────────────
# Tier-2  Forecasting inputs
# ─────────────────────────────────────────────────────────────────────────────

class WeatherForecast(BaseModel):
    timestamps: List[datetime]
    irradiance_wm2: List[float]
    ambient_temp_c: List[float]
    wind_speed_ms: List[float] = Field(default_factory=list)


class DemandForecast(BaseModel):
    timestamps: List[datetime]
    demand_kw: List[float]


class StorageStatus(BaseModel):
    battery_soc_kwh: float    = Field(..., description="State-of-charge (kWh)")
    battery_max_kwh: float
    is_charging: bool         = Field(default=False)


# ─────────────────────────────────────────────────────────────────────────────
# Tier-2  Regional Edge Layer output
# ─────────────────────────────────────────────────────────────────────────────

class HourlySetpoint(BaseModel):
    timestamp: datetime
    p_inverter_kw: float      = Field(..., description="Inverter output setpoint (kW)")
    p_battery_kw: float       = Field(..., description="+charge / −discharge (kW)")
    curtailment_fraction: float = Field(..., ge=0.0, le=1.0)
    failure_probability: float  = Field(default=0.0, ge=0.0, le=1.0)


class RegionalScheduleMessage(BaseModel):
    """Published to queue: regional.schedule"""
    farm_id: str
    timestamp: datetime
    horizon_hours: int
    setpoints: List[HourlySetpoint]
    avg_health_index: float
    solver_status: str        = Field(default="unknown")


# ─────────────────────────────────────────────────────────────────────────────
# Tier-3  Central Optimization Layer
# ─────────────────────────────────────────────────────────────────────────────

class MarketSignal(BaseModel):
    timestamp: datetime
    price_per_kwh: float
    carbon_intensity_gco2_kwh: float


class FleetConstraints(BaseModel):
    grid_demand_kw: float
    max_grid_export_kw: float
    maintenance_windows: Dict[str, List[datetime]] = Field(default_factory=dict)


class FarmFleetTarget(BaseModel):
    farm_id: str
    generation_target_kw: float
    storage_allocation_kwh: float
    maintenance_priority: float   = Field(..., ge=0.0, le=1.0)


class FleetScheduleMessage(BaseModel):
    """Published to queue: central.fleet_schedule"""
    timestamp: datetime
    horizon_hours: int
    farm_targets: List[FarmFleetTarget]
    total_generation_kw: float
    total_carbon_kg: float
    solver_status: str            = Field(default="unknown")


# ─────────────────────────────────────────────────────────────────────────────
# CDAH-MPC: SeoulTech TTA prediction sample (consensus buffer entry)
# ─────────────────────────────────────────────────────────────────────────────

class TTAPredictionSample(BaseModel):
    """One hourly TTA prediction stored in the 5-hour consensus buffer."""
    timestamp: datetime
    data_id: str
    adapted_predictions_denorm: List[float]   = Field(..., description="48-step power forecast (kW)")
    adaptation_gap: float                     = Field(default=0.0, ge=0.0,
                                                      description="Magnitude of base→TTA adaptation; health indicator")


# ─────────────────────────────────────────────────────────────────────────────
# CDAH-MPC: IDB Protect GO real-time telemetry
# ─────────────────────────────────────────────────────────────────────────────

class IDBTelemetry(BaseModel):
    """Published by IDB Protect GO hardware to RabbitMQ fanout exchange (5–60 s interval)."""
    timestamp: datetime
    battery_soc_kwh: float        = Field(..., description="State of charge (kWh)")
    battery_soc_max_kwh: float    = Field(default=1000.0)
    battery_temp_c: float         = Field(..., description="Battery pack temperature (°C)")
    battery_power_kw: float       = Field(..., description="Battery power; positive = charging (kW)")
    solar_power_kw: float         = Field(default=0.0)
    grid_exchange_kw: float       = Field(default=0.0, description="Grid exchange; positive = import (kW)")
    compressor_vibration_g: float = Field(default=0.0, description="Compressor vibration (g); normal < 0.3")
    compressor_load_pct: float    = Field(default=0.0, description="Compressor load (%)")


# ─────────────────────────────────────────────────────────────────────────────
# CDAH-MPC: Consensus Engine output  (5-hour rolling buffer → metrics)
# ─────────────────────────────────────────────────────────────────────────────

class ConsensusMetrics(BaseModel):
    """Published to queue: consensus.metrics once 5 TTA samples are buffered."""
    timestamp: datetime
    window_start: datetime
    window_end: datetime
    n_samples: int
    consensus_forecast: List[float] = Field(..., description="Element-wise mean 48-step forecast (kW)")
    health_stress: float            = Field(..., ge=0.0, le=1.0,
                                            description="Mean TTA adaptation magnitude; 0=healthy, 1=critical")
    degradation_trend: float        = Field(..., description="Slope of adaptation gaps per hour; positive = worsening")
    forecast_confidence: float      = Field(..., ge=0.0, le=1.0, description="1.0 − health_stress")


# ─────────────────────────────────────────────────────────────────────────────
# CDAH-MPC: AI Agent Strategy Profile  (LLM output → MPC constraints)
# ─────────────────────────────────────────────────────────────────────────────

class StrategyProfile(BaseModel):
    """Published to queue: strategy.profile by AI Agent every 5 hours."""
    timestamp: datetime
    health_class: str          = Field(..., description="healthy | stable | degrading | critical")
    max_charge_rate_kw: float  = Field(..., description="MPC inverter/charge rate ceiling (kW)")
    thermal_limit_c: float     = Field(..., description="Maximum safe operating temperature (°C)")
    maintenance_window_hours: int = Field(default=0, description="Hours until required maintenance; 0 = none")
    lambda_economic: float     = Field(..., ge=0.0, le=1.0, description="Weight on economic objective in MPC")
    lambda_degradation: float  = Field(..., ge=0.0, le=1.0, description="Weight on degradation penalty in MPC")
    rationale: str             = Field(..., description="Natural language operator explanation")
