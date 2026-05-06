"""
Unit tests for the three decision tiers (no RabbitMQ required).
Run with:  pytest engine/tests/ -v
"""
import pytest
from datetime import datetime, timezone

# ── Make imports work from repo root ─────────────────────────────────────────
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.iot_layer import compute_panel_health, process_sensor_data
from src.models.data_models import (
    PanelSensorReading, SensorDataMessage,
    WeatherForecast, DemandForecast, StorageStatus,
    PanelHealthMessage,
)


# ─────────────────────────────────────────────────────────────────────────────
# Tier-1: IoT Layer
# ─────────────────────────────────────────────────────────────────────────────

class TestIoTLayer:

    def test_healthy_panel(self):
        """Normal operating conditions → HI close to 1.0, no degradation."""
        ph = compute_panel_health(
            panel_id="P001",
            power_kw=0.392,          # 0.20 * 1000 W/m² * 1.96 m² = 392 W
            irradiance_wm2=1000.0,
            inverter_temp_c=40.0,    # well within 85 °C limit
        )
        assert 0.8 <= ph.health_index <= 1.0
        assert not ph.degradation_flag
        assert ph.efficiency == pytest.approx(1.0, rel=0.05)

    def test_degraded_panel(self):
        """Low efficiency below tolerance triggers degradation_flag."""
        ph = compute_panel_health(
            panel_id="P002",
            power_kw=0.150,          # much less than expected 392 W
            irradiance_wm2=1000.0,
            inverter_temp_c=50.0,
        )
        assert ph.degradation_flag
        assert ph.health_index < 0.8

    def test_overheated_panel(self):
        """Temperature near max reduces HI significantly."""
        ph = compute_panel_health(
            panel_id="P003",
            power_kw=0.392,
            irradiance_wm2=1000.0,
            inverter_temp_c=84.0,    # just below 85 °C max
        )
        assert ph.health_index < 0.6  # temp component ≈ 0.01

    def test_night_time_zero_irradiance(self):
        """Zero irradiance → HI=0, no degradation flag."""
        ph = compute_panel_health(
            panel_id="P004",
            power_kw=0.0,
            irradiance_wm2=0.0,
            inverter_temp_c=25.0,
        )
        assert ph.health_index == 0.0
        assert not ph.degradation_flag

    def test_hi_clamped_between_0_and_1(self):
        """HI must always be within [0, 1]."""
        ph = compute_panel_health(
            panel_id="P005",
            power_kw=5.0,            # unrealistically high → clamped
            irradiance_wm2=1000.0,
            inverter_temp_c=20.0,
        )
        assert 0.0 <= ph.health_index <= 1.0

    def test_process_sensor_data_aggregates(self):
        """process_sensor_data computes avg HI over multiple panels."""
        now = datetime.now(tz=timezone.utc)
        panels = [
            PanelSensorReading(
                panel_id=f"P{i:03d}",
                timestamp=now,
                power_kw=0.392,
                irradiance_wm2=1000.0,
                inverter_temp_c=40.0,
            )
            for i in range(5)
        ]
        msg = SensorDataMessage(farm_id="farm_00", timestamp=now, panels=panels)
        health = process_sensor_data(msg)
        assert health.farm_id == "farm_00"
        assert len(health.panel_health) == 5
        assert 0.0 <= health.avg_health_index <= 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Tier-2: Regional Edge Layer (pure computation, no RabbitMQ)
# ─────────────────────────────────────────────────────────────────────────────

class TestRegionalEdgeLayer:

    def _make_health_msg(self, hi: float = 0.85) -> PanelHealthMessage:
        now = datetime.now(tz=timezone.utc)
        from src.models.data_models import PanelHealth
        ph = PanelHealth(
            panel_id="P001",
            health_index=hi,
            efficiency=0.19,
            degradation_flag=False,
            inverter_temp_c=45.0,
        )
        return PanelHealthMessage(
            farm_id="farm_00",
            timestamp=now,
            panel_health=[ph],
            avg_health_index=hi,
        )

    def test_run_regional_mpc_returns_schedule(self):
        from src.regional_edge_layer import run_regional_mpc
        health_msg = self._make_health_msg()
        schedule = run_regional_mpc(health_msg)
        assert schedule.farm_id == "farm_00"
        assert len(schedule.setpoints) > 0
        for sp in schedule.setpoints:
            assert sp.p_inverter_kw >= 0
            assert 0.0 <= sp.curtailment_fraction <= 1.0

    def test_low_hi_reduces_inverter_cap(self):
        """Low HI should result in lower average inverter output."""
        from src.regional_edge_layer import run_regional_mpc
        hi_schedule   = run_regional_mpc(self._make_health_msg(hi=0.95))
        low_schedule  = run_regional_mpc(self._make_health_msg(hi=0.30))

        avg_hi  = sum(s.p_inverter_kw for s in hi_schedule.setpoints)  / len(hi_schedule.setpoints)
        avg_low = sum(s.p_inverter_kw for s in low_schedule.setpoints) / len(low_schedule.setpoints)
        # Hi-health farm should dispatch more or equal
        assert avg_hi >= avg_low - 1.0   # 1 kW tolerance for solver rounding


# ─────────────────────────────────────────────────────────────────────────────
# Tier-3: Central Optimization Layer (pure computation)
# ─────────────────────────────────────────────────────────────────────────────

class TestCentralLayer:

    def _make_regional_schedule(self, farm_id: str, hi: float = 0.85):
        from src.regional_edge_layer import run_regional_mpc
        from src.models.data_models import PanelHealth, PanelHealthMessage
        now = datetime.now(tz=timezone.utc)
        ph = PanelHealth(
            panel_id="P001",
            health_index=hi,
            efficiency=0.19,
            degradation_flag=False,
            inverter_temp_c=45.0,
        )
        health_msg = PanelHealthMessage(
            farm_id=farm_id,
            timestamp=now,
            panel_health=[ph],
            avg_health_index=hi,
        )
        return run_regional_mpc(health_msg)

    def test_fleet_schedule_covers_all_farms(self):
        from src.central_layer import run_central_optimization
        schedules = [
            self._make_regional_schedule("farm_00"),
            self._make_regional_schedule("farm_01"),
            self._make_regional_schedule("farm_02"),
        ]
        fleet = run_central_optimization(schedules)
        assert len(fleet.farm_targets) == 3
        farm_ids = {t.farm_id for t in fleet.farm_targets}
        assert farm_ids == {"farm_00", "farm_01", "farm_02"}

    def test_fleet_total_generation_positive(self):
        from src.central_layer import run_central_optimization
        schedules = [self._make_regional_schedule(f"farm_{i:02d}") for i in range(2)]
        fleet = run_central_optimization(schedules)
        assert fleet.total_generation_kw >= 0.0

    def test_fleet_carbon_positive(self):
        from src.central_layer import run_central_optimization
        schedules = [self._make_regional_schedule("farm_00")]
        fleet = run_central_optimization(schedules)
        assert fleet.total_carbon_kg >= 0.0
