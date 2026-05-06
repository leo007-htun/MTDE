"""
PV Generation Forecasting Utility
==================================
Wraps pvlib to translate a WeatherForecast message into predicted AC
power output (kW) across the optimisation horizon.

Uses the simple PVWatts-style calculation:
    P_dc(t) = G(t) × A_panel × η_panel × N_panels
    P_ac(t) = P_dc(t) × η_inverter × (1 – loss_factor)

For a rigorous study, replace with pvlib ModelChain + CEC database.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Tuple

import pandas as pd
import pvlib
from pvlib.location import Location

from config.settings import (
    EXPECTED_EFFICIENCY,
    PANEL_AREA_M2,
    P_MAX_KW,
    SITE_ALTITUDE_M,
    SITE_LATITUDE,
    SITE_LONGITUDE,
    SITE_TIMEZONE,
)
from src.models.data_models import WeatherForecast

logger = logging.getLogger(__name__)

# ── Site singleton ────────────────────────────────────────────────────────────
_SITE = Location(
    latitude=SITE_LATITUDE,
    longitude=SITE_LONGITUDE,
    altitude=SITE_ALTITUDE_M,
    tz=SITE_TIMEZONE,
    name="MTDE_site",
)

# Default physical assumptions
INVERTER_EFFICIENCY = 0.96
LOSS_FACTOR = 0.05          # wiring, mismatch, soiling …
# Approximate number of panels that sum to P_MAX_KW at STC
_N_PANELS = int((P_MAX_KW * 1_000) / (1_000 * PANEL_AREA_M2 * EXPECTED_EFFICIENCY))


def pv_forecast(weather: WeatherForecast, hi_avg: float = 1.0) -> List[float]:
    """
    Compute predicted AC generation (kW) for each timestamp in *weather*.

    Parameters
    ----------
    weather : WeatherForecast
        Hourly forecast with irradiance (W/m²) and ambient temperature (°C).
    hi_avg  : float
        Fleet average Health Index [0–1].  Scales effective efficiency.

    Returns
    -------
    List[float]   predicted AC power (kW), one value per forecast step.
    """
    if not weather.timestamps:
        return []

    times = pd.DatetimeIndex(weather.timestamps)
    if times.tz is None:
        times = times.tz_localize(SITE_TIMEZONE)
    else:
        times = times.tz_convert(SITE_TIMEZONE)
    irradiance = pd.Series(weather.irradiance_wm2, index=times)
    temp_air   = pd.Series(weather.ambient_temp_c,  index=times)

    # Cell temperature correction using Sandia model
    wind = (
        pd.Series(weather.wind_speed_ms, index=times)
        if weather.wind_speed_ms
        else pd.Series(1.0, index=times)
    )
    temp_cell = pvlib.temperature.sapm_cell(
        poa_global=irradiance,
        temp_air=temp_air,
        wind_speed=wind,
        a=-3.56,   # glass/cell/polymer (open rack)
        b=-0.075,
        deltaT=3,
    )

    # Effective efficiency including HI degradation
    eff_effective = EXPECTED_EFFICIENCY * hi_avg

    # DC power per panel (W)
    p_dc_panel = irradiance * PANEL_AREA_M2 * eff_effective * (
        1 - 0.004 * (temp_cell - 25)   # temperature coefficient −0.4 %/°C
    )
    p_dc_panel = p_dc_panel.clip(lower=0.0)

    # Fleet DC → AC
    p_ac_kw = (p_dc_panel * _N_PANELS * INVERTER_EFFICIENCY * (1 - LOSS_FACTOR)) / 1_000

    # Cap at P_MAX
    p_ac_kw = p_ac_kw.clip(upper=P_MAX_KW)

    logger.debug("PV forecast: %d steps, peak=%.1f kW", len(p_ac_kw), p_ac_kw.max())
    return p_ac_kw.tolist()


def build_solar_position(timestamps: List[datetime]) -> pd.DataFrame:
    """Return solar position DataFrame for the configured site."""
    times = pd.DatetimeIndex(timestamps, tz=SITE_TIMEZONE)
    return _SITE.get_solarposition(times)
