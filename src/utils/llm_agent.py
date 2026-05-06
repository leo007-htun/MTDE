"""
CDAH-MPC – LLM-based AI Agent helper
=====================================
Uses OpenAI API (gpt-4o-mini reasoning model) to generate a StrategyProfile
from ConsensusMetrics + IDB telemetry + market context.

Rule-based classification provides safe defaults; the LLM refines parameters
and generates the natural-language operator rationale.
Falls back to pure rule-based output if the LLM call fails.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from config.settings import (
    OPENAI_API_KEY,
    OPENAI_MODEL_ID,
    HEALTH_STRESS_HEALTHY,
    HEALTH_STRESS_STABLE,
    HEALTH_STRESS_DEGRADING,
    TREND_THRESHOLD,
    P_MAX_KW,
)
from src.models.data_models import ConsensusMetrics, IDBTelemetry, MarketSignal, StrategyProfile

logger = logging.getLogger(__name__)

# ── Health classification ──────────────────────────────────────────────────────

def classify_health(health_stress: float, degradation_trend: float) -> str:
    """Rule-based classification per CDAH-MPC thresholds."""
    if health_stress >= HEALTH_STRESS_DEGRADING:
        return "critical"
    if health_stress >= HEALTH_STRESS_STABLE and degradation_trend > TREND_THRESHOLD:
        return "degrading"
    if health_stress >= HEALTH_STRESS_HEALTHY and abs(degradation_trend) < TREND_THRESHOLD:
        return "stable"
    if health_stress < HEALTH_STRESS_HEALTHY:
        return "healthy"
    return "stable"


# ── Rule-based defaults ────────────────────────────────────────────────────────

_RULE_DEFAULTS = {
    "healthy":   dict(max_charge_rate_kw=P_MAX_KW,        thermal_limit_c=85, maintenance_window_hours=0,  lambda_economic=0.80, lambda_degradation=0.20),
    "stable":    dict(max_charge_rate_kw=P_MAX_KW * 0.85, thermal_limit_c=75, maintenance_window_hours=0,  lambda_economic=0.60, lambda_degradation=0.40),
    "degrading": dict(max_charge_rate_kw=P_MAX_KW * 0.65, thermal_limit_c=65, maintenance_window_hours=48, lambda_economic=0.30, lambda_degradation=0.70),
    "critical":  dict(max_charge_rate_kw=P_MAX_KW * 0.40, thermal_limit_c=55, maintenance_window_hours=4,  lambda_economic=0.10, lambda_degradation=0.90),
}


# ── Prompt builder ─────────────────────────────────────────────────────────────

def _build_system_prompt() -> str:
    return (
        "You are an industrial energy management AI agent for a solar microgrid with battery storage. "
        "Analyse the provided equipment health metrics and return a JSON strategy profile. "
        "Respond ONLY with valid JSON — no explanation, no markdown fences."
    )


def _build_user_prompt(
    consensus: ConsensusMetrics,
    health_class: str,
    telemetry: Optional[IDBTelemetry],
    market: Optional[MarketSignal],
    defaults: dict,
) -> str:
    telem_lines = ""
    if telemetry:
        soc_pct = (telemetry.battery_soc_kwh / telemetry.battery_soc_max_kwh * 100
                   if telemetry.battery_soc_max_kwh > 0 else 0.0)
        telem_lines = (
            f"\nTELEMETRY:"
            f"\n- Battery SoC: {telemetry.battery_soc_kwh:.1f}/{telemetry.battery_soc_max_kwh:.1f} kWh ({soc_pct:.0f}%)"
            f"\n- Battery Temp: {telemetry.battery_temp_c:.1f}°C"
            f"\n- Solar: {telemetry.solar_power_kw:.1f} kW"
            f"\n- Grid Exchange: {telemetry.grid_exchange_kw:+.1f} kW"
            f"\n- Compressor Vibration: {telemetry.compressor_vibration_g:.3f}g  Load: {telemetry.compressor_load_pct:.0f}%"
        )

    market_lines = ""
    if market:
        market_lines = (
            f"\nMARKET:"
            f"\n- Price: £{market.price_per_kwh:.4f}/kWh"
            f"\n- Carbon: {market.carbon_intensity_gco2_kwh:.0f} gCO2/kWh"
        )

    return (
        "HEALTH METRICS:\n"
        f"- Health Stress: {consensus.health_stress:.3f}  (0=healthy, 1=critical)\n"
        f"- Degradation Trend: {consensus.degradation_trend:+.4f}/hr  (positive=worsening)\n"
        f"- Forecast Confidence: {consensus.forecast_confidence:.3f}\n"
        f"- Classification: {health_class}\n"
        f"- Samples in window: {consensus.n_samples}"
        f"{telem_lines}{market_lines}\n\n"
        "RULE-BASED DEFAULTS (adjust only if telemetry/market warrants it):\n"
        f"{json.dumps(defaults, indent=2)}\n\n"
        "Return JSON with exactly these keys:\n"
        "{\n"
        '  "max_charge_rate_kw": <float>,\n'
        '  "thermal_limit_c": <float>,\n'
        '  "maintenance_window_hours": <int>,\n'
        '  "lambda_economic": <float 0-1>,\n'
        '  "lambda_degradation": <float 0-1>,\n'
        '  "rationale": "<one sentence for operators>"\n'
        "}"
    )


# ── JSON extraction ────────────────────────────────────────────────────────────

def _extract_json(text: str) -> dict:
    """Extract JSON object from model response, stripping markdown fences if present."""
    text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if match:
        return json.loads(match.group())
    raise ValueError(f"No JSON found in model response: {text[:300]!r}")


# ── Public API ─────────────────────────────────────────────────────────────────

def generate_strategy_profile(
    consensus: ConsensusMetrics,
    telemetry: Optional[IDBTelemetry] = None,
    market: Optional[MarketSignal] = None,
) -> StrategyProfile:
    """
    Call OpenAI to produce a StrategyProfile.
    Returns a rule-based fallback profile if the API call fails.
    """
    health_class = classify_health(consensus.health_stress, consensus.degradation_trend)
    defaults = _RULE_DEFAULTS[health_class].copy()

    try:
        from openai import OpenAI  # lazy import – not required in unit-test context

        client = OpenAI(api_key=OPENAI_API_KEY)

        response = client.chat.completions.create(
            model=OPENAI_MODEL_ID,
            messages=[
                {"role": "system", "content": _build_system_prompt()},
                {"role": "user",   "content": _build_user_prompt(
                    consensus, health_class, telemetry, market, defaults
                )},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
        )

        raw_text = response.choices[0].message.content or ""
        parsed = _extract_json(raw_text)

        logger.info(
            "OpenAI strategy | model=%s | class=%s | tokens=%d | rationale=%s",
            OPENAI_MODEL_ID,
            health_class,
            response.usage.total_tokens if response.usage else 0,
            str(parsed.get("rationale", ""))[:100],
        )

        return StrategyProfile(
            timestamp=datetime.now(tz=timezone.utc),
            health_class=health_class,
            max_charge_rate_kw=float(parsed.get("max_charge_rate_kw",          defaults["max_charge_rate_kw"])),
            thermal_limit_c=float(parsed.get("thermal_limit_c",                defaults["thermal_limit_c"])),
            maintenance_window_hours=int(parsed.get("maintenance_window_hours", defaults["maintenance_window_hours"])),
            lambda_economic=float(parsed.get("lambda_economic",                defaults["lambda_economic"])),
            lambda_degradation=float(parsed.get("lambda_degradation",          defaults["lambda_degradation"])),
            rationale=str(parsed.get("rationale",
                                     f"Equipment is {health_class}; strategy adjusted accordingly.")),
        )

    except Exception as exc:
        logger.warning("OpenAI agent failed (%s) — using rule-based fallback", exc)
        return StrategyProfile(
            timestamp=datetime.now(tz=timezone.utc),
            health_class=health_class,
            rationale=(
                f"[Rule-based] Equipment {health_class}: "
                f"stress={consensus.health_stress:.3f}, trend={consensus.degradation_trend:+.4f}/hr."
            ),
            **defaults,
        )
