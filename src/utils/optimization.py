"""
Optimisation helpers used by the Regional Edge and Central Layers.
Uses Pyomo with a GLPK (or CBC/IPOPT) LP/MIP backend.

Regional problem  (horizon × 3 variables):
    Maximise  Σ_t [ G_pred(t)·HI - λ_fail·P_fail(t) - λ_curt·C(t) ]
    s.t.
        P_inv(t)  ≤  P_max · HI
        0 ≤ P_batt(t) ≤ Batt_max
        P_inv(t) + P_batt(t) ≥ D(t)·(1 − C(t))    ∀t
        0 ≤ C(t) ≤ 1

Fleet problem (N_farms × 3 variables):
    Maximise  Σ_f [ Gen_f - λ_fail·Risk_f - λ_maint·M_f - λ_carbon·CO2_f ]
    s.t.
        Σ_f Gen_f ≥ grid_demand
        0 ≤ Gen_f ≤ cap_f
        0 ≤ Storage_f ≤ Batt_f_max
"""
from __future__ import annotations

import logging
from typing import Dict, List, Tuple

import pyomo.environ as pyo
from pyomo.opt import SolverStatus, TerminationCondition

from config.settings import (
    BATT_MAX_KWH,
    LAMBDA_CARBON,
    LAMBDA_CURTAILED,
    LAMBDA_FAILURE,
    LAMBDA_MAINTENANCE,
    P_MAX_KW,
    SOLVER_NAME,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_solver() -> pyo.SolverFactory:
    solver = pyo.SolverFactory(SOLVER_NAME)
    if not solver.available():
        # fallback to CBC if glpk not installed
        logger.warning("%s not available – falling back to cbc", SOLVER_NAME)
        solver = pyo.SolverFactory("cbc")
    return solver


def _solve(model: pyo.ConcreteModel, solver_name: str = SOLVER_NAME) -> str:
    solver = pyo.SolverFactory(solver_name)
    if not solver.available():
        solver = pyo.SolverFactory("cbc")
    result = solver.solve(model, tee=False)

    if (result.solver.status == SolverStatus.ok and
            result.solver.termination_condition == TerminationCondition.optimal):
        return "optimal"
    elif result.solver.termination_condition == TerminationCondition.feasible:
        return "feasible"
    else:
        logger.warning("Solver status: %s | condition: %s",
                       result.solver.status,
                       result.solver.termination_condition)
        return "infeasible"


# ─────────────────────────────────────────────────────────────────────────────
# Regional Edge optimisation
# ─────────────────────────────────────────────────────────────────────────────

def regional_optimize(
    predicted_generation: List[float],   # kW per horizon step
    demand_forecast: List[float],         # kW per horizon step
    hi_avg: float,
    battery_soc_kwh: float,
    p_max_kw: float = P_MAX_KW,
    batt_max_kwh: float = BATT_MAX_KWH,
    lambda_failure: float = LAMBDA_FAILURE,
    lambda_curtailed: float = LAMBDA_CURTAILED,
) -> Tuple[List[float], List[float], List[float], str]:
    """
    Solve the regional MPC optimisation.

    Returns
    -------
    (p_inv, p_batt, curtailment, solver_status)
    Each list has len == len(predicted_generation).
    """
    T = range(len(predicted_generation))

    model = pyo.ConcreteModel(name="RegionalEdge")
    model.T = pyo.Set(initialize=T)

    # Decision variables
    model.P_inv  = pyo.Var(model.T, within=pyo.NonNegativeReals, bounds=(0, p_max_kw * hi_avg))
    model.P_batt = pyo.Var(model.T, within=pyo.Reals,            bounds=(-batt_max_kwh, batt_max_kwh))
    model.C      = pyo.Var(model.T, within=pyo.NonNegativeReals, bounds=(0.0, 1.0))

    # Battery SoC continuity (simplified: SoC_t = SoC_{t-1} + P_batt_t)
    model.SoC    = pyo.Var(model.T, within=pyo.NonNegativeReals, bounds=(0, batt_max_kwh))

    # Failure probability proxy: linear in (1 − HI) and curtailment
    fail_prob = [(1 - hi_avg) * 0.5 + predicted_generation[t] / (p_max_kw + 1e-9) * 0.1
                 for t in T]

    # Objective
    def obj_rule(m):
        return sum(
            predicted_generation[t] * hi_avg
            - lambda_failure  * fail_prob[t]
            - lambda_curtailed * m.C[t]
            for t in m.T
        )
    model.obj = pyo.Objective(rule=obj_rule, sense=pyo.maximize)

    # Constraints
    def inv_cap(m, t):
        return m.P_inv[t] <= p_max_kw * hi_avg
    model.c_inv_cap = pyo.Constraint(model.T, rule=inv_cap)

    def demand_balance(m, t):
        return m.P_inv[t] + m.P_batt[t] >= demand_forecast[t] * (1 - m.C[t])
    model.c_demand = pyo.Constraint(model.T, rule=demand_balance)

    def soc_init(m):
        return m.SoC[0] == battery_soc_kwh + m.P_batt[0]
    model.c_soc_init = pyo.Constraint(rule=soc_init)

    def soc_continuity(m, t):
        if t == 0:
            return pyo.Constraint.Skip
        return m.SoC[t] == m.SoC[t - 1] + m.P_batt[t]
    model.c_soc = pyo.Constraint(model.T, rule=soc_continuity)

    status = _solve(model)

    if status in ("optimal", "feasible"):
        p_inv  = [pyo.value(model.P_inv[t])  or 0.0 for t in T]
        p_batt = [pyo.value(model.P_batt[t]) or 0.0 for t in T]
        curt   = [pyo.value(model.C[t])       or 0.0 for t in T]
    else:
        # Fallback: proportional dispatch
        logger.warning("Regional optimiser infeasible – using fallback heuristic")
        p_inv  = [min(g * hi_avg, p_max_kw) for g in predicted_generation]
        p_batt = [0.0] * len(T)
        curt   = [0.0] * len(T)

    return p_inv, p_batt, curt, status


# ─────────────────────────────────────────────────────────────────────────────
# Fleet / Central optimisation
# ─────────────────────────────────────────────────────────────────────────────

def fleet_optimize(
    farm_ids: List[str],
    generation_capacity: Dict[str, float],   # max generation per farm (kW)
    failure_risk: Dict[str, float],           # normalised [0,1] per farm
    maintenance_cost: Dict[str, float],       # £/period per farm
    carbon_intensity: Dict[str, float],       # kg CO2/kWh per farm
    grid_demand_kw: float,
    max_grid_export_kw: float,
    batt_max_per_farm: Dict[str, float] | None = None,
    price_per_kwh: float = 0.08,             # electricity market price (£/kWh)
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float], str]:
    """
    Fleet-level LP: allocate generation targets, maintenance priority,
    and storage allocation across farms.

    Objective (maximise):
        Σ_f [ price × Gen_f
              − λ_fail    × Risk_f   × Gen_f
              − λ_maint   × M_f      × Maint_f
              − λ_carbon  × CO2_f    × Gen_f ]

    Returns
    -------
    (gen_targets, maint_priority, storage_alloc, solver_status)
    """
    F = range(len(farm_ids))
    farm_map = {i: fid for i, fid in enumerate(farm_ids)}
    batt_max = batt_max_per_farm or {fid: BATT_MAX_KWH for fid in farm_ids}

    model = pyo.ConcreteModel(name="CentralFleet")
    model.F = pyo.Set(initialize=F)

    model.Gen   = pyo.Var(model.F, within=pyo.NonNegativeReals)
    model.Maint = pyo.Var(model.F, within=pyo.NonNegativeReals, bounds=(0.0, 1.0), initialize=0.0)
    model.Store = pyo.Var(model.F, within=pyo.NonNegativeReals)

    def obj_rule(m):
        return sum(
            price_per_kwh * m.Gen[i]
            - LAMBDA_FAILURE     * failure_risk[farm_map[i]]     * m.Gen[i]
            - LAMBDA_MAINTENANCE * maintenance_cost[farm_map[i]] * m.Maint[i]
            - LAMBDA_CARBON      * carbon_intensity[farm_map[i]] * m.Gen[i]
            for i in m.F
        )
    model.obj = pyo.Objective(rule=obj_rule, sense=pyo.maximize)

    # Grid demand must be met
    model.c_demand = pyo.Constraint(
        expr=sum(model.Gen[i] for i in model.F) >= grid_demand_kw
    )

    # Export cap
    model.c_export = pyo.Constraint(
        expr=sum(model.Gen[i] for i in model.F) <= max_grid_export_kw
    )

    # Per-farm capacity limits
    def cap_rule(m, i):
        return m.Gen[i] <= generation_capacity[farm_map[i]]
    model.c_cap = pyo.Constraint(model.F, rule=cap_rule)

    # Storage limits
    def store_rule(m, i):
        return m.Store[i] <= batt_max[farm_map[i]]
    model.c_store = pyo.Constraint(model.F, rule=store_rule)

    status = _solve(model)

    if status in ("optimal", "feasible"):
        gen_targets   = {farm_map[i]: pyo.value(model.Gen[i])   or 0.0 for i in F}
        maint_prio    = {farm_map[i]: (pyo.value(model.Maint[i], exception=False) or 0.0) for i in F}
        storage_alloc = {farm_map[i]: pyo.value(model.Store[i]) or 0.0 for i in F}
    else:
        logger.warning("Fleet optimiser infeasible – equal split fallback")
        equal_share = grid_demand_kw / max(len(farm_ids), 1)
        gen_targets   = {fid: min(equal_share, generation_capacity[fid]) for fid in farm_ids}
        maint_prio    = {fid: 0.5 for fid in farm_ids}
        storage_alloc = {fid: 0.0 for fid in farm_ids}

    return gen_targets, maint_prio, storage_alloc, status
