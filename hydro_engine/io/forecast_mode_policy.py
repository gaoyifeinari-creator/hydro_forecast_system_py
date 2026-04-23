from __future__ import annotations

from typing import Optional

from hydro_engine.domain.nodes.reservoir import ReservoirNode

FORECAST_MODE_REALTIME = "realtime_forecast"
FORECAST_MODE_HISTORICAL = "historical_simulation"
VALID_FORECAST_MODES = (FORECAST_MODE_REALTIME, FORECAST_MODE_HISTORICAL)


def normalize_forecast_mode(
    forecast_mode: Optional[str],
    *,
    default: str = FORECAST_MODE_REALTIME,
) -> str:
    """
    统一 forecast_mode 归一化与合法性校验。

    返回小写规范值；非法值抛 ValueError。
    """
    mode = str(forecast_mode or default).strip().lower()
    if mode not in VALID_FORECAST_MODES:
        raise ValueError(
            f"forecast_mode must be one of: {FORECAST_MODE_REALTIME}, {FORECAST_MODE_HISTORICAL}"
        )
    return mode


def is_realtime_forecast_mode(mode: str) -> bool:
    return str(mode).strip().lower() == FORECAST_MODE_REALTIME


def is_historical_simulation_mode(mode: str) -> bool:
    return str(mode).strip().lower() == FORECAST_MODE_HISTORICAL


def allow_scenario_rainfall_injection(mode: str) -> bool:
    """
    是否允许数值预报/情景面雨注入。
    现行业务：仅 realtime_forecast 允许。
    """
    return is_realtime_forecast_mode(mode)


def allow_node_observed_routing_after_forecast(mode: str, node: object) -> bool:
    """
    是否允许节点在预报段继续实测接力。
    现行业务：historical_simulation 下仅 ReservoirNode 允许。
    """
    return is_historical_simulation_mode(mode) and isinstance(node, ReservoirNode)
