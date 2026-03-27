from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

from hydro_engine.core.context import ForecastTimeContext
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries


@dataclass
class DataPool:
    """
    中心化数据池：
    - observed: 实测数据（不区分方案）
    - forecast_scenarios: 预报数据（按 scenario_id 区分）
    """

    # 原始站点时序（不区分 catchment；仅用于后续空间汇聚/合成）
    # {station_id: {ForcingKind: TimeSeries}}
    _station_data: Dict[str, Dict[ForcingKind, TimeSeries]] = field(default_factory=dict)

    # 预报方案站点时序
    # {scenario_id: {station_id: {ForcingKind: TimeSeries}}}
    _forecast_station_data: Dict[str, Dict[str, Dict[ForcingKind, TimeSeries]]] = field(
        default_factory=dict
    )

    # 子流域合成后的多维强迫（核心：scenario/catchment 级别 ForcingData 池）
    # {scenario_id: {catchment_id: ForcingData}}
    _catchment_forcing_pool: Dict[str, Dict[str, ForcingData]] = field(default_factory=dict)

    def add_observed(self, station_id: str, kind: ForcingKind, series: TimeSeries) -> None:
        if not station_id:
            raise ValueError("station_id must not be empty for observed data")
        station_map = self._station_data.setdefault(station_id, {})
        station_map[kind] = series

    def add_forecast(
        self,
        scenario_id: str,
        station_id: str,
        kind: ForcingKind,
        series: TimeSeries,
    ) -> None:
        if not scenario_id:
            raise ValueError("scenario_id must not be empty for forecast data")
        if not station_id:
            raise ValueError("station_id must not be empty for forecast data")
        scenario_map = self._forecast_station_data.setdefault(scenario_id, {})
        station_map = scenario_map.setdefault(station_id, {})
        station_map[kind] = series

    def add_catchment_forcing(
        self, scenario_id: str, catchment_id: str, forcing: ForcingData
    ) -> None:
        if not scenario_id:
            raise ValueError("scenario_id must not be empty when adding catchment forcing")
        if not catchment_id:
            raise ValueError("catchment_id must not be empty when adding catchment forcing")
        scenario_map = self._catchment_forcing_pool.setdefault(scenario_id, {})
        scenario_map[catchment_id] = forcing

    def get_catchment_forcing(self, scenario_id: str, catchment_id: str) -> ForcingData:
        if scenario_id not in self._catchment_forcing_pool:
            raise ValueError(f"Unknown scenario_id='{scenario_id}' for catchment_forcing_pool")
        if catchment_id not in self._catchment_forcing_pool[scenario_id]:
            raise ValueError(
                f"Missing catchment forcing for scenario_id='{scenario_id}', catchment_id='{catchment_id}'. "
                "Did you run the catchment data synthesizer?"
            )
        return self._catchment_forcing_pool[scenario_id][catchment_id]

    def get_combined_forcing(
        self,
        scenario_id: str,
        station_id: str,
        kind: ForcingKind,
        context: ForecastTimeContext,
    ) -> TimeSeries:
        obs = self._station_data.get(station_id, {}).get(kind)
        fcst = self._forecast_station_data.get(scenario_id, {}).get(station_id, {}).get(kind)

        if obs is None and fcst is None:
            raise ValueError(
                "No forcing data found for "
                f"scenario='{scenario_id}', station='{station_id}', kind='{kind.value}'. "
                "Neither observed nor forecast series is available."
            )

        if obs is not None and fcst is not None:
            combined = self._blend_observed_and_forecast(
                scenario_id=scenario_id,
                station_id=station_id,
                kind=kind,
                obs=obs,
                forecast=fcst,
                context=context,
            )
        elif obs is not None:
            combined = obs
        else:
            combined = fcst  # fcst is guaranteed not None here

        try:
            return combined.slice(context.warmup_start_time, context.end_time)
        except ValueError as exc:
            raise ValueError(
                "Combined forcing cannot be sliced to context range "
                f"[{context.warmup_start_time.isoformat()}, {context.end_time.isoformat()}) "
                f"for scenario='{scenario_id}', station='{station_id}', kind='{kind.value}'."
            ) from exc

    def _blend_observed_and_forecast(
        self,
        scenario_id: str,
        station_id: str,
        kind: ForcingKind,
        obs: TimeSeries,
        forecast: TimeSeries,
        context: ForecastTimeContext,
    ) -> TimeSeries:
        """
        规则：
        - T < T0 取 obs
        - T >= T0 取 forecast

        复用 TimeSeries.blend：它在 `<= t0` 取 other，在 `> t0` 取 self。
        所以传入 self=forecast, other=obs，并将边界设为 T0-1step。
        """
        boundary = context.forecast_start_time - forecast.time_step
        try:
            return forecast.blend(obs, boundary)
        except ValueError as exc:
            raise ValueError(
                "Observed/forecast series are incompatible for blending "
                f"(scenario='{scenario_id}', station='{station_id}', kind='{kind.value}'). "
                f"obs(start={obs.start_time.isoformat()}, step={obs.time_step}, len={len(obs)}), "
                f"forecast(start={forecast.start_time.isoformat()}, step={forecast.time_step}, len={len(forecast)})."
            ) from exc
