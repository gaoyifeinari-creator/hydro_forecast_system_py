from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from hydro_engine.core.context import ForecastTimeContext
from hydro_engine.core.forcing import ForcingData, ForcingKind, parse_forcing_kind, validate_forcing_contract
from hydro_engine.core.data_pool import DataPool
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.processing.aggregator import SpatialAggregator


class CatchmentDataSynthesizer:
    """
    把“站点强迫”合成“子流域多维 ForcingData”。
    """

    def __init__(self) -> None:
        self._spatial = SpatialAggregator()

    def synthesize(
        self,
        *,
        scheme: Any,
        data_pool: DataPool,
        scenario_id: str,
        binding_specs: List[Dict[str, Any]],
        time_context: ForecastTimeContext,
    ) -> None:
        for spec in binding_specs:
            catchment_id = str(spec["catchment_id"])
            if catchment_id not in scheme.catchments:
                raise ValueError(f"Unknown catchment_id='{catchment_id}' in binding_specs")

            runoff_model = scheme.catchments[catchment_id].runoff_model

            forcing_by_kind: Dict[ForcingKind, TimeSeries] = {}
            for var in self._iter_variable_specs(spec):
                kind = parse_forcing_kind(str(var.get("kind") or var.get("forcing_kind")))
                method = str(var.get("method") or self._default_method_for_kind(kind))

                # PET 新增开关：允许显式选择“仅用月值”，而不是优先用测站序列。
                # - True（默认）：优先取测站；测站全部缺失时（且有 monthly_values）再回退。
                # - False：无条件使用 monthly_values（若缺失 monthly_values 则报错）。
                use_station_pet = True
                if kind is ForcingKind.POTENTIAL_EVAPOTRANSPIRATION:
                    use_station_pet = bool(var.get("use_station_pet", True))

                stations = var.get("stations") or []
                monthly_values = var.get("monthly_values")
                if kind is ForcingKind.POTENTIAL_EVAPOTRANSPIRATION and not use_station_pet:
                    if monthly_values is None:
                        raise ValueError(
                            "Forcing kind 'potential_evapotranspiration' with use_station_pet=false "
                            f"requires `monthly_values` for catchment_id='{catchment_id}'."
                        )
                    aggregated = self._build_monthly_evap_series(
                        monthly_values=monthly_values,
                        kind=kind,
                        time_context=time_context,
                    )
                    aggregated = self._preprocess_kind_series(kind, aggregated)
                    forcing_by_kind[kind] = aggregated
                    continue

                if not stations and kind is not ForcingKind.POTENTIAL_EVAPOTRANSPIRATION:
                    raise ValueError(
                        f"Missing stations for catchment_id='{catchment_id}', kind='{kind.value}'"
                    )

                series_by_station: Dict[str, TimeSeries] = {}
                weights: Dict[str, float] = {}
                missing_all = True
                for st in stations:
                    station_id = str(st.get("id") or st.get("station_id"))
                    if not station_id:
                        raise ValueError(
                            f"Invalid station entry in catchment_id='{catchment_id}', kind='{kind.value}': {st}"
                        )
                    w = float(st.get("weight", 1.0))
                    try:
                        ts = data_pool.get_combined_forcing(
                            scenario_id=scenario_id,
                            station_id=station_id,
                            kind=kind,
                            context=time_context,
                        )
                    except ValueError:
                        # 对 PET：当所有站点都缺失时可回退月数据；对其它 kind：直接报错
                        continue

                    missing_all = False
                    ts = self._preprocess_kind_series(kind, ts)
                    series_by_station[station_id] = ts
                    weights[station_id] = w

                # PET：站点数据缺失时回退月数据
                if missing_all:
                    if kind is ForcingKind.POTENTIAL_EVAPOTRANSPIRATION and monthly_values is not None:
                        aggregated = self._build_monthly_evap_series(
                            monthly_values=monthly_values, kind=kind, time_context=time_context
                        )
                        # 仍走一次统一预处理（边界置 NaN/插补等）
                        aggregated = self._preprocess_kind_series(kind, aggregated)
                        forcing_by_kind[kind] = aggregated
                        continue

                    raise ValueError(
                        f"No station forcing available for catchment_id='{catchment_id}', kind='{kind.value}'. "
                        f"stations={[s.get('id') or s.get('station_id') for s in stations]}. "
                        "Provide station time series or configure monthly_values for PET."
                    )

                aggregated = self._spatial.aggregate_time_series(
                    series_by_station=series_by_station,
                    weights=weights,
                    kind=kind,
                    method=method,
                )
                forcing_by_kind[kind] = aggregated

            forcing = ForcingData.from_pairs([(k, ts) for k, ts in forcing_by_kind.items()])
            try:
                validate_forcing_contract(runoff_model, forcing)
            except ValueError as exc:
                raise ValueError(
                    f"Catchment forcing is incomplete for catchment_id='{catchment_id}'. "
                    f"runoff_model={type(runoff_model).__name__}: {exc}"
                ) from exc

            data_pool.add_catchment_forcing(
                scenario_id=scenario_id,
                catchment_id=catchment_id,
                forcing=forcing,
            )

    def _iter_variable_specs(self, spec: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if spec.get("variables") is not None:
            return list(spec["variables"])

        # legacy format: spec["bindings"] = [{forcing_kind, station_id}, ...]
        bindings = spec.get("bindings", [])
        if not bindings:
            raise ValueError(f"Missing both 'variables' and legacy 'bindings' in spec: {spec}")

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for b in bindings:
            fk = str(b["forcing_kind"])
            station_id = str(b["station_id"])
            grouped.setdefault(fk, []).append({"id": station_id, "weight": 1.0})

        variables: List[Dict[str, Any]] = []
        for fk, stations in grouped.items():
            variables.append(
                {
                    "kind": fk,
                    "method": self._default_method_for_kind(parse_forcing_kind(fk)),
                    "stations": stations,
                }
            )
        return variables

    def _default_method_for_kind(self, kind: ForcingKind) -> str:
        if kind is ForcingKind.PRECIPITATION:
            return "weighted_average"
        return "arithmetic_mean"

    def _preprocess_kind_series(self, kind: ForcingKind, ts: TimeSeries) -> TimeSeries:
        try:
            if kind is ForcingKind.AIR_TEMPERATURE:
                return ts.replace_outliers_with_nan(min_value=-60.0, max_value=60.0).interpolate_nan_linear()
            if kind in (ForcingKind.PRECIPITATION, ForcingKind.SNOW_DEPTH, ForcingKind.POTENTIAL_EVAPOTRANSPIRATION):
                return ts.replace_outliers_with_nan(min_value=0.0).interpolate_nan_linear()
            # 其他要素：只做插补（不做边界裁剪）
            return ts.interpolate_nan_linear()
        except ValueError as exc:
            raise ValueError(
                f"Failed to preprocess time series for kind='{kind.value}' "
                f"(start={ts.start_time.isoformat()}, step={ts.time_step}, len={len(ts.values)}): {exc}"
            ) from exc

    def _build_monthly_evap_series(
        self,
        *,
        monthly_values: Any,
        kind: ForcingKind,
        time_context: ForecastTimeContext,
    ) -> TimeSeries:
        if kind is not ForcingKind.POTENTIAL_EVAPOTRANSPIRATION:
            raise ValueError("_build_monthly_evap_series only supports POTENTIAL_EVAPOTRANSPIRATION")

        if not isinstance(monthly_values, list) or len(monthly_values) != 12:
            raise ValueError("monthly_values must be a list of 12 numbers (Jan..Dec)")

        monthly = [float(x) for x in monthly_values]
        # 约定：legacy `evapArr` / monthly_values 表示“月平均日蒸发量（mm/day）”。
        # 需要折算到当前时段（mm/step），否则在 Hour/Minute 方案会被按“每小时都取日值”放大，导致产流偏小。
        step_days = time_context.time_delta.total_seconds() / 86400.0
        if step_days <= 0:
            raise ValueError("time_context.time_delta must be positive")
        out: List[float] = []
        for i in range(time_context.step_count):
            t = time_context.warmup_start_time + time_context.time_delta * i
            month_idx = int(t.month) - 1
            out.append(monthly[month_idx] * step_days)

        return TimeSeries(
            start_time=time_context.warmup_start_time,
            time_step=time_context.time_delta,
            values=out,
        )

