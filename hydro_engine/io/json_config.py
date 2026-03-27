from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from hydro_engine.core.context import (
    ForecastTimeContext,
    TimeType,
    parse_time_type,
)
from hydro_engine.core.forcing import (
    ForcingData,
    ForcingKind,
    parse_forcing_kind,
    validate_station_package_covers_binding,
)
from hydro_engine.core.data_pool import DataPool
from hydro_engine.core.interfaces import IHydrologicalModel, IErrorUpdater
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.domain.nodes.base import AbstractNode, NodeCorrectionConfig
from hydro_engine.domain.nodes.cross_section import CrossSectionNode
from hydro_engine.domain.nodes.diversion import DiversionNode
from hydro_engine.domain.nodes.reservoir import (
    CurvePoint,
    ReservoirCurve,
    ReservoirLevelFeatures,
    ReservoirNode,
    ReservoirOperationConstraints,
)
from hydro_engine.domain.reach import RiverReach
from hydro_engine.engine.calculator import CalculationEngine, CalculationResult
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.models.correction import AR1ErrorUpdater
from hydro_engine.models.routing import DummyRoutingModel, MuskingumRoutingModel
from hydro_engine.models.runoff import (
    DummyRunoffModel,
    SnowmeltRunoffModel,
    TankParams,
    TankRunoffModel,
    TankState,
    XinanjiangCSParams,
    XinanjiangCSRunoffModel,
    XinanjiangCSState,
    XinanjiangParams,
    XinanjiangRunoffModel,
    XinanjiangState,
)
from hydro_engine.processing.pipeline import CatchmentDataSynthesizer


def _normalize_catchment_forcing_bindings(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if "catchment_forcing_bindings" in data:
        return list(data["catchment_forcing_bindings"])
    legacy = data.get("catchment_station_bindings", [])
    return [
        {
            "catchment_id": str(x["catchment_id"]),
            "bindings": [
                {
                    "forcing_kind": ForcingKind.PRECIPITATION.value,
                    "station_id": str(x["station_id"]),
                }
            ],
        }
        for x in legacy
    ]


def _find_scheme(
    schemes: List[Dict[str, Any]], time_type: TimeType, step_size: int
) -> Dict[str, Any]:
    for s in schemes:
        if parse_time_type(str(s["time_type"])) != time_type:
            continue
        if int(s["step_size"]) != step_size:
            continue
        return s
    raise ValueError(
        f"No scheme matches time_type={time_type!s} step_size={step_size}. "
        f"Check JSON `schemes` list."
    )


def _infer_legacy_time_scale(data: Dict[str, Any]) -> Tuple[TimeType, int]:
    ta = data.get("time_axis", {})
    if "time_step_hours" in ta:
        raw = float(ta["time_step_hours"])
        if abs(raw - round(raw)) > 1e-9:
            raise ValueError("Legacy time_axis.time_step_hours must be a whole number")
        return TimeType.HOUR, int(round(raw))
    raise ValueError(
        "Legacy config requires time_axis.time_step_hours, or use `schemes` with time_type/step_size."
    )


def _parse_time_axis_dict(
    ta: Dict[str, Any],
    time_type: TimeType,
    step_size: int,
    warmup_start_time: datetime | None = None,
) -> ForecastTimeContext:
    """
    推荐：四段连续步数（预热 / 校正 / 历史显示 / 预报），日历锚点为运行时的 **warmup_start_time**。

    旧版 ``start_time`` + ``length``：若传入 ``warmup_start_time`` 则覆盖文件中的 ``start_time``。
    """
    td = _make_timedelta_from_type_step(time_type, step_size)

    if "base_t0" in ta:
        raise ValueError(
            "time_axis must not contain base_t0; pass warmup_start_time when loading/running."
        )

    if "warmup_period_steps" in ta:
        if warmup_start_time is None:
            raise ValueError(
                "warmup_start_time is required when time_axis uses period step counts. "
                "Pass it at calculation time."
            )
        required = (
            "warmup_period_steps",
            "correction_period_steps",
            "historical_display_period_steps",
            "forecast_period_steps",
        )
        for k in required:
            if k not in ta:
                raise ValueError(f"time_axis must include all of: {required}")
        return ForecastTimeContext.from_period_counts(
            warmup_start_time,
            time_type,
            step_size,
            warmup_period_steps=int(ta["warmup_period_steps"]),
            correction_period_steps=int(ta["correction_period_steps"]),
            historical_display_period_steps=int(ta["historical_display_period_steps"]),
            forecast_period_steps=int(ta["forecast_period_steps"]),
        )

    if "forecast_start_time" in ta:
        return ForecastTimeContext.from_absolute_times(
            warmup_start_time=_parse_datetime(ta["warmup_start_time"]),
            correction_start_time=_parse_datetime(ta["correction_start_time"]),
            forecast_start_time=_parse_datetime(ta["forecast_start_time"]),
            display_start_time=_parse_datetime(ta["display_start_time"]),
            end_time=_parse_datetime(ta["end_time"]),
            time_type=time_type,
            step_size=step_size,
        )

    if "start_time" in ta and "length" in ta:
        st = (
            warmup_start_time
            if warmup_start_time is not None
            else _parse_datetime(ta["start_time"])
        )
        length = int(ta["length"])
        if length <= 0:
            raise ValueError("time_axis.length must be positive")
        end = st + td * length
        return ForecastTimeContext.from_absolute_times(
            warmup_start_time=st,
            correction_start_time=st,
            forecast_start_time=st,
            display_start_time=st,
            end_time=end,
            time_type=time_type,
            step_size=step_size,
        )

    raise ValueError(
        "time_axis must define period counts (warmup_period_steps, correction_period_steps, "
        "historical_display_period_steps, forecast_period_steps), "
        "or legacy start_time+length, or absolute phase times (forecast_start_time, …)."
    )


def _make_timedelta_from_type_step(time_type: TimeType, step_size: int) -> timedelta:
    if time_type is TimeType.MINUTE:
        return timedelta(minutes=step_size)
    if time_type is TimeType.HOUR:
        return timedelta(hours=step_size)
    if time_type is TimeType.DAY:
        return timedelta(days=step_size)
    raise ValueError(f"Unsupported time_type: {time_type}")


def _require_scheme_keys(scheme_data: Dict[str, Any]) -> None:
    """多尺度配置下，每个 scheme 自包含拓扑与参数；缺少键则提前报错。"""
    required = (
        "time_axis",
        "nodes",
        "reaches",
        "catchments",
        "catchment_forcing_bindings",
        "stations",
    )
    for key in required:
        if key not in scheme_data:
            raise ValueError(
                f"Each entry in `schemes` must include `{key}` "
                f"(use [] for empty catchments/stations/bindings if applicable)."
            )
    raw_st = scheme_data.get("stations")
    if raw_st is not None and not isinstance(raw_st, (list, dict)):
        raise ValueError(
            "`stations` must be a flat list (legacy) or a categorized object "
            "(see `flatten_stations_catalog`)."
        )


def flatten_stations_catalog(raw: Any) -> List[Dict[str, Any]]:
    """
    将方案中的 ``stations`` 元数据规范为扁平列表，便于校验与 UI 展示。

    **兼容两种写法**：

    1. **旧版**：``[ { "id": "...", "name": "..." }, ... ]``
    2. **推荐**：按业务类型分组，键名即 ``catalog_category``（与模型用途对应）::

           {
             "rain_gauges": [ { "id", "name", ... }, ... ],
             "evapotranspiration_stations": [ ... ],
             "air_temperature_stations": [ ... ],
             "flow_stations": [ ... ],
             "stage_stations": [ ... ],
             "reservoir": [ { "node_id", "name", "stations": [ { "id", "name", ... }, ... ] } ]
           }

    每条记录会追加 ``catalog_category``（若条目中已有 ``station_type`` 则保留该字段语义）。
    同一 ``id`` 不得在多个分类中重复出现。
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        out: List[Dict[str, Any]] = []
        for i, item in enumerate(raw):
            if not isinstance(item, dict):
                raise ValueError(f"stations[{i}] must be an object")
            entry = dict(item)
            if "id" not in entry:
                raise ValueError(f"stations[{i}] must include 'id'")
            entry.setdefault("catalog_category", "flat_list")
            out.append(entry)
        _ensure_unique_station_ids(out, source="flat list")
        return out
    if isinstance(raw, dict):
        out: List[Dict[str, Any]] = []
        for category, items in raw.items():
            if not isinstance(items, list):
                raise ValueError(
                    f"stations['{category}'] must be a list of station objects"
                )
            for j, item in enumerate(items):
                if not isinstance(item, dict):
                    raise ValueError(
                        f"stations['{category}'][{j}] must be an object"
                    )
                # 支持“容器项”：例如 reservoir 里按 node_id 分组，容器本身不是 station，
                # 但其内部 stations[] 是 station 元数据列表。
                if "stations" in item and isinstance(item.get("stations"), list):
                    container_id = str(item.get("node_id") or item.get("id") or "")
                    nested = list(item["stations"])
                    for k, st in enumerate(nested):
                        if not isinstance(st, dict):
                            raise ValueError(
                                f"stations['{category}'][{j}].stations[{k}] must be an object"
                            )
                        entry = dict(st)
                        if "id" not in entry:
                            raise ValueError(
                                f"stations['{category}'][{j}].stations[{k}] must include 'id'"
                            )
                        suffix = f".{container_id}" if container_id else ""
                        entry.setdefault("catalog_category", f"{category}{suffix}")
                        out.append(entry)
                    continue

                entry = dict(item)
                if "id" not in entry:
                    raise ValueError(
                        f"stations['{category}'][{j}] must include 'id'"
                    )
                entry.setdefault("catalog_category", str(category))
                out.append(entry)
        _ensure_unique_station_ids(out, source="categorized stations")
        return out
    raise ValueError(
        "`stations` must be a list or a dict of categorized station arrays"
    )


def _ensure_unique_station_ids(entries: List[Dict[str, Any]], *, source: str) -> None:
    seen: Dict[str, str] = {}
    for e in entries:
        sid = str(e["id"])
        cat = str(e.get("catalog_category", ""))
        if sid in seen:
            raise ValueError(
                f"Duplicate station id '{sid}' in {source}: "
                f"first in category '{seen[sid]}', then '{cat}'"
            )
        seen[sid] = cat


def load_scheme_from_json(
    file_path: str | Path,
    time_type: str,
    step_size: int,
    warmup_start_time: datetime | None = None,
) -> Tuple[ForecastingScheme, List[Dict[str, Any]], ForecastTimeContext]:
    """
    从 JSON 加载与 ``time_type`` + ``step_size`` 匹配的计算方案。

    **warmup_start_time**：计算时传入的日历锚点，即**预热段第一个时间步**的绝对时刻。
    使用 ``schemes`` 且 ``time_axis`` 为四段步数配置时**必填**；不得再在配置中写 ``base_t0``。

    当根级存在 ``schemes`` 列表时：只使用**匹配到的那一项**；该项须自包含
    ``time_axis``、``nodes``、``reaches``、``catchments``、``stations``、
    ``catchment_forcing_bindings``。不同时间类型/步长的方案彼此独立，可有不同拓扑与参数。

    无 ``schemes`` 时按旧版单文件根级字段解析（``nodes`` / ``reaches`` / ``catchments`` 等均在根级）。
    """
    path = Path(file_path)
    data = json.loads(path.read_text(encoding="utf-8"))

    tt = parse_time_type(time_type)
    sz = int(step_size)

    if data.get("schemes"):
        scheme_data = _find_scheme(list(data["schemes"]), tt, sz)
        _require_scheme_keys(scheme_data)
        time_context = _parse_time_axis_dict(
            scheme_data["time_axis"], tt, sz, warmup_start_time=warmup_start_time
        )
        nodes_src = scheme_data["nodes"]
        reaches_src = scheme_data["reaches"]
        config_src = scheme_data
    else:
        tt_l, sz_l = _infer_legacy_time_scale(data)
        if tt is not tt_l or sz != sz_l:
            raise ValueError(
                f"Legacy config implies time_type={tt_l!s} step_size={sz_l}, "
                f"but load_scheme_from_json was called with time_type={tt!s} step_size={sz}"
            )
        time_context = _parse_time_axis_dict(
            data["time_axis"], tt_l, sz_l, warmup_start_time=warmup_start_time
        )
        nodes_src = data["nodes"]
        reaches_src = data["reaches"]
        config_src = data

    scheme = ForecastingScheme()

    for node_data in nodes_src:
        node = _build_node(node_data)
        scheme.add_node(node)

    for reach_data in reaches_src:
        routing_model = _build_model(reach_data["routing_model"])
        reach = RiverReach(
            id=reach_data["id"],
            upstream_node_id=reach_data["upstream_node_id"],
            downstream_node_id=reach_data["downstream_node_id"],
            routing_model=routing_model,
        )
        scheme.add_reach(reach)

    for catchment_data in config_src.get("catchments", []):
        runoff_model = _build_model(catchment_data["runoff_model"])
        routing_model_data = catchment_data.get("routing_model")
        if not isinstance(routing_model_data, dict):
            raise ValueError(
                f"Catchment '{catchment_data.get('id', '')}' must include 'routing_model'."
            )
        routing_model = _build_model(routing_model_data)
        downstream_node_id = str(catchment_data.get("downstream_node_id", "")).strip()
        if not downstream_node_id:
            raise ValueError(
                f"Catchment '{catchment_data.get('id', '')}' must include 'downstream_node_id'."
            )
        catchment = SubCatchment(
            id=catchment_data["id"],
            runoff_model=runoff_model,
            routing_model=routing_model,
            downstream_node_id=downstream_node_id,
        )
        scheme.add_catchment(catchment)

    binding_specs = _normalize_catchment_forcing_bindings(config_src)
    spec_catchment_ids = {str(s["catchment_id"]) for s in binding_specs}
    for cid in scheme.catchments.keys():
        if cid not in spec_catchment_ids:
            raise ValueError(f"Missing forcing binding spec for catchment: {cid}")

    # 校验 ``stations`` 写法（扁平或分类目录），并保证 id 不重复。
    if "stations" in config_src:
        flatten_stations_catalog(config_src["stations"])

    return scheme, binding_specs, time_context


def _validate_series_in_context(series: TimeSeries, ctx: ForecastTimeContext) -> None:
    if series.start_time != ctx.warmup_start_time:
        raise ValueError("Series start_time must equal time_axis.warmup_start_time")
    if series.time_step != ctx.time_delta:
        raise ValueError("Series time_step must equal ForecastTimeContext.time_delta (native scale)")
    if len(series.values) != ctx.step_count:
        raise ValueError("Series length does not match ForecastTimeContext.step_count")


def build_catchment_forcing_from_station_packages(
    config_path: str | Path,
    station_packages: Dict[str, ForcingData],
    time_type: str,
    step_size: int,
    warmup_start_time: datetime | None = None,
) -> Tuple[ForecastingScheme, Dict[str, ForcingData], ForecastTimeContext]:
    scheme, binding_specs, time_context = load_scheme_from_json(
        config_path, time_type, step_size, warmup_start_time=warmup_start_time
    )

    # 兼容旧 API：station_packages 仅提供一套（相当于 observed），此处不区分 forecast scenarios。
    scenario_id = "__legacy__"
    pool = DataPool()

    for station_id, pkg in station_packages.items():
        for kind, series in pkg.as_mapping().items():
            _validate_series_in_context(series, time_context)
            pool.add_observed(station_id, kind, series)

    synthesizer = CatchmentDataSynthesizer()
    synthesizer.synthesize(
        scheme=scheme,
        data_pool=pool,
        scenario_id=scenario_id,
        binding_specs=binding_specs,
        time_context=time_context,
    )

    catchment_forcing: Dict[str, ForcingData] = {}
    for cid in scheme.catchments.keys():
        catchment_forcing[cid] = pool.get_catchment_forcing(scenario_id, cid)

    return scheme, catchment_forcing, time_context


def legacy_rainfall_dict_to_station_packages(
    station_rainfalls: Dict[str, TimeSeries],
) -> Dict[str, ForcingData]:
    return {
        sid: ForcingData.single(ForcingKind.PRECIPITATION, ts)
        for sid, ts in station_rainfalls.items()
    }


def run_calculation_from_json(
    config_path: str | Path,
    station_packages: Dict[str, ForcingData],
    time_type: str,
    step_size: int,
    warmup_start_time: datetime | None = None,
    observed_flows: Optional[Dict[str, TimeSeries]] = None,
    forecast_mode: Optional[str] = None,
    catchment_workers: Optional[int] = None,
) -> Dict[str, Any]:
    scheme, binding_specs, time_context = load_scheme_from_json(
        file_path=config_path,
        time_type=time_type,
        step_size=step_size,
        warmup_start_time=warmup_start_time,
    )
    # 对外接口模式：
    # - realtime_forecast: 预报时段后不使用实测气象；节点接力按 forecast 边界切换到计算值
    # - historical_simulation: 预报时段后继续使用实测气象；若节点配置 use_observed_for_routing=true，
    #   则允许预报时段后继续用实测断面接力
    resolved_mode = str(forecast_mode or "realtime_forecast").strip().lower()
    if resolved_mode not in {"realtime_forecast", "historical_simulation"}:
        raise ValueError(
            "forecast_mode must be one of: realtime_forecast, historical_simulation"
        )

    if resolved_mode == "realtime_forecast":
        forecast_start_idx = int(
            (time_context.forecast_start_time - time_context.warmup_start_time)
            / time_context.time_delta
        )
        for station_id, pkg in list(station_packages.items()):
            patched = pkg
            for kind in (
                ForcingKind.PRECIPITATION,
                ForcingKind.POTENTIAL_EVAPOTRANSPIRATION,
                ForcingKind.AIR_TEMPERATURE,
            ):
                ts = patched.get(kind)
                if ts is None:
                    continue
                vals = list(ts.values)
                if forecast_start_idx < len(vals):
                    vals[forecast_start_idx:] = [0.0] * (len(vals) - forecast_start_idx)
                patched = patched.with_series(
                    kind,
                    TimeSeries(start_time=ts.start_time, time_step=ts.time_step, values=vals),
                )
            station_packages[station_id] = patched

    for node in scheme.nodes.values():
        setattr(
            node,
            "use_observed_for_routing_after_forecast",
            bool(getattr(node, "use_observed_for_routing", False))
            and resolved_mode == "historical_simulation",
        )

    # 按模式后的输入合成流域强迫。
    scenario_id = "__legacy__"
    pool = DataPool()
    for station_id, pkg in station_packages.items():
        for kind, series in pkg.as_mapping().items():
            _validate_series_in_context(series, time_context)
            pool.add_observed(station_id, kind, series)
    synthesizer = CatchmentDataSynthesizer()
    synthesizer.synthesize(
        scheme=scheme,
        data_pool=pool,
        scenario_id=scenario_id,
        binding_specs=binding_specs,
        time_context=time_context,
    )
    catchment_forcing = {
        cid: pool.get_catchment_forcing(scenario_id, cid) for cid in scheme.catchments.keys()
    }

    result = CalculationEngine().run(
        scheme,
        catchment_forcing,
        time_context,
        observed_flows or {},
        catchment_workers=catchment_workers,
    )
    payload = _serialize_result(scheme, result)
    payload["forecast_mode"] = resolved_mode
    return payload


def _serialize_result(
    scheme: ForecastingScheme, result: CalculationResult
) -> Dict[str, Any]:
    tc = result.time_context
    payload: Dict[str, Any] = {
        "topological_order": scheme.topological_order(),
        "node_total_inflows": {
            node_id: series.values for node_id, series in result.node_total_inflows.items()
        },
        "node_outflows": {
            node_id: series.values for node_id, series in result.node_outflows.items()
        },
        "node_observed_flows": {
            node_id: series.values for node_id, series in result.node_observed_flows.items()
        },
        "catchment_runoffs": {
            catchment_id: series.values for catchment_id, series in result.catchment_runoffs.items()
        },
        "catchment_routed_flows": {
            catchment_id: series.values for catchment_id, series in result.catchment_routed_flows.items()
        },
        "catchment_debug_traces": result.catchment_debug_traces,
        "reach_flows": {
            reach_id: series.values for reach_id, series in result.reach_flows.items()
        },
    }
    if tc is not None:
        payload["time_context"] = {
            "time_type": tc.time_type.name,
            "step_size": tc.step_size,
            "time_delta_seconds": tc.time_delta.total_seconds(),
            "warmup_start_time": tc.warmup_start_time.isoformat(),
            "correction_start_time": tc.correction_start_time.isoformat(),
            "forecast_start_time": tc.forecast_start_time.isoformat(),
            "display_start_time": tc.display_start_time.isoformat(),
            "end_time": tc.end_time.isoformat(),
        }
        payload["display_results"] = {
            k: v.values for k, v in result.get_display_results().items()
        }
    return payload


def _parse_node_correction(raw: Optional[Dict[str, Any]]) -> Optional[NodeCorrectionConfig]:
    if not raw:
        return None
    updater: Optional[IErrorUpdater] = None
    # 兼容旧版字段：允许在 `correction` 内出现 `updater`，也允许新结构在 `correction_config.updater_model`。
    u = raw.get("updater_model", None) or raw.get("updater", None)
    if isinstance(u, dict) and u.get("name"):
        updater = _build_updater(u)
    return NodeCorrectionConfig(updater_model=updater)


def _build_updater(data: Dict[str, Any]) -> IErrorUpdater:
    name = data["name"]
    params = data.get("params", {})
    if name == "AR1ErrorUpdater":
        return AR1ErrorUpdater(decay_factor=float(params.get("decay_factor", 0.8)))
    raise ValueError(f"Unsupported updater name: {name}")


def _build_node(node_data: Dict[str, Any]) -> AbstractNode:
    node_type = node_data["type"]
    correction_raw = node_data.get("correction_config") or node_data.get("correction")

    # 新增开关：是否启用基于实测数据的误差校正（即使用 correction_config 的 updater_model）。
    # 向后兼容：若该字段不存在，则保持旧行为（只要提供 correction_config 就会解析）。
    enable_observed_correction = node_data.get("enable_observed_correction", None)
    if enable_observed_correction is None:
        correction = _parse_node_correction(correction_raw)
    else:
        correction = (
            _parse_node_correction(correction_raw)
            if bool(enable_observed_correction)
            else None
        )

    # 节点一级字段：实测站与接力开关，默认来自新版；若节点顶级没配置则尝试从旧 correction 中回填。
    station_binding = node_data.get("station_binding") or {}

    # `observed_station_id`：用于“节点输出”接力/缝合（cross_section 输出=输入；reservoir 输出=出库）
    observed_station_id = str(
        node_data.get(
            "observed_station_id",
            node_data.get(
                "observed_outflow_station_id",
                (correction_raw or {}).get(
                    "observed_station_id",
                    station_binding.get("outflow_station_id") or station_binding.get("flow_station_id") or "",
                ),
            ),
        )
    )
    use_observed_for_routing = bool(
        node_data.get(
            "use_observed_for_routing",
            node_data.get(
                "bHisCalcToPar",
                (correction_raw or {}).get("use_observed_for_routing", False),
            ),
        )
    )
    use_observed_for_routing_after_forecast = bool(
        node_data.get("use_observed_for_routing_after_forecast", False)
    )

    # `observed_inflow_station_id`：用于“节点输入”注入（通常用于 reservoir：注入预报入库驱动未来出库）
    observed_inflow_station_id = str(
        node_data.get(
            "observed_inflow_station_id",
            station_binding.get("inflow_station_id", ""),
        )
    )
    use_observed_inflow_for_simulation = bool(
        node_data.get("use_observed_inflow_for_simulation", False)
    )
    common_kwargs: Dict[str, Any] = {
        "id": node_data["id"],
        "name": node_data.get("name", str(node_data["id"])),
        "incoming_reach_ids": node_data.get("incoming_reach_ids", []),
        "outgoing_reach_ids": node_data.get("outgoing_reach_ids", []),
        "local_catchment_ids": node_data.get("local_catchment_ids", []),
        # 节点一级实测元数据（展示/比对/接力）
        "observed_station_id": observed_station_id,
        "use_observed_for_routing": use_observed_for_routing,
        "use_observed_for_routing_after_forecast": use_observed_for_routing_after_forecast,
        # 节点输入注入（用于驱动水库调度输出未来出库）
        "observed_inflow_station_id": observed_inflow_station_id,
        "use_observed_inflow_for_simulation": use_observed_inflow_for_simulation,
        # 节点算法相关配置
        "correction_config": correction,
    }
    params = node_data.get("params", {})

    if node_type == "cross_section":
        return CrossSectionNode(**common_kwargs)
    if node_type == "reservoir":
        level_features_data = params.get("level_features")
        operation_constraints_data = params.get("operation_constraints", {})
        curves_data = params.get("curves", [])

        level_features = None
        if level_features_data is not None:
            level_features = ReservoirLevelFeatures(
                dead_level=float(level_features_data["dead_level"]),
                normal_level=float(level_features_data["normal_level"]),
                flood_limit_level=float(level_features_data["flood_limit_level"]),
                check_flood_level=float(level_features_data["check_flood_level"]),
            )

        operation_constraints = ReservoirOperationConstraints(
            min_release=float(operation_constraints_data.get("min_release", 0.0)),
            max_release=float(operation_constraints_data.get("max_release", 1.0e12)),
        )
        curves: List[ReservoirCurve] = []
        for curve_data in curves_data:
            points = [
                CurvePoint(x=float(point["x"]), y=float(point["y"]))
                for point in curve_data.get("points", [])
            ]
            curves.append(
                ReservoirCurve(
                    name=str(curve_data["name"]),
                    direction=str(curve_data["direction"]),
                    points=points,
                )
            )

        return ReservoirNode(
            inflow_attenuation=float(params.get("inflow_attenuation", 0.8)),
            dispatch_model_alg_type=str(params.get("dispatch_model_alg_type", "Attenuation")),
            level_features=level_features,
            operation_constraints=operation_constraints,
            curves=curves,
            **common_kwargs,
        )
    if node_type == "diversion":
        return DiversionNode(
            main_channel_id=str(params["main_channel_id"]),
            bypass_channel_id=str(params["bypass_channel_id"]),
            main_channel_capacity=float(params["main_channel_capacity"]),
            **common_kwargs,
        )
    raise ValueError(f"Unsupported node type: {node_type}")


def _build_model(model_data: Dict[str, Any]) -> IHydrologicalModel:
    name = model_data["name"]
    params = model_data.get("params", {})
    state = model_data.get("state", {})

    model_map = {
        "DummyRunoffModel": DummyRunoffModel,
        "DummyRoutingModel": DummyRoutingModel,
        "SnowmeltRunoffModel": SnowmeltRunoffModel,
        "XinanjiangRunoffModel": XinanjiangRunoffModel,
        "XinanjiangCSRunoffModel": XinanjiangCSRunoffModel,
        "TankRunoffModel": TankRunoffModel,
        "MuskingumRoutingModel": MuskingumRoutingModel,
    }
    if name not in model_map:
        raise ValueError(f"Unsupported model name: {name}")

    if name == "XinanjiangRunoffModel":
        ug = params.get("unit_graph")
        unit_graph = (
            (float(ug[0]), float(ug[1]), float(ug[2]))
            if isinstance(ug, (list, tuple)) and len(ug) == 3
            else (0.2, 0.7, 0.1)
        )
        return XinanjiangRunoffModel(
            params=XinanjiangParams(
                wum=float(params.get("wum", 20.0)),
                wlm=float(params.get("wlm", 40.0)),
                wdm=float(params.get("wdm", 40.0)),
                k=float(params.get("k", 0.8)),
                c=float(params.get("c", 0.1)),
                b=float(params.get("b", 0.3)),
                imp=float(params.get("imp", 0.02)),
                sm=float(params.get("sm", 30.0)),
                ex=float(params.get("ex", 1.2)),
                kss=float(params.get("kss", 0.4)),
                kg=float(params.get("kg", 0.3)),
                kkss=float(params.get("kkss", 0.9)),
                kkg=float(params.get("kkg", 0.95)),
                area=float(params.get("area", 0.0)),
                unit_graph=unit_graph,
            ),
            state=XinanjiangState(
                wu=float(state.get("wu", params.get("wu0", 5.0))),
                wl=float(state.get("wl", params.get("wl0", 10.0))),
                wd=float(state.get("wd", params.get("wd0", 20.0))),
                fr=float(state.get("fr", params.get("fr0", 0.01))),
                s=float(state.get("s", params.get("s0", 6.0))),
                qrss0=float(state.get("qrss0", params.get("qrss0", 18.0))),
                qrg0=float(state.get("qrg0", params.get("qrg0", 20.0))),
            ),
        )
    if name == "XinanjiangCSRunoffModel":
        return XinanjiangCSRunoffModel(
            params=XinanjiangCSParams(
                lag=int(params.get("lag", 1)),
                wum=float(params.get("wum", 20.0)),
                wlm=float(params.get("wlm", 40.0)),
                wdm=float(params.get("wdm", 40.0)),
                k=float(params.get("k", 0.8)),
                c=float(params.get("c", 0.1)),
                b=float(params.get("b", 0.3)),
                imp=float(params.get("imp", 0.02)),
                sm=float(params.get("sm", 30.0)),
                ex=float(params.get("ex", 1.2)),
                kss=float(params.get("kss", 0.4)),
                kg=float(params.get("kg", 0.3)),
                kkss=float(params.get("kkss", 0.9)),
                kkg=float(params.get("kkg", 0.95)),
                cs=float(params.get("cs", 0.8)),
                area=float(params.get("area", 0.0)),
            ),
            state=XinanjiangCSState(
                wu=float(state.get("wu", params.get("wu0", 5.0))),
                wl=float(state.get("wl", params.get("wl0", 10.0))),
                wd=float(state.get("wd", params.get("wd0", 20.0))),
                fr=float(state.get("fr", params.get("fr0", 0.01))),
                s=float(state.get("s", params.get("s0", 6.0))),
                qrss0=float(state.get("qrss0", params.get("qrss0", 18.0))),
                qrg0=float(state.get("qrg0", params.get("qrg0", 20.0))),
                qs0=float(state.get("qs0", params.get("qrs0", 20.0))),
            ),
            debug_trace=bool(params.get("debug_trace", False)),
        )
    if name == "TankRunoffModel":
        return TankRunoffModel(
            params=TankParams(
                upper_outflow_coeff=float(params.get("upper_outflow_coeff", 0.30)),
                lower_outflow_coeff=float(params.get("lower_outflow_coeff", 0.10)),
                percolation_coeff=float(params.get("percolation_coeff", 0.20)),
                evap_coeff=float(params.get("evap_coeff", 0.05)),
            ),
            state=TankState(
                upper_storage=float(
                    state.get(
                        "upper_storage",
                        params.get("upper_initial_storage", 20.0),
                    )
                ),
                lower_storage=float(
                    state.get(
                        "lower_storage",
                        params.get("lower_initial_storage", 60.0),
                    )
                ),
            ),
        )
    if name == "SnowmeltRunoffModel":
        return SnowmeltRunoffModel(
            temperature_melt_threshold=float(params.get("temperature_melt_threshold", 0.0)),
            melt_degree_factor=float(params.get("melt_degree_factor", 0.02)),
            rain_runoff_factor=float(params.get("rain_runoff_factor", 0.4)),
        )
    return model_map[name](**params)


def _parse_datetime(dt_text: str) -> datetime:
    dt_norm = dt_text.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_norm)
