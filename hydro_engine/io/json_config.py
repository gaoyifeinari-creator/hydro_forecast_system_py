from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta
from pathlib import Path
import math
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast

import numpy as np

from hydro_engine.core.context import (
    ForecastTimeContext,
    TimeType,
    native_time_delta,
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
from hydro_engine.core.timeseries import TimeSeries, summarize_for_display_json
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
from hydro_engine.models import MODEL_REGISTRY, _make_model_from_registry
from hydro_engine.processing.pipeline import CatchmentDataSynthesizer
from hydro_engine.forecast.catchment_forecast_rainfall import CatchmentForecastRainfall
from hydro_engine.forecast.scenario_forcing import (
    ScenarioName,
    patch_catchment_scenario_precipitation,
)


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


def _forecast_rules_key_to_forcing_kind(key: str) -> ForcingKind:
    k = str(key).strip().lower()
    if k == "precipitation":
        return ForcingKind.PRECIPITATION
    if k in {"temperature", "air_temperature", "tmp", "t"}:
        return ForcingKind.AIR_TEMPERATURE
    raise ValueError(f"Unsupported catchment_forecast_rules key: {key!r}")


def _build_catchment_forecast_fusion_plan(
    rules: Dict[str, Any],
    catchment_ids: Iterable[str],
) -> Dict[str, Any]:
    """
    从 `catchment_foreast_rules` 生成融合计划。

    约定：
    - 对每个 rule.key(precipitation/temperature) 与 catchment_id 生成一个“虚拟 station_id”（用于 binding_specs）。
    - 虚拟 station_id 使用 `source_id_template`，把 `{subtype}` 替换为 `default_profile`。
    - 真实 source_id 按 `profiles[default_profile]` 的顺序作为优先级数组做逐时嵌套兜底融合。
    """
    virtual_bindings: Dict[str, Dict[str, Any]] = {}
    raw_senids: Set[str] = set()
    virtual_station_id_by_catchment_kind: Dict[str, Dict[str, str]] = {}

    catchment_ids = [str(c).strip() for c in catchment_ids if str(c).strip()]
    for rule_key, rule_data in (rules or {}).items():
        kind = _forecast_rules_key_to_forcing_kind(rule_key)
        if not isinstance(rule_data, dict):
            raise ValueError(f"catchment_forecast_rules[{rule_key!r}] must be an object")
        unit = rule_data.get("unit")
        source_id_template = str(rule_data.get("source_id_template") or "").strip()
        default_profile = str(rule_data.get("default_profile") or "").strip()
        profiles = rule_data.get("profiles") or {}

        if not source_id_template:
            raise ValueError(f"catchment_forecast_rules[{rule_key!r}] missing source_id_template")
        if not default_profile:
            raise ValueError(f"catchment_forecast_rules[{rule_key!r}] missing default_profile")
        if not isinstance(profiles, dict) or not profiles:
            raise ValueError(f"catchment_forecast_rules[{rule_key!r}] must include profiles object")
        if default_profile not in profiles:
            raise ValueError(
                f"catchment_forecast_rules[{rule_key!r}] default_profile={default_profile!r} "
                f"not found in profiles keys {sorted(map(str, profiles.keys()))}"
            )

        priority_subtypes = profiles[default_profile]
        if not isinstance(priority_subtypes, list) or not priority_subtypes:
            raise ValueError(
                f"catchment_forecast_rules[{rule_key!r}] profiles[{default_profile!r}] must be a non-empty list"
            )
        priority_subtypes = [str(x).strip() for x in priority_subtypes if str(x).strip()]
        if not priority_subtypes:
            raise ValueError(
                f"catchment_forecast_rules[{rule_key!r}] profiles[{default_profile!r}] has no valid subtype"
            )

        # template 校验（仅检查占位符是否能格式化）
        try:
            _ = source_id_template.format(subtype=priority_subtypes[0], catchment_id=catchment_ids[0])
            _ = source_id_template.format(subtype=default_profile, catchment_id=catchment_ids[0])
        except Exception as exc:
            raise ValueError(
                f"catchment_forecast_rules[{rule_key!r}] source_id_template is invalid: {exc!s}"
            ) from exc

        for cid in catchment_ids:
            virtual_id = source_id_template.format(subtype=default_profile, catchment_id=cid)
            source_ids = [
                source_id_template.format(subtype=subtype, catchment_id=cid)
                for subtype in priority_subtypes
            ]
            raw_senids.update(source_ids)

            virtual_station_id_by_catchment_kind.setdefault(cid, {})[kind.value] = virtual_id
            virtual_bindings[virtual_id] = {
                "catchment_id": cid,
                "kind": kind,
                "unit": unit,
                "priority_subtypes": priority_subtypes,
                "source_ids": source_ids,
            }

    return {
        "raw_senids": sorted(raw_senids),
        "virtual_bindings": virtual_bindings,
        "virtual_station_id_by_catchment_kind": virtual_station_id_by_catchment_kind,
    }


def _apply_catchment_forecast_rules_to_binding_specs(
    *,
    scheme: ForecastingScheme,
    binding_specs: List[Dict[str, Any]],
    forecast_rules: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    把 `catchment_forecast_rules` 转成 `binding_specs` 中的站点绑定。
    - 若 binding_specs 已有该 catchment/kind：不覆盖
    - 若缺失：补齐（使用虚拟 station_id）
    """
    if not forecast_rules:
        return binding_specs, {}

    fusion_plan = _build_catchment_forecast_fusion_plan(
        rules=forecast_rules,
        catchment_ids=scheme.catchments.keys(),
    )

    # catchment_id -> spec
    by_cid: Dict[str, Dict[str, Any]] = {str(s["catchment_id"]): s for s in binding_specs if "catchment_id" in s}

    # 补齐所有 catchment 的绑定 spec（用于“仅靠 catchment_forecast_rules”配置）
    for cid in scheme.catchments.keys():
        scid = str(cid)
        by_cid.setdefault(scid, {"catchment_id": scid, "variables": []})

    # 添加 precipitation / temperature 的变量（若缺失）
    kind_keys = list(forecast_rules.keys())
    for rule_key in kind_keys:
        kind = _forecast_rules_key_to_forcing_kind(rule_key)
        for cid, kind_to_virtual in fusion_plan["virtual_station_id_by_catchment_kind"].items():
            virtual_id = kind_to_virtual.get(kind.value, "")
            if not virtual_id:
                continue

            spec = by_cid.get(str(cid))
            if spec is None:
                continue

            # variables 格式
            if "variables" in spec and isinstance(spec["variables"], list):
                exists = any(str(v.get("kind") or "").strip() == kind.value for v in spec["variables"])
                if not exists:
                    spec["variables"].append(
                        {"kind": kind.value, "stations": [{"id": virtual_id, "weight": 1.0}]}
                    )
                continue

            # legacy bindings 格式
            bindings = spec.get("bindings")
            if isinstance(bindings, list):
                exists = any(str(b.get("forcing_kind") or "").strip() == kind.value for b in bindings)
                if not exists:
                    bindings.append({"forcing_kind": kind.value, "station_id": virtual_id})
                continue

            # 兜底：如果 spec 既没有 variables 也没有 bindings，则强制切换到 variables
            spec["variables"] = [{"kind": kind.value, "stations": [{"id": virtual_id, "weight": 1.0}]}]
            spec.pop("bindings", None)

    # 返回按 catchment_id 稳定排序后的 list
    out = [by_cid[str(cid)] for cid in sorted(by_cid.keys())]
    return out, fusion_plan


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
    推荐：四段步数（预热总长 W / 校正尾段 C / 历史展示尾段 H / 预报 F），语义见
    :meth:`ForecastTimeContext.from_period_counts`；日历锚点为运行时的 **warmup_start_time**。

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
    return native_time_delta(time_type=time_type, step_size=step_size)


def _require_scheme_keys(scheme_data: Dict[str, Any]) -> None:
    """多尺度配置下，每个 scheme 自包含拓扑与参数；缺少键则提前报错。"""
    required_always = (
        "time_axis",
        "nodes",
        "reaches",
        "catchments",
        "stations",
    )
    for key in required_always:
        if key not in scheme_data:
            raise ValueError(
                f"Each entry in `schemes` must include `{key}` "
                f"(use [] for empty catchments/stations if applicable)."
            )
    if "catchment_forcing_bindings" not in scheme_data and "catchment_forecast_rules" not in scheme_data:
        raise ValueError(
            "Each entry in `schemes` must include either `catchment_forcing_bindings` "
            "or `catchment_forecast_rules`."
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
    _bind_node_reaches_from_edges(scheme)

    # ------------------------------------------------------------
    # Catchment 拓扑推导：配置层面尽量只保留单向关联。
    #
    # - 节点侧：`nodes[].local_catchment_ids` 指定哪些子流域挂在该节点上（由此得到 catchment -> owner_node）
    #
    # 当 catchment 配置缺失 `downstream_node_id` 时，自动回退：
    #   downstream_node_id := owner_node
    # ------------------------------------------------------------
    catchment_owner_node_id: Dict[str, str] = {}
    for node_id, node in scheme.nodes.items():
        for cid in list(getattr(node, "local_catchment_ids", []) or []):
            cid = str(cid).strip()
            if not cid:
                continue
            if cid in catchment_owner_node_id and catchment_owner_node_id[cid] != str(node_id):
                raise ValueError(
                    f"Catchment '{cid}' is mounted on multiple nodes: "
                    f"{catchment_owner_node_id[cid]} and {node_id}"
                )
            catchment_owner_node_id[cid] = str(node_id)

    for catchment_data in config_src.get("catchments", []):
        runoff_model = _build_model(catchment_data["runoff_model"])
        routing_model_data = catchment_data.get("routing_model")
        if not isinstance(routing_model_data, dict):
            raise ValueError(
                f"Catchment '{catchment_data.get('id', '')}' must include 'routing_model'."
            )
        routing_model = _build_model(routing_model_data)

        catchment_id = str(catchment_data["id"]).strip()
        configured_downstream_node_id = str(catchment_data.get("downstream_node_id", "")).strip()

        if not configured_downstream_node_id:
            owner_node_id = catchment_owner_node_id.get(catchment_id, "")
            if not owner_node_id:
                raise ValueError(
                    f"Catchment '{catchment_id}' is not mounted on any node.local_catchment_ids"
                )
            # 以配置员维护的“节点->子流域归属关系”为单向真相：
            # catchment.outflow 注入到 owner node 本身（由 catchment.routing_model 完成子流域到节点的过程）。
            downstream_node_id = owner_node_id
        else:
            downstream_node_id = configured_downstream_node_id
            # 如果给了 downstream_node_id，就做一致性校验：它应该与 owner node 一致。
            owner_node_id = catchment_owner_node_id.get(catchment_id, "")
            if owner_node_id:
                if downstream_node_id != owner_node_id:
                    raise ValueError(
                        f"Catchment '{catchment_id}' downstream_node_id='{downstream_node_id}' conflicts with "
                        f"nodes.local_catchment_ids owner_node_id='{owner_node_id}'."
                    )

        catchment = SubCatchment(
            id=catchment_id,
            runoff_model=runoff_model,
            routing_model=routing_model,
            downstream_node_id=downstream_node_id,
        )
        scheme.add_catchment(catchment)

    binding_specs = _normalize_catchment_forcing_bindings(config_src)

    # --- catchment_forecast_rules：把多源面预报规则转成 binding_specs（虚拟 station_id）
    forecast_rules = config_src.get("catchment_forecast_rules") or {}
    if forecast_rules:
        binding_specs, fusion_plan = _apply_catchment_forecast_rules_to_binding_specs(
            scheme=scheme,
            binding_specs=binding_specs,
            forecast_rules=forecast_rules,
        )
        scheme.catchment_forecast_rules = dict(forecast_rules)
        scheme.catchment_forecast_fusion_plan = dict(fusion_plan)

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
    if series.time_steps != ctx.step_count:
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


def apply_realtime_forecast_observed_meteorology_cutoff(
    station_packages: Dict[str, ForcingData],
    *,
    time_context: ForecastTimeContext,
) -> None:
    """
    实时预报：预报起点 T0 为第一个预报步，该时刻及之后尚无可用实测气象强迫。
    将各测站 P / PET / 气温序列在 [T0, end] 置零（原地修改 station_packages）。
    """
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
            vals = np.array(ts.values, dtype=np.float64, copy=True)
            if forecast_start_idx < ts.time_steps:
                vals[..., forecast_start_idx:] = 0.0
            patched = patched.with_series(
                kind,
                TimeSeries(start_time=ts.start_time, time_step=ts.time_step, values=vals),
            )
        station_packages[station_id] = patched


def run_calculation_from_json(
    config_path: str | Path,
    station_packages: Dict[str, ForcingData],
    time_type: str,
    step_size: int,
    warmup_start_time: datetime | None = None,
    observed_flows: Optional[Dict[str, TimeSeries]] = None,
    forecast_mode: Optional[str] = None,
    catchment_workers: Optional[int] = None,
    *,
    catchment_scenario_rainfall: Optional[Dict[str, CatchmentForecastRainfall]] = None,
    scenario_precipitation: str = "expected",
    forecast_multiscenario: bool = False,
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
        apply_realtime_forecast_observed_meteorology_cutoff(
            station_packages, time_context=time_context
        )

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

    scenario_map = catchment_scenario_rainfall or {}
    primary_scen = str(scenario_precipitation or "expected").strip().lower()
    if primary_scen not in {"expected", "upper", "lower"}:
        raise ValueError(
            "scenario_precipitation must be one of: expected, upper, lower "
            f"(got {scenario_precipitation!r})"
        )

    def _forcing_for_scenario(scen: str) -> Dict[str, ForcingData]:
        out = copy.deepcopy(catchment_forcing)
        for cid, rain in scenario_map.items():
            patch_catchment_scenario_precipitation(
                out,
                time_context=time_context,
                catchment_id=cid,
                rainfall=rain,
                scenario=cast(ScenarioName, scen),
            )
        return out

    multiscenario_engine_outputs: Optional[Dict[str, Dict[str, Any]]] = None
    if scenario_map:
        if forecast_multiscenario:
            multiscenario_engine_outputs = {}
            primary_result: Optional[CalculationResult] = None
            for scen in ("expected", "upper", "lower"):
                patched = _forcing_for_scenario(scen)
                res = CalculationEngine().run(
                    scheme,
                    patched,
                    time_context,
                    observed_flows or {},
                    catchment_workers=catchment_workers,
                )
                multiscenario_engine_outputs[scen] = _serialize_result(scheme, res)
                if scen == primary_scen:
                    primary_result = res
            assert primary_result is not None
            result = primary_result
        else:
            patched = _forcing_for_scenario(primary_scen)
            result = CalculationEngine().run(
                scheme,
                patched,
                time_context,
                observed_flows or {},
                catchment_workers=catchment_workers,
            )
    else:
        result = CalculationEngine().run(
            scheme,
            catchment_forcing,
            time_context,
            observed_flows or {},
            catchment_workers=catchment_workers,
        )

    payload = _serialize_result(scheme, result)
    payload["forecast_mode"] = resolved_mode
    if multiscenario_engine_outputs is not None:
        payload["multiscenario_engine_outputs"] = multiscenario_engine_outputs
    return payload


def _engine_series_to_json_list(series: TimeSeries) -> Any:
    """完整引擎序列 JSON：1D 为列表；2D 为嵌套列表（集合维 × 时间）。"""
    return series.values.tolist()


def _serialize_result(
    scheme: ForecastingScheme, result: CalculationResult
) -> Dict[str, Any]:
    tc = result.time_context
    payload: Dict[str, Any] = {
        "topological_order": scheme.topological_order(),
        "node_total_inflows": {
            node_id: _engine_series_to_json_list(series) for node_id, series in result.node_total_inflows.items()
        },
        "node_outflows": {
            node_id: _engine_series_to_json_list(series) for node_id, series in result.node_outflows.items()
        },
        "node_observed_flows": {
            node_id: _engine_series_to_json_list(series)
            for node_id, series in result.node_observed_flows.items()
        },
        "catchment_runoffs": {
            catchment_id: _engine_series_to_json_list(series)
            for catchment_id, series in result.catchment_runoffs.items()
        },
        "catchment_routed_flows": {
            catchment_id: _engine_series_to_json_list(series)
            for catchment_id, series in result.catchment_routed_flows.items()
        },
        "catchment_debug_traces": result.catchment_debug_traces,
        "reach_flows": {
            reach_id: _engine_series_to_json_list(series) for reach_id, series in result.reach_flows.items()
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
            k: summarize_for_display_json(v) for k, v in result.get_display_results().items()
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
    """通过模型注册表实例化误差校正器。"""
    name = str(data["name"])
    # 误差校正器通过 register_model("AR1ErrorUpdater", ...) 已注册到 MODEL_REGISTRY
    # 其工厂函数接收完整 data（含 params），此处透传
    return _make_model_from_registry(name, data)


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
        # 拓扑单一真相：节点入/出边由 reaches 自动推导，不再要求在 nodes 中重复维护。
        "incoming_reach_ids": [],
        "outgoing_reach_ids": [],
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


def _bind_node_reaches_from_edges(scheme: ForecastingScheme) -> None:
    """
    以 reaches 为单一拓扑真相，回填每个节点的 incoming/outgoing reach 列表。
    """
    incoming: Dict[str, List[str]] = {str(nid): [] for nid in scheme.nodes.keys()}
    outgoing: Dict[str, List[str]] = {str(nid): [] for nid in scheme.nodes.keys()}

    for rid, reach in scheme.reaches.items():
        uid = str(reach.upstream_node_id)
        did = str(reach.downstream_node_id)
        if uid not in outgoing or did not in incoming:
            # 理论上 add_reach 已校验；此处仅防御性保护。
            raise ValueError(f"Reach '{rid}' references unknown node(s): {uid} -> {did}")
        outgoing[uid].append(str(rid))
        incoming[did].append(str(rid))

    for nid, node in scheme.nodes.items():
        node.incoming_reach_ids = incoming.get(str(nid), [])
        node.outgoing_reach_ids = outgoing.get(str(nid), [])


def _build_model(model_data: Dict[str, Any]) -> IHydrologicalModel:
    """通过模型注册表实例化水文模型。

    所有产流模型、河道演进模型均通过 ``register_model`` 自注册到 ``MODEL_REGISTRY``。
    新增模型只需在对应 ``__init__.py`` 末尾调用 ``register_model``，无需修改本函数。

    Args:
        model_data: JSON 中 ``model`` 节点的完整 dict，含 ``name`` / ``params`` / ``state`` 等。

    Raises:
        ValueError: 模型名称未在注册表中注册。
    """
    name = str(model_data["name"])
    return _make_model_from_registry(name, model_data)


def _parse_datetime(dt_text: str) -> datetime:
    dt_norm = dt_text.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_norm)
