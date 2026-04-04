from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _pick(d: Dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _to_float(v: Any, default: float = 1.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _normalize_name(v: Any, fallback: str) -> str:
    s = str(v).strip() if v is not None else ""
    return s if s else fallback


def _normalize_monthly(values: Any) -> Optional[List[float]]:
    if not isinstance(values, list) or len(values) != 12:
        return None
    try:
        return [float(x) for x in values]
    except (TypeError, ValueError):
        return None


def _parse_monthly_from_string(v: Any) -> Optional[List[float]]:
    if not isinstance(v, str):
        return None
    parts = [x.strip() for x in v.split(",") if x.strip()]
    if len(parts) != 12:
        return None
    try:
        return [float(x) for x in parts]
    except (TypeError, ValueError):
        return None


def _infer_node_type(sec: Dict[str, Any], ds: Dict[str, Any]) -> str:
    raw = str(_pick(sec, ["nodeType", "type", "secType"], "")).lower()
    if "reservoir" in raw or "water" in raw or "res" in raw:
        return "reservoir"

    # 数据源中出现明显的入/出库字段时按 reservoir 处理。
    if any(
        k in ds
        for k in (
            "inflowStationId",
            "inflow_station_id",
            "outflowStationId",
            "outflow_station_id",
            "reservoirInflowStationId",
            "reservoirOutflowStationId",
        )
    ):
        return "reservoir"
    return "cross_section"


def _build_station_binding(node_type: str, sec: Dict[str, Any], ds: Dict[str, Any]) -> Dict[str, str]:
    if node_type == "reservoir":
        inflow = str(
            _pick(
                ds,
                [
                    "inflowStationId",
                    "inflow_station_id",
                    "reservoirInflowStationId",
                    "inStationId",
                    "flowid",
                ],
                "",
            )
        )
        outflow = str(
            _pick(
                ds,
                [
                    "outflowStationId",
                    "outflow_station_id",
                    "reservoirOutflowStationId",
                    "outStationId",
                    "outflowID",
                    "outFlowID",
                ],
                "",
            )
        )
        stage = str(_pick(ds, ["stageStationId", "stage_station_id", "waterLevelStationId"], ""))
        if not stage:
            stage = str(_pick(ds, ["staqeID", "stageID"], ""))
        return {
            "inflow_station_id": inflow,
            "outflow_station_id": outflow,
            "stage_station_id": stage,
        }

    flow = str(
        _pick(
            ds,
            ["flowStationId", "flow_station_id", "stationId", "flowid"],
            _pick(sec, ["secid", "SecId"], ""),
        )
    )
    stage = str(_pick(ds, ["stageStationId", "stage_station_id", "waterLevelStationId", "staqeID"], ""))
    return {
        "flow_station_id": flow,
        "stage_station_id": stage,
    }


def _wrap_routing_model(raw_model: Any) -> Dict[str, Any]:
    # 允许旧配置直接给 {"name": "...", "params": {...}}
    if isinstance(raw_model, dict) and raw_model.get("name"):
        return {
            "name": str(raw_model["name"]),
            "params": dict(raw_model.get("params", {})),
        }

    # 兼容某些旧格式：{"modelName": "...", ...params}
    if isinstance(raw_model, dict):
        ctype = str(_pick(raw_model, ["cmodelType", "modelName", "name"], "")).strip()
        if ctype in ("MSKModel", "MuskingumRoutingModel", ""):
            ke = _to_float(_pick(raw_model, ["ke", "k_hours", "k"], 1.0), 1.0)
            xe = _to_float(_pick(raw_model, ["xe", "x"], 0.2), 0.2)
            n_seg = int(_to_float(_pick(raw_model, ["n", "n_segments"], 2), 2.0))
            if n_seg < 0:
                n_seg = 0
            return {
                "name": "MuskingumRoutingModel",
                "params": {"k_hours": ke, "x": xe, "n_segments": n_seg},
            }
        if ctype in ("DummyRoutingModel", "DUMMY"):
            att = _to_float(_pick(raw_model, ["attenuation"], 1.0), 1.0)
            return {"name": "DummyRoutingModel", "params": {"attenuation": att}}
        # 尝试透传
        name = str(_pick(raw_model, ["modelName", "name"], "MuskingumRoutingModel"))
        params = dict(raw_model.get("params", {}))
        if not params:
            for k, v in raw_model.items():
                if k not in ("modelName", "name"):
                    params[k] = v
        return {"name": name, "params": params}

    return {"name": "MuskingumRoutingModel", "params": {}}


def _wrap_runoff_model(raw_model: Any) -> Dict[str, Any]:
    # 允许 unitgmodel 直接为项目所需结构
    if isinstance(raw_model, dict) and raw_model.get("name"):
        out = {"name": str(raw_model["name"]), "params": dict(raw_model.get("params", {}))}
        if isinstance(raw_model.get("state"), dict):
            out["state"] = dict(raw_model["state"])
        return out

    # 兼容 modelName + 其余字段作为 params
    if isinstance(raw_model, dict):
        gtype = str(_pick(raw_model, ["gmodelType", "modelName", "name"], "")).strip()
        if gtype in ("XAJCSModel", "XinanjiangCSRunoffModel"):
            params = {
                "lag": int(_pick(raw_model, ["lag"], 1)),
                "wum": _to_float(_pick(raw_model, ["wum"], 20.0), 20.0),
                "wlm": _to_float(_pick(raw_model, ["wlm"], 40.0), 40.0),
                "wdm": _to_float(_pick(raw_model, ["wdm"], 40.0), 40.0),
                "k": _to_float(_pick(raw_model, ["k"], 0.8), 0.8),
                "c": _to_float(_pick(raw_model, ["c"], 0.1), 0.1),
                "b": _to_float(_pick(raw_model, ["b"], 0.3), 0.3),
                "imp": _to_float(_pick(raw_model, ["imp"], 0.02), 0.02),
                "sm": _to_float(_pick(raw_model, ["sm"], 30.0), 30.0),
                "ex": _to_float(_pick(raw_model, ["ex"], 1.2), 1.2),
                "kss": _to_float(_pick(raw_model, ["kss"], 0.4), 0.4),
                "kg": _to_float(_pick(raw_model, ["kg"], 0.3), 0.3),
                "kkss": _to_float(_pick(raw_model, ["kkss"], 0.9), 0.9),
                "kkg": _to_float(_pick(raw_model, ["kkg"], 0.95), 0.95),
                "cs": _to_float(_pick(raw_model, ["cs"], 0.8), 0.8),
                "area": _to_float(_pick(raw_model, ["area"], 0.0), 0.0),
            }
            state = {
                "wu": _to_float(_pick(raw_model, ["wu0"], 5.0), 5.0),
                "wl": _to_float(_pick(raw_model, ["wl0"], 10.0), 10.0),
                "wd": _to_float(_pick(raw_model, ["wd0"], 20.0), 20.0),
                "fr": _to_float(_pick(raw_model, ["fr0"], 0.01), 0.01),
                "s": _to_float(_pick(raw_model, ["s0"], 6.0), 6.0),
                "qrss0": _to_float(_pick(raw_model, ["qrss0"], 18.0), 18.0),
                "qrg0": _to_float(_pick(raw_model, ["qrg0"], 20.0), 20.0),
                "qs0": _to_float(_pick(raw_model, ["qrs0", "qs0"], 20.0), 20.0),
            }
            return {"name": "XinanjiangCSRunoffModel", "params": params, "state": state}
        if gtype in ("XAJModel", "XinanjiangRunoffModel"):
            params = {
                "wum": _to_float(_pick(raw_model, ["wum"], 20.0), 20.0),
                "wlm": _to_float(_pick(raw_model, ["wlm"], 40.0), 40.0),
                "wdm": _to_float(_pick(raw_model, ["wdm"], 40.0), 40.0),
                "k": _to_float(_pick(raw_model, ["k"], 0.8), 0.8),
                "c": _to_float(_pick(raw_model, ["c"], 0.1), 0.1),
                "b": _to_float(_pick(raw_model, ["b"], 0.3), 0.3),
                "imp": _to_float(_pick(raw_model, ["imp"], 0.02), 0.02),
                "sm": _to_float(_pick(raw_model, ["sm"], 30.0), 30.0),
                "ex": _to_float(_pick(raw_model, ["ex"], 1.2), 1.2),
                "kss": _to_float(_pick(raw_model, ["kss"], 0.4), 0.4),
                "kg": _to_float(_pick(raw_model, ["kg"], 0.3), 0.3),
                "kkss": _to_float(_pick(raw_model, ["kkss"], 0.9), 0.9),
                "kkg": _to_float(_pick(raw_model, ["kkg"], 0.95), 0.95),
                "area": _to_float(_pick(raw_model, ["area"], 0.0), 0.0),
            }
            state = {
                "wu": _to_float(_pick(raw_model, ["wu0"], 5.0), 5.0),
                "wl": _to_float(_pick(raw_model, ["wl0"], 10.0), 10.0),
                "wd": _to_float(_pick(raw_model, ["wd0"], 20.0), 20.0),
                "fr": _to_float(_pick(raw_model, ["fr0"], 0.01), 0.01),
                "s": _to_float(_pick(raw_model, ["s0"], 6.0), 6.0),
                "qrss0": _to_float(_pick(raw_model, ["qrss0"], 18.0), 18.0),
                "qrg0": _to_float(_pick(raw_model, ["qrg0"], 20.0), 20.0),
            }
            return {"name": "XinanjiangRunoffModel", "params": params, "state": state}

        name = str(_pick(raw_model, ["modelName", "name"], "XinanjiangRunoffModel"))
        params = dict(raw_model.get("params", {}))
        state = raw_model.get("state")
        if not params:
            for k, v in raw_model.items():
                if k not in ("modelName", "name", "state", "params", "gmodelType"):
                    params[k] = v
        out = {"name": name, "params": params}
        if isinstance(state, dict):
            out["state"] = dict(state)
        return out

    return {"name": "XinanjiangRunoffModel", "params": {}}


def _register_flow_station_catalog(
    flow_station_seen: Dict[str, Dict[str, Any]],
    sid: str,
    *,
    ds_name: str,
) -> None:
    """流量站目录：名称优先采用 HFDataSource.name，缺省回退为站号 id。"""
    s = str(sid).strip()
    if not s or s in ("-99", "0"):
        return
    display = str(ds_name or "").strip() or s
    if s not in flow_station_seen:
        flow_station_seen[s] = {"id": s, "name": display, "unit": "m3/s"}
        return
    cur = str(flow_station_seen[s].get("name", "") or "").strip()
    if cur == s and display != s:
        flow_station_seen[s]["name"] = display


def _register_stage_station_catalog(
    stage_station_seen: Dict[str, Dict[str, Any]],
    sid: str,
    *,
    ds_name: str,
) -> None:
    """水位站目录：名称优先采用 HFDataSource.name，缺省回退为站号 id。"""
    s = str(sid).strip()
    if not s or s in ("-99", "0"):
        return
    display = str(ds_name or "").strip() or s
    if s not in stage_station_seen:
        stage_station_seen[s] = {"id": s, "name": display, "unit": "m"}
        return
    cur = str(stage_station_seen[s].get("name", "") or "").strip()
    if cur == s and display != s:
        stage_station_seen[s]["name"] = display


def _iter_units(sec: Dict[str, Any]) -> List[Dict[str, Any]]:
    units = _pick(sec, ["units", "Units"], [])
    if isinstance(units, list):
        return [u for u in units if isinstance(u, dict)]
    return []


def _build_forcing_vars_from_unit(unit: Dict[str, Any]) -> List[Dict[str, Any]]:
    variables: List[Dict[str, Any]] = []

    prestations = _pick(unit, ["prestations", "preStations"], []) or []
    rain_stations: List[Dict[str, Any]] = []
    for st in prestations:
        if not isinstance(st, dict):
            continue
        sid = str(_pick(st, ["id", "stationId", "station_id", "stcd", "preStaId", "preStaSenid"], "")).strip()
        if not sid:
            continue
        pre_name = str(
            _pick(st, ["preStaName", "preSta_name", "stationName", "staName", "name"], "")
        ).strip()
        entry: Dict[str, Any] = {
            "id": sid,
            "weight": _to_float(_pick(st, ["weight", "w", "preStaWeight"], 1.0), 1.0),
        }
        if pre_name:
            entry["name"] = pre_name
        rain_stations.append(entry)

    if rain_stations:
        variables.append(
            {
                "kind": "precipitation",
                "method": "weighted_average",
                "stations": rain_stations,
            }
        )

    evap_station_obj = _pick(unit, ["evapStation"], None)
    evap_stations_raw = unit.get("evapstations", []) or unit.get("petstations", []) or []
    if isinstance(evap_station_obj, dict):
        evap_stations_raw = [evap_station_obj]
    evap_stations: List[Dict[str, Any]] = []
    for st in evap_stations_raw:
        if not isinstance(st, dict):
            continue
        sid = str(_pick(st, ["id", "stationId", "station_id", "stcd", "evapStaId", "evapStaSenid"], "")).strip()
        if sid in ("0", "-99"):
            sid = ""
        if not sid:
            continue
        evap_stations.append(
            {"id": sid, "weight": _to_float(_pick(st, ["weight", "w", "evapStaWeight"], 1.0), 1.0)}
        )

    evap_extract_type = str(
        _pick(unit, ["evapExtractType", "evap_extract_type"], _pick(evap_station_obj or {}, ["evapExtractType"], ""))
    ).lower()
    monthly_values = _normalize_monthly(
        _pick(
            unit,
            ["evapMonthlyValues", "monthlyEvap", "monthly_values", "evapMonthAvg"],
            _pick(evap_station_obj or {}, ["monthly_values"], None),
        )
    )
    if monthly_values is None:
        monthly_values = _parse_monthly_from_string(_pick(evap_station_obj or {}, ["evapArr"], None))

    if evap_stations or monthly_values is not None:
        pet_var: Dict[str, Any] = {
            "kind": "potential_evapotranspiration",
            "method": "weighted_average",
            "stations": evap_stations,
        }
        if evap_extract_type == "local":
            pet_var["use_station_pet"] = False
            if monthly_values is not None:
                pet_var["monthly_values"] = monthly_values
        else:
            pet_var["use_station_pet"] = True
            if monthly_values is not None:
                pet_var["monthly_values"] = monthly_values
        variables.append(pet_var)

    return variables


def _legacy_section_keys_for_time(time_type: str) -> List[str]:
    """
    旧系统常见根键：RSHour / RSDay / RSMinute 等，与 time_type 对齐时优先取对应数组。
    """
    t = str(time_type).strip().lower()
    common_tail = ["sections", "secList", "sectionList", "nodes"]
    if t == "hour":
        return ["RSHour", "RSDay", *common_tail]
    if t == "day":
        return ["RSDay", "RSHour", *common_tail]
    if t == "minute":
        return ["RSMinute", "RSHour", "RSDay", *common_tail]
    return ["RSHour", "RSDay", *common_tail]


def _extract_legacy_sections(legacy: Dict[str, Any], time_type: str) -> List[Any]:
    for key in _legacy_section_keys_for_time(time_type):
        raw = legacy.get(key)
        if isinstance(raw, list) and len(raw) > 0:
            return raw
    return []


def _scheme_payload_from_sections(
    sections: List[Any],
    *,
    time_type: str,
    step_size: int,
    warmup_steps: int,
    correction_steps: int,
    historical_steps: int,
    forecast_steps: int,
) -> Dict[str, Any]:
    """由旧版断面列表生成单个 `schemes[]` 元素（nodes/reaches/catchments/...）。"""
    if not isinstance(sections, list) or not sections:
        raise ValueError("sections 必须为非空列表")

    nodes: List[Dict[str, Any]] = []
    reaches: List[Dict[str, Any]] = []
    catchments: Dict[str, Dict[str, Any]] = {}
    forcing_bindings: Dict[str, Dict[str, Any]] = {}

    rain_station_seen: Dict[str, Dict[str, Any]] = {}
    pet_station_seen: Dict[str, Dict[str, Any]] = {}
    flow_station_seen: Dict[str, Dict[str, Any]] = {}
    stage_station_seen: Dict[str, Dict[str, Any]] = {}
    reservoir_catalog: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"node_id": "", "name": "", "stations": []})

    upstream_index: Dict[str, List[str]] = defaultdict(list)
    outgoing_index: Dict[str, List[str]] = defaultdict(list)

    for sec in sections:
        if not isinstance(sec, dict):
            continue
        secid = str(_pick(sec, ["secid", "secId", "SecId", "id"], "")).strip()
        if not secid:
            continue

        down_raw = _pick(sec, ["downSecId", "downsecid", "down_id"], "")
        down_id = "" if down_raw in (None, "", "null") else str(down_raw).strip()
        ds = _pick(sec, ["HFDataSource", "hfDataSource"], {})
        if not isinstance(ds, dict):
            ds = {}
        ds_name = str(_pick(ds, ["name"], "")).strip()
        node_type = _infer_node_type(sec, ds)
        station_binding = _build_station_binding(node_type, sec, ds)
        node_id = secid

        local_catchment_ids: List[str] = []
        units = _iter_units(sec)
        for u in units:
            unit_id = str(_pick(u, ["unitid", "unitId", "id"], "")).strip()
            if not unit_id:
                continue
            local_catchment_ids.append(unit_id)
            if unit_id not in catchments:
                catchments[unit_id] = {
                    "id": unit_id,
                    "name": _normalize_name(
                        _pick(u, ["unitName", "unit_name", "name"], None),
                        unit_id,
                    ),
                    # 正确规则：unit 的下游节点就是其所属 sec 本身
                    "downstream_node_id": secid,
                    "routing_model": _wrap_routing_model(_pick(u, ["unitCModel", "unitcmodel"], {})),
                    "runoff_model": _wrap_runoff_model(_pick(u, ["unitgmodel", "unitGModel", "runoffModel"], {})),
                }
            if unit_id not in forcing_bindings:
                forcing_bindings[unit_id] = {
                    "catchment_id": unit_id,
                    "variables": _build_forcing_vars_from_unit(u),
                }

            for v in forcing_bindings[unit_id]["variables"]:
                if v.get("kind") == "precipitation":
                    for st in v.get("stations", []):
                        sid = str(st.get("id", ""))
                        if not sid:
                            continue
                        display = str(st.get("name", "") or "").strip() or sid
                        if sid not in rain_station_seen:
                            rain_station_seen[sid] = {"id": sid, "name": display, "unit": "mm"}
                        else:
                            cur = str(rain_station_seen[sid].get("name", "") or "").strip()
                            if cur == sid and display != sid:
                                rain_station_seen[sid]["name"] = display
                if v.get("kind") == "potential_evapotranspiration":
                    for st in v.get("stations", []):
                        sid = str(st.get("id", ""))
                        if sid and sid not in pet_station_seen:
                            pet_station_seen[sid] = {"id": sid, "name": sid, "unit": "mm"}

        node = {
            "id": node_id,
            "name": _normalize_name(_pick(sec, ["secName", "name"], _pick(ds, ["name"], None)), node_id),
            "type": node_type,
            "incoming_reach_ids": [],  # 后续回填
            "outgoing_reach_ids": [],  # 后续回填
            "local_catchment_ids": sorted(set(local_catchment_ids)),
            "station_binding": station_binding,
            # 旧字段兼容：bHisCalcToPar 表示“是否用实测值演进”
            "use_observed_for_routing": bool(
                _pick(sec, ["bHisCalcToPar", "hisCalcToPar", "useObservedForRouting"], False)
            ),
        }
        if node_type == "reservoir":
            node["params"] = {
                "dispatch_model_alg_type": str(
                    _pick(sec, ["HFDispatchModelAlgType", "dispatchModelAlgType"], "Attenuation")
                ),
                "inflow_attenuation": 1.0,
            }

        nodes.append(node)

        # 测站目录（节点级）
        if node_type == "reservoir":
            in_sid = str(station_binding.get("inflow_station_id", "")).strip()
            out_sid = str(station_binding.get("outflow_station_id", "")).strip()
            lvl_sid = str(station_binding.get("stage_station_id", "")).strip()

            reservoir_item = reservoir_catalog[node_id]
            reservoir_item["node_id"] = node_id
            reservoir_item["name"] = node["name"]
            for sid, label in ((in_sid, "入库流量站"), (out_sid, "出库流量站"), (lvl_sid, "水位站")):
                if not sid:
                    continue
                existed = {x.get("id") for x in reservoir_item["stations"]}
                if sid not in existed:
                    if ds_name and label in ("入库流量站", "出库流量站", "水位站"):
                        st_nm = ds_name
                    else:
                        st_nm = f"{node_id}{label}"
                    reservoir_item["stations"].append({"id": sid, "name": st_nm})
        else:
            f_sid = str(station_binding.get("flow_station_id", "")).strip()
            z_sid = str(station_binding.get("stage_station_id", "")).strip()
            _register_flow_station_catalog(flow_station_seen, f_sid, ds_name=ds_name)
            _register_stage_station_catalog(stage_station_seen, z_sid, ds_name=ds_name)

        # 河道拓扑
        if down_id:
            first_unit = units[0] if units else {}
            reach_id = f"R_{secid}_TO_{down_id}"
            # 断面到下一个断面的河段演进参数，优先使用 secCModel（旧系统语义）
            sec_cmodel = _pick(sec, ["secCModel", "sec_c_model", "sectionCModel"], None)
            reaches.append(
                {
                    "id": reach_id,
                    "upstream_node_id": secid,
                    "downstream_node_id": down_id,
                    "routing_model": _wrap_routing_model(
                        _pick(
                            sec,
                            ["routingModel", "unitCModel", "unitcmodel"],
                            sec_cmodel if sec_cmodel is not None else _pick(first_unit, ["unitCModel"], {}),
                        )
                    ),
                }
            )
            outgoing_index[secid].append(reach_id)
            upstream_index[down_id].append(reach_id)

    node_map = {n["id"]: n for n in nodes}
    for nid, n in node_map.items():
        n["incoming_reach_ids"] = upstream_index.get(nid, [])
        n["outgoing_reach_ids"] = outgoing_index.get(nid, [])

    return {
        "time_type": time_type,
        "step_size": int(step_size),
        "time_axis": {
            "warmup_period_steps": int(warmup_steps),
            "correction_period_steps": int(correction_steps),
            "historical_display_period_steps": int(historical_steps),
            "forecast_period_steps": int(forecast_steps),
        },
        "nodes": nodes,
        "reaches": reaches,
        "catchments": list(catchments.values()),
        "stations": {
            "rain_gauges": list(rain_station_seen.values()),
            "evapotranspiration_stations": list(pet_station_seen.values()),
            "air_temperature_stations": [],
            "flow_stations": list(flow_station_seen.values()),
            "stage_stations": list(stage_station_seen.values()),
            "reservoir": list(reservoir_catalog.values()),
        },
        "catchment_forcing_bindings": list(forcing_bindings.values()),
    }


def convert_legacy_to_project_config(
    legacy: Any,
    *,
    time_type: str = "Hour",
    step_size: int = 1,
    warmup_steps: int = 0,
    correction_steps: int = 0,
    historical_steps: int = 0,
    forecast_steps: int = 24,
) -> Dict[str, Any]:
    """
    旧版 JSON → 新项目 `metadata` + `schemes[]`。

    若根对象为 dict 且 **同时** 存在非空的 `RSHour` 与 `RSDay`，则生成 **两个** scheme：
    - RSHour → time_type=Hour，step_size 使用参数 `step_size`
    - RSDay → time_type=Day，step_size=1

    否则行为与原先一致：只生成一个 scheme（按 time_type 选取 RSHour/RSDay 之一或 sections 等）。
    """
    legacy_meta: Dict[str, Any] = {}
    ta_kw = dict(
        warmup_steps=int(warmup_steps),
        correction_steps=int(correction_steps),
        historical_steps=int(historical_steps),
        forecast_steps=int(forecast_steps),
    )

    if isinstance(legacy, list):
        legacy_meta = {}
        schemes_out = [
            _scheme_payload_from_sections(
                legacy,
                time_type=time_type,
                step_size=int(step_size),
                **ta_kw,
            )
        ]
    elif isinstance(legacy, dict):
        legacy_meta = legacy
        hour = legacy.get("RSHour")
        day = legacy.get("RSDay")
        hour_ok = isinstance(hour, list) and len(hour) > 0
        day_ok = isinstance(day, list) and len(day) > 0

        if hour_ok and day_ok:
            schemes_out = [
                _scheme_payload_from_sections(
                    hour,
                    time_type="Hour",
                    step_size=int(step_size),
                    **ta_kw,
                ),
                _scheme_payload_from_sections(
                    day,
                    time_type="Day",
                    step_size=1,
                    **ta_kw,
                ),
            ]
        else:
            sections = _extract_legacy_sections(legacy, time_type)
            if not sections:
                sections = _pick(legacy, ["sections", "secList", "sectionList", "nodes"], [])
            if not isinstance(sections, list) or not sections:
                raise ValueError(
                    "Legacy config must contain a non-empty section list. "
                    "Supported keys include: RSHour, RSDay, RSMinute, sections, secList, sectionList, nodes "
                    "(for dict roots: if both RSHour and RSDay are non-empty, both are converted)."
                )
            schemes_out = [
                _scheme_payload_from_sections(
                    sections,
                    time_type=time_type,
                    step_size=int(step_size),
                    **ta_kw,
                )
            ]
    else:
        raise ValueError("Legacy config must be list or dict")

    desc = "Converted from legacy secid/downSecId config by convert_legacy_config.py"
    if isinstance(legacy, dict):
        h = legacy.get("RSHour")
        d = legacy.get("RSDay")
        if isinstance(h, list) and isinstance(d, list) and len(h) > 0 and len(d) > 0:
            desc += " Multi-scale: RSHour→Hour scheme, RSDay→Day scheme."

    return {
        "metadata": {
            "name": _normalize_name(_pick(legacy_meta, ["name", "projectName"], None), "converted_legacy_project"),
            "description": desc,
        },
        "schemes": schemes_out,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert legacy secid/downSecId config to hydro_project config"
    )
    parser.add_argument("--input", required=True, help="Path to legacy JSON config file")
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "configs" / "forecastSchemeConf.json"),
        help="Path to output JSON file (default: configs/forecastSchemeConf.json)",
    )
    parser.add_argument("--time-type", default="Hour", help="Minute/Hour/Day")
    parser.add_argument("--step-size", type=int, default=1, help="Scheme step size")
    parser.add_argument("--warmup-steps", type=int, default=0)
    parser.add_argument("--correction-steps", type=int, default=0)
    parser.add_argument("--historical-steps", type=int, default=0)
    parser.add_argument("--forecast-steps", type=int, default=24)

    args = parser.parse_args()
    in_path = Path(args.input)
    out_path = Path(args.output)

    legacy = json.loads(in_path.read_text(encoding="utf-8"))
    converted = convert_legacy_to_project_config(
        legacy,
        time_type=args.time_type,
        step_size=args.step_size,
        warmup_steps=args.warmup_steps,
        correction_steps=args.correction_steps,
        historical_steps=args.historical_steps,
        forecast_steps=args.forecast_steps,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(converted, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[ok] converted: {in_path} -> {out_path}")


if __name__ == "__main__":
    main()

