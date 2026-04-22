"""
Application-level data builder helpers.

把原先 `scripts/calculation_app_common.py` 里的“df -> ForcingData/TimeSeries”等拼装逻辑下沉到引擎层，
避免 `hydro_engine` 依赖 `scripts`。
"""

from __future__ import annotations

from datetime import datetime
import math
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd

from hydro_engine.core.forcing import ForcingData, ForcingKind, parse_forcing_kind
from hydro_engine.core.timeseries import TimeSeries


def extract_station_series(
    df: pd.DataFrame,
    station_id: str,
    times: pd.DatetimeIndex,
    *,
    value_col: str,
    fill_mode: str,
) -> List[float]:
    if value_col not in df.columns:
        raise ValueError(f"CSV missing value column '{value_col}'")

    sub = df[df["SENID"] == str(station_id)].copy()
    if sub.empty:
        if fill_mode == "zero":
            return [0.0] * len(times)
        raise ValueError(f"Station {station_id} not found in CSV")

    sub[value_col] = pd.to_numeric(sub[value_col], errors="coerce")
    sub = sub.dropna(subset=["TIME_DT"]).sort_values("TIME_DT")
    if sub.empty:
        if fill_mode == "zero":
            return [0.0] * len(times)
        raise ValueError(f"Station {station_id} has no valid time rows")

    s = sub.groupby("TIME_DT")[value_col].mean().sort_index()
    s = s.reindex(times)

    if fill_mode == "zero":
        s = s.fillna(0.0)
    elif fill_mode == "interp":
        s = s.interpolate(method="time").ffill().bfill().fillna(0.0)
    else:
        raise ValueError(f"Unsupported fill_mode: {fill_mode}")

    return [float(x) for x in s.to_list()]


def extract_station_series_keep_nan(
    df: pd.DataFrame,
    station_id: str,
    times: pd.DatetimeIndex,
    *,
    value_col: str,
) -> List[float]:
    """
    从 df 中提取某站点的时间序列，但不对缺测做填补。

    - df 中找不到该 SENID 或者缺少 TIME_DT：返回全 NaN
    - 结果长度与 times 等长，缺测点为 NaN
    """
    if value_col not in df.columns:
        raise ValueError(f"CSV missing value column '{value_col}'")
    if "TIME_DT" not in df.columns:
        raise ValueError("CSV missing TIME_DT column")

    sub = df[df["SENID"] == str(station_id)].copy()
    if sub.empty:
        return [float("nan")] * len(times)

    sub[value_col] = pd.to_numeric(sub[value_col], errors="coerce")
    sub = sub.dropna(subset=["TIME_DT"]).sort_values("TIME_DT")
    if sub.empty:
        return [float("nan")] * len(times)

    s = sub.groupby("TIME_DT")[value_col].mean().sort_index()
    s = s.reindex(times)
    # 保留 NaN，后续用于“按优先级兜底融合”
    return [float(x) if not (isinstance(x, float) and math.isnan(x)) else float("nan") for x in s.to_list()]


def apply_catchment_forecast_fusion_to_station_packages(
    *,
    station_packages: Dict[str, ForcingData],
    fusion_plan: Dict[str, Any],
    rain_df: pd.DataFrame,
    times: pd.DatetimeIndex,
    start_time: datetime,
    time_step,
) -> Dict[str, ForcingData]:
    """
    对 `catchment_forecast_rules` 生成的“融合虚拟 station_id”做按优先级的逐时兜底融合。

    fusion_plan 结构：
    - fusion_plan["virtual_bindings"][virtual_station_id] = {
        "kind": ForcingKind, "source_ids": [真实 subtype source_id...]
      }
    """
    if not fusion_plan:
        return station_packages
    vb = fusion_plan.get("virtual_bindings") or {}
    if not vb:
        return station_packages

    updated = dict(station_packages)
    for virtual_id, entry in vb.items():
        kind = entry.get("kind")
        if kind is None:
            continue
        source_ids = entry.get("source_ids") or []
        if not source_ids:
            continue

        # 对每个时间步：取第一个非 NaN 的优先级源值
        source_series: List[List[float]] = []
        for sid in source_ids:
            source_series.append(
                extract_station_series_keep_nan(rain_df, sid, times, value_col="V")
            )

        fused_values: List[float] = [float("nan")] * len(times)
        for i in range(len(times)):
            chosen = float("nan")
            for src in source_series:
                v = src[i]
                if not (isinstance(v, float) and math.isnan(v)):
                    chosen = float(v)
                    break
            fused_values[i] = chosen

        fused_ts = TimeSeries(start_time=start_time, time_step=time_step, values=fused_values)

        existing = updated.get(virtual_id)
        if existing is None:
            updated[virtual_id] = ForcingData.single(kind, fused_ts)
        else:
            updated[virtual_id] = existing.with_series(kind, fused_ts)

    return updated


def iter_variable_specs(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    if spec.get("variables") is not None:
        return list(spec["variables"])
    return []


def build_station_packages(
    binding_specs: List[Dict[str, Any]],
    rain_df: pd.DataFrame,
    times: pd.DatetimeIndex,
    start_time: datetime,
    time_step,
) -> Tuple[Dict[str, ForcingData], List[str]]:
    """
    Build station forcing packages from raw station table (rain_df).
    """
    station_kind_values: Dict[str, Dict[ForcingKind, List[float]]] = {}
    warnings: List[str] = []

    for spec in binding_specs:
        for var in iter_variable_specs(spec):
            kind = parse_forcing_kind(str(var.get("kind") or var.get("forcing_kind")))

            use_station = True
            if kind is ForcingKind.POTENTIAL_EVAPOTRANSPIRATION:
                use_station = bool(var.get("use_station_pet", True))
            if not use_station:
                continue

            stations = var.get("stations") or []
            for st_item in stations:
                sid = str(st_item.get("id") or st_item.get("station_id") or "").strip()
                if not sid:
                    continue

                value_col = "V" if kind is ForcingKind.PRECIPITATION else "V"
                try:
                    values = extract_station_series(
                        rain_df,
                        sid,
                        times,
                        value_col=value_col,
                        fill_mode="zero",
                    )
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"站点 {sid} 读取失败（{kind.value}）: {exc}")
                    values = [0.0] * len(times)

                station_kind_values.setdefault(sid, {})[kind] = values

    station_packages: Dict[str, ForcingData] = {}
    for sid, kv in station_kind_values.items():
        pairs = [
            (k, TimeSeries(start_time=start_time, time_step=time_step, values=v))
            for k, v in kv.items()
        ]
        station_packages[sid] = ForcingData.from_pairs(pairs)

    return station_packages, warnings


def build_observed_flows(
    scheme: Any,
    flow_df: pd.DataFrame,
    times: pd.DatetimeIndex,
    start_time: datetime,
    time_step,
) -> Tuple[Dict[str, TimeSeries], List[str]]:
    station_ids = set()
    for node in scheme.nodes.values():
        for sid in (
            getattr(node, "observed_station_id", ""),
            getattr(node, "observed_inflow_station_id", ""),
        ):
            if str(sid).strip():
                station_ids.add(str(sid).strip())

    out: Dict[str, TimeSeries] = {}
    warnings: List[str] = []

    for sid in sorted(station_ids):
        try:
            vals = extract_station_series(
                flow_df,
                sid,
                times,
                value_col="AVGV",
                fill_mode="interp",
            )
            out[sid] = TimeSeries(start_time=start_time, time_step=time_step, values=vals)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"流量站 {sid} 读取失败: {exc}")

    return out, warnings


def build_catchment_precip_series(
    binding_specs: List[Dict[str, Any]],
    rain_df: pd.DataFrame,
    times: pd.DatetimeIndex,
) -> Tuple[Dict[str, List[float]], List[str]]:
    """
    Build catchment areal precipitation from weighted station bindings.
    """
    out: Dict[str, List[float]] = {}
    warnings: List[str] = []

    for spec in binding_specs:
        catchment_id = str(spec.get("catchment_id") or "").strip()
        if not catchment_id:
            continue

        precip_var = None
        for var in iter_variable_specs(spec):
            kind = parse_forcing_kind(str(var.get("kind") or var.get("forcing_kind")))
            if kind is ForcingKind.PRECIPITATION:
                precip_var = var
                break
        if precip_var is None:
            continue

        stations = precip_var.get("stations") or []
        tl = len(times)
        out[catchment_id] = _weighted_catchment_precip_from_bindings(
            catchment_id=catchment_id,
            stations=stations,
            times_len=tl,
            get_station_precip_series=lambda sid: extract_station_series(
                rain_df,
                sid,
                times,
                value_col="V",
                fill_mode="zero",
            ),
            warnings=warnings,
        )

    return out, warnings


def _precip_values_from_station_package(
    station_packages: Dict[str, ForcingData],
    station_id: str,
    times_len: int,
) -> List[float]:
    pkg = station_packages.get(station_id)
    if pkg is None:
        return [0.0] * times_len
    ts = pkg.get(ForcingKind.PRECIPITATION)
    if ts is None:
        return [0.0] * times_len
    if ts.values.ndim != 1:
        raise ValueError("station package precipitation must be 1-D for catchment precip extraction")
    vals = [float(x) for x in ts.values.tolist()]
    if len(vals) != times_len:
        # 与网格约定不一致时兜底为占位零，避免前端崩溃
        if len(vals) < times_len:
            vals = vals + [0.0] * (times_len - len(vals))
        else:
            vals = vals[:times_len]
    return vals


def _weighted_catchment_precip_from_bindings(
    *,
    catchment_id: str,
    stations: list,
    times_len: int,
    get_station_precip_series: Callable[[str], List[float]],
    warnings: List[str],
) -> List[float]:
    """
    按 bindings 中降水站的权重聚合面雨量（核心逻辑，供 CSV 路径与 station_packages 路径共用）。
    """
    if not stations:
        return [0.0] * times_len

    weighted_sum = [0.0] * times_len
    weight_total = 0.0
    valid_station_count = 0

    for st_item in stations:
        sid = str(st_item.get("id") or st_item.get("station_id") or "").strip()
        if not sid:
            continue
        try:
            series = get_station_precip_series(sid)
            w = float(st_item.get("weight", 1.0))
            valid_station_count += 1
            if w > 0:
                weight_total += w
                for i, v in enumerate(series):
                    weighted_sum[i] += w * v
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"流域 {catchment_id} 雨量站 {sid} 读取失败: {exc}")

    if weight_total > 0:
        return [v / weight_total for v in weighted_sum]
    if valid_station_count > 0:
        eq_sum = [0.0] * times_len
        count = 0
        for st_item in stations:
            sid = str(st_item.get("id") or st_item.get("station_id") or "").strip()
            if not sid:
                continue
            try:
                series = get_station_precip_series(sid)
                count += 1
                for i, v in enumerate(series):
                    eq_sum[i] += v
            except Exception:
                pass
        return [v / max(count, 1) for v in eq_sum]
    return [0.0] * times_len


def build_catchment_precip_series_from_station_packages(
    binding_specs: List[Dict[str, Any]],
    station_packages: Dict[str, ForcingData],
    times_len: int,
) -> Tuple[Dict[str, List[float]], List[str]]:
    """
    与 `build_catchment_precip_series` 相同权重，但用已组装的 station_packages（可与引擎输入一致）。
    用于实时预报在 T0 起清空实测气象后，面雨量仍与测站序列一致。
    """
    out: Dict[str, List[float]] = {}
    warnings: List[str] = []

    for spec in binding_specs:
        catchment_id = str(spec.get("catchment_id") or "").strip()
        if not catchment_id:
            continue

        precip_var = None
        for var in iter_variable_specs(spec):
            kind = parse_forcing_kind(str(var.get("kind") or var.get("forcing_kind")))
            if kind is ForcingKind.PRECIPITATION:
                precip_var = var
                break
        if precip_var is None:
            continue

        stations = precip_var.get("stations") or []
        out[catchment_id] = _weighted_catchment_precip_from_bindings(
            catchment_id=catchment_id,
            stations=stations,
            times_len=times_len,
            get_station_precip_series=lambda sid: _precip_values_from_station_package(
                station_packages, sid, times_len
            ),
            warnings=warnings,
        )

    return out, warnings


def _guess_catchment_area(catchment_obj: Any) -> float:
    runoff_model = getattr(catchment_obj, "runoff_model", None)
    if runoff_model is None:
        return 1.0
    for attr in ("area",):
        v = getattr(runoff_model, attr, None)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except Exception:
                pass
    params = getattr(runoff_model, "params", None)
    if isinstance(params, dict):
        try:
            fv = float(params.get("area", 1.0))
            if fv > 0:
                return fv
        except Exception:
            pass
    return 1.0


def build_node_precip_series(
    scheme: Any,
    catchment_precip: Dict[str, List[float]],
) -> Dict[str, List[float]]:
    """
    Aggregate catchment precipitation to node precipitation (area-weighted).
    """
    out: Dict[str, List[float]] = {}
    for node_id, node in scheme.nodes.items():
        cids = list(getattr(node, "local_catchment_ids", []) or [])
        if not cids:
            continue

        length = 0
        for cid in cids:
            if cid in catchment_precip:
                length = len(catchment_precip[cid])
                break
        if length == 0:
            continue

        acc = [0.0] * length
        area_total = 0.0
        for cid in cids:
            if cid not in catchment_precip:
                continue
            catchment_obj = scheme.catchments.get(cid)
            area = _guess_catchment_area(catchment_obj)
            area_total += area
            vals = catchment_precip[cid]
            for i, v in enumerate(vals):
                acc[i] += area * v

        if area_total > 0:
            out[str(node_id)] = [v / area_total for v in acc]
        else:
            out[str(node_id)] = [0.0] * length

    return out


def build_node_observed_flow_series(
    scheme: Any,
    observed_flows: Dict[str, TimeSeries],
) -> Dict[str, List[float]]:
    out: Dict[str, List[float]] = {}
    for node_id, node in scheme.nodes.items():
        sid_candidates = [
            str(getattr(node, "observed_inflow_station_id", "")).strip(),
            str(getattr(node, "observed_station_id", "")).strip(),
            str(getattr(node, "inflow_station_id", "")).strip(),
        ]
        sid = next((x for x in sid_candidates if x), "")
        if sid and sid in observed_flows:
            ts = observed_flows[sid]
            if ts.values.ndim != 1:
                raise ValueError("build_node_observed_flow_series requires 1-D observed flow series")
            out[str(node_id)] = ts.values.tolist()
    return out


def build_catchment_observed_flow_series(
    scheme: Any,
    node_observed: Dict[str, List[float]],
) -> Dict[str, List[float]]:
    """Map catchment observed flow to its downstream node observed inflow."""
    out: Dict[str, List[float]] = {}
    for cid, catchment in scheme.catchments.items():
        dnid = str(getattr(catchment, "downstream_node_id", "")).strip()
        if dnid and dnid in node_observed:
            out[str(cid)] = list(node_observed[dnid])
    return out

