"""
Web/Service 计算入口（通用计算 pipeline runner）。

职责：
1) 读数（rain/flow）到标准化结构
2) 把读到的数据构造成引擎需要的 station_packages / observed_flows
3) 组装 web/桌面共享所需的 aux 字段（用于图表与测站查看）
4) 可选执行计算（compute_forecast=True/False）
5) 提供内存缓存复用（给 Streamlit 的“读取数据”和“预报计算”分离使用）

注意：
- 这里不做“UI 渲染”，仅提供计算与数据准备函数。
- 需要与 `scripts/web_calculation_app.py` 的调用签名完全对齐。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from hydro_engine.core.forcing import ForcingKind
from hydro_engine.core.context import parse_time_type, TimeType
from hydro_engine.io.calculation_app_data_builder import (
    apply_catchment_forecast_fusion_to_station_packages,
    build_catchment_observed_flow_series,
    build_catchment_precip_series_from_station_packages,
    build_node_observed_flow_series,
    build_node_precip_series,
    build_observed_flows,
    build_station_packages,
)
from hydro_engine.io.calculation_app_data_loader import (
    build_times,
    clip_station_dataframe_rows_before_forecast_start,
    collect_all_station_ids_for_calculation,
    collect_observed_flow_station_ids,
    collect_rain_station_ids,
    load_rain_flow_for_calculation,
    read_config,
    read_jdbc_daydb_normalize_time_to_midnight_from_path,
    station_observation_query_end_realtime,
)
from hydro_engine.io.calculation_app_data_processors import (
    apply_loaded_data_processors,
    standardize_loaded_inputs,
)
from hydro_engine.forecast.catchment_forecast_rainfall import CatchmentForecastRainfall
from hydro_engine.forecast.scenario_forcing import load_catchment_forecast_rainfall_map_from_csv
from hydro_engine.io.json_config import (
    apply_realtime_forecast_observed_meteorology_cutoff,
    flatten_stations_catalog,
    load_scheme_from_json,
    run_calculation_from_json,
)

# 从 scripts 层复用：把 UI 传入的四段步数写入临时 config（避免修改真实配置）
from calculation_app_common import write_temp_config_with_periods


OnLog = Optional[Callable[[str], None]]


def _is_unified_db_station_source(jdbc_config_path: str, rain_csv: str, flow_csv: str) -> bool:
    """雨量/流量/气温等是否来自同一库表（可一次 IN 查询）。"""
    jc = str(jdbc_config_path or "").strip()
    if jc and Path(jc).is_file():
        return True
    rp, fp = str(rain_csv or "").strip(), str(flow_csv or "").strip()
    return bool(rp and rp == fp and Path(rp).suffix.lower() == ".json")


def _read_station_catalog_names(config_path: str, time_type: str, step_size: int) -> Dict[str, str]:
    """
    从与当前 time_type / step_size 匹配的方案中读取 stations 目录，得到测站 id -> 配置名称。
    无名称或未配置 stations 时不在字典中出现（由 UI 仅展示 id）。
    """
    try:
        data = read_config(config_path)
    except Exception:
        return {}
    schemes = data.get("schemes")
    if not isinstance(schemes, list):
        return {}
    tt = str(time_type).strip()
    sz = int(step_size)
    target: Optional[Dict[str, Any]] = None
    for s in schemes:
        if not isinstance(s, dict):
            continue
        if str(s.get("time_type", "")).strip() == tt and int(s.get("step_size", -999999)) == sz:
            target = s
            break
    if not isinstance(target, dict):
        return {}
    raw = target.get("stations")
    if raw is None:
        return {}
    try:
        flat = flatten_stations_catalog(raw)
    except Exception:
        return {}
    out: Dict[str, str] = {}
    for e in flat:
        sid = str(e.get("id", "")).strip()
        if not sid:
            continue
        nm = str(e.get("name", "") or "").strip()
        if nm:
            out[sid] = nm
    return out


def _read_catchment_catalog_names(config_path: str, time_type: str, step_size: int) -> Dict[str, str]:
    """从当前方案 catchments[] 读取子流域 id -> name（用于 Web 展示）。"""
    try:
        data = read_config(config_path)
    except Exception:
        return {}
    schemes = data.get("schemes")
    if not isinstance(schemes, list):
        return {}
    tt = str(time_type).strip()
    sz = int(step_size)
    target: Optional[Dict[str, Any]] = None
    for s in schemes:
        if not isinstance(s, dict):
            continue
        if str(s.get("time_type", "")).strip() == tt and int(s.get("step_size", -999999)) == sz:
            target = s
            break
    if not isinstance(target, dict):
        return {}
    out: Dict[str, str] = {}
    for c in target.get("catchments") or []:
        if not isinstance(c, dict):
            continue
        cid = str(c.get("id", "")).strip()
        if not cid:
            continue
        nm = str(c.get("name", "") or "").strip()
        if nm:
            out[cid] = nm
    return out


def _read_scheme_dbtype(config_path: str, time_type: str, step_size: int) -> int:
    """
    读取当前方案的前后时标配置：
    -1: 前时标（默认）
     0: 后时标

    兼容：若配置缺失/异常，回退为 -1。
    """
    try:
        data = read_config(config_path)
    except Exception:
        return -1
    schemes = data.get("schemes")
    if not isinstance(schemes, list):
        return -1
    tt = str(time_type).strip()
    sz = int(step_size)
    for s in schemes:
        if not isinstance(s, dict):
            continue
        if str(s.get("time_type", "")).strip() != tt:
            continue
        if int(s.get("step_size", -999999)) != sz:
            continue
        raw = s.get("dbtype", -1)
        try:
            return int(raw)
        except Exception:
            return -1
    return -1


def _log(msg: str, on_log: OnLog) -> None:
    if on_log is None:
        return
    try:
        on_log(msg)
    except Exception:
        # on_log 绝对不能影响主流程
        pass


def _parse_warmup_start(warmup_start: Any) -> datetime:
    # 兼容 "2025-09-01 00:00:00" / ISO 字符串 / datetime
    if isinstance(warmup_start, datetime):
        return warmup_start
    return pd.to_datetime(warmup_start).to_pydatetime()


def _load_catchment_scenario_rain_map(
    csv_path: str,
    default_catchment_ids: Optional[List[str]],
    *,
    on_log: OnLog,
) -> Dict[str, CatchmentForecastRainfall]:
    p = str(csv_path or "").strip()
    if not p:
        return {}
    try:
        return load_catchment_forecast_rainfall_map_from_csv(
            p,
            default_catchment_ids=default_catchment_ids,
        )
    except Exception as exc:  # noqa: BLE001
        _log(f"[forecast_scenario_rain] load failed: {exc}", on_log)
        return {}


def _make_timedelta_for_time_type(time_type: str, step_size: int) -> timedelta:
    """
    根据 time_type + step_size 得到引擎原生时间步长。
    注意：这里是“原生粒度”，不是换算到小时/天。
    """
    tt = parse_time_type(time_type)
    s = int(step_size)
    if tt is TimeType.MINUTE:
        return timedelta(minutes=s)
    if tt is TimeType.HOUR:
        return timedelta(hours=s)
    if tt is TimeType.DAY:
        return timedelta(days=s)
    # parse_time_type 理论上已覆盖，这里兜底
    return timedelta(seconds=s)


def _resolve_actual_forecast_start(
    forecast_start_time_input: datetime,
    *,
    time_delta: timedelta,
    dbtype: int,
) -> datetime:
    """
    前后时标下的实际预报起点：
    - dbtype == -1（前时标）：不平移
    - 其余（后时标）：向后平移 1 个原生步长
    """
    if int(dbtype) == -1:
        return forecast_start_time_input
    return forecast_start_time_input + time_delta


def _shift_station_df_time_label_for_dbtype(
    df: pd.DataFrame,
    *,
    time_delta: timedelta,
    dbtype: int,
) -> pd.DataFrame:
    """
    前时标下将测站表时间标签统一回拨 1 个步长。

    语义对齐 Java 版：dbtype=-1 时，库中落在“时段末”的值展示为“时段起”标签。
    """
    if df is None or df.empty:
        return df
    if int(dbtype) != -1:
        return df
    out = df.copy()
    for col in ("TIME_DT", "TIME"):
        if col not in out.columns:
            continue
        ts = pd.to_datetime(out[col], errors="coerce")
        out[col] = ts - time_delta
    return out


def _infer_debug_table_columns(rows: List[Dict[str, Any]], *, max_cols: int = 40) -> List[str]:
    """
    为 debug_trace 的 table 推断列集合：
    - 保持“首次出现顺序”以获得稳定展示
    - 对极端大字段数做截断保护
    """
    seen: Dict[str, None] = {}
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        for k in r.keys():
            ks = str(k)
            if ks not in seen:
                seen[ks] = None
                if len(seen) >= max_cols:
                    return list(seen.keys())
    return list(seen.keys())


def _compute_forecast_start_idx(time_context: Any) -> int:
    # ForecastTimeContext: (forecast_start - warmup_start) / time_delta
    td = time_context.time_delta
    delta = time_context.forecast_start_time - time_context.warmup_start_time
    # timedelta 除法在 python 中返回 float，这里用 int 安全下取整（网格约束应保证整除）
    return int(delta / td)


def _build_station_series_maps(
    *,
    station_packages: Dict[str, Any],
) -> Tuple[Dict[str, List[float]], Dict[str, List[float]], Dict[str, List[float]]]:
    station_precip: Dict[str, List[float]] = {}
    station_pet: Dict[str, List[float]] = {}
    station_temp: Dict[str, List[float]] = {}
    for sid, pkg in (station_packages or {}).items():
        # 对应 web：雨量站 / 蒸发站 / 气温站
        if pkg is None:
            continue
        for kind, target in (
            (ForcingKind.PRECIPITATION, station_precip),
            (ForcingKind.POTENTIAL_EVAPOTRANSPIRATION, station_pet),
            (ForcingKind.AIR_TEMPERATURE, station_temp),
        ):
            ts = pkg.get(kind)
            if ts is None:
                continue
            target[str(sid)] = [float(x) for x in ts.values]
    return station_precip, station_pet, station_temp


def _apply_daydb_rain_fallback_and_diagnostics(
    *,
    on_log: OnLog,
    time_type: str,
    rain_df: Any,
    rain_senids: List[str],
) -> Any:
    """
    Day 方案（DayDB）读取诊断与最小回填：
    - 若检测到 rain_df.V 几乎全为 NaN，而 rain_df.AVGV 有有效值，则做 V <- AVGV
    - 并输出 rain_df 读取诊断日志（用于你之前的定位）
    """
    try:
        if str(time_type).strip().lower() == "day" and isinstance(rain_df, pd.DataFrame):
            # 额外诊断：确认最终用到的 V/AVGV 列是否各自有数据
            try:
                v_notna = int(rain_df["V"].notna().sum()) if "V" in rain_df.columns else None
                avgv_notna = int(rain_df["AVGV"].notna().sum()) if "AVGV" in rain_df.columns else None
                _log(
                    f"[data][day][diag_cols] V_notna={v_notna} AVGV_notna={avgv_notna}",
                    on_log,
                )
            except Exception:
                pass
            # DAYDB 部分库表中，降雨/蒸发的真实值列可能与 HOURDB 不完全一致。
            # 若检测到 rain_df 的 V 列几乎全为 NaN，而 AVGV 列有有效值，则做最小回填，避免“读不到数据（全空/全 NaN）”。
            if "V" in rain_df.columns and "AVGV" in rain_df.columns:
                v_all_nan = bool(rain_df["V"].isna().all())
                avgv_has_data = bool(rain_df["AVGV"].notna().any())
                _log(
                    f"[data][day][diag_fallback_check] V_all_nan={v_all_nan} AVGV_has_data={avgv_has_data}",
                    on_log,
                )

                # 进一步诊断：如果真的执行 V <- AVGV，
                # 哪些“单站”会从中受益（V 在该站全缺测，而 AVGV 有数据）。
                # 注意：当前兜底是“全局条件”，因此这里是“可能受益站点”的清单。
                try:
                    candidate_sids: List[str] = []
                    v_missing_cnt = 0
                    avgv_present_cnt = 0
                    if "SENID" in rain_df.columns:
                        for sid in list(rain_senids or []):
                            ssid = str(sid)
                            sub = rain_df[rain_df["SENID"].astype(str) == ssid]
                            if sub.empty:
                                continue
                            v_notna = int(sub["V"].notna().sum()) if "V" in sub.columns else 0
                            avgv_notna = (
                                int(sub["AVGV"].notna().sum()) if "AVGV" in sub.columns else 0
                            )
                            if v_notna == 0:
                                v_missing_cnt += 1
                            if avgv_notna > 0:
                                avgv_present_cnt += 1
                            if v_notna == 0 and avgv_notna > 0:
                                candidate_sids.append(ssid)
                    _log(
                        "[data][day][diag_fallback_station_candidates] "
                        f"v_missing_cnt={v_missing_cnt} avgv_present_cnt={avgv_present_cnt} "
                        f"candidate_sids_count={len(candidate_sids)} examples={candidate_sids[:10]}",
                        on_log,
                    )
                except Exception:
                    pass
                if v_all_nan and avgv_has_data:
                    _log("[data][daydb] rain_df.V 全为 NaN，自动回填：V <- AVGV", on_log)
                    rain_df = rain_df.copy()
                    rain_df["V"] = rain_df["AVGV"]
    except Exception:
        # 回填检测失败不影响主流程
        pass

    try:
        if (
            str(time_type).strip().lower() == "day"
            and isinstance(rain_df, pd.DataFrame)
            and not rain_df.empty
        ):
            senid_unique = 0
            v_notna: Optional[int] = None
            avgv_notna: Optional[int] = None
            time_min = None
            time_max = None
            if "SENID" in rain_df.columns:
                senid_unique = int(rain_df["SENID"].nunique())
            if "V" in rain_df.columns:
                v_notna = int(rain_df["V"].notna().sum())
            if "AVGV" in rain_df.columns:
                avgv_notna = int(rain_df["AVGV"].notna().sum())
            if "TIME_DT" in rain_df.columns:
                time_min = rain_df["TIME_DT"].min()
                time_max = rain_df["TIME_DT"].max()
            _log(
                f"[data][day][diag] rain_rows={len(rain_df)}, senids={senid_unique}, V_notna={v_notna}, "
                f"AVGV_notna={avgv_notna}, "
                f"TIME_DT_min={time_min}, TIME_DT_max={time_max}, "
                f"rain_senids_req={len(rain_senids)}",
                on_log,
            )
    except Exception:
        pass

    return rain_df


def run_calculation_pipeline(
    *,
    config_path: str,
    jdbc_config_path: str,
    rain_csv: str,
    flow_csv: str,
    warmup_start: str,
    forecast_mode: str,
    catchment_workers: Optional[int],
    time_type: str,
    step_size: int,
    warmup_steps: int,
    correction_steps: int,
    historical_steps: int,
    forecast_steps: int,
    compute_forecast: bool,
    forecast_scenario_rain_csv: str = "",
    forecast_scenario_default_catchment_ids: Optional[List[str]] = None,
    forecast_scenario_precipitation: str = "expected",
    forecast_run_multiscenario: bool = False,
    on_log: OnLog = None,
) -> Tuple[Dict[str, Any], pd.DatetimeIndex, List[str], Dict[str, Any]]:
    """
    Streamlit/桌面共享入口：
    - compute_forecast=False：仅读取 + 组装 station/observed/catchment_rain 等供查看
    - compute_forecast=True：在已组装好的输入基础上执行 CalculationEngine.run
    """
    _log("[ui] pipeline start", on_log)

    # UI 输入字段语义：`预报起报时间`
    # 这里把它当作 forecast_start_time（预报起点），再往回推 warmup_start_time，
    # 以保证雨量站/测站序列的“读数时间轴”与用户选择的预报起点一致。
    forecast_start_time_input = _parse_warmup_start(warmup_start)
    td = _make_timedelta_for_time_type(str(time_type), int(step_size))
    dbtype = _read_scheme_dbtype(
        config_path=str(config_path),
        time_type=str(time_type),
        step_size=int(step_size),
    )
    forecast_start_time = _resolve_actual_forecast_start(
        forecast_start_time_input,
        time_delta=td,
        dbtype=dbtype,
    )
    _log(
        "[time] dbtype "
        f"dbtype={dbtype} "
        f"input_forecast_start={forecast_start_time_input.isoformat()} "
        f"actual_forecast_start={forecast_start_time.isoformat()}",
        on_log,
    )
    # 嵌套时间轴：预热总长 W 自 T0 向历史回溯；总预热步数不再与 C、H 相加。
    pre_steps = int(warmup_steps)
    t0 = forecast_start_time - td * pre_steps  # warmup_start_time

    # 把 UI 的四段步数写入临时 config，让引擎使用与你选择一致的 time_axis
    config_used_path = write_temp_config_with_periods(
        config_path,
        time_type=str(time_type),
        step_size=int(step_size),
        warmup_steps=int(warmup_steps),
        correction_steps=int(correction_steps),
        historical_steps=int(historical_steps),
        forecast_steps=int(forecast_steps),
    )
    _log(f"[cfg] temp config={config_used_path}", on_log)

    # 1) 加载 scheme + time_context（由 temp config 决定四段步数）
    scheme, binding_specs, time_context = load_scheme_from_json(
        file_path=config_used_path,
        time_type=str(time_type),
        step_size=int(step_size),
        warmup_start_time=t0,
    )
    _log(
        "[time] resolved "
        f"warmup_start={time_context.warmup_start_time.isoformat()} "
        f"forecast_start={time_context.forecast_start_time.isoformat()} "
        f"display_start={time_context.display_start_time.isoformat()} "
        f"end_time={time_context.end_time.isoformat()}",
        on_log,
    )
    times = build_times(
        context_start=time_context.warmup_start_time,
        step=time_context.time_delta,
        count=int(time_context.step_count),
    )

    # 2) 读数：收集测站 id（雨/PET/气温绑定 + 节点流量/入流）
    rain_senids = sorted(collect_rain_station_ids(binding_specs))
    flow_senids = sorted(collect_observed_flow_station_ids(scheme))
    all_station_senids = collect_all_station_ids_for_calculation(binding_specs, scheme)
    unified_for_load: Optional[List[str]] = None
    if _is_unified_db_station_source(str(jdbc_config_path or ""), str(rain_csv or ""), str(flow_csv or "")):
        unified_for_load = all_station_senids
    _log(
        f"[data] station_senids binding_meteo={len(rain_senids)} flow_nodes={len(flow_senids)} "
        f"union_all={len(all_station_senids)} unified_query={bool(unified_for_load)}",
        on_log,
    )

    read_time_start = time_context.warmup_start_time
    read_time_end = time_context.end_time
    station_obs_end = None
    if str(forecast_mode).strip().lower() == "realtime_forecast":
        station_obs_end = station_observation_query_end_realtime(time_context)
    if int(dbtype) == -1:
        # 前时标：读数窗口整体前移 1 步（读取“后置标签”数据），随后再统一回拨标签。
        # 这可避免末端少一条导致 interp 复制前值。
        read_time_start = read_time_start + time_context.time_delta
        read_time_end = read_time_end + time_context.time_delta
        if station_obs_end is not None:
            station_obs_end = station_obs_end + time_context.time_delta
    if station_obs_end is not None:
        _log(
            "[data] realtime_forecast: station table t_end capped at "
            f"{station_obs_end.isoformat()} (all types: rain/flow/temp)",
            on_log,
        )
    _log(
        "[data] station read window "
        f"start={read_time_start.isoformat()} end={read_time_end.isoformat()} dbtype={dbtype}",
        on_log,
    )

    # 3) 读取测站表（单库源：一次 IN；双 CSV：各一次，共用 query_end）
    rain_df, flow_df, jdbc_warns = load_rain_flow_for_calculation(
        jdbc_config_path=str(jdbc_config_path or ""),
        rain_csv=str(rain_csv or ""),
        flow_csv=str(flow_csv or ""),
        time_start=read_time_start,
        time_end=read_time_end,
        rain_senids=rain_senids,
        flow_senids=flow_senids,
        time_type=str(time_type),
        station_table_query_end=station_obs_end,
        unified_station_senids=unified_for_load,
    )
    # 前时标：测站数据时间标签回拨 1 步；后时标保持原样。
    # 单库源路径下 rain_df/flow_df 可能是同一对象，避免重复回拨。
    if flow_df is rain_df:
        shifted = _shift_station_df_time_label_for_dbtype(
            rain_df,
            time_delta=time_context.time_delta,
            dbtype=dbtype,
        )
        rain_df = shifted
        flow_df = shifted
    else:
        rain_df = _shift_station_df_time_label_for_dbtype(
            rain_df,
            time_delta=time_context.time_delta,
            dbtype=dbtype,
        )
        flow_df = _shift_station_df_time_label_for_dbtype(
            flow_df,
            time_delta=time_context.time_delta,
            dbtype=dbtype,
        )
    if int(dbtype) == -1:
        _log(
            f"[time] dbtype=-1: station dataframe labels shifted by -{time_context.time_delta}",
            on_log,
        )

    if station_obs_end is not None:
        fs_t = time_context.forecast_start_time
        flow_ref = flow_df
        rain_df, dr = clip_station_dataframe_rows_before_forecast_start(rain_df, forecast_start=fs_t)
        if dr:
            _log(
                f"[data] realtime_forecast: clipped {dr} station rows at/after forecast_start (CSV/file safety)",
                on_log,
            )
        if flow_ref is not rain_df:
            flow_df, df_ = clip_station_dataframe_rows_before_forecast_start(flow_ref, forecast_start=fs_t)
            if df_:
                _log(
                    f"[data] realtime_forecast: clipped {df_} flow-file rows at/after forecast_start",
                    on_log,
                )
        else:
            flow_df = rain_df

    if str(time_type).strip().lower() == "day":
        _log(
            "[data][day][cfg] floodForecastJdbc daydb.normalize_time_to_midnight="
            f"{read_jdbc_daydb_normalize_time_to_midnight_from_path(str(jdbc_config_path or ''))}",
            on_log,
        )

    # 4) DayDB 回填与诊断日志（用于定位“读不到雨量站”问题）
    rain_df = _apply_daydb_rain_fallback_and_diagnostics(
        on_log=on_log,
        time_type=str(time_type),
        rain_df=rain_df,
        rain_senids=rain_senids,
    )

    # 5) 读取后标准化 + 预留处理管线（当前 default 为 noop）
    loaded = standardize_loaded_inputs(
        rain_df=rain_df,
        flow_df=flow_df,
        warns=list(jdbc_warns or []),
        rain_senids=rain_senids,
        flow_senids=flow_senids,
        time_type=str(time_type),
        time_start=time_context.warmup_start_time,
        time_end=time_context.end_time,
    )
    loaded = apply_loaded_data_processors(
        loaded,
        time_type=str(time_type),
        on_log=on_log,
    )

    rain_df = loaded.rain_df
    flow_df = loaded.flow_df
    warns: List[str] = list(loaded.warns or [])

    # 6) 组装 station_packages / observed_flows
    station_packages, pkg_warns = build_station_packages(
        binding_specs=binding_specs,
        rain_df=rain_df,
        times=times,
        start_time=time_context.warmup_start_time,
        time_step=time_context.time_delta,
    )
    warns.extend(pkg_warns or [])

    observed_flows, obs_warns = build_observed_flows(
        scheme=scheme,
        flow_df=flow_df,
        times=times,
        start_time=time_context.warmup_start_time,
        time_step=time_context.time_delta,
    )
    warns.extend(obs_warns or [])

    # 实时预报：T0 起无实测气象；与引擎 run_calculation_from_json 一致，先清强迫再组 aux
    if str(forecast_mode).strip().lower() == "realtime_forecast":
        apply_realtime_forecast_observed_meteorology_cutoff(
            station_packages, time_context=time_context
        )

    # 7) catchment 面雨量（供前端绘制“雨量条形图”；与 station_packages 一致，避免 T0 仍用 rain_df 实测）
    catchment_rain, rain_warns = build_catchment_precip_series_from_station_packages(
        binding_specs=binding_specs,
        station_packages=station_packages,
        times_len=len(times),
    )
    warns.extend(rain_warns or [])

    scenario_rain_map = _load_catchment_scenario_rain_map(
        forecast_scenario_rain_csv,
        forecast_scenario_default_catchment_ids,
        on_log=on_log,
    )
    if scenario_rain_map:
        _log(
            f"[forecast_scenario_rain] loaded catchments={sorted(scenario_rain_map.keys())} "
            f"scenario={forecast_scenario_precipitation!r} multiscenario={forecast_run_multiscenario}",
            on_log,
        )

    # 8) aux：测站序列
    station_precip, station_pet, station_temp = _build_station_series_maps(
        station_packages=station_packages,
    )
    station_flow: Dict[str, List[float]] = {
        str(sid): [float(x) for x in ts.values] for sid, ts in (observed_flows or {}).items()
    }

    # 9) aux：node observed inflow/outflow 拆分
    node_observed_inflows: Dict[str, List[float]] = {}
    node_observed_outflows: Dict[str, List[float]] = {}
    for node_id, node in scheme.nodes.items():
        nid = str(node_id)

        infl_sid = str(getattr(node, "observed_inflow_station_id", "") or "").strip()
        out_sid = str(getattr(node, "observed_station_id", "") or "").strip()

        if infl_sid and infl_sid in observed_flows:
            node_observed_inflows[nid] = [float(x) for x in observed_flows[infl_sid].values]
        else:
            node_observed_inflows[nid] = [float("nan")] * len(times)

        if out_sid and out_sid in observed_flows:
            node_observed_outflows[nid] = [float(x) for x in observed_flows[out_sid].values]
        else:
            node_observed_outflows[nid] = [float("nan")] * len(times)

    # 10) aux：node 名称映射（用于下拉框显示）
    node_name_map: Dict[str, str] = {}
    for node_id, node in scheme.nodes.items():
        node_name_map[str(node_id)] = str(getattr(node, "name", str(node_id)))

    # 11) Day 方案：对齐诊断（请求 rain_senids vs station_precip keys）
    try:
        if str(time_type).strip().lower() == "day":
            req = set(str(s) for s in rain_senids or [])
            gen = set(station_precip.keys())
            inter = req & gen
            missing = sorted(req - gen)
            # 统计样本里“全 0”的比例（若 fill_mode=zero 导致缺测会显示为 0）
            all_zero = 0
            total = 0
            nonzero_total = 0
            for sid in list(inter)[:10]:
                vals = station_precip.get(sid) or []
                if not vals:
                    continue
                total += 1
                try:
                    fv = [float(x) for x in vals if x is not None]
                    # 统计“全零”与“非零总数”（区分 DB 返回 0 和 缺测->填 0）
                    if all((abs(x) == 0.0) for x in fv):
                        all_zero += 1
                    nonzero_total += sum(1 for x in fv if (x is not None and abs(x) != 0.0))
                except Exception:
                    pass
            _log(
                f"[data][day][diag2] station_precip_keys={len(gen)}, req_rain_senids={len(req)}, "
                f"inter={len(inter)}, missing_examples={missing[:5]}, "
                f"sample_all_zero={all_zero}/{max(total,1)}, sample_nonzero_total={nonzero_total}",
                on_log,
            )
    except Exception:
        pass

    station_catalog_names = _read_station_catalog_names(
        config_used_path, time_type=str(time_type), step_size=int(step_size)
    )
    catchment_catalog_names = _read_catchment_catalog_names(
        config_used_path, time_type=str(time_type), step_size=int(step_size)
    )

    aux_base: Dict[str, Any] = {
        "time_type": str(time_context.time_type.value),
        "dbtype": int(dbtype),
        "forecast_start_idx": _compute_forecast_start_idx(time_context),
        "node_observed_inflows": node_observed_inflows,
        "node_observed_outflows": node_observed_outflows,
        "node_name_map": node_name_map,
        "station_catalog_names": station_catalog_names,
        "catchment_catalog_names": catchment_catalog_names,
        "station_precip": station_precip,
        "station_pet": station_pet,
        "station_temp": station_temp,
        "station_flow": station_flow,
        "catchment_rain": catchment_rain,
    }

    runtime_cache: Dict[str, Any] = {
        "config_used_path": config_used_path,
        "warmup_start_time": time_context.warmup_start_time,
        "binding_specs": binding_specs,
        "station_packages": station_packages,
        "observed_flows": observed_flows,
        "time_context": time_context,
        "times": times,
        "scheme": scheme,
        "warns": warns,
        "aux_base": aux_base,
        "catchment_scenario_rainfall": scenario_rain_map,
        "forecast_scenario_precipitation": str(forecast_scenario_precipitation or "expected"),
        "forecast_run_multiscenario": bool(forecast_run_multiscenario),
    }

    # 12) 输出
    if not compute_forecast:
        nan_series = [float("nan")] * len(times)
        out: Dict[str, Any] = {
            "node_total_inflows": {str(nid): list(nan_series) for nid in scheme.nodes.keys()},
            "node_outflows": {str(nid): list(nan_series) for nid in scheme.nodes.keys()},
            "node_observed_flows": {},
            "catchment_runoffs": {},
            "catchment_routed_flows": {},
            "catchment_debug_traces": {},
            "reach_flows": {},
        }
    else:
        out = run_calculation_from_json(
            config_path=config_used_path,
            station_packages=station_packages,
            time_type=str(time_type),
            step_size=int(step_size),
            warmup_start_time=time_context.warmup_start_time,
            observed_flows=observed_flows,
            forecast_mode=forecast_mode,
            catchment_workers=catchment_workers,
            catchment_scenario_rainfall=scenario_rain_map or None,
            scenario_precipitation=str(forecast_scenario_precipitation or "expected"),
            forecast_multiscenario=bool(forecast_run_multiscenario),
        )
    aux = dict(aux_base)
    aux["_runtime_cache"] = runtime_cache
    return out, times, warns, aux


def run_forecast_from_runtime_cache(
    *,
    runtime_cache: Dict[str, Any],
    forecast_mode: str,
    catchment_workers: Optional[int],
    time_type: str,
    step_size: int,
    catchment_scenario_rainfall: Optional[Dict[str, CatchmentForecastRainfall]] = None,
    scenario_precipitation: Optional[str] = None,
    forecast_multiscenario: Optional[bool] = None,
    on_log: OnLog = None,
) -> Tuple[Dict[str, Any], pd.DatetimeIndex, List[str], Dict[str, Any]]:
    """
    Streamlit “预报计算”阶段：直接使用已准备好的 runtime_cache 避免重复读库/拼装。
    """
    if not runtime_cache:
        raise ValueError("runtime_cache is empty")

    _log("[ui] forecast from cache", on_log)

    config_used_path = runtime_cache["config_used_path"]
    warmup_start_time = runtime_cache["warmup_start_time"]
    station_packages = runtime_cache["station_packages"]
    observed_flows = runtime_cache["observed_flows"]
    time_context = runtime_cache["time_context"]
    times = runtime_cache["times"]
    warns = list(runtime_cache.get("warns") or [])
    aux_base = dict(runtime_cache.get("aux_base") or {})

    scen_map = catchment_scenario_rainfall
    if scen_map is None:
        scen_map = runtime_cache.get("catchment_scenario_rainfall") or {}
    scen_precip = (
        str(scenario_precipitation).strip()
        if scenario_precipitation is not None
        else str(runtime_cache.get("forecast_scenario_precipitation") or "expected")
    )
    scen_multi = (
        bool(forecast_multiscenario)
        if forecast_multiscenario is not None
        else bool(runtime_cache.get("forecast_run_multiscenario"))
    )

    out = run_calculation_from_json(
        config_path=config_used_path,
        station_packages=station_packages,
        time_type=str(time_type),
        step_size=int(step_size),
        warmup_start_time=warmup_start_time,
        observed_flows=observed_flows,
        forecast_mode=forecast_mode,
        catchment_workers=catchment_workers,
        catchment_scenario_rainfall=scen_map or None,
        scenario_precipitation=scen_precip,
        forecast_multiscenario=scen_multi,
    )

    aux = dict(aux_base)
    aux["_runtime_cache"] = runtime_cache

    # 保险：forecast_start_idx / time_type 以当前 time_context 为准
    try:
        aux["forecast_start_idx"] = _compute_forecast_start_idx(time_context)
        aux["time_type"] = str(time_context.time_type.value)
    except Exception:
        pass

    # 实时预报：run_calculation_from_json 会原地修补 station_packages；aux_base 在首次读数时已生成，
    # 这里用修补后的包重建雨量相关 aux，与引擎输入一致。
    if str(forecast_mode).strip().lower() == "realtime_forecast":
        bs = runtime_cache.get("binding_specs")
        if bs is None:
            try:
                _, bs, _ = load_scheme_from_json(
                    file_path=config_used_path,
                    time_type=str(time_type),
                    step_size=int(step_size),
                    warmup_start_time=warmup_start_time,
                )
            except Exception:
                bs = None
        if bs is not None:
            sp, spt, st = _build_station_series_maps(station_packages=station_packages)
            cr, _ = build_catchment_precip_series_from_station_packages(
                bs, station_packages, len(times)
            )
            aux["station_precip"] = sp
            aux["station_pet"] = spt
            aux["station_temp"] = st
            aux["catchment_rain"] = cr
            try:
                ab = dict(runtime_cache.get("aux_base") or {})
                ab.update(
                    {
                        "station_precip": sp,
                        "station_pet": spt,
                        "station_temp": st,
                        "catchment_rain": cr,
                    }
                )
                runtime_cache["aux_base"] = ab
            except Exception:
                pass

    return out, times, warns, aux

