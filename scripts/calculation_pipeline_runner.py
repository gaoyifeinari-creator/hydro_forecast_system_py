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

import numpy as np
import pandas as pd

from hydro_engine.core.forcing import ForcingKind
from hydro_engine.core.context import native_time_delta
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
from hydro_engine.forecast.multisource_areal_rainfall import (
    CompileRequest,
    MultiSourceArealRainfallCompiler,
    SqlAlchemyForecastRainRepository,
    load_forecast_db_config_from_jdbc_json,
    parse_forecast_rain_config_from_scheme,
)
from hydro_engine.forecast.scenario_forcing import load_catchment_forecast_rainfall_map_from_csv
from hydro_engine.io.json_config import (
    apply_realtime_forecast_observed_meteorology_cutoff,
    load_scheme_from_json,
    run_calculation_from_json,
)
from hydro_engine.io.scheme_config_utils import (
    catchment_catalog_names_from_scheme,
    read_schemes_list,
    select_scheme_dict_exact,
    scheme_dbtype,
    station_catalog_names_from_scheme,
)
from hydro_engine.domain.nodes.reservoir import ReservoirNode

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
    schemes = read_schemes_list(config_path)
    target = select_scheme_dict_exact(schemes, time_type=time_type, step_size=step_size)
    return station_catalog_names_from_scheme(target)


def _read_catchment_catalog_names(config_path: str, time_type: str, step_size: int) -> Dict[str, str]:
    """从当前方案 catchments[] 读取子流域 id -> name（用于 Web 展示）。"""
    schemes = read_schemes_list(config_path)
    target = select_scheme_dict_exact(schemes, time_type=time_type, step_size=step_size)
    return catchment_catalog_names_from_scheme(target)


def _read_scheme_dbtype(config_path: str, time_type: str, step_size: int) -> int:
    """
    读取当前方案的前后时标配置：
    -1: 前时标（默认）
     0: 后时标

    兼容：若配置缺失/异常，回退为 -1。
    """
    schemes = read_schemes_list(config_path)
    target = select_scheme_dict_exact(schemes, time_type=time_type, step_size=step_size)
    return scheme_dbtype(target, default=-1)


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


def _load_forecast_rain_from_scheme_db(
    *,
    config_path: str,
    jdbc_config_path: str,
    time_type: str,
    step_size: int,
    time_context: Any,
    dbtype: int,
    on_log: OnLog,
    debug_info_out: Optional[Dict[str, Any]] = None,
) -> Dict[str, CatchmentForecastRainfall]:
    """
    从 scheme.future_rainfall 读取配置，查库整编并生成 catchment 级预报雨量情景。
    """
    try:
        schemes = read_schemes_list(config_path)
        target = select_scheme_dict_exact(
            schemes, time_type=str(time_type), step_size=int(step_size)
        )
        if not isinstance(target, dict):
            return {}

        future_cfg = target.get("future_rainfall")
        if not isinstance(future_cfg, dict):
            return {}

        bundle = parse_forecast_rain_config_from_scheme(target)
        reg_ids = [str((c or {}).get("id", "")).strip() for c in (target.get("catchments") or []) if str((c or {}).get("id", "")).strip()]
        if not reg_ids:
            _log("[forecast_scenario_rain] scheme has no catchments, skip db forecast rain", on_log)
            return {}

        repo_cfg = load_forecast_db_config_from_jdbc_json(
            jdbc_config_path,
        )
        repo = SqlAlchemyForecastRainRepository(repo_cfg)
        compiler = MultiSourceArealRainfallCompiler(repo)
        rain_read_begin, rain_read_end = _resolve_forecast_rain_read_anchor_window(
            forecast_start_time=time_context.forecast_start_time,
            end_time=time_context.end_time,
            time_delta=time_context.time_delta,
            dbtype=int(dbtype),
        )
        debug_records: List[Tuple[str, int, List[Any]]] = []

        def _debug_records_hook(subtype: str, span: int, records: Any) -> None:
            try:
                debug_records.append((str(subtype), int(span), list(records or [])))
            except Exception:
                pass

        req = CompileRequest(
            forecast_begin=rain_read_begin,
            forecast_end=rain_read_end,
            target_time_type=str(time_type),
            target_time_step=int(step_size),
            # WEA_GFSFORRAIN 源数据是前时标；按当前方案 dbtype 做一次锚点转换（前->后），仅在整编阶段执行。
            dbtype=int(dbtype),
            reg_ids=reg_ids,
            source_config=bundle.selected_source,
            distribution_params=bundle.distribution_params,
            fluctuate_range=float(future_cfg.get("fluctuate_range", 0.0) or 0.0),
            use_min_max_from_db=bool(future_cfg.get("use_min_max_from_db", True)),
            latest_ftime_lookback_days=int(future_cfg.get("latest_ftime_lookback_days", 6) or 6),
            debug_records_hook=_debug_records_hook,
        )
        _log(
            "[forecast_scenario_rain][debug] request "
            f"time_type={req.target_time_type} step={req.target_time_step} dbtype={req.dbtype} "
            f"display_begin={time_context.forecast_start_time} display_end={time_context.end_time} "
            f"read_anchor_begin={req.forecast_begin} read_anchor_end={req.forecast_end}",
            on_log,
        )
        points = compiler.compile(req)
        source_debug_rows: List[Dict[str, Any]] = []
        # 打印“统一 FTIME + 明细记录”对账信息，便于与 HPS 逐条核对。
        for subtype, span, records in debug_records:
            if not records:
                source_debug_rows.append(
                    {
                        "subtype": str(subtype),
                        "span_hours": int(span),
                        "records": 0,
                        "ftime": [],
                    }
                )
                _log(
                    f"[forecast_scenario_rain][debug] subtype={subtype} span={span}h records=0",
                    on_log,
                )
                continue
            ftime_set = sorted({str(getattr(r, "ftime", "")) for r in records})
            source_debug_rows.append(
                {
                    "subtype": str(subtype),
                    "span_hours": int(span),
                    "records": int(len(records)),
                    "ftime": list(ftime_set),
                }
            )
            _log(
                f"[forecast_scenario_rain][debug] subtype={subtype} span={span}h records={len(records)} "
                f"ftime={ftime_set}",
                on_log,
            )
            rec_154034 = [r for r in records if str(getattr(r, "reg_id", "")) == "154034"]
            for r in sorted(rec_154034, key=lambda x: getattr(x, "btime", datetime.min)):
                _log(
                    "[forecast_scenario_rain][debug][154034][raw] "
                    f"btime={getattr(r, 'btime', '')} span={getattr(r, 'time_span_hours', '')} "
                    f"aver={float(getattr(r, 'aver_pre', 0.0)):.6f}",
                    on_log,
                )
        if not points:
            _log("[forecast_scenario_rain] db forecast rain compiled 0 points", on_log)
            return {}

        grouped: Dict[str, List[Any]] = {}
        for p in points:
            grouped.setdefault(str(p.reg_id), []).append(p)

        out: Dict[str, CatchmentForecastRainfall] = {}
        for cid, arr in grouped.items():
            arr_sorted = sorted(arr, key=lambda x: x.time)
            t_index = pd.DatetimeIndex([x.time for x in arr_sorted])
            out[cid] = CatchmentForecastRainfall.from_aligned_arrays(
                catchment_id=cid,
                time_index=t_index,
                expected=[float(x.value) for x in arr_sorted],
                upper=[float(x.max_value) for x in arr_sorted],
                lower=[float(x.min_value) for x in arr_sorted],
                time_step=time_context.time_delta,
            )
            if str(cid) == "154034":
                for p in arr_sorted:
                    _log(
                        "[forecast_scenario_rain][debug][154034][compiled] "
                        f"time={p.time} value={float(p.value):.6f} min={float(p.min_value):.6f} "
                        f"max={float(p.max_value):.6f}",
                        on_log,
                    )

        _log(
            f"[forecast_scenario_rain] db compiled catchments={len(out)} "
            f"source={bundle.selected_source.name}",
            on_log,
        )
        if debug_info_out is not None:
            debug_info_out.clear()
            debug_info_out.update(
                {
                    "request": {
                        "time_type": str(req.target_time_type),
                        "step_size": int(req.target_time_step),
                        "dbtype": int(req.dbtype),
                        "forecast_begin": str(req.forecast_begin),
                        "forecast_end": str(req.forecast_end),
                    },
                    "source_rows": source_debug_rows,
                    "selected_source_name": str(bundle.selected_source.name),
                }
            )
        return out
    except Exception as exc:  # noqa: BLE001
        _log(f"[forecast_scenario_rain] db compile failed: {exc}", on_log)
        return {}


def _resolve_actual_forecast_start(
    forecast_start_time_input: datetime,
    *,
    time_delta: timedelta,
    dbtype: int,
) -> datetime:
    """
    实际预报起点：
    - 输入时间即“界面显示的起报时间（预报第一个时刻标签）”。
    - 前后时标的数据库读数锚点在预报降雨读库阶段单独处理，不在此处平移。
    """
    _ = time_delta
    _ = dbtype
    return forecast_start_time_input


def _resolve_forecast_rain_read_anchor_window(
    *,
    forecast_start_time: datetime,
    end_time: datetime,
    time_delta: timedelta,
    dbtype: int,
) -> Tuple[datetime, datetime]:
    """
    解析预报降雨读库锚点窗口。

    规则：
    - 前时标（dbtype=-1）：读库锚点与展示时标一致。
    - 后时标（dbtype!= -1）：读库锚点整体回拨 1 步，
      即展示首时刻 T0 对应的库锚点为 T0-time_delta。
    """
    if int(dbtype) == -1:
        return forecast_start_time, end_time
    return forecast_start_time - time_delta, end_time - time_delta


def _shift_station_df_time_label_for_dbtype(
    df: pd.DataFrame,
    *,
    time_delta: timedelta,
    dbtype: int,
) -> pd.DataFrame:
    """
    按“实况库源为后时标”将标签映射到方案展示时标：

    - dbtype=0（后时标展示）：不平移（库时刻即展示时刻）
    - dbtype=-1（前时标展示）：标签回拨 1 步（例如库 05:00 -> 展示 04:00）

    说明：预报降雨 WEA_GFSFORRAIN 的前时标处理在预报面雨整编链路中单独执行，
    不与本函数混用。
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


def _resolve_station_read_window_for_dbtype(
    *,
    read_time_start: datetime,
    read_time_end: datetime,
    station_obs_end: Optional[datetime],
    time_delta: timedelta,
    dbtype: int,
) -> Tuple[datetime, datetime, Optional[datetime]]:
    """
    解析测站读库时间锚点。

    站点实况源（hourdb/daydb）固定为后时标：

    - dbtype=0（后时标展示）：读窗不平移
    - dbtype=-1（前时标展示）：读窗 +1 步（读取后时标源），随后展示标签再 -1 步

    说明：预报降雨 WEA_GFSFORRAIN 的读库锚点平移在
    `_resolve_forecast_rain_read_anchor_window` 单独处理。
    """
    if int(dbtype) != -1:
        return read_time_start, read_time_end, station_obs_end
    shifted_start = read_time_start + time_delta
    shifted_end = read_time_end + time_delta
    shifted_obs_end = station_obs_end + time_delta if station_obs_end is not None else None
    # 日方案下，部分 daydb 记录时间可能位于当日白天（如 08:00）。
    # 若仅平移 1 天，实时 t_end 可能卡在 00:00，导致“最后一个历史日”漏读；
    # 这里额外放宽 1 天，随后仍由 clip(<forecast_start) 剪掉预报段，确保展示正确。
    if shifted_obs_end is not None and time_delta >= timedelta(days=1):
        shifted_obs_end = shifted_obs_end + time_delta
    return shifted_start, shifted_end, shifted_obs_end


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
            target[str(sid)] = [float(x) for x in np.asarray(ts.values, dtype=np.float64).ravel().tolist()]
    return station_precip, station_pet, station_temp


def _overlay_forecast_rain_to_catchment_series(
    *,
    base_series: Dict[str, List[float]],
    scenario_rain_map: Dict[str, CatchmentForecastRainfall],
    times: pd.DatetimeIndex,
    forecast_start_idx: int,
    precipitation_field: str = "expected",
) -> Dict[str, List[float]]:
    """
    用预报面雨覆盖 UI 展示用 catchment_rain 的预报段，历史段保留实测聚合。
    """
    out = {str(k): list(v) for k, v in (base_series or {}).items()}
    if not scenario_rain_map:
        return out
    fs = max(0, int(forecast_start_idx))
    if fs >= len(times):
        return out

    for cid, scen in (scenario_rain_map or {}).items():
        arr = list(out.get(str(cid), [0.0] * len(times)))
        if len(arr) < len(times):
            arr = arr + [0.0] * (len(times) - len(arr))
        vals = scen.expected
        if str(precipitation_field).strip().lower() in ("upper", "max"):
            vals = scen.upper
        elif str(precipitation_field).strip().lower() in ("lower", "min"):
            vals = scen.lower

        by_time = {pd.Timestamp(t): float(v) for t, v in zip(scen.time_index, vals)}
        for i in range(fs, len(times)):
            t = pd.Timestamp(times[i])
            if t in by_time:
                arr[i] = float(by_time[t])
        out[str(cid)] = arr
    return out


def _align_scenario_rainfall_to_engine_grid(
    *,
    scenario_rain_map: Dict[str, CatchmentForecastRainfall],
    forecast_times: pd.DatetimeIndex,
    time_step: timedelta,
    precipitation_field: str = "expected",
    dbtype: int = -1,
) -> Dict[str, CatchmentForecastRainfall]:
    """
    将情景雨量对齐到引擎预报网格（首时刻必须等于 forecast_start_time）。
    缺失时刻补 0，避免因后时标序列首时刻 +1 步导致注入失败。
    """
    if not scenario_rain_map:
        return {}
    out: Dict[str, CatchmentForecastRainfall] = {}
    for cid, scen in (scenario_rain_map or {}).items():
        vals = scen.expected
        if str(precipitation_field).strip().lower() in ("upper", "max"):
            vals = scen.upper
        elif str(precipitation_field).strip().lower() in ("lower", "min"):
            vals = scen.lower
        by_time = {pd.Timestamp(t): float(v) for t, v in zip(scen.time_index, vals)}
        aligned_expected: List[float] = []
        aligned_upper: List[float] = []
        aligned_lower: List[float] = []
        by_u = {pd.Timestamp(t): float(v) for t, v in zip(scen.time_index, scen.upper)}
        by_l = {pd.Timestamp(t): float(v) for t, v in zip(scen.time_index, scen.lower)}
        for t in forecast_times:
            ts = pd.Timestamp(t)
            aligned_expected.append(float(by_time.get(ts, 0.0)))
            aligned_upper.append(float(by_u.get(ts, 0.0)))
            aligned_lower.append(float(by_l.get(ts, 0.0)))
        out[str(cid)] = CatchmentForecastRainfall.from_aligned_arrays(
            catchment_id=str(cid),
            time_index=pd.DatetimeIndex(forecast_times),
            expected=aligned_expected,
            upper=aligned_upper,
            lower=aligned_lower,
            time_step=time_step,
        )
    return out


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
    td = native_time_delta(time_type=str(time_type), step_size=int(step_size))
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
    _log(
        "[time] timestamp_convention "
        "station_hourdb_daydb=backward_label (read/display direct), "
        "forecast_wea_gfsforrain=forward_label (handled in forecast-rain pipeline)",
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
    reservoir_node_ids = sorted(
        str(nid) for nid, node in scheme.nodes.items() if isinstance(node, ReservoirNode)
    )
    _log(
        "[interval][debug] loaded scheme node-types "
        f"reservoir_count={len(reservoir_node_ids)} "
        f"contains_154034={('154034' in reservoir_node_ids)} "
        f"contains_154850={('154850' in reservoir_node_ids)}",
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
    read_time_start, read_time_end, station_obs_end = _resolve_station_read_window_for_dbtype(
        read_time_start=read_time_start,
        read_time_end=read_time_end,
        station_obs_end=station_obs_end,
        time_delta=time_context.time_delta,
        dbtype=int(dbtype),
    )
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
    # 测站实况源固定为后时标：
    # - 后时标方案：标签不动
    # - 前时标方案：标签回拨 1 步（例如库 05:00 -> 展示 04:00）
    # 单库源路径下 rain_df/flow_df 可能是同一对象，避免重复处理。
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
    station_shift_label = (
        f"-{time_context.time_delta}" if int(dbtype) == -1 else "0:00:00"
    )
    _log(
        "[time] station dataframe labels shifted "
        f"dbtype={dbtype} shift={station_shift_label}",
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

    fs_idx = _compute_forecast_start_idx(time_context)
    forecast_rain_ftime_info: Dict[str, Any] = {}
    is_historical_simulation = str(forecast_mode).strip().lower() == "historical_simulation"

    if is_historical_simulation:
        scenario_rain_map: Dict[str, CatchmentForecastRainfall] = {}
        _log(
            "[forecast_scenario_rain] historical_simulation: skip CSV/DB numerical forecast areal rain; "
            "catchment rain is station-synthesized for the full window",
            on_log,
        )
        catchment_rain_ui = {str(k): list(v) for k, v in (catchment_rain or {}).items()}
    else:
        scenario_rain_map = _load_catchment_scenario_rain_map(
            forecast_scenario_rain_csv,
            forecast_scenario_default_catchment_ids,
            on_log=on_log,
        )
        if not scenario_rain_map:
            scenario_rain_map = _load_forecast_rain_from_scheme_db(
                config_path=config_used_path,
                jdbc_config_path=str(jdbc_config_path or ""),
                time_type=str(time_type),
                step_size=int(step_size),
                time_context=time_context,
                dbtype=int(dbtype),
                on_log=on_log,
                debug_info_out=forecast_rain_ftime_info,
            )
        if scenario_rain_map:
            _log(
                f"[forecast_scenario_rain] loaded catchments={sorted(scenario_rain_map.keys())} "
                f"scenario={forecast_scenario_precipitation!r} multiscenario={forecast_run_multiscenario}",
                on_log,
            )

        forecast_times = pd.DatetimeIndex(times[fs_idx:])
        scenario_rain_map = _align_scenario_rainfall_to_engine_grid(
            scenario_rain_map=scenario_rain_map,
            forecast_times=forecast_times,
            time_step=time_context.time_delta,
            precipitation_field=str(forecast_scenario_precipitation or "expected"),
            dbtype=int(dbtype),
        )
        catchment_rain_ui = _overlay_forecast_rain_to_catchment_series(
            base_series=catchment_rain,
            scenario_rain_map=scenario_rain_map,
            times=times,
            forecast_start_idx=fs_idx,
            precipitation_field=str(forecast_scenario_precipitation or "expected"),
        )

    # 8) aux：测站序列
    station_precip, station_pet, station_temp = _build_station_series_maps(
        station_packages=station_packages,
    )
    station_flow: Dict[str, List[float]] = {
        str(sid): [float(x) for x in np.asarray(ts.values, dtype=np.float64).ravel().tolist()]
        for sid, ts in (observed_flows or {}).items()
    }

    # 9) aux：node observed inflow/outflow 拆分
    node_observed_inflows: Dict[str, List[float]] = {}
    node_observed_outflows: Dict[str, List[float]] = {}
    for node_id, node in scheme.nodes.items():
        nid = str(node_id)

        infl_sid = str(getattr(node, "observed_inflow_station_id", "") or "").strip()
        out_sid = str(getattr(node, "observed_station_id", "") or "").strip()

        if infl_sid and infl_sid in observed_flows:
            node_observed_inflows[nid] = [
                float(x) for x in np.asarray(observed_flows[infl_sid].values, dtype=np.float64).ravel().tolist()
            ]
        else:
            node_observed_inflows[nid] = [float("nan")] * len(times)

        if out_sid and out_sid in observed_flows:
            node_observed_outflows[nid] = [
                float(x) for x in np.asarray(observed_flows[out_sid].values, dtype=np.float64).ravel().tolist()
            ]
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
        "forecast_mode": str(forecast_mode).strip().lower(),
        "forecast_start_idx": fs_idx,
        "node_observed_inflows": node_observed_inflows,
        "node_observed_outflows": node_observed_outflows,
        "node_name_map": node_name_map,
        "station_catalog_names": station_catalog_names,
        "catchment_catalog_names": catchment_catalog_names,
        "station_precip": station_precip,
        "station_pet": station_pet,
        "station_temp": station_temp,
        "station_flow": station_flow,
        "catchment_rain": catchment_rain_ui,
        "forecast_rain_ftime_info": dict(forecast_rain_ftime_info),
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
        interval_channels: List[str] = []
        seen_interval_channels = set()
        for c in list(getattr(scheme, "custom_interval_channels", []) or []):
            name = str((c or {}).get("name", "")).strip()
            if not name or name in seen_interval_channels:
                continue
            seen_interval_channels.add(name)
            interval_channels.append(name)
        if "default" not in seen_interval_channels:
            interval_channels.insert(0, "default")
        out: Dict[str, Any] = {
            "node_total_inflows": {str(nid): list(nan_series) for nid in scheme.nodes.keys()},
            "node_outflows": {str(nid): list(nan_series) for nid in scheme.nodes.keys()},
            "node_observed_flows": {},
            "catchment_runoffs": {},
            "catchment_routed_flows": {},
            "catchment_debug_traces": {},
            "reach_flows": {},
            "interval_channels": list(interval_channels),
            "node_interval_inflows": {
                str(nid): {ch: list(nan_series) for ch in interval_channels}
                for nid in scheme.nodes.keys()
            },
            "node_interval_outflows": {
                str(nid): {ch: list(nan_series) for ch in interval_channels}
                for nid in scheme.nodes.keys()
            },
            "reach_interval_flows": {ch: {} for ch in interval_channels},
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
    fs_idx = _compute_forecast_start_idx(time_context)
    if str(forecast_mode).strip().lower() == "historical_simulation":
        scen_map = {}
        _log(
            "[forecast_scenario_rain] historical_simulation: ignore cached/aligned scenario rain "
            "(full-window station-synthesized areal rain only)",
            on_log,
        )
    else:
        scen_map = _align_scenario_rainfall_to_engine_grid(
            scenario_rain_map=scen_map or {},
            forecast_times=pd.DatetimeIndex(times[fs_idx:]),
            time_step=time_context.time_delta,
            precipitation_field=scen_precip,
            dbtype=int(aux_base.get("dbtype", -1)),
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
    aux["forecast_mode"] = str(forecast_mode).strip().lower()

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
            cr = _overlay_forecast_rain_to_catchment_series(
                base_series=cr,
                scenario_rain_map=scen_map or {},
                times=times,
                forecast_start_idx=fs_idx,
                precipitation_field=scen_precip,
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

    elif str(forecast_mode).strip().lower() == "historical_simulation":
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

