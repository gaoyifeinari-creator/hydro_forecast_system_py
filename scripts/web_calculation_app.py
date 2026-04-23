"""
Streamlit 版水文计算测试界面，与通用计算入口（calculation_pipeline_runner）数据流对齐：
读取数据 / 预报计算分离、内存缓存复用、多标签图表与测站查看。

---------------------------------------------------------------------------
访问地址（默认端口 8501）: http://127.0.0.1:8501
---------------------------------------------------------------------------
启动方式（任选）:
- 直接运行本脚本（会自动改为 streamlit run，避免裸跑警告）::
    python scripts/web_calculation_app.py
- 或显式::
    streamlit run scripts/web_calculation_app.py --server.address 127.0.0.1 --server.port 8501
- 或双击: start_web_calculation_app.bat

图表使用 matplotlib。
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPTS_DIR.parent

# 必须在 import streamlit 之前：裸用 python 执行本文件时改为 streamlit run（父进程不加载 streamlit）
if __name__ == "__main__" and "streamlit" not in sys.modules:
    os.chdir(_PROJECT_ROOT)
    raise SystemExit(
        subprocess.call(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(Path(__file__).resolve()),
                "--server.address",
                "127.0.0.1",
                "--server.port",
                "8501",
                "--browser.gatherUsageStats",
                "false",
            ]
        )
    )

import math
import json
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from hydro_engine.io.calculation_app_data_loader import DEFAULT_FLOOD_JDBC_CONFIG
from hydro_engine.io.scheme_config_utils import (
    read_schemes_list,
    select_scheme_dict_exact,
    select_scheme_dict_smallest_step,
)

from calculation_pipeline_runner import (
    _infer_debug_table_columns,
    run_calculation_pipeline,
    run_forecast_from_runtime_cache,
)


def _configure_matplotlib_fonts() -> None:
    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans", "sans-serif"]
    plt.rcParams["axes.unicode_minus"] = False


def _station_ui_label(sid: str, name_map: Optional[Dict[str, Any]]) -> str:
    """测站下拉与标题：有配置名称时为「名称（id）」，否则仅 id。"""
    s = str(sid)
    nm = str((name_map or {}).get(s) or "").strip()
    if nm:
        return f"{nm}（{s}）"
    return s


def _to_float_series(vals: List[Any]) -> List[float]:
    out: List[float] = []
    for v in vals:
        if v is None:
            out.append(float("nan"))
        else:
            try:
                fv = float(v)
                out.append(fv if not (isinstance(fv, float) and math.isnan(fv)) else float("nan"))
            except (TypeError, ValueError):
                out.append(float("nan"))
    return out


def _st_pyplot(fig: Any) -> None:
    st.pyplot(fig)
    plt.close(fig)


def _init_session() -> None:
    defaults = {
        "hydro_logs": [],
        "hydro_output": None,
        "hydro_times": None,
        "hydro_warns": [],
        "hydro_aux": None,
        "hydro_runtime_cache": None,
        "hydro_runtime_cache_key": None,
        "hydro_status": "就绪",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _append_log(msg: str) -> None:
    st.session_state.hydro_logs.append(f"[{datetime.now():%H:%M:%S}] {msg}")


def _cache_key_from_params(p: Dict[str, Any]) -> Tuple[Any, ...]:
    cfg_path = str(p["config_path"] or "").strip()
    cfg_mtime_ns = 0
    if cfg_path:
        try:
            cfg_mtime_ns = Path(cfg_path).stat().st_mtime_ns
        except OSError:
            cfg_mtime_ns = 0
    default_ids = p.get("forecast_scenario_default_catchment_ids") or ()
    return (
        p["config_path"],
        cfg_mtime_ns,
        p["jdbc_config_path"],
        p["rain_csv"],
        p["flow_csv"],
        p["warmup_start"],
        p["time_type"],
        p["step_size"],
        p["warmup_steps"],
        p["correction_steps"],
        p["historical_steps"],
        p["forecast_steps"],
        str(p.get("forecast_scenario_rain_csv") or "").strip(),
        tuple(default_ids) if isinstance(default_ids, (list, tuple)) else (default_ids,),
    )


def _parse_forecast_default_catchment_ids(raw: str) -> Optional[List[str]]:
    parts = [p.strip() for p in str(raw or "").split(",") if p.strip()]
    return parts or None


def _gather_params(side: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "config_path": str(side["config_path"]).strip(),
        "jdbc_config_path": str(side["jdbc_config_path"]).strip(),
        "rain_csv": str(side["rain_csv"]).strip(),
        "flow_csv": str(side["flow_csv"]).strip(),
        "warmup_start": str(side["warmup_start"]).strip(),
        "forecast_mode": str(side["forecast_mode"]).strip(),
        "catchment_workers": 1 if bool(side["single_thread"]) else None,
        "time_type": str(side["time_type"]).strip(),
        "step_size": int(side["step_size"]),
        "warmup_steps": int(side["warmup_steps"]),
        "correction_steps": int(side["correction_steps"]),
        "historical_steps": int(side["historical_steps"]),
        "forecast_steps": max(1, int(side["forecast_steps"])),
        "forecast_scenario_rain_csv": str(side.get("forecast_scenario_rain_csv") or "").strip(),
        "forecast_scenario_default_catchment_ids": _parse_forecast_default_catchment_ids(
            str(side.get("forecast_scenario_default_catchment_ids") or "")
        ),
        "forecast_scenario_precipitation": str(side.get("forecast_scenario_precipitation") or "expected").strip(),
        "forecast_run_multiscenario": bool(side.get("forecast_run_multiscenario")),
    }


def _slice_map(m: Optional[Dict[str, List[float]]], start: int) -> Dict[str, List[float]]:
    if not m:
        return {}
    out: Dict[str, List[float]] = {}
    for k, v in m.items():
        out[str(k)] = list(v[start:]) if start < len(v) else []
    return out


def _mask_observed_after_forecast_start_realtime_only(
    values: Optional[List[float]],
    rel_forecast_start_idx: int,
) -> Optional[List[float]]:
    """
    仅用于实时预报的 Node 实测展示：在已对齐到 display_start 的序列上，
    将「起报步及之后」(index >= rel_forecast_start_idx) 置为 None，避免展示未来「伪实况」。
    历史模拟模式不得调用（应全窗展示实测）。
    """
    if values is None:
        return None
    fs = max(0, int(rel_forecast_start_idx))
    return [v if i < fs else None for i, v in enumerate(values)]


def _is_day_precision(aux: Optional[Dict[str, Any]]) -> bool:
    if not aux:
        return False
    return str(aux.get("time_type", "")).strip().lower() == "day"


def _format_ts(t: Any, day_prec: bool) -> str:
    if day_prec:
        return pd.Timestamp(t).strftime("%Y-%m-%d")
    return pd.Timestamp(t).strftime("%Y-%m-%d %H:%M")


def _matplotlib_axis_date_fmt(times: pd.DatetimeIndex, *, day_precision: bool) -> str:
    """
    时间轴刻度/表格时间列格式：日方案为 yyyy-MM-dd；否则按跨度省略年份或日期中的「日以上部分」。
    同一天内省略日期只保留时分；同年跨日省略年份为 MM-dd HH；跨年为 yyyy-MM-dd HH。
    """
    times = pd.to_datetime(times)
    if len(times) == 0:
        return "%Y-%m-%d %H"
    if day_precision:
        return "%Y-%m-%d"
    t0, t1 = times.min(), times.max()
    if t0.date() == t1.date():
        return "%H:%M"
    if t0.year == t1.year:
        return "%m-%d %H"
    return "%Y-%m-%d %H"


def _style_ax_time_x(ax: Any, times: pd.DatetimeIndex, *, day_precision: bool) -> None:
    fmt = _matplotlib_axis_date_fmt(times, day_precision=day_precision)
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=32, ha="right")


def _time_cell_strings(display_times: pd.DatetimeIndex, *, day_precision: bool) -> List[str]:
    fmt = _matplotlib_axis_date_fmt(pd.to_datetime(display_times), day_precision=day_precision)
    return [pd.Timestamp(t).strftime(fmt) for t in display_times]


def _bar_width_days(x_dt: pd.DatetimeIndex) -> float:
    if len(x_dt) < 2:
        return 1.0 / 24.0
    d0 = mdates.date2num(x_dt[0].to_pydatetime())
    d1 = mdates.date2num(x_dt[1].to_pydatetime())
    return max((d1 - d0) * 0.75, 1e-6)


def _catchment_display_label(cid: str, name_map: Any) -> str:
    s = str(cid)
    nm = str((name_map or {}).get(s) or "").strip()
    return f"{nm}（{s}）" if nm else s


def _node_display_label(node_id: str, node_name_map: Optional[Dict[str, Any]]) -> str:
    s = str(node_id)
    nm = str((node_name_map or {}).get(s) or "").strip()
    return nm if nm else s


def _reach_node_pair_label(
    reach_id: str,
    *,
    aux: Dict[str, Any],
) -> str:
    """
    河段展示标签：优先显示“上游节点名 -> 下游节点名”，缺失时回退为 reach_id。
    """
    rid = str(reach_id)
    node_name_map = aux.get("node_name_map") or {}
    runtime_cache = aux.get("_runtime_cache") or {}
    scheme = runtime_cache.get("scheme")
    if scheme is None:
        return rid
    try:
        reach = scheme.reaches.get(rid)
    except Exception:
        reach = None
    if reach is None:
        return rid
    up = _node_display_label(str(getattr(reach, "upstream_node_id", "")), node_name_map)
    down = _node_display_label(str(getattr(reach, "downstream_node_id", "")), node_name_map)
    if up and down:
        return f"{up} -> {down}"
    return rid


def _plot_node_tab(
    output: Dict[str, Any],
    aux: Dict[str, Any],
    times: pd.DatetimeIndex,
    hist_steps: int,
    node_id: str,
) -> None:
    fs = int(aux.get("forecast_start_idx", 0))
    hs = max(0, int(hist_steps))
    d0 = max(0, fs - hs)
    display_times = times[d0:]
    rel_fs = max(0, fs - d0)

    fin = _slice_map(output.get("node_total_inflows") or {}, d0).get(node_id, [])
    oin_full = list((aux.get("node_observed_inflows") or {}).get(node_id, []) or [])
    oout_full = list((aux.get("node_observed_outflows") or {}).get(node_id, []) or [])

    oin = oin_full[d0:] if len(oin_full) >= len(times) else [None] * len(display_times)
    oout = oout_full[d0:] if len(oout_full) >= len(times) else [None] * len(display_times)
    if len(oin) != len(display_times):
        oin = [None] * len(display_times)
    if len(oout) != len(display_times):
        oout = [None] * len(display_times)
    mode = str(aux.get("forecast_mode", "realtime_forecast")).strip().lower()
    if mode == "realtime_forecast":
        oin = _mask_observed_after_forecast_start_realtime_only(oin, rel_fs) or [None] * len(
            display_times
        )
        oout = _mask_observed_after_forecast_start_realtime_only(oout, rel_fs) or [None] * len(
            display_times
        )

    day_p = _is_day_precision(aux)
    t_idx = pd.to_datetime(display_times)
    fin_f = _to_float_series(fin)
    oin_f = _to_float_series(oin)
    oout_f = _to_float_series(oout)

    # 兼容：当某侧序列 key 缺失时会是空列表（例如只读数据时预测侧可能为空）。
    # Streamlit 的 line_chart/DataFrame 要求 values 长度与 index 完全一致，这里对齐长度。
    n = len(t_idx)
    if len(fin_f) != n:
        fin_f = (fin_f + [float("nan")] * n)[:n]
    if len(oin_f) != n:
        oin_f = (oin_f + [float("nan")] * n)[:n]
    if len(oout_f) != n:
        oout_f = (oout_f + [float("nan")] * n)[:n]

    _configure_matplotlib_fonts()
    fig, ax = plt.subplots(figsize=(10, 3.8))
    ax.plot(t_idx, fin_f, label="预报入库", linewidth=1.2)
    ax.plot(t_idx, oin_f, label="实测入库", linewidth=1.0, alpha=0.85)
    ax.plot(t_idx, oout_f, label="实测出库", linewidth=1.0, alpha=0.85)
    ax.set_ylabel("流量 (m³/s)")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(True, alpha=0.25)
    _style_ax_time_x(ax, t_idx, day_precision=day_p)
    fig.tight_layout()
    _st_pyplot(fig)

    t_cells = _time_cell_strings(pd.to_datetime(display_times), day_precision=day_p)
    rows: List[Dict[str, Any]] = []
    for i, t in enumerate(display_times):
        rows.append(
            {
                "时间": t_cells[i] if i < len(t_cells) else _format_ts(t, day_p),
                "预报入库": fin_f[i] if i < len(fin_f) else None,
                "实测入库": oin_f[i] if i < len(oin_f) else None,
                "实测出库": oout_f[i] if i < len(oout_f) else None,
            }
        )

    df_table = pd.DataFrame(rows)
    max_rows = 400
    if len(df_table) > max_rows:
        head = df_table.head(max_rows // 2)
        tail = df_table.tail(max_rows // 2)
        df_table = pd.concat([head, tail], axis=0)
        st.caption(f"表格仅展示前后各 {max_rows // 2} 行（总共 {len(rows)} 行），完整数据请看 JSON。")

    st.dataframe(df_table, use_container_width=True, height=320)


def _plot_hydro_pair_tab(
    title_prefix: str,
    forecast_map: Dict[str, List[float]],
    rain_map: Dict[str, List[float]],
    times: pd.DatetimeIndex,
    cid: str,
    hist_steps: int,
    forecast_start_idx: int,
    aux: Dict[str, Any],
    *,
    show_right_table: bool = False,
    display_label: str = "",
) -> None:
    fs = int(forecast_start_idx)
    hs = max(0, int(hist_steps))
    # 嵌套时间轴：fs = (T0−warmup_start)/Δt = W。侧栏「历史展示步数」= H 时，
    # d0 = W − H 为「历史展示段」起点（display_start）索引，非全序列第 0 步。
    d0 = max(0, fs - hs)
    display_times = times[d0:]

    fc = _slice_map(forecast_map, d0).get(cid, [])
    rain_full = list((rain_map or {}).get(cid, []) or [])
    rain = rain_full[d0:] if len(rain_full) >= len(times) else [0.0] * len(display_times)
    if len(rain) != len(display_times):
        rain = [0.0] * len(display_times)
    if len(fc) != len(display_times):
        fc = [float("nan")] * len(display_times)

    x_dt = pd.to_datetime(display_times)
    day_p = _is_day_precision(aux)
    label = display_label or str(cid)
    st.subheader(f"{title_prefix} · {label}")
    st.caption(
        f"时间轴（嵌套）：`forecast_start_idx`={fs}=W（T0 相对预热起点），历史展示步数={hs}=H，"
        f"本图从第 **{d0}** 步起（d0=W−H，即 display_start）。全序列第 0 步为预热起点。"
    )

    rain_f = _to_float_series(rain)
    fc_f = _to_float_series(fc)
    t_cells = _time_cell_strings(x_dt, day_precision=day_p)
    rows: List[Dict[str, Any]] = [
        {
            "时间": t_cells[i] if i < len(t_cells) else "",
            "面雨量(mm)": rain_f[i] if i < len(rain_f) else None,
            "流量(m³/s)": fc_f[i] if i < len(fc_f) else None,
        }
        for i in range(len(x_dt))
    ]
    df_table = pd.DataFrame(rows)
    max_rows = 400
    table_caption: Optional[str] = None
    if len(df_table) > max_rows:
        half = max_rows // 2
        df_table = pd.concat([df_table.head(half), df_table.tail(half)], axis=0)
        table_caption = f"表格仅展示前后各 {half} 行（共 {len(rows)} 行）。"

    _configure_matplotlib_fonts()
    fig, (ax0, ax1) = plt.subplots(
        2,
        1,
        figsize=(10, 5.2),
        sharex=True,
        gridspec_kw={"height_ratios": [1, 1.15], "hspace": 0.12},
    )
    bw = _bar_width_days(x_dt)
    ax0.bar(x_dt, rain_f, width=bw, color="#4C78A8", align="center", edgecolor="none")
    ax0.set_ylabel("雨量(mm)")
    ax0.set_title(f"{title_prefix} — 面雨量")
    ax1.plot(x_dt, fc_f, color="#F58518", linewidth=1.2)
    ax1.set_ylabel("流量(m³/s)")
    ax1.set_title(f"{title_prefix} — 预测流量")
    for ax in (ax0, ax1):
        _style_ax_time_x(ax, x_dt, day_precision=day_p)
    fig.tight_layout()

    if show_right_table:
        c_left, c_right = st.columns([1.45, 1])
        with c_left:
            _st_pyplot(fig)
        with c_right:
            if table_caption:
                st.caption(table_caption)
            st.dataframe(df_table, use_container_width=True, height=480)
    else:
        _st_pyplot(fig)


def _plot_reach_tab(
    output: Dict[str, Any],
    times: pd.DatetimeIndex,
    hist_steps: int,
    forecast_start_idx: int,
    rid: str,
    aux: Dict[str, Any],
) -> None:
    fs = int(forecast_start_idx)
    hs = max(0, int(hist_steps))
    d0 = max(0, fs - hs)
    display_times = times[d0:]
    series = _slice_map(output.get("reach_flows") or {}, d0).get(rid, [])
    if len(series) != len(display_times):
        series = list(series)[: len(display_times)] + [float("nan")] * max(0, len(display_times) - len(series))

    x_dt = pd.to_datetime(display_times)
    day_p = _is_day_precision(aux)
    st.subheader(f"河段 {rid}")
    _configure_matplotlib_fonts()
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.plot(x_dt, _to_float_series(series), color="#54A24B", linewidth=1.2)
    ax.set_ylabel("流量 (m³/s)")
    ax.grid(True, alpha=0.25)
    _style_ax_time_x(ax, x_dt, day_precision=day_p)
    fig.tight_layout()
    _st_pyplot(fig)


def _plot_interval_channel_tab(
    output: Dict[str, Any],
    aux: Dict[str, Any],
    times: pd.DatetimeIndex,
    hist_steps: int,
    forecast_start_idx: int,
) -> None:
    channels = [str(x) for x in (output.get("interval_channels") or []) if str(x).strip()]
    if not channels:
        st.info("无区间通道结果（默认通道未输出或未启用）")
        return

    fs = int(forecast_start_idx)
    hs = max(0, int(hist_steps))
    d0 = max(0, fs - hs)
    display_times = times[d0:]
    x_dt = pd.to_datetime(display_times)
    day_p = _is_day_precision(aux)

    ch = st.selectbox("区间通道", options=channels, key="interval_channel_pick")
    metric = st.selectbox(
        "通道数据类型",
        options=["河段区间流量", "节点区间入流", "节点区间出流"],
        key="interval_metric_pick",
    )
    if metric == "河段区间流量":
        m = output.get("reach_interval_flows") or {}
        scope_map = dict((m.get(ch) or {}))
        scope_label = "河段"
    elif metric == "节点区间入流":
        m = output.get("node_interval_inflows") or {}
        scope_map = {str(nid): (vals.get(ch) if isinstance(vals, dict) else []) for nid, vals in m.items()}
        scope_label = "节点"
    else:
        m = output.get("node_interval_outflows") or {}
        scope_map = {str(nid): (vals.get(ch) if isinstance(vals, dict) else []) for nid, vals in m.items()}
        scope_label = "节点"

    keys = sorted(str(k) for k in scope_map.keys())
    if not keys:
        st.info(f"通道 {ch} 下无可展示的{scope_label}数据")
        return
    if metric == "河段区间流量":
        pick = st.selectbox(
            scope_label,
            options=keys,
            key=f"interval_scope_pick_{metric}",
            format_func=lambda x: _reach_node_pair_label(str(x), aux=aux),
        )
        pick_label = _reach_node_pair_label(str(pick), aux=aux)
    else:
        node_name_map = aux.get("node_name_map") or {}
        pick = st.selectbox(
            scope_label,
            options=keys,
            key=f"interval_scope_pick_{metric}",
            format_func=lambda x, m=node_name_map: _node_display_label(str(x), m),
        )
        pick_label = _node_display_label(str(pick), node_name_map)
    values = _slice_map({str(pick): list(scope_map.get(str(pick), []) or [])}, d0).get(str(pick), [])
    if len(values) != len(display_times):
        values = list(values)[: len(display_times)] + [float("nan")] * max(0, len(display_times) - len(values))

    st.subheader(f"{metric} · 通道 {ch} · {scope_label} {pick_label}")
    _configure_matplotlib_fonts()
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.plot(x_dt, _to_float_series(values), color="#4C78A8", linewidth=1.2)
    ax.set_ylabel("流量 (m³/s)")
    ax.grid(True, alpha=0.25)
    _style_ax_time_x(ax, x_dt, day_precision=day_p)
    fig.tight_layout()
    _st_pyplot(fig)

    t_cells = _time_cell_strings(pd.to_datetime(display_times), day_precision=day_p)
    rows = []
    vals_f = _to_float_series(values)
    for i, t in enumerate(display_times):
        rows.append(
            {
                "时间": t_cells[i] if i < len(t_cells) else _format_ts(t, day_p),
                "区间流量(m³/s)": vals_f[i] if i < len(vals_f) else None,
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, height=260)


def _plot_station_tab(
    aux: Dict[str, Any],
    times: pd.DatetimeIndex,
    kind: str,
    sid: str,
    hist_steps: int,
    forecast_start_idx: int,
) -> None:
    key_map = {
        "雨量站": "station_precip",
        "蒸发站": "station_pet",
        "气温站": "station_temp",
        "流量站": "station_flow",
    }
    k = key_map.get(kind, "station_precip")
    series_map = dict(aux.get(k) or {})
    values = list(series_map.get(sid, [])) if sid else []

    fs = max(0, int(forecast_start_idx))
    hs = max(0, int(hist_steps))
    i0 = max(0, fs - hs)
    i1 = fs
    hist_times = times[i0:i1]
    hist_values = values[i0:i1] if values else []

    day_p = _is_day_precision(aux)
    if not sid or len(hist_times) == 0:
        st.info("无测站数据或历史展示时段为空")
        return
    x_dt = pd.to_datetime(hist_times)
    st_label = _station_ui_label(sid, aux.get("station_catalog_names"))
    st.subheader(f"{kind} · {st_label}（历史展示时段）")
    _configure_matplotlib_fonts()
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.plot(x_dt, _to_float_series(hist_values), color="#B279A2", linewidth=1.1)
    ax.set_ylabel("数值")
    ax.grid(True, alpha=0.25)
    _style_ax_time_x(ax, x_dt, day_precision=day_p)
    fig.tight_layout()
    _st_pyplot(fig)

    t_cells = _time_cell_strings(pd.to_datetime(hist_times), day_precision=day_p)
    rows = []
    for i, t in enumerate(hist_times):
        rows.append(
            {
                "时间": t_cells[i] if i < len(t_cells) else _format_ts(t, day_p),
                "数值": hist_values[i] if i < len(hist_values) else None,
            }
        )
    df_table = pd.DataFrame(rows)
    max_rows = 400
    if len(df_table) > max_rows:
        half = max_rows // 2
        df_table = pd.concat([df_table.head(half), df_table.tail(half)], axis=0)
        st.caption(f"表格仅展示前后各 {half} 行（完整数据看 JSON）。")
    st.dataframe(df_table, use_container_width=True, height=260)


def _load_best_scheme_for_time_type(config_path: str, time_type: str) -> Optional[Dict[str, Any]]:
    """
    与桌面端一致：同一 time_type 下取 `step_size` 最小的 scheme 作为默认填充值。
    """
    cfg = str(config_path or "").strip()
    if not cfg or not Path(cfg).is_file():
        return None
    schemes = read_schemes_list(cfg)
    return select_scheme_dict_smallest_step(schemes, time_type=str(time_type or ""))


def _load_scheme_for_time_scale(config_path: str, time_type: str, step_size: int) -> Optional[Dict[str, Any]]:
    """
    按 time_type + step_size 精确匹配 scheme。
    """
    cfg = str(config_path or "").strip()
    if not cfg or not Path(cfg).is_file():
        return None
    try:
        sz = int(step_size)
    except Exception:
        return None
    schemes = read_schemes_list(cfg)
    return select_scheme_dict_exact(schemes, time_type=str(time_type or ""), step_size=sz)


def _scheme_time_axis_defaults(config_path: str, time_type: str) -> Dict[str, Any]:
    """
    返回：step_size、forecast_mode、warmup_steps、correction_steps、historical_steps、forecast_steps。
    缺省时回退到当前输入值（由调用方决定），因此本函数尽量不抛异常。
    """
    scheme = _load_best_scheme_for_time_type(config_path, time_type)
    if not scheme:
        return {}

    axis = scheme.get("time_axis") or {}
    out: Dict[str, Any] = {}
    try:
        out["step_size"] = int(scheme.get("step_size", 1))
    except Exception:
        out["step_size"] = 1

    out["forecast_mode"] = str(scheme.get("forecast_mode") or "realtime_forecast").strip()

    def _get_axis_int(k: str, default: int) -> int:
        try:
            return int(axis.get(k, default))
        except Exception:
            return default

    out["warmup_steps"] = _get_axis_int("warmup_period_steps", 0)
    out["correction_steps"] = _get_axis_int("correction_period_steps", 0)
    out["historical_steps"] = _get_axis_int("historical_display_period_steps", 0)
    out["forecast_steps"] = _get_axis_int("forecast_period_steps", 24)
    return out


def _scheme_dbtype_mode_label(config_path: str, time_type: str, step_size: int) -> str:
    """
    只读展示当前时标模式：
    - dbtype=-1: 前时标
    - 其他值: 后时标
    """
    scheme = _load_scheme_for_time_scale(config_path, time_type, step_size)
    if not scheme:
        return "未匹配到当前 time_type+步长 方案（默认前时标）"
    try:
        dbtype = int(scheme.get("dbtype", -1))
    except Exception:
        dbtype = -1
    if dbtype == -1:
        return f"前时标（dbtype={dbtype}）"
    return f"后时标（dbtype={dbtype}）"


def _sync_time_axis_from_scheme() -> None:
    """
    Streamlit 回调：当用户切换 `time_type` 或修改配置路径时，
    自动从 `forecastSchemeConf.json`（对应 scheme）填入 step_size / time_axis 四段步数。
    """
    cfg = st.session_state.get("ui_config_path", "")
    tt = st.session_state.get("ui_time_type", "Hour")
    defaults = _scheme_time_axis_defaults(cfg, tt)
    if not defaults:
        return
    st.session_state["ui_step_size"] = int(defaults["step_size"])
    st.session_state["ui_forecast_mode"] = str(defaults["forecast_mode"])
    st.session_state["ui_warmup_steps"] = int(defaults["warmup_steps"])
    st.session_state["ui_correction_steps"] = int(defaults["correction_steps"])
    st.session_state["ui_historical_steps"] = int(defaults["historical_steps"])
    st.session_state["ui_forecast_steps"] = int(defaults["forecast_steps"])


def main() -> None:
    _configure_matplotlib_fonts()
    st.set_page_config(page_title="水文计算测试 (Streamlit)", layout="wide")
    _init_session()

    st.title("水文计算测试（Streamlit）")
    st.caption("通用计算入口 `run_calculation_pipeline` / 缓存复用；适合联调与演示。")

    default_cfg = str(PROJECT_ROOT / "configs" / "forecastSchemeConf.json")
    default_jdbc = str(DEFAULT_FLOOD_JDBC_CONFIG)
    default_rain = str(PROJECT_ROOT / "tests" / "佛子岭雨量.csv")
    default_flow = str(PROJECT_ROOT / "tests" / "佛子岭流量.csv")

    # 初始化：切换 time_type 时自动回填 scheme 的 time_axis / step_size
    if "ui_config_path" not in st.session_state:
        st.session_state["ui_config_path"] = default_cfg
    if "ui_time_type" not in st.session_state:
        st.session_state["ui_time_type"] = "Hour"
    if "ui_warmup_start" not in st.session_state:
        st.session_state["ui_warmup_start"] = "2025-09-01 00:00:00"
    if "ui_single_thread" not in st.session_state:
        st.session_state["ui_single_thread"] = False

    _init_defaults = _scheme_time_axis_defaults(st.session_state["ui_config_path"], st.session_state["ui_time_type"])
    if "ui_step_size" not in st.session_state:
        st.session_state["ui_step_size"] = int(_init_defaults.get("step_size", 1) or 1)
    if "ui_forecast_mode" not in st.session_state:
        st.session_state["ui_forecast_mode"] = str(_init_defaults.get("forecast_mode", "realtime_forecast") or "realtime_forecast")
    if "ui_warmup_steps" not in st.session_state:
        st.session_state["ui_warmup_steps"] = int(_init_defaults.get("warmup_steps", 0) or 0)
    if "ui_correction_steps" not in st.session_state:
        st.session_state["ui_correction_steps"] = int(_init_defaults.get("correction_steps", 0) or 0)
    if "ui_historical_steps" not in st.session_state:
        st.session_state["ui_historical_steps"] = int(_init_defaults.get("historical_steps", 0) or 0)
    if "ui_forecast_steps" not in st.session_state:
        st.session_state["ui_forecast_steps"] = int(_init_defaults.get("forecast_steps", 24) or 24)

    with st.sidebar:
        st.header("参数")
        config_path = st.text_input(
            "预报方案 JSON",
            value=default_cfg,
            key="ui_config_path",
            on_change=_sync_time_axis_from_scheme,
        )
        jdbc_config_path = st.text_input("floodForecastJdbc.json", value=default_jdbc)
        rain_csv = st.text_input("雨量 CSV 或库表配置", value=default_rain)
        flow_csv = st.text_input("流量 CSV 或库表配置", value=default_flow)
        warmup_start = st.text_input("预报起报时间", value=st.session_state["ui_warmup_start"], key="ui_warmup_start")
        forecast_mode = st.selectbox(
            "预报模式",
            options=["realtime_forecast", "historical_simulation"],
            index=0,
            key="ui_forecast_mode",
        )
        single_thread = st.checkbox("单线程计算（子流域）", value=st.session_state["ui_single_thread"], key="ui_single_thread")
        time_type = st.selectbox(
            "时间类型",
            options=["Hour", "Day", "Minute"],
            index=["Hour", "Day", "Minute"].index(str(st.session_state.get("ui_time_type", "Hour"))),
            key="ui_time_type",
            on_change=_sync_time_axis_from_scheme,
        )
        step_size = st.number_input("步长", min_value=1, value=int(st.session_state["ui_step_size"]), step=1, key="ui_step_size")
        dbtype_mode_label = _scheme_dbtype_mode_label(str(config_path), str(time_type), int(step_size))
        st.text_input(
            "当前时标模式（只读展示）",
            value=dbtype_mode_label,
            disabled=True,
            help="按当前 time_type + 步长精确匹配方案读取 dbtype。",
        )
        warmup_steps = st.number_input(
            "总预热步数",
            min_value=0,
            value=int(st.session_state["ui_warmup_steps"]),
            step=1,
            key="ui_warmup_steps",
            help="与配置中 warmup_period_steps 一致：自预报起点 T0 向历史的总预热长度 W（步），"
            "不是分段之一；模拟从 T0−W·Δt 起至 T0 前共 W 步。须 ≥ 历史展示 H 与校正 C。",
        )
        correction_steps = st.number_input(
            "校正步数",
            min_value=0,
            value=int(st.session_state["ui_correction_steps"]),
            step=1,
            key="ui_correction_steps",
            help="T0 前最近 C 步，时间窗 [T0−C·Δt, T0)，用于实测校正（如 AR1 残差）。须 C≤H≤W。",
        )
        historical_steps = st.number_input(
            "历史展示步数",
            min_value=0,
            value=int(st.session_state["ui_historical_steps"]),
            step=1,
            key="ui_historical_steps",
            help="T0 前最近 H 步，时间窗 [T0−H·Δt, T0)；Web 产流图从 display_start 起画（d0=W−H）。须 H≤W。",
        )
        forecast_steps = st.number_input(
            "预报步数", min_value=1, value=int(st.session_state["ui_forecast_steps"]), step=1, key="ui_forecast_steps"
        )
        st.caption("预报面雨情景 CSV（可选）：列 time / expected / upper / lower；可选 pet；可选 catchment_id。")
        forecast_scenario_rain_csv = st.text_input(
            "预报面雨情景 CSV 路径",
            value=str(st.session_state.get("ui_forecast_scenario_rain_csv", "") or ""),
            key="ui_forecast_scenario_rain_csv",
            help="与引擎 forecast_start 起报对齐；步长须与方案一致。无 catchment_id 列时需填下方子流域 id（唯一）。",
        )
        forecast_scenario_default_catchment_ids = st.text_input(
            "单表时的子流域 ID（逗号分隔）",
            value=str(st.session_state.get("ui_forecast_scenario_default_cids", "") or ""),
            key="ui_forecast_scenario_default_cids",
        )
        _scen_opts = ["expected", "upper", "lower"]
        _scen_cur = str(st.session_state.get("ui_forecast_scenario_precip", "expected") or "expected").lower()
        forecast_scenario_precipitation = st.selectbox(
            "主结果降水情景",
            options=_scen_opts,
            index=_scen_opts.index(_scen_cur) if _scen_cur in _scen_opts else 0,
            key="ui_forecast_scenario_precip",
        )
        forecast_run_multiscenario = st.checkbox(
            "同时计算三情景（expected/upper/lower）",
            value=bool(st.session_state.get("ui_forecast_run_multiscenario", False)),
            key="ui_forecast_run_multiscenario",
            help="勾选后引擎跑三遍，主图表仍用「主结果降水情景」；完整结果在输出字典 multiscenario_engine_outputs。",
        )

        side = {
            "config_path": config_path,
            "jdbc_config_path": jdbc_config_path,
            "rain_csv": rain_csv,
            "flow_csv": flow_csv,
            "warmup_start": warmup_start,
            "forecast_mode": forecast_mode,
            "single_thread": single_thread,
            "time_type": time_type,
            "step_size": step_size,
            "warmup_steps": warmup_steps,
            "correction_steps": correction_steps,
            "historical_steps": historical_steps,
            "forecast_steps": forecast_steps,
            "forecast_scenario_rain_csv": forecast_scenario_rain_csv,
            "forecast_scenario_default_catchment_ids": forecast_scenario_default_catchment_ids,
            "forecast_scenario_precipitation": forecast_scenario_precipitation,
            "forecast_run_multiscenario": forecast_run_multiscenario,
        }

        col_a, col_b = st.columns(2)
        with col_a:
            load_btn = st.button("读取数据", type="secondary", use_container_width=True)
        with col_b:
            run_btn = st.button("预报计算", type="primary", use_container_width=True)

        st.divider()
        st.caption(f"缓存状态: {'已加载（可复用）' if st.session_state.hydro_runtime_cache is not None else '无'}")
        if st.button("清除会话缓存"):
            st.session_state.hydro_runtime_cache = None
            st.session_state.hydro_runtime_cache_key = None
            st.session_state.hydro_output = None
            st.session_state.hydro_times = None
            st.session_state.hydro_aux = None
            st.session_state.hydro_warns = []
            st.session_state.hydro_logs = []
            st.rerun()

    params = _gather_params(side)
    ck = _cache_key_from_params(params)

    if load_btn:
        st.session_state.hydro_logs = []
        _append_log("[ui] 读取数据…")
        try:
            with st.spinner("正在读取数据…"):
                out, times, warns, aux = run_calculation_pipeline(
                    config_path=params["config_path"],
                    jdbc_config_path=params["jdbc_config_path"],
                    rain_csv=params["rain_csv"],
                    flow_csv=params["flow_csv"],
                    warmup_start=params["warmup_start"],
                    forecast_mode=params["forecast_mode"],
                    catchment_workers=params["catchment_workers"],
                    time_type=params["time_type"],
                    step_size=params["step_size"],
                    warmup_steps=params["warmup_steps"],
                    correction_steps=params["correction_steps"],
                    historical_steps=params["historical_steps"],
                    forecast_steps=params["forecast_steps"],
                    compute_forecast=False,
                    forecast_scenario_rain_csv=params["forecast_scenario_rain_csv"],
                    forecast_scenario_default_catchment_ids=params["forecast_scenario_default_catchment_ids"],
                    forecast_scenario_precipitation=params["forecast_scenario_precipitation"],
                    forecast_run_multiscenario=params["forecast_run_multiscenario"],
                    on_log=_append_log,
                )
            st.session_state.hydro_output = out
            st.session_state.hydro_times = times
            st.session_state.hydro_warns = warns
            st.session_state.hydro_aux = aux
            st.session_state.hydro_runtime_cache = aux.get("_runtime_cache")
            st.session_state.hydro_runtime_cache_key = ck
            st.session_state.hydro_status = "数据读取完成"
            _append_log("[done] 读取完成")
            st.success("数据读取完成（未跑预报）；可查看测站与 Node 实测侧，或点「预报计算」。")
        except Exception as exc:  # noqa: BLE001
            st.session_state.hydro_status = "失败"
            _append_log(f"[error] {exc}\n{traceback.format_exc()}")
            st.error(f"读取失败: {exc}")

    if run_btn:
        st.session_state.hydro_logs = []
        _append_log("[ui] 预报计算…")
        try:
            with st.spinner("正在计算…"):
                use_cache = (
                    st.session_state.hydro_runtime_cache is not None
                    and st.session_state.hydro_runtime_cache_key == ck
                )
                if use_cache:
                    _append_log("[ui] 使用内存缓存（跳过读库）")
                    out, times, warns, aux = run_forecast_from_runtime_cache(
                        runtime_cache=st.session_state.hydro_runtime_cache,
                        forecast_mode=params["forecast_mode"],
                        catchment_workers=params["catchment_workers"],
                        time_type=params["time_type"],
                        step_size=params["step_size"],
                        scenario_precipitation=params["forecast_scenario_precipitation"],
                        forecast_multiscenario=params["forecast_run_multiscenario"],
                        on_log=_append_log,
                    )
                else:
                    out, times, warns, aux = run_calculation_pipeline(
                        config_path=params["config_path"],
                        jdbc_config_path=params["jdbc_config_path"],
                        rain_csv=params["rain_csv"],
                        flow_csv=params["flow_csv"],
                        warmup_start=params["warmup_start"],
                        forecast_mode=params["forecast_mode"],
                        catchment_workers=params["catchment_workers"],
                        time_type=params["time_type"],
                        step_size=params["step_size"],
                        warmup_steps=params["warmup_steps"],
                        correction_steps=params["correction_steps"],
                        historical_steps=params["historical_steps"],
                        forecast_steps=params["forecast_steps"],
                        compute_forecast=True,
                        forecast_scenario_rain_csv=params["forecast_scenario_rain_csv"],
                        forecast_scenario_default_catchment_ids=params["forecast_scenario_default_catchment_ids"],
                        forecast_scenario_precipitation=params["forecast_scenario_precipitation"],
                        forecast_run_multiscenario=params["forecast_run_multiscenario"],
                        on_log=_append_log,
                    )
                    st.session_state.hydro_runtime_cache = aux.get("_runtime_cache")
                    st.session_state.hydro_runtime_cache_key = ck

            st.session_state.hydro_output = out
            st.session_state.hydro_times = times
            st.session_state.hydro_warns = warns
            st.session_state.hydro_aux = aux
            st.session_state.hydro_status = "计算完成"
            _append_log("[done] 计算完成")
            st.success("预报计算完成")
        except Exception as exc:  # noqa: BLE001
            st.session_state.hydro_status = "失败"
            _append_log(f"[error] {exc}\n{traceback.format_exc()}")
            st.error(f"计算失败: {exc}")

    st.subheader(f"状态: {st.session_state.hydro_status}")

    with st.expander("运行日志", expanded=False):
        if st.session_state.hydro_logs:
            st.code("\n".join(st.session_state.hydro_logs[-200:]), language="text")
        else:
            st.caption("尚无日志")

    out = st.session_state.hydro_output
    times = st.session_state.hydro_times
    aux = st.session_state.hydro_aux or {}
    warns = st.session_state.hydro_warns or []

    if warns:
        with st.expander(f"数据提示 ({len(warns)} 条)", expanded=False):
            st.text("\n".join(warns[:80]) + ("\n…" if len(warns) > 80 else ""))

    ftime_info = aux.get("forecast_rain_ftime_info") or {}
    source_rows = ftime_info.get("source_rows") or []
    if source_rows:
        with st.expander("预报降雨 FTIME 信息", expanded=False):
            req = ftime_info.get("request") or {}
            if req:
                st.caption(
                    "请求参数："
                    f"time_type={req.get('time_type')} "
                    f"step={req.get('step_size')} "
                    f"dbtype={req.get('dbtype')} "
                    f"begin={req.get('forecast_begin')} "
                    f"end={req.get('forecast_end')}"
                )
            rows: List[Dict[str, Any]] = []
            for r in source_rows:
                ftime_list = [str(x) for x in (r.get("ftime") or [])]
                rows.append(
                    {
                        "subtype": str(r.get("subtype", "")),
                        "span(h)": int(r.get("span_hours", 0) or 0),
                        "records": int(r.get("records", 0) or 0),
                        "FTIME": ", ".join(ftime_list),
                    }
                )
            st.dataframe(pd.DataFrame(rows), use_container_width=True, height=220)

    if out is None or times is None:
        st.info("请先在侧栏配置路径与时间参数，点击「读取数据」或「预报计算」。")
        return

    hist_ui = int(side["historical_steps"])
    fs_idx = int(aux.get("forecast_start_idx", 0))

    tab_node, tab_runoff, tab_routed, tab_reach, tab_interval, tab_station, tab_dbg, tab_json = st.tabs(
        ["Node 流量", "流域产流", "流域汇流", "河段", "区间通道", "测站数据", "Debug", "JSON"]
    )

    node_name_map = aux.get("node_name_map") or {}
    node_ids = sorted(set((out.get("node_total_inflows") or {}).keys()) | set((out.get("node_outflows") or {}).keys()))
    labels_n = [f"{node_name_map.get(n, n)} ({n})" if node_name_map.get(n, n) != str(n) else str(n) for n in node_ids]

    with tab_node:
        if not node_ids:
            st.info("无 Node 序列")
        else:
            pick = st.selectbox("选择节点", options=node_ids, format_func=lambda x: labels_n[node_ids.index(x)])
            _plot_node_tab(out, aux, times, hist_ui, pick)

    catch_map = out.get("catchment_runoffs") or {}
    cids = sorted(catch_map.keys())
    cnm = aux.get("catchment_catalog_names") or {}
    with tab_runoff:
        if not cids:
            st.info("无产流序列（若仅读取数据，需先预报计算）")
        else:
            pick = st.selectbox(
                "子流域",
                options=cids,
                key="runoff_cid",
                format_func=lambda x, m=cnm: _catchment_display_label(str(x), m),
            )
            rain_src = aux.get("catchment_rain") or {}
            _plot_hydro_pair_tab(
                "产流",
                catch_map,
                rain_src,
                times,
                pick,
                hist_ui,
                fs_idx,
                aux,
                show_right_table=True,
                display_label=_catchment_display_label(pick, cnm),
            )

    routed_map = out.get("catchment_routed_flows") or {}
    rids = sorted(routed_map.keys())
    with tab_routed:
        if not rids:
            st.info("无汇流序列")
        else:
            pick = st.selectbox(
                "子流域",
                options=rids,
                key="routed_cid",
                format_func=lambda x, m=cnm: _catchment_display_label(str(x), m),
            )
            rain_src = aux.get("catchment_rain") or {}
            _plot_hydro_pair_tab(
                "汇流",
                routed_map,
                rain_src,
                times,
                pick,
                hist_ui,
                fs_idx,
                aux,
                show_right_table=True,
                display_label=_catchment_display_label(pick, cnm),
            )

    reach_map = out.get("reach_flows") or {}
    rkeys = sorted(reach_map.keys())
    with tab_reach:
        if not rkeys:
            st.info("无河段序列")
        else:
            pick = st.selectbox("河段", options=rkeys)
            _plot_reach_tab(out, times, hist_ui, fs_idx, pick, aux)

    with tab_interval:
        _plot_interval_channel_tab(out, aux, times, hist_ui, fs_idx)

    with tab_station:
        kind = st.selectbox("测站类型", options=["雨量站", "蒸发站", "气温站", "流量站"])
        smap = {
            "雨量站": aux.get("station_precip") or {},
            "蒸发站": aux.get("station_pet") or {},
            "气温站": aux.get("station_temp") or {},
            "流量站": aux.get("station_flow") or {},
        }
        sids = sorted((smap.get(kind) or {}).keys())
        if not sids:
            st.info("该类型无测站序列")
        else:
            nm_map = dict(aux.get("station_catalog_names") or {})
            sid = st.selectbox(
                "测站",
                options=sids,
                format_func=lambda x, m=nm_map: _station_ui_label(str(x), m),
            )
            _plot_station_tab(aux, times, kind, sid, hist_ui, fs_idx)

    dbg = out.get("catchment_debug_traces") or {}
    dbg_ids = sorted(dbg.keys()) if isinstance(dbg, dict) else []
    with tab_dbg:
        if not dbg_ids:
            st.info("无 debug_trace（可在产流模型 params 中设 debug_trace=true）")
        else:
            dc = st.selectbox("子流域", options=dbg_ids)
            rows = dbg.get(dc) or []
            cols = _infer_debug_table_columns(rows)
            if cols and rows:
                st.dataframe(pd.DataFrame([{c: r.get(c, "") for c in cols} for r in rows]), use_container_width=True)
            else:
                st.caption("无行数据")

    with tab_json:
        import json

        blob = json.dumps(out, ensure_ascii=False, indent=2)
        st.download_button("下载 JSON", data=blob.encode("utf-8"), file_name="hydro_output.json", mime="application/json")
        st.text_area("输出预览（可滚动）", value=blob[:120000] + ("\n… 已截断" if len(blob) > 120000 else ""), height=400)


if __name__ == "__main__":
    main()
