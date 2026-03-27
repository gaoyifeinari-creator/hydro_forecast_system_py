"""本地桌面界面：水文计算测试（tkinter + matplotlib，不依赖浏览器与 Streamlit）。"""

from __future__ import annotations

import json
import tempfile
import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import sys

SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from calculation_app_common import (
    build_catchment_observed_flow_series,
    build_catchment_precip_series,
    build_node_observed_flow_series,
    build_node_precip_series,
    build_observed_flows,
    build_station_packages,
    build_times,
    load_csv,
    read_config,
)
from hydro_engine.core.forcing import ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.io.json_config import load_scheme_from_json, run_calculation_from_json


def _write_temp_config_with_forecast_anchor(
    config_path: str,
    *,
    forecast_start: datetime,
    forecast_mode: str,
    time_type: str,
    step_size: int,
    warmup_steps: int,
    correction_steps: int,
    historical_steps: int,
    forecast_steps: int,
) -> str:
    data = read_config(config_path)
    schemes = data.get("schemes") or []
    if not schemes:
        raise ValueError("配置缺少 schemes")

    target = None
    for s in schemes:
        if str(s.get("time_type")) == str(time_type) and int(s.get("step_size")) == int(step_size):
            target = s
            break
    if target is None:
        raise ValueError(f"未找到匹配方案：time_type={time_type}, step_size={step_size}")

    ttype = str(time_type).strip().lower()
    if ttype == "hour":
        td = pd.Timedelta(hours=int(step_size))
    elif ttype == "day":
        td = pd.Timedelta(days=int(step_size))
    elif ttype == "minute":
        td = pd.Timedelta(minutes=int(step_size))
    else:
        raise ValueError(f"Unsupported time_type: {time_type}")

    fc = pd.Timestamp(forecast_start)
    w = int(warmup_steps)
    c = int(correction_steps)
    h = int(historical_steps)
    f = int(forecast_steps)
    if f < 1:
        raise ValueError("预报步数必须 >= 1")
    if min(w, c, h) < 0:
        raise ValueError("预热/校正/历史展示步数必须 >= 0")

    warmup_start = fc - td * w
    correction_start = fc - td * c
    display_start = fc - td * h
    end_time = fc + td * f

    target["time_axis"] = {
        "warmup_start_time": warmup_start.to_pydatetime().isoformat(sep=" "),
        "correction_start_time": correction_start.to_pydatetime().isoformat(sep=" "),
        "display_start_time": display_start.to_pydatetime().isoformat(sep=" "),
        "forecast_start_time": fc.to_pydatetime().isoformat(sep=" "),
        "end_time": end_time.to_pydatetime().isoformat(sep=" "),
    }
    target["forecast_mode"] = str(forecast_mode)

    # 历史模拟模式：允许“使用实测值演进”的节点在预报时段之后继续使用实测出流接力。
    if str(forecast_mode) == "historical_simulation":
        for node in (target.get("nodes") or []):
            if bool(node.get("use_observed_for_routing", node.get("bHisCalcToPar", False))):
                node["use_observed_for_routing_after_forecast"] = True

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
    json.dump(data, tmp, ensure_ascii=False, indent=2)
    tmp.close()
    return tmp.name


def _configure_matplotlib_fonts() -> None:
    import matplotlib

    matplotlib.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans", "sans-serif"]
    matplotlib.rcParams["axes.unicode_minus"] = False


def run_calculation_pipeline(
    *,
    config_path: str,
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
) -> Tuple[Dict[str, Any], pd.DatetimeIndex, List[str], Dict[str, Any]]:
    forecast_start_dt = datetime.fromisoformat(warmup_start.replace("T", " "))
    temp_cfg = _write_temp_config_with_forecast_anchor(
        config_path,
        forecast_start=forecast_start_dt,
        forecast_mode=str(forecast_mode),
        time_type=time_type,
        step_size=int(step_size),
        warmup_steps=int(warmup_steps),
        correction_steps=int(correction_steps),
        historical_steps=int(historical_steps),
        forecast_steps=int(forecast_steps),
    )

    scheme, binding_specs, time_context = load_scheme_from_json(
        temp_cfg,
        time_type=time_type,
        step_size=int(step_size),
        warmup_start_time=None,
    )

    times = build_times(
        context_start=time_context.warmup_start_time,
        step=time_context.time_delta,
        count=time_context.step_count,
    )

    rain_df = load_csv(rain_csv)
    flow_df = load_csv(flow_csv)

    station_packages, warn_a = build_station_packages(
        binding_specs,
        rain_df,
        times,
        time_context.warmup_start_time,
        time_context.time_delta,
    )
    # 实时预报模式：起报时刻之后不使用实测气象（未来应由外部预报驱动）。
    forecast_start_idx = times.get_indexer([pd.Timestamp(time_context.forecast_start_time)])[0]
    if forecast_start_idx < 0:
        raise ValueError("无法在时间轴上定位预报起报时刻")
    is_realtime = str(forecast_mode) == "realtime_forecast"
    if is_realtime:
        for sid, pkg in list(station_packages.items()):
            patched = pkg
            for kind in (ForcingKind.PRECIPITATION, ForcingKind.POTENTIAL_EVAPOTRANSPIRATION):
                s = patched.get(kind)
                if s is None:
                    continue
                vals = list(s.values)
                if forecast_start_idx < len(vals):
                    vals[forecast_start_idx:] = [0.0] * (len(vals) - forecast_start_idx)
                patched = patched.with_series(
                    kind,
                    TimeSeries(start_time=s.start_time, time_step=s.time_step, values=vals),
                )
            station_packages[sid] = patched
    observed_flows, warn_b = build_observed_flows(
        scheme,
        flow_df,
        times,
        time_context.warmup_start_time,
        time_context.time_delta,
    )

    node_observed_inflows: Dict[str, List[float]] = {}
    node_observed_outflows: Dict[str, List[float]] = {}
    for nid, node in scheme.nodes.items():
        in_sid = str(getattr(node, "observed_inflow_station_id", "")).strip()
        out_sid = str(getattr(node, "observed_station_id", "")).strip()
        if in_sid and in_sid in observed_flows:
            node_observed_inflows[str(nid)] = list(observed_flows[in_sid].values)
        if out_sid and out_sid in observed_flows:
            node_observed_outflows[str(nid)] = list(observed_flows[out_sid].values)

    catchment_rain, warn_c = build_catchment_precip_series(binding_specs, rain_df, times)
    if is_realtime:
        for cid, vals in list(catchment_rain.items()):
            if forecast_start_idx < len(vals):
                vals = list(vals)
                vals[forecast_start_idx:] = [0.0] * (len(vals) - forecast_start_idx)
                catchment_rain[cid] = vals
    node_rain = build_node_precip_series(scheme, catchment_rain)
    node_obs = build_node_observed_flow_series(scheme, observed_flows)
    catchment_obs = build_catchment_observed_flow_series(scheme, node_obs)

    output = run_calculation_from_json(
        config_path=temp_cfg,
        station_packages=station_packages,
        time_type=time_type,
        step_size=int(step_size),
        warmup_start_time=None,
        observed_flows=observed_flows,
        forecast_mode=str(forecast_mode),
        catchment_workers=catchment_workers,
    )

    aux = {
        "node_rain": node_rain,
        "catchment_rain": catchment_rain,
        "node_observed": node_obs,
        "node_observed_inflows": node_observed_inflows,
        "node_observed_outflows": node_observed_outflows,
        "catchment_observed": catchment_obs,
        "forecast_start_idx": int(forecast_start_idx),
        "forecast_mode": str(forecast_mode),
        "node_name_map": {str(nid): (getattr(n, "name", "") or str(nid)) for nid, n in scheme.nodes.items()},
        "catchment_name_map": {
            str(cid): (str(cid))
            for cid in scheme.catchments.keys()
        },
        "reach_name_map": {str(rid): str(rid) for rid in scheme.reaches.keys()},
    }
    return output, times, warn_a + warn_b + warn_c, aux


class DesktopCalculationApp:
    def __init__(self) -> None:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk

        self._tk = tk
        self._filedialog = filedialog
        self._messagebox = messagebox
        self._ttk = ttk

        _configure_matplotlib_fonts()
        import matplotlib
        matplotlib.use("TkAgg")
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        from matplotlib.figure import Figure

        self._Figure = Figure
        self._FigureCanvasTkAgg = FigureCanvasTkAgg

        self.root = tk.Tk()
        self.root.title("水文计算测试（本地客户端）")
        self.root.minsize(960, 640)
        self.root.geometry("1100x720")

        default_cfg = str(PROJECT_ROOT / "configs" / "forecastSchemeConf.json")
        default_rain = str(PROJECT_ROOT / "tests" / "佛子岭雨量.csv")
        default_flow = str(PROJECT_ROOT / "tests" / "佛子岭流量.csv")

        main = ttk.Frame(self.root, padding=8)
        main.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(main, width=360)
        left.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 8))

        right = ttk.Frame(main)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self._var_cfg = tk.StringVar(value=default_cfg)
        self._var_rain = tk.StringVar(value=default_rain)
        self._var_flow = tk.StringVar(value=default_flow)
        self._var_warmup_start = tk.StringVar(value="2024-01-01 01:00:00")
        self._var_forecast_mode = tk.StringVar(value="realtime_forecast")
        self._var_single_thread = tk.BooleanVar(value=False)
        self._var_time_type = tk.StringVar(value="Hour")
        self._var_step = tk.IntVar(value=1)
        self._var_warmup_steps = tk.IntVar(value=0)
        self._var_corr_steps = tk.IntVar(value=0)
        self._var_hist_steps = tk.IntVar(value=0)
        self._var_fc_steps = tk.IntVar(value=24)

        self._status = tk.StringVar(value="就绪")
        self._last_output: Optional[Dict[str, Any]] = None
        self._last_times: Optional[pd.DatetimeIndex] = None
        self._last_aux: Dict[str, Any] = {}
        self._panel_tables: Dict[str, Any] = {}
        self._panel_info_vars: Dict[str, Any] = {}
        self._panel_data: Dict[str, Dict[str, Any]] = {}
        self._panel_hover_cids: Dict[str, int] = {}
        self._combo_id_maps: Dict[str, Dict[str, str]] = {}
        self._debug_rows_by_catchment: Dict[str, List[Dict[str, Any]]] = {}

        row = 0
        for label, var, browse in [
            ("预报方案 JSON", self._var_cfg, "json"),
            ("雨量 CSV（V）", self._var_rain, "csv"),
            ("流量 CSV（AVGV）", self._var_flow, "csv"),
        ]:
            ttk.Label(left, text=label).grid(row=row, column=0, sticky=tk.W, pady=2)
            e = ttk.Entry(left, textvariable=var, width=42)
            e.grid(row=row, column=1, sticky=tk.EW, pady=2)
            if browse == "json":
                ttk.Button(left, text="浏览…", command=lambda v=var: self._pick_file(v, [("JSON", "*.json")])).grid(
                    row=row, column=2, padx=4
                )
            else:
                ttk.Button(left, text="浏览…", command=lambda v=var: self._pick_file(v, [("CSV", "*.csv")])).grid(
                    row=row, column=2, padx=4
                )
            row += 1

        left.columnconfigure(1, weight=1)

        ttk.Label(left, text="预报起报时间").grid(row=row, column=0, sticky=tk.W, pady=2)
        ttk.Entry(left, textvariable=self._var_warmup_start, width=42).grid(row=row, column=1, columnspan=2, sticky=tk.EW)
        row += 1

        ttk.Label(left, text="预报模式").grid(row=row, column=0, sticky=tk.W, pady=2)
        mode_combo = ttk.Combobox(
            left,
            textvariable=self._var_forecast_mode,
            values=("realtime_forecast", "historical_simulation"),
            state="readonly",
            width=20,
        )
        mode_combo.grid(row=row, column=1, sticky=tk.W)
        row += 1

        self._ttk.Checkbutton(left, text="单线程计算", variable=self._var_single_thread).grid(
            row=row, column=0, columnspan=2, sticky=tk.W, pady=2
        )
        row += 1

        ttk.Label(left, text="时间类型").grid(row=row, column=0, sticky=tk.W, pady=2)
        ttk.Combobox(
            left,
            textvariable=self._var_time_type,
            values="Hour Day Minute".split(),
            state="readonly",
            width=12,
        ).grid(row=row, column=1, sticky=tk.W)
        row += 1

        def spin_row(label: str, var: tk.IntVar, frm: int, to: int) -> None:
            nonlocal row
            ttk.Label(left, text=label).grid(row=row, column=0, sticky=tk.W, pady=2)
            ttk.Spinbox(left, from_=frm, to=to, textvariable=var, width=12).grid(row=row, column=1, sticky=tk.W)
            row += 1

        spin_row("步长", self._var_step, 1, 999999)
        spin_row("预热步数", self._var_warmup_steps, 0, 999999)
        spin_row("校正步数", self._var_corr_steps, 0, 999999)
        spin_row("历史展示步数", self._var_hist_steps, 0, 999999)
        spin_row("预报步数", self._var_fc_steps, 1, 999999)

        btn_frame = ttk.Frame(left)
        btn_frame.grid(row=row, column=0, columnspan=3, pady=12, sticky=tk.EW)
        row += 1
        ttk.Button(btn_frame, text="开始计算", command=self._on_run).pack(side=tk.LEFT)
        ttk.Label(btn_frame, textvariable=self._status).pack(side=tk.LEFT, padx=12)

        nb = ttk.Notebook(right)
        nb.pack(fill=tk.BOTH, expand=True)

        self._fig_node = Figure(figsize=(7, 4), dpi=100)
        self._fig_catch = Figure(figsize=(7, 4), dpi=100)
        self._ax_node = self._fig_node.add_subplot(111)
        self._ax_catch = self._fig_catch.add_subplot(111)

        frame_node = ttk.Frame(nb, padding=4)
        ttk.Label(frame_node, text="Node 流量").pack(anchor=tk.W)
        self._combo_node = ttk.Combobox(frame_node, state="readonly", width=48)
        self._combo_node.pack(anchor=tk.W, pady=4)
        self._combo_node.bind("<<ComboboxSelected>>", lambda e: self._redraw_plot("node"))
        node_area = ttk.Frame(frame_node)
        node_area.pack(fill=tk.BOTH, expand=True)
        node_plot_holder = self._install_plot_table_panel("node_inflow", node_area)
        self._canvas_node = self._FigureCanvasTkAgg(self._fig_node, master=node_plot_holder)
        self._canvas_node.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        frame_catch = ttk.Frame(nb, padding=4)
        ttk.Label(frame_catch, text="流域产流（Catchment runoffs）").pack(anchor=tk.W)
        self._combo_runoff = ttk.Combobox(frame_catch, state="readonly", width=48)
        self._combo_runoff.pack(anchor=tk.W, pady=4)
        self._combo_runoff.bind("<<ComboboxSelected>>", lambda e: self._redraw_plot("runoff"))
        runoff_area = ttk.Frame(frame_catch)
        runoff_area.pack(fill=tk.BOTH, expand=True)
        runoff_plot_holder = self._install_plot_table_panel("runoff", runoff_area)
        self._canvas_runoff = self._FigureCanvasTkAgg(self._fig_catch, master=runoff_plot_holder)
        self._canvas_runoff.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        ttk.Separator(frame_catch, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=8)
        ttk.Label(frame_catch, text="流域汇流（Catchment routed）").pack(anchor=tk.W)
        self._combo_routed = ttk.Combobox(frame_catch, state="readonly", width=48)
        self._combo_routed.pack(anchor=tk.W, pady=4)
        self._combo_routed.bind("<<ComboboxSelected>>", lambda e: self._redraw_plot("routed"))
        self._fig_catch2 = Figure(figsize=(7, 3.5), dpi=100)
        self._ax_catch2 = self._fig_catch2.add_subplot(111)
        routed_area = ttk.Frame(frame_catch)
        routed_area.pack(fill=tk.BOTH, expand=True)
        routed_plot_holder = self._install_plot_table_panel("routed", routed_area)
        self._canvas_routed = self._FigureCanvasTkAgg(self._fig_catch2, master=routed_plot_holder)
        self._canvas_routed.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        frame_reach = ttk.Frame(nb, padding=4)
        ttk.Label(frame_reach, text="河段").pack(anchor=tk.W)
        self._combo_reach = ttk.Combobox(frame_reach, state="readonly", width=48)
        self._combo_reach.pack(anchor=tk.W, pady=4)
        self._combo_reach.bind("<<ComboboxSelected>>", lambda e: self._redraw_plot("reach"))
        self._fig_reach = Figure(figsize=(7, 4), dpi=100)
        self._ax_reach = self._fig_reach.add_subplot(111)
        reach_area = ttk.Frame(frame_reach)
        reach_area.pack(fill=tk.BOTH, expand=True)
        reach_plot_holder = self._install_plot_table_panel("reach", reach_area)
        self._canvas_reach = self._FigureCanvasTkAgg(self._fig_reach, master=reach_plot_holder)
        self._canvas_reach.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        frame_json = ttk.Frame(nb, padding=4)
        self._json_text = tk.Text(frame_json, wrap=tk.NONE, height=20, font=("Consolas", 9))
        ys = ttk.Scrollbar(frame_json, orient=tk.VERTICAL, command=self._json_text.yview)
        xs = ttk.Scrollbar(frame_json, orient=tk.HORIZONTAL, command=self._json_text.xview)
        self._json_text.configure(yscrollcommand=ys.set, xscrollcommand=xs.set)
        self._json_text.grid(row=0, column=0, sticky="nsew")
        ys.grid(row=0, column=1, sticky="ns")
        xs.grid(row=1, column=0, sticky="ew")
        frame_json.rowconfigure(0, weight=1)
        frame_json.columnconfigure(0, weight=1)

        frame_debug = ttk.Frame(nb, padding=4)
        top_debug = ttk.Frame(frame_debug)
        top_debug.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(top_debug, text="Catchment").pack(side=tk.LEFT)
        self._combo_debug_catch = ttk.Combobox(top_debug, state="readonly", width=24)
        self._combo_debug_catch.pack(side=tk.LEFT, padx=6)
        self._combo_debug_catch.bind("<<ComboboxSelected>>", lambda e: self._refresh_debug_table())
        self._debug_hint = tk.StringVar(value="开启某个流域 runoff_model.params.debug_trace=true 后可在此查看逐步中间量")
        ttk.Label(top_debug, textvariable=self._debug_hint).pack(side=tk.LEFT, padx=8)

        cols = ("time", "step", "p", "pet", "pe", "r", "rs", "rss", "rg", "qtr", "out", "wu", "wl", "wd", "fr", "s")
        self._debug_tree = ttk.Treeview(frame_debug, columns=cols, show="headings", height=18)
        for c in cols:
            self._debug_tree.heading(c, text=c)
            self._debug_tree.column(c, width=86 if c != "time" else 150, anchor=tk.E if c != "time" else tk.CENTER)
        d_ys = ttk.Scrollbar(frame_debug, orient=tk.VERTICAL, command=self._debug_tree.yview)
        d_xs = ttk.Scrollbar(frame_debug, orient=tk.HORIZONTAL, command=self._debug_tree.xview)
        self._debug_tree.configure(yscrollcommand=d_ys.set, xscrollcommand=d_xs.set)
        self._debug_tree.pack(fill=tk.BOTH, expand=True)
        d_ys.pack(side=tk.RIGHT, fill=tk.Y)
        d_xs.pack(fill=tk.X)

        nb.add(frame_node, text="Node 流量")
        nb.add(frame_catch, text="Catchment")
        nb.add(frame_reach, text="Reach")
        nb.add(frame_json, text="原始 JSON")
        nb.add(frame_debug, text="XAJCS Debug")

        self._combo_node["values"] = ()
        self._combo_runoff["values"] = ()
        self._combo_routed["values"] = ()
        self._combo_reach["values"] = ()
        self._combo_debug_catch["values"] = ()
        self._load_time_defaults_from_config(default_cfg)

    def _load_time_defaults_from_config(self, config_path: str) -> None:
        try:
            data = read_config(config_path)
        except Exception:
            return
        schemes = data.get("schemes") or []
        if not schemes:
            return
        scheme = schemes[0]
        time_type = str(scheme.get("time_type") or "").strip()
        if time_type in {"Hour", "Day", "Minute"}:
            self._var_time_type.set(time_type)
        try:
            self._var_step.set(int(scheme.get("step_size", self._var_step.get())))
        except Exception:
            pass

        axis = scheme.get("time_axis") or {}
        mode = str(scheme.get("forecast_mode", self._var_forecast_mode.get() or "realtime_forecast")).strip()
        if mode in {"realtime_forecast", "historical_simulation"}:
            self._var_forecast_mode.set(mode)
        for k, var in [
            ("warmup_period_steps", self._var_warmup_steps),
            ("correction_period_steps", self._var_corr_steps),
            ("historical_display_period_steps", self._var_hist_steps),
            ("forecast_period_steps", self._var_fc_steps),
        ]:
            try:
                if k in axis:
                    var.set(int(axis[k]))
            except Exception:
                pass

    def _pick_file(self, var: Any, types: List[Tuple[str, str]]) -> None:
        path = self._filedialog.askopenfilename(filetypes=types)
        if path:
            var.set(path)
            if var is self._var_cfg:
                self._load_time_defaults_from_config(path)

    def _install_plot_table_panel(self, panel: str, parent: Any) -> Any:
        import tkinter as tk

        left = self._ttk.Frame(parent)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        right = self._ttk.Frame(parent, width=300)
        right.pack(side=tk.LEFT, fill=tk.BOTH)
        right.pack_propagate(False)

        info_var = tk.StringVar(value="悬停图上查看该时刻各要素")
        self._ttk.Label(right, textvariable=info_var, wraplength=280, justify=tk.LEFT).pack(anchor=tk.W, padx=4, pady=2)

        cols = ("time", "rain", "forecast", "observed")
        tree = self._ttk.Treeview(right, columns=cols, show="headings", height=12)
        tree.heading("time", text="时间")
        if panel == "node_inflow":
            tree.heading("rain", text="实测入库")
            tree.heading("forecast", text="预报入库")
            tree.heading("observed", text="实测出库")
        else:
            tree.heading("rain", text="雨量")
            tree.heading("forecast", text="预测")
            tree.heading("observed", text="实测")
        tree.column("time", width=132, anchor=tk.CENTER)
        tree.column("rain", width=52, anchor=tk.E)
        tree.column("forecast", width=52, anchor=tk.E)
        tree.column("observed", width=52, anchor=tk.E)
        ysb = self._ttk.Scrollbar(right, orient=tk.VERTICAL, command=tree.yview)
        xsb = self._ttk.Scrollbar(right, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        tree.pack(fill=tk.BOTH, expand=True, padx=4)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(fill=tk.X, padx=4)

        self._panel_tables[panel] = tree
        self._panel_info_vars[panel] = info_var
        return left

    def _fmt_val(self, v: Any) -> str:
        if v is None:
            return ""
        try:
            fv = float(v)
        except Exception:
            return str(v)
        return f"{fv:.3f}"

    def _selected_id(self, panel: str, combo: Any) -> str:
        label = str(combo.get() or "")
        mapping = self._combo_id_maps.get(panel, {})
        return mapping.get(label, label)

    def _refresh_panel_table(
        self,
        panel: str,
        times: pd.DatetimeIndex,
        rain: List[float],
        forecast: List[float],
        observed: Optional[List[float]],
    ) -> None:
        tree = self._panel_tables.get(panel)
        if tree is None:
            return
        for iid in tree.get_children():
            tree.delete(iid)
        obs = observed if observed is not None else [None] * len(times)
        for i, t in enumerate(times):
            row = (
                t.strftime("%Y-%m-%d %H:%M"),
                self._fmt_val(rain[i] if i < len(rain) else None),
                self._fmt_val(forecast[i] if i < len(forecast) else None),
                self._fmt_val(obs[i] if i < len(obs) else None),
            )
            tree.insert("", "end", iid=str(i), values=row)

    def _ensure_hover_handler(self, panel: str, fig: Any, canvas: Any) -> None:
        if panel in self._panel_hover_cids:
            return

        def _on_move(event: Any) -> None:
            pdata = self._panel_data.get(panel)
            if not pdata:
                return
            axes = pdata.get("axes", [])
            if event.inaxes not in axes or event.xdata is None:
                return

            xnum = pdata["xnum"]
            if len(xnum) == 0:
                return
            idx = int(min(range(len(xnum)), key=lambda i: abs(xnum[i] - event.xdata)))
            t = pdata["times"][idx]
            rain = pdata["rain"][idx] if idx < len(pdata["rain"]) else None
            qf = pdata["forecast"][idx] if idx < len(pdata["forecast"]) else None
            qo = pdata["observed"][idx] if pdata["observed"] is not None and idx < len(pdata["observed"]) else None

            for vl in pdata.get("vlines", []):
                vl.set_xdata([t, t])
            canvas.draw_idle()

            info = self._panel_info_vars.get(panel)
            if info is not None:
                labels = pdata.get("labels", {"rain": "雨量", "forecast": "预测", "observed": "实测"})
                info.set(
                    f"{t:%Y-%m-%d %H:%M} | "
                    f"{labels.get('rain','雨量')}={self._fmt_val(rain)} | "
                    f"{labels.get('forecast','预测')}={self._fmt_val(qf)} | "
                    f"{labels.get('observed','实测')}={self._fmt_val(qo)}"
                )

            tree = self._panel_tables.get(panel)
            if tree is not None:
                iid = str(idx)
                if tree.exists(iid):
                    tree.selection_set(iid)
                    tree.see(iid)

        self._panel_hover_cids[panel] = fig.canvas.mpl_connect("motion_notify_event", _on_move)

    def _refresh_debug_table(self) -> None:
        cid = str(self._combo_debug_catch.get() or "")
        rows = self._debug_rows_by_catchment.get(cid, [])
        for iid in self._debug_tree.get_children():
            self._debug_tree.delete(iid)
        if not rows:
            self._debug_hint.set("当前流域无 debug_trace 数据（请在该流域 runoff_model.params 里设置 debug_trace=true）")
            return
        self._debug_hint.set(f"{cid} 共 {len(rows)} 步")

        def _g(r: Dict[str, Any], k: str) -> str:
            if k not in r:
                return ""
            v = r.get(k)
            if isinstance(v, float):
                return f"{v:.6f}"
            return str(v)

        for i, r in enumerate(rows):
            vals = (
                _g(r, "time"),
                _g(r, "step"),
                _g(r, "p"),
                _g(r, "pet"),
                _g(r, "pe"),
                _g(r, "r"),
                _g(r, "rs"),
                _g(r, "rss"),
                _g(r, "rg"),
                _g(r, "qtr"),
                _g(r, "out"),
                _g(r, "wu"),
                _g(r, "wl"),
                _g(r, "wd"),
                _g(r, "fr"),
                _g(r, "s"),
            )
            self._debug_tree.insert("", "end", iid=str(i), values=vals)

    def _on_run(self) -> None:
        self._status.set("计算中…")
        self.root.update_idletasks()

        def work() -> None:
            try:
                out, times, warns, aux = run_calculation_pipeline(
                    config_path=self._var_cfg.get().strip(),
                    rain_csv=self._var_rain.get().strip(),
                    flow_csv=self._var_flow.get().strip(),
                    warmup_start=self._var_warmup_start.get().strip(),
                    forecast_mode=self._var_forecast_mode.get().strip(),
                    catchment_workers=1 if bool(self._var_single_thread.get()) else None,
                    time_type=self._var_time_type.get().strip(),
                    step_size=int(self._var_step.get()),
                    warmup_steps=int(self._var_warmup_steps.get()),
                    correction_steps=int(self._var_corr_steps.get()),
                    historical_steps=int(self._var_hist_steps.get()),
                    forecast_steps=int(self._var_fc_steps.get()),
                )
                self.root.after(0, lambda o=out, ti=times, w=warns, a=aux: self._on_success(o, ti, w, a))
            except Exception as exc:  # noqa: BLE001
                tb = traceback.format_exc()
                self.root.after(0, lambda e=exc, t=tb: self._on_error(e, t))

        threading.Thread(target=work, daemon=True).start()

    def _on_success(
        self,
        output: Dict[str, Any],
        times: pd.DatetimeIndex,
        warns: List[str],
        aux: Dict[str, Any],
    ) -> None:
        self._last_output = output
        self._last_times = times
        self._last_aux = aux
        self._status.set("计算完成")

        if warns:
            self._messagebox.showwarning("数据提示", "\n".join(warns))

        node_inflows = output.get("node_total_inflows") or {}
        node_outflows = output.get("node_outflows") or {}
        runoffs = output.get("catchment_runoffs") or {}
        routed = output.get("catchment_routed_flows") or {}
        reaches = output.get("reach_flows") or {}
        node_name_map = aux.get("node_name_map", {})
        catch_name_map = aux.get("catchment_name_map", {})
        reach_name_map = aux.get("reach_name_map", {})

        def set_combo(c: Any, panel: str, keys: List[str], name_map: Dict[str, str]) -> None:
            ids = sorted(keys)
            label_to_id: Dict[str, str] = {}
            labels: List[str] = []
            used: set[str] = set()
            for sid in ids:
                nm = str(name_map.get(sid, sid) or sid)
                label = nm if nm not in used else f"{nm} ({sid})"
                used.add(label)
                label_to_id[label] = sid
                labels.append(label)
            self._combo_id_maps[panel] = label_to_id
            c["values"] = tuple(labels)
            c.set(labels[0] if labels else "")

        node_ids = sorted(set(node_inflows.keys()) | set(node_outflows.keys()))
        set_combo(self._combo_node, "node", node_ids, node_name_map)
        set_combo(self._combo_runoff, "runoff", list(runoffs.keys()), catch_name_map)
        set_combo(self._combo_routed, "routed", list(routed.keys()), catch_name_map)
        set_combo(self._combo_reach, "reach", list(reaches.keys()), reach_name_map)

        self._json_text.delete("1.0", self._tk.END)
        self._json_text.insert(self._tk.END, json.dumps(output, ensure_ascii=False, indent=2))

        self._debug_rows_by_catchment = output.get("catchment_debug_traces") or {}
        debug_ids = sorted(self._debug_rows_by_catchment.keys())
        self._combo_debug_catch["values"] = tuple(debug_ids)
        self._combo_debug_catch.set(debug_ids[0] if debug_ids else "")
        self._refresh_debug_table()

        self._redraw_plot("node")
        self._redraw_plot("runoff")
        self._redraw_plot("routed")
        self._redraw_plot("reach")

    def _on_error(self, exc: BaseException, tb: str) -> None:
        self._status.set("计算失败")
        self._messagebox.showerror("计算失败", f"{exc}\n\n{tb}")

    def _draw_one(self, ax: Any, times: pd.DatetimeIndex, series: Dict[str, List[float]], key: str, title: str) -> None:
        ax.clear()
        if not key or key not in series:
            ax.set_title(title)
            ax.text(0.5, 0.5, "无数据", ha="center", va="center", transform=ax.transAxes)
            return
        y = series[key]
        ax.plot(times, y, linewidth=1.2, color="tab:blue")
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="x", rotation=12)

    def _draw_flow_only_panel(
        self,
        panel: str,
        fig: Any,
        canvas: Any,
        *,
        times: pd.DatetimeIndex,
        key: str,
        title: str,
        series_map: Dict[str, List[float]],
    ) -> None:
        import matplotlib.dates as mdates

        fig.clear()
        ax = fig.add_subplot(111)
        if not key or key not in series_map:
            ax.set_title(title)
            ax.text(0.5, 0.5, "无数据", ha="center", va="center", transform=ax.transAxes)
            self._panel_data[panel] = {}
            self._refresh_panel_table(panel, times, [0.0] * len(times), [0.0] * len(times), None)
            return
        flow = series_map[key]
        ax.plot(times, flow, color="tab:blue", linewidth=1.5, label="预测流量")
        ax.set_title(title)
        ax.set_ylabel("Q")
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="x", rotation=12)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))
        ax.legend(loc="upper right")

        xnum = mdates.date2num(times.to_pydatetime())
        vline = ax.axvline(times[0], color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
        zeros = [0.0] * len(times)
        self._panel_data[panel] = {
            "times": list(times),
            "xnum": list(xnum),
            "rain": zeros,
            "forecast": list(flow),
            "observed": None,
            "vlines": [vline],
            "axes": [ax],
        }
        self._refresh_panel_table(panel, times, zeros, flow, None)
        self._ensure_hover_handler(panel, fig, canvas)

    def _mask_observed_for_history(
        self,
        values: Optional[List[float]],
        history_steps: int,
        forecast_start_idx: int,
    ) -> Optional[List[float]]:
        if values is None:
            return None
        hs = max(0, int(history_steps))
        fs = max(0, int(forecast_start_idx))
        if hs <= 0 or fs <= 0:
            return [None] * len(values)
        i0 = max(0, fs - hs)
        i1 = fs
        return [v if i0 <= i < i1 else None for i, v in enumerate(values)]

    def _mask_observed_before_forecast(
        self,
        values: Optional[List[float]],
        forecast_start_idx: int,
    ) -> Optional[List[float]]:
        if values is None:
            return None
        fs = max(0, int(forecast_start_idx))
        return [v if i < fs else None for i, v in enumerate(values)]

    def _draw_hydro_pair(
        self,
        panel: str,
        fig: Any,
        canvas: Any,
        *,
        times: pd.DatetimeIndex,
        key: str,
        title: str,
        forecast_map: Dict[str, List[float]],
        observed_map: Dict[str, List[float]],
        rain_map: Dict[str, List[float]],
        history_steps: int,
        forecast_start_idx: int,
    ) -> None:
        import matplotlib.dates as mdates

        fig.clear()
        gs = fig.add_gridspec(2, 1, height_ratios=[1, 3], hspace=0.04)
        ax_rain = fig.add_subplot(gs[0, 0])
        ax_flow = fig.add_subplot(gs[1, 0], sharex=ax_rain)

        has_data = key and key in forecast_map
        if not has_data:
            ax_flow.set_title(title)
            ax_flow.text(0.5, 0.5, "无数据", ha="center", va="center", transform=ax_flow.transAxes)
            ax_rain.axis("off")
            return

        forecast = forecast_map.get(key, [])
        observed = self._mask_observed_for_history(observed_map.get(key), history_steps, forecast_start_idx)
        rain = rain_map.get(key, [0.0] * len(times))
        if len(rain) != len(times):
            rain = [0.0] * len(times)

        xnum = mdates.date2num(times.to_pydatetime())
        width = 0.8 * max((xnum[1] - xnum[0]) if len(xnum) > 1 else (1.0 / 24.0), 1e-6)
        ax_rain.bar(times, rain, width=width, color="tab:cyan", edgecolor="tab:blue", linewidth=0.3)
        ax_rain.invert_yaxis()
        ax_rain.set_ylabel("P(mm)")
        ax_rain.grid(True, axis="y", alpha=0.25, linestyle="--")
        ax_rain.tick_params(axis="x", which="both", labelbottom=False)

        ax_flow.plot(times, forecast, color="tab:blue", linewidth=1.5, label="预测流量")
        if observed is not None:
            ax_flow.plot(times, observed, color="tab:orange", linewidth=1.2, linestyle="--", label="实测流量")
        ax_flow.set_ylabel("Q")
        ax_flow.set_title(title)
        ax_flow.grid(True, alpha=0.3)
        ax_flow.tick_params(axis="x", rotation=12)
        ax_flow.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))
        ax_flow.legend(loc="upper right")

        vline_rain = ax_rain.axvline(times[0], color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
        vline_flow = ax_flow.axvline(times[0], color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
        self._panel_data[panel] = {
            "times": list(times),
            "xnum": list(xnum),
            "rain": list(rain),
            "forecast": list(forecast),
            "observed": list(observed) if observed is not None else None,
            "vlines": [vline_rain, vline_flow],
            "axes": [ax_rain, ax_flow],
        }
        self._refresh_panel_table(panel, times, rain, forecast, observed)
        self._ensure_hover_handler(panel, fig, canvas)

    def _draw_node_three_flows(
        self,
        *,
        panel: str,
        fig: Any,
        canvas: Any,
        times: pd.DatetimeIndex,
        key: str,
        title: str,
        forecast_inflow_map: Dict[str, List[float]],
        observed_inflow_map: Dict[str, List[float]],
        observed_outflow_map: Dict[str, List[float]],
        forecast_start_idx: int,
    ) -> None:
        import matplotlib.dates as mdates

        fig.clear()
        ax = fig.add_subplot(111)
        if not key or key not in forecast_inflow_map:
            ax.set_title(title)
            ax.text(0.5, 0.5, "无数据", ha="center", va="center", transform=ax.transAxes)
            self._panel_data[panel] = {}
            self._refresh_panel_table(panel, times, [0.0] * len(times), [0.0] * len(times), [None] * len(times))
            return

        qf_in = list(forecast_inflow_map.get(key, []))
        qo_in = list(observed_inflow_map.get(key, [None] * len(times)))
        qo_out = list(observed_outflow_map.get(key, [None] * len(times)))
        if len(qo_in) != len(times):
            qo_in = [None] * len(times)
        if len(qo_out) != len(times):
            qo_out = [None] * len(times)
        qo_in = self._mask_observed_before_forecast(qo_in, forecast_start_idx) or [None] * len(times)
        qo_out = self._mask_observed_before_forecast(qo_out, forecast_start_idx) or [None] * len(times)

        ax.plot(times, qf_in, color="tab:blue", linewidth=1.6, label="预报入库")
        ax.plot(times, qo_in, color="tab:green", linewidth=1.2, linestyle="--", label="实测入库")
        ax.plot(times, qo_out, color="tab:orange", linewidth=1.2, linestyle="-.", label="实测出库")
        ax.set_title(title)
        ax.set_ylabel("Q")
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="x", rotation=12)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))
        ax.legend(loc="upper right")

        xnum = mdates.date2num(times.to_pydatetime())
        vline = ax.axvline(times[0], color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
        # 表格复用三列：rain->实测入库，forecast->预报入库，observed->实测出库
        self._panel_data[panel] = {
            "times": list(times),
            "xnum": list(xnum),
            "rain": qo_in,
            "forecast": qf_in,
            "observed": qo_out,
            "vlines": [vline],
            "axes": [ax],
            "labels": {"rain": "实测入库", "forecast": "预报入库", "observed": "实测出库"},
        }
        self._refresh_panel_table(panel, times, qo_in, qf_in, qo_out)
        self._ensure_hover_handler(panel, fig, canvas)

    def _redraw_plot(self, which: str) -> None:
        if self._last_output is None or self._last_times is None:
            return
        times = self._last_times
        out = self._last_output
        aux = self._last_aux or {}
        history_steps = int(self._var_hist_steps.get())
        forecast_start_idx = int(aux.get("forecast_start_idx", 0))
        display_start_idx = max(0, forecast_start_idx - max(0, history_steps))
        rel_forecast_start_idx = max(0, forecast_start_idx - display_start_idx)

        display_times = times[display_start_idx:]

        def _slice_map(m: Dict[str, List[float]]) -> Dict[str, List[float]]:
            out_map: Dict[str, List[float]] = {}
            for k, v in (m or {}).items():
                out_map[k] = list(v[display_start_idx:])
            return out_map

        if which == "node":
            label = str(self._combo_node.get() or "")
            key = self._selected_id("node", self._combo_node)
            self._draw_node_three_flows(
                panel="node_inflow",
                fig=self._fig_node,
                canvas=self._canvas_node,
                times=display_times,
                key=key,
                title=f"Node流量对比 ({label or key})",
                forecast_inflow_map=_slice_map(out.get("node_total_inflows") or {}),
                observed_inflow_map=_slice_map(aux.get("node_observed_inflows", {})),
                observed_outflow_map=_slice_map(aux.get("node_observed_outflows", {})),
                forecast_start_idx=rel_forecast_start_idx,
            )
            self._canvas_node.draw()
        elif which == "runoff":
            label = str(self._combo_runoff.get() or "")
            key = self._selected_id("runoff", self._combo_runoff)
            self._draw_hydro_pair(
                "runoff",
                self._fig_catch,
                self._canvas_runoff,
                times=display_times,
                key=key,
                title=f"Catchment 产流过程 ({label or key})",
                forecast_map=_slice_map(out.get("catchment_runoffs") or {}),
                observed_map={},
                rain_map=_slice_map(aux.get("catchment_rain", {})),
                history_steps=history_steps,
                forecast_start_idx=rel_forecast_start_idx,
            )
            self._canvas_runoff.draw()
        elif which == "routed":
            label = str(self._combo_routed.get() or "")
            key = self._selected_id("routed", self._combo_routed)
            self._draw_hydro_pair(
                "routed",
                self._fig_catch2,
                self._canvas_routed,
                times=display_times,
                key=key,
                title=f"Catchment 汇流过程 ({label or key})",
                forecast_map=_slice_map(out.get("catchment_routed_flows") or {}),
                observed_map={},
                rain_map=_slice_map(aux.get("catchment_rain", {})),
                history_steps=history_steps,
                forecast_start_idx=rel_forecast_start_idx,
            )
            self._canvas_routed.draw()
        elif which == "reach":
            label = str(self._combo_reach.get() or "")
            key = self._selected_id("reach", self._combo_reach)
            self._draw_flow_only_panel(
                "reach",
                self._fig_reach,
                self._canvas_reach,
                times=display_times,
                key=key,
                title=f"河段 ({label or key})",
                series_map=_slice_map(out.get("reach_flows") or {}),
            )
            self._canvas_reach.draw()

    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    try:
        import matplotlib  # noqa: F401
    except ImportError as exc:
        print("请先安装 matplotlib: pip install matplotlib")
        raise SystemExit(1) from exc

    app = DesktopCalculationApp()
    app.run()


if __name__ == "__main__":
    main()
