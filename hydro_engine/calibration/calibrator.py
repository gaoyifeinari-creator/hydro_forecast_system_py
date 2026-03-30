"""
水文模型自动率定器 - 将 SCE-UA 与 hydro_engine 集成。

职责：
1. 管理待率定参数（名称、边界、当前值）
2. 从参数向量构建 / 更新模型参数
3. 执行单次模拟并计算目标函数值
4. 运行 SCE-UA 优化
5. 将率定结果输出为新的模型方案 JSON 文件
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from hydro_engine.calibration.sceua import SCEUAConfig, SCEUAOptimizer
from hydro_engine.core.forcing import ForcingData
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.io.json_config import load_scheme_from_json, run_calculation_from_json

from hydro_engine.io.calculation_app_data_builder import (
    build_catchment_observed_flow_series,
    build_catchment_precip_series,
    build_node_observed_flow_series,
    build_observed_flows,
    build_station_packages,
)
from hydro_engine.io.calculation_app_data_loader import (
    build_times,
    load_rain_flow_for_calculation,
    read_config,
)

logger = logging.getLogger(__name__)


# ============================================================
# 参数定义
# ============================================================

@dataclass
class CalibParam:
    """单个率定参数。"""

    name: str
    catchment_id: str
    lower: float
    upper: float
    default: float
    included: bool = True


XINANJIANG_CS_PARAMS = ["k", "sm", "kss", "kg", "kkss", "kkg"]
XINANJIANG_PARAMS    = ["k", "sm", "kss", "kg", "kkss", "kkg"]

HARD_BOUNDS = {
    "k":     (0.20, 1.50),
    "sm":    (5.0, 100.0),
    "kss":   (0.05, 0.70),
    "kg":    (0.05, 0.70),
    "kkss":  (0.50, 1.00),
    "kkg":   (0.60, 1.00),
    "k_hours": (0.0, 12.0),
}


def build_calib_params(scheme_cfg: Dict[str, Any]) -> List[CalibParam]:
    """从配置提取所有可率定参数。"""
    params: List[CalibParam] = []
    for cat in scheme_cfg.get("catchments", []):
        cid   = str(cat["id"])
        rcfg  = cat.get("runoff_model", {})
        rname = rcfg.get("name", "")
        rparams = rcfg.get("params", {})

        avail = []
        defaults_map = {}
        if "XinanjiangCS" in rname:
            avail = XINANJIANG_CS_PARAMS
            defaults_map = {k: float(rparams.get(k, HARD_BOUNDS[k][0])) for k in avail}
        elif "Xinanjiang" in rname:
            avail = XINANJIANG_PARAMS
            defaults_map = {k: float(rparams.get(k, HARD_BOUNDS[k][0])) for k in avail}
        else:
            continue

        for pname in avail:
            lo, hi = HARD_BOUNDS.get(pname, (0.0, 1.0))
            params.append(CalibParam(
                name=pname,
                catchment_id=cid,
                lower=lo,
                upper=hi,
                default=defaults_map.get(pname, (lo + hi) / 2),
                included=True,
            ))

        # Muskingum k_hours
        mcfg = cat.get("routing_model", {})
        if "Muskingum" in mcfg.get("name", ""):
            k_def = float(mcfg.get("params", {}).get("k_hours", 1.0))
            params.append(CalibParam(
                name="k_hours",
                catchment_id=cid,
                lower=0.0,
                upper=12.0,
                default=k_def,
                included=True,
            ))

    return params


def apply_params_to_config(
    cfg: Dict[str, Any],
    param_dict: Dict[Tuple[str, str], float],
) -> None:
    """将参数字典写入选定配置（in-place）。"""
    for cat in cfg["schemes"][0].setdefault("catchments", []):
        cid = str(cat["id"])
        rparams = cat.setdefault("runoff_model", {}).setdefault("params", {})
        for pname in XINANJIANG_CS_PARAMS + XINANJIANG_PARAMS:
            key = (cid, pname)
            if key in param_dict:
                rparams[pname] = param_dict[key]
        rmparams = cat.setdefault("routing_model", {}).setdefault("params", {})
        rkey = (cid, "k_hours")
        if rkey in param_dict:
            rmparams["k_hours"] = param_dict[rkey]


# ============================================================
# 辅助指标函数
# ============================================================

def _compute_kge(fc: np.ndarray, obs: np.ndarray) -> float:
    """计算 KGE（越大越好）。"""
    o_mean, o_std = np.mean(obs), np.std(obs, ddof=0)
    f_mean, f_std = np.mean(fc), np.std(fc, ddof=0)
    if o_std < 1e-10 or o_mean == 0 or len(fc) < 2:
        return float("nan")
    r = float(np.corrcoef(fc, obs)[0, 1])
    if np.isnan(r):
        return float("nan")
    alpha = f_std / o_std
    beta  = f_mean / o_mean
    return float(1.0 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2))


# ============================================================
# 目标函数
# ============================================================

class ObjectiveFunction:
    """
    SCE-UA 目标函数封装。
    运行一次模拟，返回负 NSE（NSE 越大越好，SCE-UA 最小化 → 取负）。
    """

    def __init__(
        self,
        raw_config: Dict[str, Any],
        calib_params: List[CalibParam],
        station_packages: Dict[str, ForcingData],
        observed_flows: Dict[str, TimeSeries],
        total_start: datetime,
        step_delta: timedelta,
        calib_start: datetime,
        calib_end: datetime,
        step_size: int,
        time_type: str,
        weights: Optional[Dict[str, float]] = None,
    ) -> None:
        self.raw_config       = raw_config
        self.calib_params     = calib_params
        self.included         = [p for p in calib_params if p.included]
        self.station_packages = station_packages
        self.observed_flows   = observed_flows
        self.total_start      = total_start
        self.step_delta       = step_delta
        self.calib_start      = calib_start
        self.calib_end        = calib_end
        self.step_size        = step_size
        self.time_type        = time_type
        self.weights          = weights or {}

        # 预计算窗口步数
        self.win_start    = calib_start - timedelta(days=30)  # 预热30天
        self.win_end      = calib_end
        self.win_steps    = int((self.win_end - self.win_start).total_seconds() / 3600)
        self.fc_start_idx = 30 * 24   # 预报起始索引（小时）
        self._rain_df    = None       # 由外部注入

    def __call__(self, param_vector: np.ndarray) -> float:
        """目标函数调用（SCE-UA 最小化）。"""
        param_dict: Dict[Tuple[str, str], float] = {}
        for p, v in zip(self.included, param_vector):
            param_dict[(p.catchment_id, p.name)] = float(
                np.clip(v, p.lower, p.upper)
            )
        try:
            score = self._evaluate(param_dict)
            if not np.isfinite(score) or score < -1e12:
                return 1.0e18
            return float(score)
        except Exception as exc:
            logger.debug(f"Simulation error: {exc}")
            return 1.0e18

    def _evaluate(self, param_dict: Dict[Tuple[str, str], float]) -> float:
        """运行模拟并计算 NSE（越小越好 → 返回负 NSE）。"""
        cfg = deepcopy(self.raw_config)
        apply_params_to_config(cfg, param_dict)

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        )
        json.dump(cfg, tmp, ensure_ascii=False, indent=2)
        tmp.close()

        try:
            # 加载方案
            scm, bspecs, tc = load_scheme_from_json(
                tmp.name,
                time_type=self.time_type,
                step_size=self.step_size,
                warmup_start_time=self.win_start,
            )

            win_times = build_times(tc.warmup_start_time, tc.time_delta, tc.step_count)

            # 截取数据窗口
            ws = self.win_start
            ws_idx = int((ws - self.total_start) / self.step_delta)

            win_station_packages: Dict[str, ForcingData] = {}
            for sid, pkg in self.station_packages.items():
                new_map = {}
                for kind, series in pkg.as_mapping().items():
                    vals = list(series.values)
                    new_map[kind] = TimeSeries(
                        start_time=ws, time_step=self.step_delta,
                        values=vals[ws_idx:ws_idx + tc.step_count],
                    )
                if new_map:
                    win_station_packages[sid] = ForcingData(new_map)

            win_obs: Dict[str, TimeSeries] = {}
            for sid, series in self.observed_flows.items():
                vals = list(series.values)
                win_obs[sid] = TimeSeries(
                    start_time=ws, time_step=self.step_delta,
                    values=vals[ws_idx:ws_idx + tc.step_count],
                )

            cat_rain, _   = build_catchment_precip_series(bspecs, self._rain_df, win_times)
            node_obs_map  = build_node_observed_flow_series(scm, win_obs)
            cat_obs       = build_catchment_observed_flow_series(scm, node_obs_map)

            # 如果配置中缺失 `catchments[].downstream_node_id`，则根据：
            # - catchment 所属节点：`nodes[].local_catchment_ids`
            # - 拓扑推导：`reaches[].upstream_node_id/downstream_node_id`
            # 来推导 downstream_node_id（用于选择“该下游节点的入库/接力实测站”）。
            nodes_cfg = cfg["schemes"][0].get("nodes", [])
            reaches_cfg = cfg["schemes"][0].get("reaches", [])
            catchment_owner_node_id: Dict[str, str] = {}
            for n in nodes_cfg:
                nid = str(n.get("id", "")).strip()
                if not nid:
                    continue
                for cid in n.get("local_catchment_ids", []) or []:
                    cid_s = str(cid).strip()
                    if not cid_s:
                        continue
                    # 多节点挂载在运行阶段会直接报错；这里做“保守取第一个”不影响目标计算。
                    catchment_owner_node_id.setdefault(cid_s, nid)

            node_outgoing_downstream_node_ids: Dict[str, Set[str]] = {}
            for r in reaches_cfg:
                up = str(r.get("upstream_node_id", "")).strip()
                dn = str(r.get("downstream_node_id", "")).strip()
                if not up or not dn:
                    continue
                node_outgoing_downstream_node_ids.setdefault(up, set()).add(dn)

            # 运行
            output = run_calculation_from_json(
                tmp.name, win_station_packages,
                self.time_type, self.step_size, ws,
                win_obs, "historical_simulation", None,
            )

            node_inflows = output.get("node_total_inflows", {})

            # 提取预报时段
            fc_s = self.fc_start_idx
            fc_e = int((self.calib_end - ws) / self.step_delta)

            scores: List[float] = []
            for cat in cfg["schemes"][0].get("catchments", []):
                cid   = str(cat["id"])
                dnid  = str(cat.get("downstream_node_id", "")).strip()
                if not dnid:
                    owner = catchment_owner_node_id.get(cid, "")
                    candidates = node_outgoing_downstream_node_ids.get(owner, set()) if owner else set()
                    if len(candidates) == 1:
                        dnid = next(iter(candidates))
                node_cfg = next(
                    (n for n in cfg["schemes"][0]["nodes"] if n["id"] == dnid),
                    None,
                )
                if node_cfg is None:
                    continue
                obs_sid = str(
                    node_cfg.get("station_binding", {}).get("inflow_station_id", "")
                ).strip()
                if not obs_sid or obs_sid not in win_obs:
                    continue

                fc_arr = np.array(
                    node_inflows.get(dnid, [0.0] * tc.step_count)[fc_s:fc_e]
                )
                obs_arr = np.array(
                    list(win_obs[obs_sid].values)[fc_s:fc_e]
                )

                if len(fc_arr) < 2 or len(obs_arr) < 2:
                    continue

                nse_val = self._compute_nse(fc_arr, obs_arr)
                w = self.weights.get(cid, 1.0)
                scores.append(w * nse_val)

            if not scores:
                return 1.0e18

            # 负 NSE（SCE-UA 最小化）
            return -float(np.mean(scores))

        finally:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    @staticmethod
    def _compute_nse(fc: np.ndarray, obs: np.ndarray) -> float:
        denom = np.sum((obs - np.mean(obs)) ** 2)
        if denom < 1e-10:
            return -1e9
        return float(1.0 - np.sum((fc - obs) ** 2) / denom)


# ============================================================
# 率定结果
# ============================================================

@dataclass
class CalibrationResult:
    best_params: Dict[Tuple[str, str], float]
    best_objective: float           # 目标函数值（负 NSE）
    nse: float                      # 真实 NSE（越大越好）
    n_iterations: int
    n_function_evaluations: int
    history: List[float]
    calib_params: List[CalibParam]


# ============================================================
# 率定器主类
# ============================================================

class HydroModelCalibrator:
    """
    水文模型自动率定器（SCE-UA）。

    示例::

        calib = HydroModelCalibrator(
            config_path="configs/forecastSchemeConf.json",
            rain_csv="tests/佛子岭雨量.csv",
            flow_csv="tests/佛子岭流量.csv",
            calib_period_start=datetime(2024, 3, 1),
            calib_period_end=datetime(2024, 5, 31),
        )
        result = calib.calibrate(progress=True)
        calib.save_calibrated_scheme(result, "configs/calibrated_scheme.json")

    优先使用 ``jdbc_config_path`` 指向 ``configs/floodForecastJdbc.json`` 连库；否则使用 ``rain_csv`` / ``flow_csv``（CSV 或旧版 JSON）。
    读取时间窗为 ``total_start``～``total_end``（内部固定）。
    """

    def __init__(
        self,
        config_path: str,
        rain_csv: str,
        flow_csv: str,
        calib_period_start: datetime,
        calib_period_end: datetime,
        warmup_days: int = 30,
        time_type: str = "Hour",
        step_size: int = 1,
        weights: Optional[Dict[str, float]] = None,
        jdbc_config_path: Optional[str] = None,
    ) -> None:
        self.config_path   = Path(config_path)
        self.rain_csv      = Path(rain_csv)
        self.flow_csv      = Path(flow_csv)
        self.jdbc_config_path = (jdbc_config_path or "").strip()
        self.time_type     = time_type
        self.step_size     = step_size
        self.step_delta     = timedelta(hours=step_size)
        self.warmup_days   = warmup_days
        self.calib_start   = calib_period_start
        self.calib_end     = calib_period_end
        self.weights       = weights or {}

        self._load_data()

    def _load_data(self) -> None:
        self.raw_config  = read_config(str(self.config_path))
        scheme_cfg      = self.raw_config["schemes"][0]

        self.total_start = datetime(2024, 1, 1, 0, 0, 0)
        self.total_end   = datetime(2024, 12, 31, 23, 0, 0)
        total_steps     = int((self.total_end - self.total_start).total_seconds() / 3600) + 1
        self.full_times = build_times(self.total_start, self.step_delta, total_steps)

        self._rain_df, self._flow_df, self._jdbc_warns = load_rain_flow_for_calculation(
            jdbc_config_path=self.jdbc_config_path,
            rain_csv=str(self.rain_csv),
            flow_csv=str(self.flow_csv),
            time_start=self.total_start,
            time_end=self.total_end,
        )

        self.scheme, self.binding_specs, _ = load_scheme_from_json(
            str(self.config_path),
            time_type=self.time_type,
            step_size=self.step_size,
            warmup_start_time=self.total_start,
        )

        self.station_packages, _ = build_station_packages(
            self.binding_specs, self._rain_df, self.full_times,
            self.total_start, self.step_delta,
        )
        self.observed_flows, _ = build_observed_flows(
            self.scheme, self._flow_df, self.full_times,
            self.total_start, self.step_delta,
        )

        self.calib_params   = build_calib_params(scheme_cfg)
        self._included      = [p for p in self.calib_params if p.included]

        for w in self._jdbc_warns:
            logger.warning(w)

        logger.info(
            f"[HydroModelCalibrator] Ready: {len(self._included)} params to calibrate"
        )
        for p in self._included:
            logger.info(f"  {p.catchment_id}.{p.name}: [{p.lower}, {p.upper}], default={p.default}")

    def calibrate(
        self,
        *,
        max_iter: int = 500,
        max_nfe: int = 2000,
        n_complex: int = 3,
        convergence_tol: float = 1e-4,
        rng_seed: int = 42,
        progress: bool = True,
    ) -> CalibrationResult:
        """运行 SCE-UA 率定（以 NSE 为目标函数）。"""
        included = self._included
        n_params = len(included)

        # 目标函数
        obj_func = ObjectiveFunction(
            raw_config=self.raw_config,
            calib_params=self.calib_params,
            station_packages=self.station_packages,
            observed_flows=self.observed_flows,
            total_start=self.total_start,
            step_delta=self.step_delta,
            calib_start=self.calib_start,
            calib_end=self.calib_end,
            step_size=self.step_size,
            time_type=self.time_type,
            weights=self.weights,
        )
        # 注入 rain_df
        obj_func._rain_df = self._rain_df

        sce_config = SCEUAConfig(
            n_params=n_params,
            lower_bounds=[p.lower for p in included],
            upper_bounds=[p.upper for p in included],
            n_complex=n_complex,
            max_iter=max_iter,
            max_nfe=max_nfe,
            convergence_tol=convergence_tol,
            rng_seed=rng_seed,
        )

        optimizer = SCEUAOptimizer(sce_config)
        best_vec, best_obj = optimizer.minimize(
            obj_func, progress=progress, log_frequency=10
        )

        # 整理率定参数
        best_dict: Dict[Tuple[str, str], float] = {}
        for p, v in zip(included, best_vec):
            best_dict[(p.catchment_id, p.name)] = float(
                np.clip(v, p.lower, p.upper)
            )

        real_nse = -best_obj  # best_obj 是负 NSE

        logger.info(
            f"[Calibration] Done. NSE = {real_nse:.4f}, "
            f"NFE = {optimizer.n_function_evaluations}"
        )

        return CalibrationResult(
            best_params=best_dict,
            best_objective=float(best_obj),
            nse=real_nse,
            n_iterations=optimizer.iteration,
            n_function_evaluations=optimizer.n_function_evaluations,
            history=optimizer.convergence_history,
            calib_params=self.calib_params,
        )

    def save_calibrated_scheme(
        self,
        result: CalibrationResult,
        output_path: str,
        description: str = "",
    ) -> None:
        """将率定参数写入新的模型方案 JSON（原文件不受影响）。"""
        cfg = deepcopy(self.raw_config)
        meta = cfg.setdefault("metadata", {})
        meta["name"]        = (meta.get("name", "") + "_calibrated").strip()
        meta["description"] = (
            f"{description} | Calibrated {datetime.now().date()} "
            f"| NSE={result.nse:.4f} | params="
            + ",".join(
                f"{cid}.{pn}={v:.4f}"
                for (cid, pn), v in sorted(result.best_params.items())
            )
        )

        apply_params_to_config(cfg, result.best_params)

        os.makedirs(Path(output_path).parent, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

        logger.info(f"[Calibrator] Saved: {output_path}")

    # ------------------------------------------------------------------
    # 评估接口
    # ------------------------------------------------------------------

    def evaluate_rolling(
        self,
        param_dict: Dict[Tuple[str, str], float],
        eval_start: datetime,
        eval_end: datetime,
        warmup_days: int = 30,
        forecast_lead_hours: int = 24,
    ) -> List[Dict[str, Any]]:
        """
        滚动预报精度评估（给定参数集）。

        返回 list[record]，每条 record 含 RE/KGE/NSE/RMSE。
        """
        records: List[Dict[str, Any]] = []
        step_delta = timedelta(hours=self.step_size)
        warmup_steps = warmup_days * 24 // self.step_size
        fc_steps     = forecast_lead_hours // self.step_size
        win_total    = warmup_steps + fc_steps

        current = eval_start
        while current <= eval_end:
            fc_start   = current
            fc_end     = fc_start + timedelta(hours=forecast_lead_hours)
            ws         = fc_start - timedelta(hours=warmup_steps * self.step_size)

            if ws < self.total_start or fc_end > self.total_end:
                current += timedelta(days=1)
                continue

            cfg = deepcopy(self.raw_config)
            apply_params_to_config(cfg, param_dict)

            sc = cfg["schemes"][0]
            sc["time_axis"] = {
                "warmup_start_time":     ws.isoformat(sep=" "),
                "correction_start_time": (ws + step_delta * 48).isoformat(sep=" "),
                "display_start_time":    (ws + step_delta * (warmup_steps - 24)).isoformat(sep=" "),
                "forecast_start_time":   fc_start.isoformat(sep=" "),
                "end_time":              fc_end.isoformat(sep=" "),
            }

            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False, encoding="utf-8"
            )
            json.dump(cfg, tmp, ensure_ascii=False, indent=2)
            tmp.close()

            try:
                ws_idx = int((ws - self.total_start) / step_delta)

                win_pkgs: Dict[str, ForcingData] = {}
                for sid, pkg in self.station_packages.items():
                    new_map = {}
                    for kind, series in pkg.as_mapping().items():
                        vals = list(series.values)
                        new_map[kind] = TimeSeries(
                            start_time=ws, time_step=step_delta,
                            values=vals[ws_idx:ws_idx + win_total],
                        )
                    if new_map:
                        win_pkgs[sid] = ForcingData(new_map)

                win_obs: Dict[str, TimeSeries] = {}
                for sid, series in self.observed_flows.items():
                    vals = list(series.values)
                    win_obs[sid] = TimeSeries(
                        start_time=ws, time_step=step_delta,
                        values=vals[ws_idx:ws_idx + win_total],
                    )

                scm, bspecs, tc = load_scheme_from_json(
                    tmp.name, time_type=self.time_type,
                    step_size=self.step_size, warmup_start_time=ws,
                )
                win_times = build_times(tc.warmup_start_time, tc.time_delta, tc.step_count)
                cat_rain, _  = build_catchment_precip_series(bspecs, self._rain_df, win_times)
                node_obs_map = build_node_observed_flow_series(scm, win_obs)
                cat_obs      = build_catchment_observed_flow_series(scm, node_obs_map)

                output = run_calculation_from_json(
                    tmp.name, win_pkgs, self.time_type, self.step_size, ws,
                    win_obs, "historical_simulation", None,
                )

                node_inflows = output.get("node_total_inflows", {})
                fc_s = warmup_steps
                fc_e = warmup_steps + fc_steps

                for nid, node in scm.nodes.items():
                    fc_arr = np.array(
                        node_inflows.get(nid, [0.0] * tc.step_count)[fc_s:fc_e]
                    )
                    obs_sid = str(
                        getattr(node, "observed_inflow_station_id", "") or
                        getattr(node, "observed_station_id", "")
                    ).strip()
                    if obs_sid and obs_sid in win_obs:
                        obs_arr = np.array(
                            list(win_obs[obs_sid].values)[fc_s:fc_e]
                        )
                    else:
                        obs_arr = np.full_like(fc_arr, np.nan)

                    obs_cl = np.where(np.isnan(obs_arr), 0.0, obs_arr)
                    s = np.sum(np.abs(obs_cl))
                    re   = float(np.sum(fc_arr - obs_cl) / s) if s > 1e-10 else float("nan")
                    kge  = _compute_kge(fc_arr, obs_cl)
                    nse  = ObjectiveFunction._compute_nse(fc_arr, obs_cl)
                    rmse = float(np.sqrt(np.mean((fc_arr - obs_cl) ** 2)))

                    records.append({
                        "forecast_date":  fc_start.isoformat(sep=" "),
                        "node_id":        nid,
                        "node_name":      node.name,
                        "relative_error": round(re, 6) if np.isfinite(re) else None,
                        "kge":            round(kge, 6) if np.isfinite(kge) else None,
                        "nse":            round(nse, 6) if np.isfinite(nse) else None,
                        "rmse":           round(rmse, 4) if np.isfinite(rmse) else None,
                        "lead_hours":     forecast_lead_hours,
                    })

            except Exception as e:
                logger.warning(f"[{fc_start.date()}] Failed: {e}")

            finally:
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass

            current += timedelta(days=1)

        return records
