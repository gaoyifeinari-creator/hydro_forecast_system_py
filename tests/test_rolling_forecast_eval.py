#!/usr/bin/env python3
"""
历史模拟滚动预报测试 - 2024年2月1日 至 2024年12月15日

每天作为一个起报点，运行历史模拟预报（historical_simulation 模式），
将各节点预报入库流量与同期实测值比对，计算：
  - 相对误差（Relative Error, RE）
  - 克林-古普塔效率系数（Kling-Gupta Efficiency, KGE）
  - NSE / RMSE（辅助参考）

所有偏差记录保存到本地文件。
"""

import sys
import json
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.calculation_app_common import (
    build_catchment_precip_series,
    build_node_observed_flow_series,
    build_catchment_observed_flow_series,
    build_observed_flows,
    build_station_packages,
    build_times,
    load_csv,
    read_config,
)
from hydro_engine.core.forcing import ForcingData
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.io.json_config import load_scheme_from_json, run_calculation_from_json

# ============================================================
# 配置区
# ============================================================
FORECAST_START_DATE  = datetime(2024, 2, 1, 0, 0, 0)
FORECAST_END_DATE    = datetime(2024, 12, 15, 0, 0, 0)
FORECAST_LEAD_HOURS  = 24          # 预报预见期
WARMUP_DAYS          = 30          # 预热期（天）
CORRECTION_DAYS      = 2           # 校正期（天）
HIST_DISPLAY_DAYS    = 1           # 历史展示期（天）

CONFIG_PATH = PROJECT_ROOT / "configs" / "forecastSchemeConf.json"
RAIN_CSV    = PROJECT_ROOT / "tests" / "佛子岭雨量.csv"
FLOW_CSV    = PROJECT_ROOT / "tests" / "佛子岭流量.csv"

OUTPUT_DIR   = PROJECT_ROOT / "output" / "rolling_forecast_eval"
RESULTS_JSON = OUTPUT_DIR / "deviation_records.json"
SUMMARY_CSV  = OUTPUT_DIR / "deviation_summary.csv"
DETAIL_CSV   = OUTPUT_DIR / "deviation_detail.csv"

# ============================================================
# 指标计算
# ============================================================

def compute_re(forecast: np.ndarray, observed: np.ndarray) -> float:
    """相对误差 RE = Σ(f-o) / Σ|o|"""
    s = np.sum(np.abs(observed))
    return float(np.nan) if s < 1e-10 else float(np.sum(forecast - observed) / s)


def compute_kge(forecast: np.ndarray, observed: np.ndarray) -> float:
    """Kling-Gupta Efficiency"""
    o_mean, o_std = np.mean(observed), np.std(observed, ddof=0)
    f_mean, f_std = np.mean(forecast), np.std(forecast, ddof=0)
    if o_std < 1e-10 or len(forecast) < 2 or o_mean == 0:
        return float(np.nan)
    r = float(np.corrcoef(forecast, observed)[0, 1])
    if np.isnan(r):
        return float(np.nan)
    alpha = f_std / o_std
    beta  = f_mean / o_mean
    return float(1.0 - np.sqrt((r - 1.0)**2 + (alpha - 1.0)**2 + (beta - 1.0)**2))


def compute_rmse(forecast: np.ndarray, observed: np.ndarray) -> float:
    return float(np.sqrt(np.mean((forecast - observed)**2))) if len(forecast) else float(np.nan)


def compute_nse(forecast: np.ndarray, observed: np.ndarray) -> float:
    denom = np.sum((observed - np.mean(observed))**2)
    return float(np.nan) if denom < 1e-10 else float(1.0 - np.sum((forecast - observed)**2) / denom)


# ============================================================
# 主流程
# ============================================================

def rolling_forecast_evaluation() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("历史模拟滚动预报评估")
    print(f"  起止: {FORECAST_START_DATE.date()} ~ {FORECAST_END_DATE.date()}")
    print(f"  预见期: {FORECAST_LEAD_HOURS}h  预热: {WARMUP_DAYS}d  校正: {CORRECTION_DAYS}d")
    print("=" * 70)

    # ---- 1. 全局参数 ----
    STEP_SIZE  = 1                     # 小时（与配置文件一致）
    STEP_DELTA = timedelta(hours=1)
    fc_steps   = FORECAST_LEAD_HOURS // STEP_SIZE          # 预报步数（如 24）
    warmup_steps  = WARMUP_DAYS  * 24 // STEP_SIZE         # 预热步数（如 720）
    corr_steps    = CORRECTION_DAYS * 24 // STEP_SIZE     # 校正步数（如 48）
    hist_steps    = HIST_DISPLAY_DAYS * 24 // STEP_SIZE   # 历史展示步数（如 24）
    win_total_steps = warmup_steps + fc_steps             # 完整窗口步数

    print(f"\n[1/5] 预加载全年数据 ...")
    total_start = datetime(2024, 1, 1, 0, 0, 0)
    total_end   = datetime(2024, 12, 31, 23, 0, 0)
    total_steps = int((total_end - total_start).total_seconds() / 3600) + 1
    full_times  = build_times(total_start, STEP_DELTA, total_steps)

    rain_df = load_csv(str(RAIN_CSV))
    flow_df = load_csv(str(FLOW_CSV))
    print(f"  雨量: {len(rain_df)} 条 | 流量: {len(flow_df)} 条")
    print(f"  数据范围: {total_start.date()} ~ {total_end.date()}")

    # 加载方案 & 绑定规格（只需要 binding_specs，后续每次重建 time_context）
    scheme, binding_specs, _ = load_scheme_from_json(
        str(CONFIG_PATH), time_type="Hour", step_size=STEP_SIZE, warmup_start_time=total_start
    )

    # 预读全部时间序列（避免每次重新读 CSV）
    station_packages, _ = build_station_packages(binding_specs, rain_df, full_times, total_start, STEP_DELTA)
    observed_flows,    _ = build_observed_flows(scheme, flow_df, full_times, total_start, STEP_DELTA)
    print(f"  站点包: {len(station_packages)} | 流量站: {len(observed_flows)}")

    # ---- 2. 逐日滚动 ----
    print(f"\n[2/5] 开始逐日滚动预报 (共 {(FORECAST_END_DATE - FORECAST_START_DATE).days + 1} 天) ...")
    current_date = FORECAST_START_DATE
    all_records: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]]  = []

    fc_idx = warmup_steps   # 预报起始索引（在完整窗口内）

    while current_date <= FORECAST_END_DATE:
        fc_start    = current_date
        fc_end      = fc_start + timedelta(hours=FORECAST_LEAD_HOURS)
        warmup_start = fc_start - timedelta(hours=warmup_steps * STEP_SIZE)

        # 检查数据边界
        if warmup_start < total_start or fc_end > total_end:
            print(f"  [{fc_start.date()}] 数据越界，跳过")
            current_date += timedelta(days=1)
            continue

        try:
            # ---- 2.1 从全年数据中截取当前窗口 [warmup_start, fc_end) ----
            # 全年数据索引: warmup_start 对应 index = warmup_steps
            win_station_pkgs: Dict[str, ForcingData] = {}
            for sid, pkg in station_packages.items():
                new_map = {}
                for kind, series in pkg.as_mapping().items():
                    vals = series.values.tolist()
                    window_vals = vals[warmup_steps : warmup_steps + win_total_steps]
                    new_map[kind] = TimeSeries(
                        start_time=warmup_start, time_step=STEP_DELTA, values=window_vals
                    )
                if new_map:
                    win_station_pkgs[sid] = ForcingData(new_map)

            win_obs_flows: Dict[str, TimeSeries] = {}
            for sid, series in observed_flows.items():
                vals = series.values.tolist()
                win_obs_flows[sid] = TimeSeries(
                    start_time=warmup_start, time_step=STEP_DELTA,
                    values=vals[warmup_steps : warmup_steps + win_total_steps],
                )

            # ---- 2.2 修改配置时间轴（绝对时间格式）----
            data = read_config(str(CONFIG_PATH))
            sc = data["schemes"][0]
            sc["time_axis"] = {
                "warmup_start_time":     warmup_start.isoformat(sep=" "),
                "correction_start_time": (warmup_start + STEP_DELTA * corr_steps).isoformat(sep=" "),
                "display_start_time":    (warmup_start + STEP_DELTA * (warmup_steps - hist_steps)).isoformat(sep=" "),
                "forecast_start_time":   fc_start.isoformat(sep=" "),
                "end_time":              fc_end.isoformat(sep=" "),
            }
            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
            json.dump(data, tmp, ensure_ascii=False, indent=2)
            tmp.close()

            # ---- 2.3 重新加载方案（让 time_context 与窗口对齐）----
            scm, bspecs, tc = load_scheme_from_json(
                tmp.name, time_type="Hour", step_size=STEP_SIZE, warmup_start_time=warmup_start
            )

            win_times = build_times(tc.warmup_start_time, tc.time_delta, tc.step_count)
            catchment_rain, _ = build_catchment_precip_series(bspecs, rain_df, win_times)
            node_obs_map     = build_node_observed_flow_series(scm, win_obs_flows)
            catchment_obs_map = build_catchment_observed_flow_series(scm, node_obs_map)

            # ---- 2.4 运行历史模拟预报 ----
            output = run_calculation_from_json(
                config_path=tmp.name,
                station_packages=win_station_pkgs,
                time_type="Hour", step_size=STEP_SIZE,
                warmup_start_time=warmup_start,
                observed_flows=win_obs_flows,
                forecast_mode="historical_simulation",
                catchment_workers=None,
            )

            # ---- 2.5 提取预报时段结果，计算指标 ----
            node_inflows = output.get("node_total_inflows", {})

            for node_id, node in scm.nodes.items():
                nid = str(node_id)
                fc_vals = node_inflows.get(nid, [0.0] * tc.step_count)
                # 预报时段 [fc_idx : fc_idx + fc_steps]
                fc_arr = np.array(fc_vals[fc_idx : fc_idx + fc_steps], dtype=np.float64)

                # 找实测入库站
                obs_sid = str(
                    getattr(node, "observed_inflow_station_id", "") or
                    getattr(node, "observed_station_id", "")
                ).strip()

                if obs_sid and obs_sid in win_obs_flows:
                    obs_arr = np.array(
                        win_obs_flows[obs_sid].values.tolist()[fc_idx : fc_idx + fc_steps],
                        dtype=np.float64,
                    )
                else:
                    obs_arr = np.full(fc_steps, np.nan, dtype=np.float64)

                # 替换 NaN 为 0（用于计算）
                obs_clean = np.where(np.isnan(obs_arr), 0.0, obs_arr)

                re   = compute_re(fc_arr, obs_clean)
                kge  = compute_kge(fc_arr, obs_clean)
                rmse = compute_rmse(fc_arr, obs_clean)
                nse  = compute_nse(fc_arr, obs_clean)

                record = {
                    "forecast_date":   fc_start.isoformat(sep=" "),
                    "forecast_end":   fc_end.isoformat(sep=" "),
                    "node_id":        nid,
                    "node_name":      node.name,
                    "relative_error": round(re, 6) if not np.isnan(re) else None,
                    "kge":            round(kge, 6) if not np.isnan(kge) else None,
                    "rmse":           round(rmse, 4) if not np.isnan(rmse) else None,
                    "nse":            round(nse,  6) if not np.isnan(nse) else None,
                    "lead_hours":     FORECAST_LEAD_HOURS,
                }
                all_records.append(record)

                # 每小时详情
                for i, (fv, ov) in enumerate(zip(fc_arr, obs_arr)):
                    detail_rows.append({
                        "forecast_date":    fc_start.isoformat(sep=" "),
                        "node_id":          nid,
                        "node_name":        node.name,
                        "step_hour":        i + 1,
                        "forecast_value":   None if np.isnan(fv) else round(float(fv), 4),
                        "observed_value":   None if (isinstance(ov, float) and np.isnan(ov)) else round(float(ov), 4),
                    })

            try:
                os.unlink(tmp.name)
            except Exception:
                pass

        except Exception as e:
            print(f"  [{fc_start.date()}] 失败: {e}")
            import traceback
            traceback.print_exc()
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

        # 进度
        day_num = (fc_start - FORECAST_START_DATE).days + 1
        if day_num % 50 == 0 or fc_start == FORECAST_END_DATE:
            print(f"  进度 {fc_start.date()} ({day_num}/{(FORECAST_END_DATE - FORECAST_START_DATE).days + 1})")

        current_date += timedelta(days=1)

    # ---- 3. 保存结果 ----
    print(f"\n[3/5] 保存结果 ...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results_json = {
        "metadata": {
            "forecast_start":  FORECAST_START_DATE.isoformat(sep=" "),
            "forecast_end":    FORECAST_END_DATE.isoformat(sep=" "),
            "lead_hours":      FORECAST_LEAD_HOURS,
            "warmup_days":     WARMUP_DAYS,
            "total_records":   len(all_records),
        },
        "records": all_records,
    }
    with open(RESULTS_JSON, "w", encoding="utf-8") as f:
        json.dump(results_json, f, ensure_ascii=False, indent=2)

    if all_records:
        df_summary = pd.DataFrame(all_records)
        df_summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")

    if detail_rows:
        df_detail = pd.DataFrame(detail_rows)
        df_detail.to_csv(DETAIL_CSV, index=False, encoding="utf-8-sig")

    # ---- 4. 统计摘要 ----
    print(f"\n[4/5] 统计摘要")
    print("=" * 70)
    if not all_records:
        print("  无有效记录。")
        return

    df = pd.DataFrame(all_records)

    print(f"\n总记录数: {len(all_records)} | 预报天数: {df['forecast_date'].nunique()} | 节点数: {df['node_id'].nunique()}")

    print("\n【各节点平均指标】")
    agg = df.groupby(["node_id", "node_name"]).agg(
        RE_mean=("relative_error", "mean"),
        RE_std =("relative_error", "std"),
        KGE_mean=("kge", "mean"),
        KGE_std =("kge", "std"),
        RMSE_mean=("rmse", "mean"),
        NSE_mean =("nse", "mean"),
        count=("relative_error", "count"),
    ).round(4)
    print(agg.to_string())

    print("\n【各节点 RE 分布】")
    re_dist = df.groupby(["node_id", "node_name"])["relative_error"].describe().round(4)
    print(re_dist.to_string())

    print("\n【KGE 分布】")
    kge_dist = df.groupby(["node_id", "node_name"])["kge"].describe().round(4)
    print(kge_dist.to_string())

    # 全局统计
    print("\n【全局统计（所有节点合计）】")
    valid_re  = df["relative_error"].dropna()
    valid_kge = df["kge"].dropna()
    valid_nse = df["nse"].dropna()
    valid_rmse= df["rmse"].dropna()
    print(f"  相对误差 RE : 均值={valid_re.mean():.4f}, 标准差={valid_re.std():.4f}, 最小={valid_re.min():.4f}, 最大={valid_re.max():.4f}")
    print(f"  KGE         : 均值={valid_kge.mean():.4f}, 标准差={valid_kge.std():.4f}, 最小={valid_kge.min():.4f}, 最大={valid_kge.max():.4f}")
    print(f"  NSE         : 均值={valid_nse.mean():.4f}, 标准差={valid_nse.std():.4f}")
    print(f"  RMSE        : 均值={valid_rmse.mean():.4f}, 标准差={valid_rmse.std():.4f}")

    # ---- 5. 文件路径汇总 ----
    print(f"\n[5/5] 输出文件")
    print(f"  {RESULTS_JSON}")
    print(f"  {SUMMARY_CSV}")
    print(f"  {DETAIL_CSV}")
    print("=" * 70)


if __name__ == "__main__":
    rolling_forecast_evaluation()
