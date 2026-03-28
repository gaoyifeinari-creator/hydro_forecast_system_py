#!/usr/bin/env python3
"""
水文模型 SCE-UA 自动率定脚本。

率定期：2024年3月1日 ~ 2024年5月31日（汛期前，90天）
评估期：2024年6月1日 ~ 2024年9月30日（汛期）

率定完成后：
1. 输出率定后参数
2. 在评估期做滚动预报精度评估
3. 与率定前（原始参数）结果对比
4. 保存率定后模型方案至 configs/calibrated_scheme.json
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from hydro_engine.calibration.calibrator import (
    HydroModelCalibrator,
    CalibrationResult,
)
from hydro_engine.calibration.calibrator import _compute_kge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# 配置
# ============================================================

CONFIG_PATH   = str(PROJECT_ROOT / "configs" / "forecastSchemeConf.json")
RAIN_CSV      = str(PROJECT_ROOT / "tests"    / "佛子岭雨量.csv")
FLOW_CSV      = str(PROJECT_ROOT / "tests"    / "佛子岭流量.csv")

# 率定期（汛期前的平稳期，90天）
CALIB_START   = datetime(2024, 3, 1, 0, 0, 0)
CALIB_END     = datetime(2024, 5, 31, 23, 0, 0)

# 评估期（汛期，120天）
EVAL_START    = datetime(2024, 6, 1, 0, 0, 0)
EVAL_END      = datetime(2024, 9, 30, 23, 0, 0)

# SCE-UA 参数
SCEUA_MAX_ITER   = 500
SCEUA_MAX_NFE    = 2000
SCEUA_N_COMPLEX  = 3
SCEUA_CONV_TOL   = 5e-4
RNG_SEED         = 42

# 滚动预报评估
FORECAST_LEAD_HOURS = 24   # 预见期 24h
WARMUP_DAYS         = 30   # 预热 30d

# 输出
OUTPUT_DIR  = PROJECT_ROOT / "output" / "calibration"
EVAL_CALIB_CSV  = OUTPUT_DIR / "eval_calibrated.csv"
EVAL_ORIG_CSV   = OUTPUT_DIR / "eval_original.csv"
PARAMS_JSON     = OUTPUT_DIR / "calibrated_params.json"
SCHEME_JSON     = PROJECT_ROOT / "configs" / "calibrated_scheme.json"


# ============================================================
# 辅助函数
# ============================================================

def print_metrics(records: list, label: str) -> pd.DataFrame:
    """打印指标并返回 DataFrame。"""
    if not records:
        print(f"  [{label}] 无有效记录")
        return pd.DataFrame()
    df = pd.DataFrame(records)
    n_days = df["forecast_date"].nunique()
    n_nodes = df["node_id"].nunique()

    print(f"\n{'=' * 60}")
    print(f"  [{label}] {n_days}天 × {n_nodes}节点 = {len(records)}条记录")
    print(f"{'=' * 60}")

    agg = df.groupby(["node_id", "node_name"]).agg(
        RE_mean=("relative_error", "mean"),
        RE_std =("relative_error", "std"),
        KGE_mean=("kge", "mean"),
        KGE_std =("kge", "std"),
        NSE_mean=("nse", "mean"),
        RMSE_mean=("rmse", "mean"),
    ).round(4)
    print("\n【各节点指标】")
    print(agg.to_string())

    # 全局
    valid_re  = df["relative_error"].dropna()
    valid_kge = df["kge"].dropna()
    valid_nse = df["nse"].dropna()
    print(f"\n【全局均值】")
    print(f"  RE   : 均值={valid_re.mean():.4f} ± {valid_re.std():.4f}")
    print(f"  KGE  : 均值={valid_kge.mean():.4f} ± {valid_kge.std():.4f}")
    print(f"  NSE  : 均值={valid_nse.mean():.4f} ± {valid_nse.std():.4f}")

    return df


def compare_results(df_orig: pd.DataFrame, df_calib: pd.DataFrame, label: str) -> None:
    """打印率定前后对比。"""
    print(f"\n{'=' * 60}")
    print(f"  率定前后对比 [{label}]")
    print(f"{'=' * 60}")
    for nid in df_orig["node_id"].unique():
        orig_node  = df_orig[df_orig["node_id"] == nid]
        calib_node = df_calib[df_calib["node_id"] == nid]
        name = orig_node["node_name"].iloc[0]
        kge_o = orig_node["kge"].dropna().mean()
        kge_c = calib_node["kge"].dropna().mean()
        nse_o = orig_node["nse"].dropna().mean()
        nse_c = calib_node["nse"].dropna().mean()
        re_o  = orig_node["relative_error"].dropna().mean()
        re_c  = calib_node["relative_error"].dropna().mean()
        print(f"\n  {name} ({nid}):")
        print(f"    KGE : 原始={kge_o:+.4f}  → 率定后={kge_c:+.4f}  (Δ={kge_c - kge_o:+.4f})")
        print(f"    NSE : 原始={nse_o:+.4f}  → 率定后={nse_c:+.4f}  (Δ={nse_c - nse_o:+.4f})")
        print(f"    RE  : 原始={re_o:+.4f}  → 率定后={re_c:+.4f}  (Δ={re_c - re_o:+.4f})")


# ============================================================
# 主流程
# ============================================================

def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("=" * 60)
    logger.info("  佛子岭流域 SCE-UA 自动率定")
    logger.info("=" * 60)

    # ---- 1. 初始化率定器 ----
    logger.info("\n[1/6] 初始化率定器 ...")
    calib = HydroModelCalibrator(
        config_path=CONFIG_PATH,
        rain_csv=RAIN_CSV,
        flow_csv=FLOW_CSV,
        calib_period_start=CALIB_START,
        calib_period_end=CALIB_END,
        warmup_days=WARMUP_DAYS,
    )

    # ---- 2. 运行率定 ----
    logger.info("\n[2/6] 运行 SCE-UA 率定 ...")
    logger.info(f"  率定期: {CALIB_START.date()} ~ {CALIB_END.date()}")
    logger.info(f"  目标函数: NSE")
    logger.info(f"  SCE-UA: max_iter={SCEUA_MAX_ITER}, max_nfe={SCEUA_MAX_NFE}, n_complex={SCEUA_N_COMPLEX}")

    result = calib.calibrate(
        max_iter=SCEUA_MAX_ITER,
        max_nfe=SCEUA_MAX_NFE,
        n_complex=SCEUA_N_COMPLEX,
        convergence_tol=SCEUA_CONV_TOL,
        rng_seed=RNG_SEED,
        progress=True,
    )

    logger.info("\n【率定后参数】")
    for (cid, pname), val in sorted(result.best_params.items()):
        logger.info(f"  {cid}.{pname} = {val:.4f}")

    # ---- 3. 保存率定后方案 ----
    logger.info("\n[3/6] 保存率定后模型方案 ...")
    calib.save_calibrated_scheme(
        result,
        output_path=str(SCHEME_JSON),
        description=f"SCE-UA calibration NSE={result.nse:.4f}",
    )
    logger.info(f"  方案已保存: {SCHEME_JSON}")

    # 保存参数 JSON
    params_out = {
        "metadata": {
            "calibration_start": CALIB_START.isoformat(sep=" "),
            "calibration_end":   CALIB_END.isoformat(sep=" "),
            "nse": result.nse,
            "n_function_evaluations": result.n_function_evaluations,
            "n_iterations": result.n_iterations,
        },
        "parameters": {
            f"{cid}.{pname}": round(val, 6)
            for (cid, pname), val in result.best_params.items()
        },
        "convergence_history": [round(v, 6) for v in result.history],
    }
    with open(PARAMS_JSON, "w", encoding="utf-8") as f:
        json.dump(params_out, f, ensure_ascii=False, indent=2)
    logger.info(f"  参数已保存: {PARAMS_JSON}")

    # ---- 4. 评估期滚动预报 ----
    logger.info("\n[4/6] 评估期滚动预报（率定后参数）...")
    logger.info(f"  评估期: {EVAL_START.date()} ~ {EVAL_END.date()}")

    eval_calib_records = calib.evaluate_rolling(
        param_dict=result.best_params,
        eval_start=EVAL_START,
        eval_end=EVAL_END,
        warmup_days=WARMUP_DAYS,
        forecast_lead_hours=FORECAST_LEAD_HOURS,
    )

    # ---- 5. 原始参数滚动预报（对照）----
    logger.info("\n[5/6] 原始参数滚动预报（对照）...")
    # 构建原始参数字典
    from hydro_engine.calibration.calibrator import build_calib_params, apply_params_to_config
    raw_params = build_calib_params(calib.raw_config["schemes"][0])
    orig_dict: dict = {}
    for p in raw_params:
        if p.included:
            orig_dict[(p.catchment_id, p.name)] = p.default

    eval_orig_records = calib.evaluate_rolling(
        param_dict=orig_dict,
        eval_start=EVAL_START,
        eval_end=EVAL_END,
        warmup_days=WARMUP_DAYS,
        forecast_lead_hours=FORECAST_LEAD_HOURS,
    )

    # ---- 6. 保存 & 打印 ----
    logger.info("\n[6/6] 保存结果 ...")

    if eval_calib_records:
        df_c = pd.DataFrame(eval_calib_records)
        df_c.to_csv(EVAL_CALIB_CSV, index=False, encoding="utf-8-sig")

    if eval_orig_records:
        df_o = pd.DataFrame(eval_orig_records)
        df_o.to_csv(EVAL_ORIG_CSV, index=False, encoding="utf-8-sig")

    print_metrics(eval_orig_records, "原始参数")
    df_calib_summary = print_metrics(eval_calib_records, "率定后参数")

    if eval_orig_records and eval_calib_records:
        compare_results(
            pd.DataFrame(eval_orig_records),
            pd.DataFrame(eval_calib_records),
            f"评估期 {EVAL_START.date()} ~ {EVAL_END.date()}",
        )

    # ---- 收敛曲线 ----
    logger.info("\n【收敛曲线（前30次迭代）】")
    hist = result.history[:30]
    for i, obj in enumerate(hist):
        nse_val = -obj
        bar = "█" * int(max(0, (nse_val + 1) * 20))
        logger.info(f"  Iter {i+1:3d}: NSE = {nse_val:+.4f} | {bar}")

    logger.info("\n" + "=" * 60)
    logger.info("  率定完成！")
    logger.info("=" * 60)
    logger.info(f"  率定后方案: {SCHEME_JSON}")
    logger.info(f"  评估结果  : {EVAL_CALIB_CSV}")
    logger.info(f"  原始对照  : {EVAL_ORIG_CSV}")
    logger.info(f"  参数详情  : {PARAMS_JSON}")


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    main()
