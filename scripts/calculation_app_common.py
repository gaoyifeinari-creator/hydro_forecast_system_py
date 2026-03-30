"""
Thin wrapper for legacy imports.

历史原因：原先 `scripts/calculation_app_common.py` 同时承担了“读数/拼装”两类职责，
导致 `hydro_engine` 层无法独立复用逻辑。

本文件已瘦身为“薄封装/重导出”：
- 读数与数据库 IN 分块：见 `hydro_engine/io/calculation_app_data_loader.py`
- df -> ForcingData/TimeSeries 拼装：见 `hydro_engine/io/calculation_app_data_builder.py`
- 仅保留 UI/Web 端需要的临时配置写入函数：`write_temp_config_with_periods`
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import sys

SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hydro_engine.io.calculation_app_data_builder import (
    build_catchment_observed_flow_series,
    build_catchment_precip_series,
    build_node_observed_flow_series,
    build_observed_flows,
    build_station_packages,
)
from hydro_engine.io.calculation_app_data_loader import (
    DEFAULT_FLOOD_JDBC_CONFIG,
    build_times,
    load_csv,
    load_rain_flow_for_calculation,
    read_config,
)

__all__ = [
    "DEFAULT_FLOOD_JDBC_CONFIG",
    "read_config",
    "load_csv",
    "build_times",
    "load_rain_flow_for_calculation",
    # builder exports used by tests/rolling evaluation
    "build_catchment_precip_series",
    "build_node_observed_flow_series",
    "build_catchment_observed_flow_series",
    "build_observed_flows",
    "build_station_packages",
    # web/desktop runtime only
    "write_temp_config_with_periods",
]


def write_temp_config_with_periods(
    config_path: str,
    *,
    time_type: str,
    step_size: int,
    warmup_steps: int,
    correction_steps: int,
    historical_steps: int,
    forecast_steps: int,
) -> str:
    """
    Web/desktop：在计算启动时，把 `time_axis` 中的四段步数写入临时 JSON 文件。

    该函数属于“应用入口运行时行为”，不应下沉到 `hydro_engine`。
    """
    data = read_config(config_path)
    if not data.get("schemes"):
        raise ValueError("配置缺少 schemes")

    target = None
    for s in data["schemes"]:
        if str(s.get("time_type")) == str(time_type) and int(s.get("step_size")) == int(step_size):
            target = s
            break
    if target is None:
        raise ValueError(f"未找到匹配方案：time_type={time_type}, step_size={step_size}")

    target["time_axis"] = {
        "warmup_period_steps": int(warmup_steps),
        "correction_period_steps": int(correction_steps),
        "historical_display_period_steps": int(historical_steps),
        "forecast_period_steps": int(forecast_steps),
    }

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
    json.dump(data, tmp, ensure_ascii=False, indent=2)
    tmp.close()
    return tmp.name

