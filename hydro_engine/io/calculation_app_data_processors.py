from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, List, Optional, Tuple

import pandas as pd


@dataclass
class StandardizedLoadedData:
    """读取阶段标准化输出。"""

    rain_df: pd.DataFrame
    flow_df: pd.DataFrame
    warns: List[str]
    rain_senids: List[str]
    flow_senids: List[str]
    time_type: str
    time_start: datetime
    time_end: datetime


LoadedDataProcessor = Callable[[StandardizedLoadedData], StandardizedLoadedData]


def _default_noop_loaded_data_processor(data: StandardizedLoadedData) -> StandardizedLoadedData:
    """默认空处理器：保留原始行为。"""
    return data


def build_loaded_data_processors(time_type: str) -> List[Tuple[str, LoadedDataProcessor]]:
    """
    构建读取后处理管线。
    目前统一挂载空处理器，后续可按 time_type 注入不同算法链。
    """
    _ = str(time_type).strip().lower()
    return [("noop_passthrough", _default_noop_loaded_data_processor)]


def standardize_loaded_inputs(
    *,
    rain_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    warns: List[str],
    rain_senids: List[str],
    flow_senids: List[str],
    time_type: str,
    time_start: datetime,
    time_end: datetime,
) -> StandardizedLoadedData:
    """
    统一读取数据结构，便于后续集中扩展字段规整、缺测标识与质量检查。
    当前保持“零行为变更”。
    """
    return StandardizedLoadedData(
        rain_df=rain_df,
        flow_df=flow_df,
        warns=list(warns or []),
        rain_senids=list(rain_senids or []),
        flow_senids=list(flow_senids or []),
        time_type=str(time_type),
        time_start=time_start,
        time_end=time_end,
    )


def apply_loaded_data_processors(
    data: StandardizedLoadedData,
    *,
    time_type: str,
    on_log: Optional[Any] = None,
) -> StandardizedLoadedData:
    """
    读取后、建模前的预留处理层（管线化执行，当前算法均为空实现）。
    """
    processors = build_loaded_data_processors(time_type)
    if on_log is not None:
        try:
            names = ",".join(name for name, _ in processors) if processors else "none"
            on_log(f"[process] loaded-data processors={names}")
        except Exception:
            pass
    out = data
    for _, processor in processors:
        out = processor(out)
    return out
