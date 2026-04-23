from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd


def resolve_actual_forecast_start(
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


def resolve_forecast_rain_read_anchor_window(
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


def shift_station_df_time_label_for_dbtype(
    df: pd.DataFrame,
    *,
    time_delta: timedelta,
    dbtype: int,
) -> pd.DataFrame:
    """
    按“实况库源为后时标”将标签映射到方案展示时标：

    - dbtype=0（后时标展示）：不平移（库时刻即展示时刻）
    - dbtype=-1（前时标展示）：标签回拨 1 步（例如库 05:00 -> 展示 04:00）
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


def resolve_station_read_window_for_dbtype(
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
    - dbtype=0：读窗不平移
    - dbtype=-1：读窗 +1 步，随后展示标签再 -1 步
    """
    if int(dbtype) != -1:
        return read_time_start, read_time_end, station_obs_end
    shifted_start = read_time_start + time_delta
    shifted_end = read_time_end + time_delta
    shifted_obs_end = station_obs_end + time_delta if station_obs_end is not None else None
    # 日方案前时标：放宽 1 天，避免 daydb 白天时间戳导致最后历史日漏读。
    if shifted_obs_end is not None and time_delta >= timedelta(days=1):
        shifted_obs_end = shifted_obs_end + time_delta
    return shifted_start, shifted_end, shifted_obs_end
