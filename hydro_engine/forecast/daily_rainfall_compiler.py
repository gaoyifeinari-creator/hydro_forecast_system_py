"""
多步长预报降雨 -> 自然日日雨量整编（1 小时画布法）。

设计约束：
1) 输入记录为“前时标”语义：start_time 表示时段起点。
2) 先降维到 1h 再聚合到自然日，避免跨日 if-else 分支。
3) 日值标签支持前/后时标展示：
   - 前时标：YYYY-MM-DD 保持自然日标签
   - 后时标：标签整体 +1 天，仅改标签不改数值
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Literal


TimestampMode = Literal["forward", "backward"]


@dataclass(frozen=True)
class RainSpanRecord:
    """单条预报降雨记录。"""

    start_time: datetime
    span_hours: int
    value: float


@dataclass(frozen=True)
class DailyRainfallPoint:
    """自然日日雨量结果点。"""

    time: datetime
    value: float


def build_hourly_canvas(records: Iterable[RainSpanRecord]) -> Dict[datetime, float]:
    """
    将多步长累计雨量拆解并累加到 1 小时画布。

    每条记录按 value/span_hours 均摊到连续 span_hours 个小时起点：
    例如 (2025-10-31 23:00, span=3, value=0.8) 会写入
    23:00、00:00、01:00 三个小时桶，每桶 +0.8/3。
    """
    canvas: Dict[datetime, float] = {}
    for rec in records:
        span = int(rec.span_hours)
        if span <= 0:
            raise ValueError(f"span_hours must be > 0, got {rec.span_hours!r}")
        hourly_rain = float(rec.value) / float(span)
        t0 = rec.start_time
        for i in range(span):
            ht = t0 + timedelta(hours=i)
            canvas[ht] = float(canvas.get(ht, 0.0)) + hourly_rain
    return canvas


def aggregate_hourly_canvas_to_daily(
    hourly_canvas: Dict[datetime, float],
    *,
    timestamp_mode: TimestampMode = "forward",
    decimals: int = 2,
) -> List[DailyRainfallPoint]:
    """
    将 1 小时画布按自然日聚合。

    timestamp_mode:
    - "forward"  -> 日值标签为自然日本日 00:00
    - "backward" -> 日值标签整体 +1 天（后时标展示）
    """
    daily_map: Dict[datetime, float] = {}
    for hour_start, v in hourly_canvas.items():
        day_start = datetime(hour_start.year, hour_start.month, hour_start.day)
        daily_map[day_start] = float(daily_map.get(day_start, 0.0)) + float(v)

    out: List[DailyRainfallPoint] = []
    shift = timedelta(days=1) if str(timestamp_mode).strip().lower() == "backward" else timedelta(0)
    for day_start in sorted(daily_map.keys()):
        out.append(
            DailyRainfallPoint(
                time=day_start + shift,
                value=round(float(daily_map[day_start]), int(decimals)),
            )
        )
    return out


def compile_multispan_rain_to_daily(
    records: Iterable[RainSpanRecord],
    *,
    timestamp_mode: TimestampMode = "forward",
    decimals: int = 2,
) -> List[DailyRainfallPoint]:
    """一站式：多步长记录 -> 1h 画布 -> 自然日日雨量。"""
    canvas = build_hourly_canvas(records)
    return aggregate_hourly_canvas_to_daily(
        canvas,
        timestamp_mode=timestamp_mode,
        decimals=decimals,
    )


__all__ = [
    "RainSpanRecord",
    "DailyRainfallPoint",
    "TimestampMode",
    "build_hourly_canvas",
    "aggregate_hourly_canvas_to_daily",
    "compile_multispan_rain_to_daily",
]

