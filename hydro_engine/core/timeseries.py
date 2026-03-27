from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional


@dataclass(frozen=True)
class TimeSeries:
    """
    统一的时间序列对象。

    - start_time: 序列起始时间
    - time_step: 时间步长
    - values: 各时间步的数值（例如流量）；缺测可用 math.nan 表示
    """

    start_time: datetime
    time_step: timedelta
    values: List[float]

    def __post_init__(self) -> None:
        if self.time_step.total_seconds() <= 0:
            raise ValueError("time_step must be positive")
        if not self.values:
            raise ValueError("values must not be empty")

    def __len__(self) -> int:
        return len(self.values)

    def end_time_exclusive(self) -> datetime:
        """序列覆盖 [start_time, end_time_exclusive)。"""
        return self.start_time + self.time_step * len(self.values)

    def get_index_by_time(self, t: datetime) -> int:
        """
        获取与 t 对齐的格点索引（t 必须落在序列网格上且在范围内）。
        """
        if t < self.start_time or t >= self.end_time_exclusive():
            raise ValueError("t is outside series range")
        delta = t - self.start_time
        ws = self.time_step.total_seconds()
        sec = delta.total_seconds()
        if sec < 0 or sec % ws != 0:
            raise ValueError("t must align to the series time grid")
        return int(sec // ws)

    def slice(self, start_time: datetime, end_time: Optional[datetime] = None) -> TimeSeries:
        """
        截取子序列，区间为 [start_time, end_time)（end_time 默认到序列末尾的 exclusive end）。
        """
        if end_time is None:
            end_time = self.end_time_exclusive()
        if start_time >= end_time:
            raise ValueError("slice: start_time must be < end_time")

        i0 = self._first_index_at_or_after(start_time)
        i1 = self._first_index_at_or_after(end_time)
        if i0 >= len(self.values) or i0 >= i1:
            raise ValueError("slice range produces empty series")
        return TimeSeries(
            start_time=self.start_time + self.time_step * i0,
            time_step=self.time_step,
            values=list(self.values[i0:i1]),
        )

    def _first_index_at_or_after(self, t: datetime) -> int:
        """第一个满足 start+i*step >= t 的索引。"""
        if t <= self.start_time:
            return 0
        if t >= self.end_time_exclusive():
            return len(self.values)
        delta = t - self.start_time
        ws = self.time_step.total_seconds()
        sec = delta.total_seconds()
        idx = int(math.ceil(sec / ws - 1e-12))
        return min(len(self.values), idx)

    def blend(self, other: "TimeSeries", t0: datetime, replace_missing: bool = True) -> "TimeSeries":
        """
        缝合：在 t0 及之前时刻取 other（实测），在 t0 之后取 self（模拟/校正后）。

        若 replace_missing 为 True，other 在历史期为 nan 时用 self 对应值填补。
        """
        self._assert_compatible(other)
        out: List[float] = []
        for i, v_sim in enumerate(self.values):
            t_i = self.start_time + self.time_step * i
            if t_i <= t0:
                v_obs = other.values[i]
                if replace_missing and isinstance(v_obs, float) and math.isnan(v_obs):
                    out.append(v_sim)
                else:
                    out.append(float(v_obs))
            else:
                out.append(v_sim)
        return TimeSeries(self.start_time, self.time_step, out)

    def __add__(self, other: "TimeSeries") -> "TimeSeries":
        """
        安全相加两个序列。

        要求两者起始时间、步长和长度完全一致，避免隐式错位相加。
        """
        self._assert_compatible(other)
        return TimeSeries(
            start_time=self.start_time,
            time_step=self.time_step,
            values=[a + b for a, b in zip(self.values, other.values)],
        )

    def scale(self, factor: float) -> "TimeSeries":
        """对序列进行系数缩放。"""
        return TimeSeries(
            start_time=self.start_time,
            time_step=self.time_step,
            values=[v * factor for v in self.values],
        )

    def replace_outliers_with_nan(
        self,
        *,
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> "TimeSeries":
        """
        将异常值替换为 NaN，便于后续统一做缺测插补。

        - min_value/max_value 为空时不做对应方向裁剪；
        - NaN 原值保持 NaN。
        """
        out: List[float] = []
        for v in self.values:
            if isinstance(v, float) and math.isnan(v):
                out.append(float("nan"))
                continue
            fv = float(v)
            if min_value is not None and fv < min_value:
                out.append(float("nan"))
                continue
            if max_value is not None and fv > max_value:
                out.append(float("nan"))
                continue
            out.append(fv)
        return TimeSeries(self.start_time, self.time_step, out)

    def interpolate_nan_linear(self) -> "TimeSeries":
        """
        线性插补 NaN：
        - 内部缺测段：在相邻有效点间做线性插值；
        - 缺测开头/结尾：使用最近有效值填充（forward/backward fill）。
        """
        n = len(self.values)
        if n == 0:
            raise ValueError("cannot interpolate empty series")

        # 找有效点
        valid_indices: List[int] = [
            i for i, v in enumerate(self.values) if not (isinstance(v, float) and math.isnan(v))
        ]
        if not valid_indices:
            raise ValueError("all values are NaN; cannot interpolate")
        if len(valid_indices) == n:
            return self

        out = list(map(float, self.values))

        # 前缀：用第一个有效值填
        first = valid_indices[0]
        for i in range(0, first):
            out[i] = out[first]

        # 后缀：用最后一个有效值填
        last = valid_indices[-1]
        for i in range(last + 1, n):
            out[i] = out[last]

        # 内部：对每段缺测做线性插值
        for left_idx, right_idx in zip(valid_indices[:-1], valid_indices[1:]):
            if right_idx == left_idx + 1:
                continue  # 相邻有效点，无缺测
            lv = out[left_idx]
            rv = out[right_idx]
            span = right_idx - left_idx
            for j in range(left_idx + 1, right_idx):
                alpha = (j - left_idx) / span
                out[j] = lv * (1.0 - alpha) + rv * alpha

        return TimeSeries(self.start_time, self.time_step, out)

    def _assert_compatible(self, other: "TimeSeries") -> None:
        if self.start_time != other.start_time:
            raise ValueError("start_time mismatch")
        if self.time_step != other.time_step:
            raise ValueError("time_step mismatch")
        if len(self) != len(other):
            raise ValueError("series length mismatch")


def add_timeseries_list(series_list: Iterable[TimeSeries]) -> TimeSeries:
    """对多个 TimeSeries 安全求和。"""
    items = list(series_list)
    if not items:
        raise ValueError("series_list must not be empty")

    total = items[0]
    for item in items[1:]:
        total = total + item
    return total
