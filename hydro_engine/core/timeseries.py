from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np


def _coerce_values(values: Union[Sequence[float], np.ndarray, List[float]]) -> np.ndarray:
    """Normalize to C-contiguous float64 ndarray, shape (T,) or (S, T)."""
    arr = np.asarray(values, dtype=np.float64)
    if arr.ndim == 0:
        raise ValueError("values must be at least 1-D")
    if arr.ndim > 2:
        raise ValueError("values must be 1-D (deterministic) or 2-D (ensemble x time)")
    if arr.size == 0 or (arr.ndim == 1 and arr.shape[0] == 0) or (arr.ndim == 2 and arr.shape[1] == 0):
        raise ValueError("values must not be empty")
    return np.ascontiguousarray(arr)


@dataclass(frozen=True)
class TimeSeries:
    """
    统一的时间序列对象（NumPy 后端）。

    - start_time: 序列起始时间
    - time_step: 时间步长
    - values: 各时间步数值；缺测用 nan。形状约定：
        * **1D（确定性）**: ``(time_steps,)``
        * **2D（集合/概率）**: ``(num_scenarios, time_steps)``
    """

    start_time: datetime
    time_step: timedelta
    values: np.ndarray

    def __post_init__(self) -> None:
        if self.time_step.total_seconds() <= 0:
            raise ValueError("time_step must be positive")
        coerced = _coerce_values(self.values)
        object.__setattr__(self, "values", coerced)

    @property
    def time_steps(self) -> int:
        """时间长度（最后一维）。"""
        return int(self.values.shape[-1])

    @property
    def num_scenarios(self) -> int:
        """集合成员数；确定性序列返回 1。"""
        if self.values.ndim == 1:
            return 1
        return int(self.values.shape[0])

    @property
    def is_ensemble(self) -> bool:
        return self.values.ndim > 1

    def __len__(self) -> int:
        """与历史代码 ``len(ts)`` 对齐：表示时间步数，而非 ``values`` 的第一维。"""
        return self.time_steps

    def end_time_exclusive(self) -> datetime:
        """序列覆盖 [start_time, end_time_exclusive)。"""
        return self.start_time + self.time_step * self.time_steps

    def get_index_by_time(self, t: datetime) -> int:
        """获取与 t 对齐的格点索引（t 必须落在序列网格上且在范围内）。"""
        if t < self.start_time or t >= self.end_time_exclusive():
            raise ValueError("t is outside series range")
        delta = t - self.start_time
        ws = self.time_step.total_seconds()
        sec = delta.total_seconds()
        if sec < 0 or sec % ws != 0:
            raise ValueError("t must align to the series time grid")
        return int(sec // ws)

    def slice(self, start_time: datetime, end_time: Optional[datetime] = None) -> TimeSeries:
        """截取子序列，区间为 [start_time, end_time)（end_time 默认到序列末尾的 exclusive end）。"""
        if end_time is None:
            end_time = self.end_time_exclusive()
        if start_time >= end_time:
            raise ValueError("slice: start_time must be < end_time")

        i0 = self._first_index_at_or_after(start_time)
        i1 = self._first_index_at_or_after(end_time)
        if i0 >= self.time_steps or i0 >= i1:
            raise ValueError("slice range produces empty series")
        sl = slice(i0, i1)
        new_vals = self.values[..., sl] if self.values.ndim == 2 else self.values[sl]
        return TimeSeries(
            start_time=self.start_time + self.time_step * i0,
            time_step=self.time_step,
            values=np.asarray(new_vals, dtype=np.float64, order="C"),
        )

    def _first_index_at_or_after(self, t: datetime) -> int:
        """第一个满足 start+i*step >= t 的索引。"""
        if t <= self.start_time:
            return 0
        if t >= self.end_time_exclusive():
            return self.time_steps
        delta = t - self.start_time
        ws = self.time_step.total_seconds()
        sec = delta.total_seconds()
        idx = int(math.ceil(sec / ws - 1e-12))
        return min(self.time_steps, idx)

    def _broadcast_pair(self, other: "TimeSeries") -> Tuple[np.ndarray, np.ndarray]:
        """将两序列对齐到相同 ndarray 形状以便逐元素运算。"""
        a, b = self.values, other.values
        if a.ndim == b.ndim:
            if a.shape != b.shape:
                raise ValueError("series shape mismatch for operation")
            return a, b
        if a.ndim == 2 and b.ndim == 1:
            if b.shape[0] != a.shape[1]:
                raise ValueError("1-D series length must match time dimension of 2-D series")
            return a, np.tile(b[np.newaxis, :], (a.shape[0], 1))
        if a.ndim == 1 and b.ndim == 2:
            if a.shape[0] != b.shape[1]:
                raise ValueError("1-D series length must match time dimension of 2-D series")
            return np.tile(a[np.newaxis, :], (b.shape[0], 1)), b
        raise ValueError("incompatible series dimensionalities")

    def _assert_compatible(self, other: "TimeSeries") -> None:
        if self.start_time != other.start_time:
            raise ValueError("start_time mismatch")
        if self.time_step != other.time_step:
            raise ValueError("time_step mismatch")
        if self.time_steps != other.time_steps:
            raise ValueError("series length mismatch (time_steps)")
        if self.values.ndim != other.values.ndim:
            raise ValueError("series ndim mismatch (mixing ensemble and deterministic)")
        if self.values.shape != other.values.shape:
            raise ValueError("series shape mismatch")

    def blend(self, other: "TimeSeries", t0: datetime, replace_missing: bool = True) -> TimeSeries:
        """
        缝合：在 t0 及之前时刻取 other（实测），在 t0 之后取 self（模拟/校正后）。

        若 replace_missing 为 True，other 在历史期为 nan 时用 self 对应值填补。
        支持 self 为集合 (S,T)、other 为单序列 (T,)（other 沿情景维广播）。
        """
        if self.start_time != other.start_time or self.time_step != other.time_step:
            raise ValueError("start_time or time_step mismatch for blend")
        if self.time_steps != other.time_steps:
            raise ValueError("series length mismatch for blend")
        sim, obs = self._broadcast_pair(other)
        out = np.array(sim, dtype=np.float64, copy=True)
        for i in range(self.time_steps):
            t_i = self.start_time + self.time_step * i
            if t_i <= t0:
                o_slice = obs[..., i]
                s_slice = sim[..., i]
                if replace_missing:
                    mask = np.isnan(o_slice)
                    out[..., i] = np.where(mask, s_slice, o_slice)
                else:
                    out[..., i] = o_slice
        return TimeSeries(self.start_time, self.time_step, out)

    def __add__(self, other: "TimeSeries") -> "TimeSeries":
        """逐元素相加；须时间对齐，形状可广播。"""
        if self.start_time != other.start_time or self.time_step != other.time_step:
            raise ValueError("start_time or time_step mismatch")
        if self.time_steps != other.time_steps:
            raise ValueError("series length mismatch")
        a, b = self._broadcast_pair(other)
        return TimeSeries(self.start_time, self.time_step, a + b)

    def scale(self, factor: float) -> TimeSeries:
        return TimeSeries(
            start_time=self.start_time,
            time_step=self.time_step,
            values=self.values * float(factor),
        )

    def mean(self) -> TimeSeries:
        """集合序列沿情景维求均值，得到 (T,) ；确定性序列原样返回。"""
        if self.values.ndim == 1:
            return self
        return TimeSeries(
            start_time=self.start_time,
            time_step=self.time_step,
            values=np.mean(self.values, axis=0),
        )

    def quantiles(self, q: Sequence[float]) -> TimeSeries:
        """
        沿情景维计算分位数，返回 ``(len(q), time_steps)`` 的 :class:`TimeSeries`；
        确定性输入直接重复堆叠（与 ``np.quantile`` 标量轴行为一致）。
        """
        q_arr = np.asarray(list(q), dtype=np.float64)
        if self.values.ndim == 1:
            stacked = np.tile(self.values[np.newaxis, :], (len(q_arr), 1))
            return TimeSeries(self.start_time, self.time_step, stacked)
        qs = np.quantile(self.values, q_arr, axis=0)
        return TimeSeries(self.start_time, self.time_step, np.asarray(qs, dtype=np.float64, order="C"))

    def replace_outliers_with_nan(
        self,
        *,
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> TimeSeries:
        out = np.array(self.values, dtype=np.float64, copy=True)
        valid = ~np.isnan(out)
        if min_value is not None:
            bad = valid & (out < min_value)
            out[bad] = np.nan
        if max_value is not None:
            bad = valid & (out > max_value)
            out[bad] = np.nan
        return TimeSeries(self.start_time, self.time_step, out)

    def interpolate_nan_linear(self) -> TimeSeries:
        """线性插补 NaN（对最后一维时间维；集合序列对每个情景独立插补）。"""
        v = np.array(self.values, dtype=np.float64, copy=True)
        if v.ndim == 1:
            return TimeSeries(self.start_time, self.time_step, _interpolate_1d(v))
        rows = [_interpolate_1d(v[i, :].copy()) for i in range(v.shape[0])]
        return TimeSeries(self.start_time, self.time_step, np.vstack(rows))


def _interpolate_1d(out: np.ndarray) -> np.ndarray:
    n = out.shape[0]
    if n == 0:
        raise ValueError("cannot interpolate empty series")
    valid = np.flatnonzero(~np.isnan(out))
    if valid.size == 0:
        raise ValueError("all values are NaN; cannot interpolate")
    if valid.size == n:
        return out

    first, last = int(valid[0]), int(valid[-1])
    out[:first] = out[first]
    out[last + 1 :] = out[last]

    for left_idx, right_idx in zip(valid[:-1], valid[1:]):
        if right_idx == left_idx + 1:
            continue
        lv, rv = float(out[left_idx]), float(out[right_idx])
        span = right_idx - left_idx
        j_idx = np.arange(left_idx + 1, right_idx, dtype=np.int64)
        alpha = (j_idx - left_idx) / span
        out[j_idx] = lv * (1.0 - alpha) + rv * alpha
    return out


def add_timeseries_list(series_list: Iterable[TimeSeries]) -> TimeSeries:
    """对多个 TimeSeries 安全求和（须两两时间网格一致、形状可广播一致）。"""
    items = list(series_list)
    if not items:
        raise ValueError("series_list must not be empty")

    total = items[0]
    for item in items[1:]:
        total = total + item
    return total


def summarize_for_display_json(
    series: TimeSeries,
    *,
    quantiles: Tuple[float, ...] = (0.1, 0.5, 0.9),
) -> dict:
    """
    将序列压缩为前端友好结构：确定性为 ``deterministic``；
    集合序列为 ``ensemble_pXX`` / ``ensemble_mean``。
    """
    v = series.values
    if v.ndim == 1:
        return {"deterministic": v.tolist()}
    qv = np.quantile(v, np.asarray(quantiles, dtype=np.float64), axis=0)
    out: dict = {"ensemble_mean": np.mean(v, axis=0).tolist()}
    for q, row in zip(quantiles, qv):
        pct = int(round(q * 100))
        out[f"ensemble_p{pct}"] = row.tolist()
    return out
