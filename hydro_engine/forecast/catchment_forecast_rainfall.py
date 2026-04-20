"""
单个子流域（catchment）的预报面雨量三情景封装。

用于「实况末态 -> 多情景预报降雨 -> 多情景流量」骨架中的预报降雨输入。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional, Sequence

import pandas as pd


@dataclass(frozen=True)
class CatchmentForecastRainfall:
    """
    管理单个 catchment 在预报时段内的面雨量三情景时间序列。

    - ``time_index``：预报步对应的时刻（长度 N，单调递增）。
    - ``expected`` / ``upper`` / ``lower``：与 ``time_index`` 一一对应的面雨量（单位与实况强迫一致，如 mm/步）。
    - ``pet``（可选）：与 ``time_index`` 等长的预报期潜在蒸发能力，用于注入子流域强迫（与引擎合成后覆写预报段）。
    """

    catchment_id: str
    time_index: pd.DatetimeIndex
    expected: List[float]
    upper: List[float]
    lower: List[float]
    time_step: timedelta
    pet: Optional[List[float]] = None

    def __post_init__(self) -> None:
        self._validate_lengths()
        self._validate_time_grid()
        self._validate_bounds()

    def _validate_lengths(self) -> None:
        n = len(self.time_index)
        for name, seq in (
            ("expected", self.expected),
            ("upper", self.upper),
            ("lower", self.lower),
        ):
            if len(seq) != n:
                raise ValueError(
                    f"CatchmentForecastRainfall[{self.catchment_id!r}]: "
                    f"len({name})={len(seq)} 与 len(time_index)={n} 不一致"
                )
        if self.pet is not None and len(self.pet) != n:
            raise ValueError(
                f"CatchmentForecastRainfall[{self.catchment_id!r}]: "
                f"len(pet)={len(self.pet)} 与 len(time_index)={n} 不一致"
            )

    def _validate_time_grid(self) -> None:
        if len(self.time_index) < 2:
            return
        diffs = pd.Series(self.time_index).diff().dropna()
        if diffs.nunique() != 1:
            raise ValueError(
                f"CatchmentForecastRainfall[{self.catchment_id!r}]: "
                "time_index 步长不一致，请保证为均匀时间轴"
            )
        inferred = diffs.iloc[0].to_pytimedelta()
        if inferred != self.time_step:
            raise ValueError(
                f"CatchmentForecastRainfall[{self.catchment_id!r}]: "
                f"time_index 推断步长 {inferred!r} 与构造参数 time_step={self.time_step!r} 不一致"
            )

    def _validate_bounds(self) -> None:
        for i, t in enumerate(self.time_index):
            lo, mid, hi = float(self.lower[i]), float(self.expected[i]), float(self.upper[i])
            if lo > mid or mid > hi:
                raise ValueError(
                    f"CatchmentForecastRainfall[{self.catchment_id!r}]: "
                    f"在时刻 {t} 不满足 lower<=expected<=upper："
                    f"lower={lo}, expected={mid}, upper={hi}"
                )

    @classmethod
    def from_aligned_arrays(
        cls,
        *,
        catchment_id: str,
        time_index: pd.DatetimeIndex,
        expected: Sequence[float],
        upper: Sequence[float],
        lower: Sequence[float],
        time_step: timedelta,
        pet: Optional[Sequence[float]] = None,
    ) -> CatchmentForecastRainfall:
        """由已对齐的数组构造（内部会拷贝为 list[float]）。"""
        pet_list: Optional[List[float]] = None
        if pet is not None:
            pet_list = [float(x) for x in pet]
        return cls(
            catchment_id=str(catchment_id).strip(),
            time_index=pd.DatetimeIndex(time_index),
            expected=[float(x) for x in expected],
            upper=[float(x) for x in upper],
            lower=[float(x) for x in lower],
            time_step=time_step,
            pet=pet_list,
        )
