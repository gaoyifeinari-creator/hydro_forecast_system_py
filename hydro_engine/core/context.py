from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from hydro_engine.core.timeseries import TimeSeries


class TimeType(Enum):
    """原生时间粒度（与 step_size 组合唯一确定时间步长，引擎内不做跨尺度换算）。"""

    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


def parse_time_type(value: str) -> TimeType:
    key = str(value).strip().lower()
    mapping = {
        "minute": TimeType.MINUTE,
        "hour": TimeType.HOUR,
        "day": TimeType.DAY,
    }
    if key not in mapping:
        raise ValueError(
            f"Unknown time_type '{value}'. Expected one of: {', '.join(mapping)}"
        )
    return mapping[key]


def _make_time_delta(time_type: TimeType, step_size: int) -> timedelta:
    if step_size <= 0:
        raise ValueError("step_size must be positive")
    if time_type is TimeType.MINUTE:
        return timedelta(minutes=step_size)
    if time_type is TimeType.HOUR:
        return timedelta(hours=step_size)
    if time_type is TimeType.DAY:
        return timedelta(days=step_size)
    raise ValueError(f"Unsupported time_type: {time_type}")


@dataclass(frozen=True)
class ForecastTimeContext:
    """
    洪水预报四阶段时间上下文。

    时间步由 ``time_type`` + ``step_size`` 原生确定（``time_delta``），不做单位换算。
    例如：日方案下每个步长代表一个完整的日尺度；3 小时方案下每步代表 3 小时。

    时间轴也可由 ``from_period_counts`` 用四段步数构造（见该方法文档）：预热总长度 W 自 T0 向历史回溯，
    校正段 C、历史展示段 H 为 T0 前尾段（嵌套于 W 内），预报段 F 自 T0 起向未来。
    """

    warmup_start_time: datetime
    correction_start_time: datetime
    forecast_start_time: datetime
    display_start_time: datetime
    end_time: datetime
    time_type: TimeType
    step_size: int

    @property
    def time_delta(self) -> timedelta:
        """本方案原生时间步长（与 TimeSeries.time_step 对齐）。"""
        return _make_time_delta(self.time_type, self.step_size)

    @property
    def time_step(self) -> timedelta:
        """与 :class:`TimeSeries` 的 ``time_step`` 语义一致，等价于 ``time_delta``。"""
        return self.time_delta

    def validate(self) -> None:
        td = self.time_delta
        if td.total_seconds() <= 0:
            raise ValueError("time_delta must be positive")
        if self.forecast_start_time >= self.end_time:
            raise ValueError("forecast_start_time must be < end_time")
        if self.warmup_start_time > self.display_start_time:
            raise ValueError("warmup_start_time must be <= display_start_time")
        if self.display_start_time > self.correction_start_time:
            raise ValueError("display_start_time must be <= correction_start_time")
        if self.correction_start_time > self.forecast_start_time:
            raise ValueError("correction_start_time must be <= forecast_start_time")
        if self.display_start_time > self.end_time:
            raise ValueError("display_start_time must be <= end_time")

        ws = td.total_seconds()
        span = (self.end_time - self.warmup_start_time).total_seconds()
        if span <= 0:
            raise ValueError("end_time must be after warmup_start_time")
        if span % ws != 0:
            raise ValueError("(end_time - warmup_start_time) must be a multiple of time_delta")

        for label, t in (
            ("warmup_start_time", self.warmup_start_time),
            ("correction_start_time", self.correction_start_time),
            ("forecast_start_time", self.forecast_start_time),
            ("display_start_time", self.display_start_time),
        ):
            if (t - self.warmup_start_time).total_seconds() % ws != 0:
                raise ValueError(f"{label} must lie on the time grid from warmup_start_time")

    @property
    def step_count(self) -> int:
        """从 warmup 至 end（不含 end）的步数。"""
        td = self.time_delta
        ws = td.total_seconds()
        span = (self.end_time - self.warmup_start_time).total_seconds()
        return int(span // ws)

    def build_uniform_series(self, fill_value: float) -> TimeSeries:
        """与上下文对齐的常数时间序列（用于无入流等占位）。"""
        return TimeSeries(
            start_time=self.warmup_start_time,
            time_step=self.time_delta,
            values=[fill_value] * self.step_count,
        )

    @classmethod
    def from_period_counts(
        cls,
        warmup_start_time: datetime,
        time_type: TimeType,
        step_size: int,
        *,
        warmup_period_steps: int,
        correction_period_steps: int,
        historical_display_period_steps: int,
        forecast_period_steps: int,
    ) -> ForecastTimeContext:
        """
        以「预报起点 T0」为锚，向历史回溯嵌套构造时间轴（四段步数语义）：

        - **预热** ``warmup_period_steps`` (W)：**总**预热步数（自 T0 向历史的完整回溯长度，非分段相加）。
          数据/模拟自 ``warmup_start_time = T0 − W·Δt`` 起至 ``end_time`` 止。
        - **历史显示** ``historical_display_period_steps`` (H)：T0 前最近 H 步，时间窗
          ``[T0−H·Δt, T0)``；须 **H ≤ W**。
        - **校正** ``correction_period_steps`` (C)：T0 前最近 C 步用实测等做校正（如 AR1），
          时间窗 ``[T0−C·Δt, T0)``；须 **C ≤ H ≤ W**。
        - **预报** ``forecast_period_steps`` (F)：自 T0 起向未来 F 步（须 ≥ 1）。

        推导（记 ``warmup_start`` 为 ``ws``）：
        ``forecast_start`` (T0) = ``ws + W·Δt``；
        ``display_start`` = ``T0 − H·Δt``；
        ``correction_start`` = ``T0 − C·Δt``；
        ``end_time`` = ``T0 + F·Δt``。
        总步数 = **W + F**（T0 前 W 步，T0 起 F 步）。
        """
        td = _make_time_delta(time_type, step_size)
        w, c, h, f = (
            int(warmup_period_steps),
            int(correction_period_steps),
            int(historical_display_period_steps),
            int(forecast_period_steps),
        )
        if min(w, c, h) < 0 or f < 0:
            raise ValueError("period step counts must be non-negative")
        if f < 1:
            raise ValueError("forecast_period_steps must be >= 1")
        if w == 0 and max(h, c) > 0:
            raise ValueError(
                "warmup_period_steps must be > 0 when historical_display_period_steps "
                "or correction_period_steps is > 0"
            )
        if h > w or c > w:
            raise ValueError(
                "historical_display_period_steps and correction_period_steps must not exceed warmup_period_steps"
            )
        if c > h:
            raise ValueError(
                "correction_period_steps must be <= historical_display_period_steps "
                "(tail correction window within historical display tail)"
            )

        forecast_start = warmup_start_time + td * w
        display_start = forecast_start - td * h
        correction_start = forecast_start - td * c
        end_time = forecast_start + td * f

        return cls.from_absolute_times(
            warmup_start_time=warmup_start_time,
            correction_start_time=correction_start,
            forecast_start_time=forecast_start,
            display_start_time=display_start,
            end_time=end_time,
            time_type=time_type,
            step_size=step_size,
        )

    @classmethod
    def from_relative_steps(
        cls,
        base_t0: datetime,
        time_type: TimeType,
        step_size: int,
        *,
        warmup_step: int,
        correction_step: int,
        forecast_step: int,
        display_step: int,
        end_step: int,
    ) -> ForecastTimeContext:
        """
        由相对步数与锚点 ``base_t0`` 构造上下文；各阶段时刻均为 ``base_t0 + step * time_delta``。
        """
        td = _make_time_delta(time_type, step_size)
        return cls.from_absolute_times(
            warmup_start_time=base_t0 + td * warmup_step,
            correction_start_time=base_t0 + td * correction_step,
            forecast_start_time=base_t0 + td * forecast_step,
            display_start_time=base_t0 + td * display_step,
            end_time=base_t0 + td * end_step,
            time_type=time_type,
            step_size=step_size,
        )

    @classmethod
    def from_absolute_times(
        cls,
        warmup_start_time: datetime,
        correction_start_time: datetime,
        forecast_start_time: datetime,
        display_start_time: datetime,
        end_time: datetime,
        time_type: TimeType,
        step_size: int,
    ) -> ForecastTimeContext:
        """由绝对时间构造；须与 ``time_type``/``step_size`` 决定的网格一致。"""
        ctx = cls(
            warmup_start_time=warmup_start_time,
            correction_start_time=correction_start_time,
            forecast_start_time=forecast_start_time,
            display_start_time=display_start_time,
            end_time=end_time,
            time_type=time_type,
            step_size=step_size,
        )
        ctx.validate()
        return ctx
