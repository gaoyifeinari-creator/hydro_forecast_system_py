from __future__ import annotations

from dataclasses import dataclass
from typing import List

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class SnowmeltRunoffModel(IHydrologicalModel):
    """
    示例：融雪类产流（伪模型）。

    契约：降雨 + 气温 + 雪深；用于演示多要素声明与从 ForcingData 取值。
    出流 = 降雨折算产流 + 气温驱动的融雪项（示意公式，非业务标定结果）。
    """

    temperature_melt_threshold: float = 0.0
    melt_degree_factor: float = 0.02
    rain_runoff_factor: float = 0.4

    @classmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        return frozenset(
            {
                ForcingKind.PRECIPITATION,
                ForcingKind.AIR_TEMPERATURE,
                ForcingKind.SNOW_DEPTH,
            }
        )

    def run(self, forcing: ForcingData) -> TimeSeries:
        p = forcing.require(ForcingKind.PRECIPITATION)
        t_air = forcing.require(ForcingKind.AIR_TEMPERATURE)
        snow = forcing.require(ForcingKind.SNOW_DEPTH)
        for a, b in ((p, t_air), (p, snow), (t_air, snow)):
            if a.values.ndim != 1 or b.values.ndim != 1:
                raise ValueError("SnowmeltRunoffModel requires 1-D inputs")
            if (
                a.start_time != b.start_time
                or a.time_step != b.time_step
                or a.time_steps != b.time_steps
            ):
                raise ValueError("Snowmelt inputs must share the same time grid")

        out: List[float] = []
        for rain, temp, sd in zip(p.values.tolist(), t_air.values.tolist(), snow.values.tolist()):
            melt = max(0.0, temp - self.temperature_melt_threshold) * self.melt_degree_factor * sd
            q = self.rain_runoff_factor * rain + melt
            out.append(max(0.0, q))

        return TimeSeries(p.start_time, p.time_step, out)
