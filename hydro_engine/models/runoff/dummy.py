from __future__ import annotations

from dataclasses import dataclass

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class DummyRunoffModel(IHydrologicalModel):
    """伪产流模型：输出 = 降雨 * runoff_coefficient。"""

    runoff_coefficient: float = 1.0

    @classmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        return frozenset({ForcingKind.PRECIPITATION})

    def run(self, forcing: ForcingData) -> TimeSeries:
        if self.runoff_coefficient < 0:
            raise ValueError("runoff_coefficient must be >= 0")
        rain = forcing.require(ForcingKind.PRECIPITATION)
        return rain.scale(self.runoff_coefficient)
