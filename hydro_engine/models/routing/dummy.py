from __future__ import annotations

from dataclasses import dataclass

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class DummyRoutingModel(IHydrologicalModel):
    """伪河道模型：输出 = 入流 * attenuation。"""

    attenuation: float = 1.0

    @classmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        return frozenset({ForcingKind.ROUTING_INFLOW})

    def run(self, forcing: ForcingData) -> TimeSeries:
        if self.attenuation < 0:
            raise ValueError("attenuation must be >= 0")
        inflow = forcing.require(ForcingKind.ROUTING_INFLOW)
        return inflow.scale(self.attenuation)
