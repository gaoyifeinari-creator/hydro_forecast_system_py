from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class TankParams:
    """Tank 模型参数。"""

    upper_outflow_coeff: float = 0.30
    lower_outflow_coeff: float = 0.10
    percolation_coeff: float = 0.20
    evap_coeff: float = 0.05


@dataclass
class TankState:
    """Tank 模型状态变量（可随计算更新）。"""

    upper_storage: float = 20.0
    lower_storage: float = 60.0


@dataclass(frozen=True)
class TankRunoffModel(IHydrologicalModel):
    """双水箱 Tank 产流模型（参数与状态变量分离版）。"""

    params: TankParams = field(default_factory=TankParams)
    state: TankState = field(default_factory=TankState)

    @classmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        return frozenset({ForcingKind.PRECIPITATION})

    def run(self, forcing: ForcingData) -> TimeSeries:
        self._validate_parameters()
        input_series = forcing.require(ForcingKind.PRECIPITATION)
        upper = self.state.upper_storage
        lower = self.state.lower_storage
        runoff_values: List[float] = []

        for rainfall in input_series.values:
            effective_rain = max(0.0, rainfall * (1.0 - self.params.evap_coeff))
            upper += effective_rain
            upper_out = upper * self.params.upper_outflow_coeff
            percolation = upper * self.params.percolation_coeff
            upper -= upper_out + percolation
            lower += percolation
            lower_out = lower * self.params.lower_outflow_coeff
            lower -= lower_out
            runoff_values.append(max(0.0, upper_out + lower_out))

        self.state.upper_storage = max(0.0, upper)
        self.state.lower_storage = max(0.0, lower)
        return TimeSeries(input_series.start_time, input_series.time_step, runoff_values)

    def _validate_parameters(self) -> None:
        for name in (
            "upper_outflow_coeff",
            "lower_outflow_coeff",
            "percolation_coeff",
            "evap_coeff",
        ):
            value = getattr(self.params, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.params.upper_outflow_coeff + self.params.percolation_coeff > 1.0:
            raise ValueError("upper_outflow_coeff + percolation_coeff must be <= 1")
        if self.state.upper_storage < 0 or self.state.lower_storage < 0:
            raise ValueError("state storages must be >= 0")
