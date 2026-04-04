from __future__ import annotations

from dataclasses import dataclass

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class SubCatchment:
    """子流域实体：持有产流模型，可选挂载子流域到下游节点的演进模型。"""

    id: str
    runoff_model: IHydrologicalModel
    routing_model: IHydrologicalModel | None = None
    downstream_node_id: str = ""

    def generate_runoff(self, forcing: ForcingData) -> TimeSeries:
        return self.runoff_model.run(forcing)

    def route_runoff(self, runoff: TimeSeries) -> TimeSeries:
        """产流序列经子流域汇流模型；输出与入流同一时间网格（如马斯京根不移动时间戳）。"""
        if self.routing_model is None:
            return runoff
        routing_input = ForcingData.single(ForcingKind.ROUTING_INFLOW, runoff)
        return self.routing_model.run(routing_input)
