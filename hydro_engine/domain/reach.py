from __future__ import annotations

from dataclasses import dataclass

from hydro_engine.core.forcing import ForcingData
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class RiverReach:
    """河道链路实体：连接上下游节点，并持有河道演进模型。"""

    id: str
    upstream_node_id: str
    downstream_node_id: str
    routing_model: IHydrologicalModel

    def route(self, inflow_forcing: ForcingData) -> TimeSeries:
        return self.routing_model.run(inflow_forcing)
