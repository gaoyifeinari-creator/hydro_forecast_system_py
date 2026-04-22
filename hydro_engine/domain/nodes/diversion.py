from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from hydro_engine.core.timeseries import TimeSeries
from .base import AbstractNode


@dataclass
class DiversionNode(AbstractNode):
    """分流节点/溢流堰节点。"""

    main_channel_id: str = ""
    bypass_channel_id: str = ""
    main_channel_capacity: float = 0.0

    def _compute_simulated_outflows(self, total_inflow: TimeSeries) -> Dict[str, TimeSeries]:
        if not self.main_channel_id or not self.bypass_channel_id:
            raise ValueError(f"DiversionNode {self.id} channel ids are not configured")
        if total_inflow.values.ndim != 1:
            raise ValueError("DiversionNode requires 1-D total_inflow series")

        main_values: List[float] = []
        bypass_values: List[float] = []
        for flow in total_inflow.values.tolist():
            main_flow = min(flow, self.main_channel_capacity)
            bypass_flow = max(0.0, flow - self.main_channel_capacity)
            main_values.append(main_flow)
            bypass_values.append(bypass_flow)

        main_ts = TimeSeries(total_inflow.start_time, total_inflow.time_step, main_values)
        bypass_ts = TimeSeries(total_inflow.start_time, total_inflow.time_step, bypass_values)
        return {self.main_channel_id: main_ts, self.bypass_channel_id: bypass_ts}
