from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from hydro_engine.core.timeseries import TimeSeries
from .base import AbstractNode


@dataclass
class CrossSectionNode(AbstractNode):
    """常规断面节点。"""

    def _compute_simulated_outflows(self, total_inflow: TimeSeries) -> Dict[str, TimeSeries]:
        if not self.outgoing_reach_ids:
            return {}
        if len(self.outgoing_reach_ids) != 1:
            raise ValueError(f"CrossSectionNode {self.id} must have exactly 1 outgoing reach")
        return {self.outgoing_reach_ids[0]: total_inflow}
