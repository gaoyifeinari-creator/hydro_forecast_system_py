from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal

from hydro_engine.core.timeseries import TimeSeries
from .base import AbstractNode


@dataclass(frozen=True)
class ReservoirLevelFeatures:
    """水库特征水位。"""

    dead_level: float
    normal_level: float
    flood_limit_level: float
    check_flood_level: float


@dataclass(frozen=True)
class ReservoirOperationConstraints:
    """水库运行约束。"""

    min_release: float = 0.0
    max_release: float = 1.0e12


@dataclass(frozen=True)
class CurvePoint:
    """关系曲线点。"""

    x: float
    y: float


@dataclass(frozen=True)
class ReservoirCurve:
    """
    关系曲线定义。

    - name: 曲线名称，例如 `level_storage`、`tailwater_discharge`
    - direction: 方向，例如 `level_to_storage`
    - points: 曲线离散点
    """

    name: str
    direction: Literal[
        "level_to_storage",
        "storage_to_level",
        "tailwater_to_discharge",
        "discharge_to_tailwater",
    ]
    points: List[CurvePoint] = field(default_factory=list)


@dataclass
class ReservoirNode(AbstractNode):
    """水库节点。"""

    inflow_attenuation: float = 0.8
    dispatch_model_alg_type: str = "Attenuation"
    level_features: ReservoirLevelFeatures | None = None
    operation_constraints: ReservoirOperationConstraints = field(
        default_factory=ReservoirOperationConstraints
    )
    curves: List[ReservoirCurve] = field(default_factory=list)

    def _compute_simulated_outflows(self, total_inflow: TimeSeries) -> Dict[str, TimeSeries]:
        if not self.outgoing_reach_ids:
            return {}
        if len(self.outgoing_reach_ids) != 1:
            raise ValueError(f"ReservoirNode {self.id} must have exactly 1 outgoing reach")

        # 兼容旧系统调度模型：InOutflowBalance => 出库始终等于入库。
        if str(self.dispatch_model_alg_type).strip().lower() == "inoutflowbalance":
            return {self.outgoing_reach_ids[0]: total_inflow}

        raw_release = total_inflow.scale(self.inflow_attenuation)
        if raw_release.values.ndim != 1:
            raise ValueError("ReservoirNode attenuation path requires 1-D inflow series")
        min_release = self.operation_constraints.min_release
        max_release = self.operation_constraints.max_release
        if min_release > max_release:
            raise ValueError("operation_constraints.min_release must be <= max_release")

        clipped = [min(max(float(v), min_release), max_release) for v in raw_release.values.tolist()]
        constrained_release = TimeSeries(
            start_time=raw_release.start_time,
            time_step=raw_release.time_step,
            values=clipped,
        )
        return {self.outgoing_reach_ids[0]: constrained_release}
