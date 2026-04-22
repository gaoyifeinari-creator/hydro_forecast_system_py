from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

import networkx as nx

from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.domain.nodes.base import AbstractNode
from hydro_engine.domain.reach import RiverReach


@dataclass
class ForecastingScheme:
    """预报方案：组装实体并构建 DAG 拓扑。"""

    nodes: Dict[str, AbstractNode] = field(default_factory=dict)
    reaches: Dict[str, RiverReach] = field(default_factory=dict)
    catchments: Dict[str, SubCatchment] = field(default_factory=dict)
    graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    # 可选：多源面预报面数据的全局规则（由 json_config 解析并挂载到 scheme）
    catchment_forecast_rules: Dict[str, Any] = field(default_factory=dict)
    # 可选：由规则解析得到的融合计划（用于 runtime 批量读取与嵌套兜底融合）
    catchment_forecast_fusion_plan: Dict[str, Any] = field(default_factory=dict)
    # 区间伴随流通道配置（始终包含隐式 default 通道）
    custom_interval_channels: List[Dict[str, Any]] = field(
        default_factory=lambda: [{"name": "default", "boundary_node_ids": []}]
    )

    def add_node(self, node: AbstractNode) -> None:
        if node.id in self.nodes:
            raise ValueError(f"Duplicated node id: {node.id}")
        self.nodes[node.id] = node
        self.graph.add_node(node.id)

    def add_reach(self, reach: RiverReach) -> None:
        if reach.id in self.reaches:
            raise ValueError(f"Duplicated reach id: {reach.id}")
        if reach.upstream_node_id not in self.nodes:
            raise ValueError(f"Upstream node not found: {reach.upstream_node_id}")
        if reach.downstream_node_id not in self.nodes:
            raise ValueError(f"Downstream node not found: {reach.downstream_node_id}")

        self.reaches[reach.id] = reach
        self.graph.add_edge(reach.upstream_node_id, reach.downstream_node_id, reach_id=reach.id)

    def add_catchment(self, catchment: SubCatchment) -> None:
        if catchment.id in self.catchments:
            raise ValueError(f"Duplicated catchment id: {catchment.id}")
        self.catchments[catchment.id] = catchment

    def topological_order(self) -> List[str]:
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError("Hydrological network is not a DAG")
        return list(nx.topological_sort(self.graph))
