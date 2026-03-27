from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

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
