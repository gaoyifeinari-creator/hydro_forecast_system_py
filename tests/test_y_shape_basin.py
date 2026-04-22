from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
from datetime import datetime, timedelta

import numpy as np

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.domain.nodes.cross_section import CrossSectionNode
from hydro_engine.domain.nodes.diversion import DiversionNode
from hydro_engine.domain.nodes.reservoir import ReservoirNode
from hydro_engine.domain.reach import RiverReach
from hydro_engine.engine.calculator import CalculationEngine
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.models.routing import DummyRoutingModel
from hydro_engine.models.runoff import DummyRunoffModel


class TestYShapeBasin(unittest.TestCase):
    """Y 字型 + 分洪道集成测试。"""

    def setUp(self) -> None:
        self.scheme = ForecastingScheme()

        node1 = CrossSectionNode(id="N1", outgoing_reach_ids=["R1"], local_catchment_ids=["CA"])
        node2 = CrossSectionNode(id="N2", outgoing_reach_ids=["R2"], local_catchment_ids=["CB"])
        node3 = ReservoirNode(
            id="N3",
            incoming_reach_ids=["R1", "R2"],
            outgoing_reach_ids=["R3"],
            inflow_attenuation=1.0,
        )
        node4 = DiversionNode(
            id="N4",
            incoming_reach_ids=["R3"],
            outgoing_reach_ids=["R4", "R5"],
            main_channel_id="R4",
            bypass_channel_id="R5",
            main_channel_capacity=120.0,
        )
        node5 = CrossSectionNode(id="N5", incoming_reach_ids=["R4"], outgoing_reach_ids=[])
        node6 = CrossSectionNode(id="N6", incoming_reach_ids=["R5"], outgoing_reach_ids=[])

        for node in [node1, node2, node3, node4, node5, node6]:
            self.scheme.add_node(node)

        reaches = [
            RiverReach(id="R1", upstream_node_id="N1", downstream_node_id="N3", routing_model=DummyRoutingModel(1.0)),
            RiverReach(id="R2", upstream_node_id="N2", downstream_node_id="N3", routing_model=DummyRoutingModel(1.0)),
            RiverReach(id="R3", upstream_node_id="N3", downstream_node_id="N4", routing_model=DummyRoutingModel(1.0)),
            RiverReach(id="R4", upstream_node_id="N4", downstream_node_id="N5", routing_model=DummyRoutingModel(1.0)),
            RiverReach(id="R5", upstream_node_id="N4", downstream_node_id="N6", routing_model=DummyRoutingModel(1.0)),
        ]
        for reach in reaches:
            self.scheme.add_reach(reach)

        self.scheme.add_catchment(
            SubCatchment(
                id="CA",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(1.0),
                downstream_node_id="N3",
            )
        )
        self.scheme.add_catchment(
            SubCatchment(
                id="CB",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(1.0),
                downstream_node_id="N3",
            )
        )

        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        ts_ca = TimeSeries(start, step, [100.0, 130.0, 160.0, 140.0, 120.0])
        ts_cb = TimeSeries(start, step, [90.0, 110.0, 140.0, 130.0, 100.0])
        self.catchment_forcing = {
            "CA": ForcingData.single(ForcingKind.PRECIPITATION, ts_ca),
            "CB": ForcingData.single(ForcingKind.PRECIPITATION, ts_cb),
        }
        end = start + step * 5
        self.time_context = ForecastTimeContext.from_absolute_times(
            warmup_start_time=start,
            correction_start_time=start,
            forecast_start_time=start,
            display_start_time=start,
            end_time=end,
            time_type=TimeType.HOUR,
            step_size=1,
        )

    def test_topology_and_diversion(self) -> None:
        engine = CalculationEngine()
        result = engine.run(self.scheme, self.catchment_forcing, self.time_context)

        topo = self.scheme.topological_order()
        print("Topological order:", topo)

        self.assertLess(topo.index("N1"), topo.index("N3"))
        self.assertLess(topo.index("N2"), topo.index("N3"))
        self.assertLess(topo.index("N3"), topo.index("N4"))

        self.assertIn("R5", result.reach_flows)
        bypass_flow = result.reach_flows["R5"].values
        print("Bypass flow (R5):", bypass_flow)

        self.assertTrue(any(v > 0.0 for v in bypass_flow))
        self.assertIn("N6", result.node_total_inflows)
        n6_inflow = result.node_total_inflows["N6"].values
        print("Node6 inflow:", n6_inflow)

        self.assertTrue(np.array_equal(np.asarray(bypass_flow), np.asarray(n6_inflow)))


if __name__ == "__main__":
    unittest.main()
