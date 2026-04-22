from __future__ import annotations

import unittest
from datetime import datetime

from tests import _sys_path  # noqa: F401

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.domain.nodes.cross_section import CrossSectionNode
from hydro_engine.domain.nodes.reservoir import ReservoirNode
from hydro_engine.domain.reach import RiverReach
from hydro_engine.engine.calculator import CalculationEngine
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.models.routing.dummy import DummyRoutingModel
from hydro_engine.models.runoff.dummy import DummyRunoffModel


class TestIntervalTracking(unittest.TestCase):
    def test_tagged_interval_tracking_with_reservoir_zeroing(self) -> None:
        scheme = ForecastingScheme()
        n_a = ReservoirNode(id="A", name="A", dispatch_model_alg_type="InOutflowBalance")
        n_b = ReservoirNode(id="B", name="B", dispatch_model_alg_type="InOutflowBalance")
        n_c = CrossSectionNode(id="C", name="C")
        n_a.local_catchment_ids = ["CA"]
        n_b.local_catchment_ids = ["CB"]
        n_a.outgoing_reach_ids = ["R1"]
        n_b.incoming_reach_ids = ["R1"]
        n_b.outgoing_reach_ids = ["R2"]
        n_c.incoming_reach_ids = ["R2"]
        scheme.add_node(n_a)
        scheme.add_node(n_b)
        scheme.add_node(n_c)
        scheme.add_reach(RiverReach(id="R1", upstream_node_id="A", downstream_node_id="B", routing_model=DummyRoutingModel(1.0)))
        scheme.add_reach(RiverReach(id="R2", upstream_node_id="B", downstream_node_id="C", routing_model=DummyRoutingModel(1.0)))
        scheme.add_catchment(
            SubCatchment(
                id="CA",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(1.0),
                downstream_node_id="A",
            )
        )
        scheme.add_catchment(
            SubCatchment(
                id="CB",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(1.0),
                downstream_node_id="B",
            )
        )
        scheme.custom_interval_channels = [
            {"name": "default", "boundary_node_ids": []},
            {"name": "generalized_A_to_C", "boundary_node_ids": ["B"]},
        ]

        tc = ForecastTimeContext.from_period_counts(
            datetime(2026, 1, 1, 0, 0, 0),
            TimeType.HOUR,
            1,
            warmup_period_steps=0,
            correction_period_steps=0,
            historical_display_period_steps=0,
            forecast_period_steps=3,
        )
        rain_a = TimeSeries(tc.warmup_start_time, tc.time_delta, [1.0, 1.0, 1.0])
        rain_b = TimeSeries(tc.warmup_start_time, tc.time_delta, [2.0, 2.0, 2.0])
        forcing = {
            "CA": ForcingData.single(ForcingKind.PRECIPITATION, rain_a),
            "CB": ForcingData.single(ForcingKind.PRECIPITATION, rain_b),
        }

        out = CalculationEngine().run(
            scheme=scheme,
            catchment_forcing=forcing,
            time_context=tc,
            observed_flows={},
        )

        self.assertEqual(out.interval_channels, ["default", "generalized_A_to_C"])
        self.assertEqual(out.reach_flows["R2"].values.tolist(), [3.0, 3.0, 3.0])
        # default 通道在所有水库节点清零：B 为水库，故其下游 R2 应为 0。
        self.assertEqual(
            out.reach_interval_flows["default"]["R2"].values.tolist(),
            [0.0, 0.0, 0.0],
        )
        self.assertEqual(
            out.node_interval_inflows["B"]["default"].values.tolist(),
            [2.0, 2.0, 2.0],
        )
        self.assertEqual(
            out.node_interval_outflows["B"]["default"].values.tolist(),
            [0.0, 0.0, 0.0],
        )
        self.assertEqual(
            out.node_interval_inflows["B"]["generalized_A_to_C"].values.tolist(),
            [3.0, 3.0, 3.0],
        )
        self.assertEqual(
            out.node_interval_outflows["B"]["generalized_A_to_C"].values.tolist(),
            [0.0, 0.0, 0.0],
        )
        self.assertEqual(
            out.reach_interval_flows["generalized_A_to_C"]["R2"].values.tolist(),
            [0.0, 0.0, 0.0],
        )

    def test_default_channel_supports_explicit_boundary_on_non_reservoir_node(self) -> None:
        scheme = ForecastingScheme()
        n_a = CrossSectionNode(id="A", name="A")
        n_b = CrossSectionNode(id="B", name="B")
        n_a.local_catchment_ids = ["CA"]
        n_a.outgoing_reach_ids = ["R1"]
        n_b.incoming_reach_ids = ["R1"]
        scheme.add_node(n_a)
        scheme.add_node(n_b)
        scheme.add_reach(
            RiverReach(
                id="R1",
                upstream_node_id="A",
                downstream_node_id="B",
                routing_model=DummyRoutingModel(1.0),
            )
        )
        scheme.add_catchment(
            SubCatchment(
                id="CA",
                runoff_model=DummyRunoffModel(2.0),
                routing_model=DummyRoutingModel(1.0),
                downstream_node_id="A",
            )
        )
        # A 在该方案中建模为 cross_section，而非 ReservoirNode；
        # 显式 boundary_node_ids 仍应让 default 通道在 A 清零。
        scheme.custom_interval_channels = [
            {"name": "default", "boundary_node_ids": ["A"]},
        ]

        tc = ForecastTimeContext.from_period_counts(
            datetime(2026, 1, 1, 0, 0, 0),
            TimeType.HOUR,
            1,
            warmup_period_steps=0,
            correction_period_steps=0,
            historical_display_period_steps=0,
            forecast_period_steps=3,
        )
        rain_a = TimeSeries(tc.warmup_start_time, tc.time_delta, [1.0, 1.0, 1.0])
        forcing = {"CA": ForcingData.single(ForcingKind.PRECIPITATION, rain_a)}

        out = CalculationEngine().run(
            scheme=scheme,
            catchment_forcing=forcing,
            time_context=tc,
            observed_flows={},
        )

        self.assertEqual(out.reach_flows["R1"].values.tolist(), [2.0, 2.0, 2.0])
        self.assertEqual(
            out.reach_interval_flows["default"]["R1"].values.tolist(),
            [0.0, 0.0, 0.0],
        )
        self.assertEqual(
            out.node_interval_inflows["B"]["default"].values.tolist(),
            [0.0, 0.0, 0.0],
        )


if __name__ == "__main__":
    unittest.main()
