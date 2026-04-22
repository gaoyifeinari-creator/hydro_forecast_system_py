from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
from datetime import datetime, timedelta

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.domain.nodes.cross_section import CrossSectionNode
from hydro_engine.domain.reach import RiverReach
from hydro_engine.engine.calculator import CalculationEngine
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.models.routing import DummyRoutingModel
from hydro_engine.models.runoff import DummyRunoffModel


class TestCatchmentRoutingToDownstream(unittest.TestCase):
    def test_catchment_runoff_routed_to_next_node(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        end = start + step * 5

        scheme = ForecastingScheme()
        scheme.add_node(
            CrossSectionNode(
                id="N1",
                incoming_reach_ids=[],
                outgoing_reach_ids=["R1"],
                local_catchment_ids=["CA"],
            )
        )
        scheme.add_node(
            CrossSectionNode(
                id="N2",
                incoming_reach_ids=["R1"],
                outgoing_reach_ids=[],
                local_catchment_ids=[],
            )
        )
        scheme.add_reach(
            RiverReach(
                id="R1",
                upstream_node_id="N1",
                downstream_node_id="N2",
                routing_model=DummyRoutingModel(attenuation=1.0),
            )
        )
        scheme.add_catchment(
            SubCatchment(
                id="CA",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(attenuation=1.0),
                downstream_node_id="N2",
            )
        )

        runoff_values = [1.0, 2.0, 3.0, 4.0, 5.0]
        catchment_forcing = {
            "CA": ForcingData.single(
                ForcingKind.PRECIPITATION,
                TimeSeries(start, step, runoff_values),
            )
        }
        time_context = ForecastTimeContext.from_absolute_times(
            warmup_start_time=start,
            correction_start_time=start,
            forecast_start_time=start,
            display_start_time=start,
            end_time=end,
            time_type=TimeType.HOUR,
            step_size=1,
        )

        result = CalculationEngine().run(scheme, catchment_forcing, time_context)

        self.assertEqual(result.reach_flows["R1"].values.tolist(), [0.0] * 5)
        self.assertEqual(result.node_total_inflows["N2"].values.tolist(), runoff_values)


if __name__ == "__main__":
    unittest.main()

