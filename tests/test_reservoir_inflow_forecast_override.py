from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
import warnings
from datetime import datetime, timedelta

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.domain.nodes.cross_section import CrossSectionNode
from hydro_engine.domain.nodes.reservoir import ReservoirNode
from hydro_engine.domain.reach import RiverReach
from hydro_engine.engine.calculator import CalculationEngine
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.models.routing import DummyRoutingModel
from hydro_engine.models.runoff import DummyRunoffModel


class TestReservoirInflowForecastOverride(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 1, 1, 0, 0, 0)
        self.step = timedelta(hours=1)
        self.end = self.start + self.step * 5

        self.time_context = ForecastTimeContext.from_absolute_times(
            warmup_start_time=self.start,
            correction_start_time=self.start,
            forecast_start_time=self.start + self.step,
            display_start_time=self.start,
            end_time=self.end,
            time_type=TimeType.HOUR,
            step_size=1,
        )

        self.sim_inflow = [100.0, 100.0, 100.0, 100.0, 100.0]
        self.sim_precip = self.sim_inflow
        self.obs_inflow = [999.0, 200.0, 200.0, 200.0, 200.0]  # idx=0 不应生效

        self.ts_sim = TimeSeries(self.start, self.step, self.sim_inflow)
        self.ts_obs_inflow = TimeSeries(self.start, self.step, self.obs_inflow)

        # scheme：CA 挂在上游节点 N1，经 catchment routing 注入 ReservoirNode(N2)
        self.scheme = ForecastingScheme()
        self.scheme.add_catchment(
            SubCatchment(
                id="CA",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(attenuation=1.0),
                downstream_node_id="N2",
            )
        )
        self.scheme.add_node(
            CrossSectionNode(
                id="N1",
                incoming_reach_ids=[],
                outgoing_reach_ids=[],
                local_catchment_ids=["CA"],
            )
        )

        self.reservoir = ReservoirNode(
            id="N2",
            incoming_reach_ids=[],
            outgoing_reach_ids=["R2"],
            local_catchment_ids=[],
            inflow_attenuation=0.5,
            observed_inflow_station_id="IN1",
            use_observed_inflow_for_simulation=True,
        )
        self.downstream = CrossSectionNode(
            id="N3",
            incoming_reach_ids=["R2"],
            outgoing_reach_ids=[],
        )
        self.scheme.add_node(self.reservoir)
        self.scheme.add_node(self.downstream)
        self.scheme.add_reach(
            RiverReach(
                id="R2",
                upstream_node_id="N2",
                downstream_node_id="N3",
                routing_model=DummyRoutingModel(attenuation=1.0),
            )
        )

        self.catchment_forcing = {
            "CA": ForcingData.single(ForcingKind.PRECIPITATION, TimeSeries(self.start, self.step, self.sim_precip))
        }

    def test_future_outflow_follows_injected_inflow_forecast(self) -> None:
        engine = CalculationEngine()
        observed_flows = {"IN1": self.ts_obs_inflow}

        result = engine.run(
            self.scheme,
            self.catchment_forcing,
            self.time_context,
            observed_flows=observed_flows,
        )

        # forecast_start = start+1step，boundary = start
        # idx=0 使用模拟 inflow(100)；idx>=1 使用注入 observed inflow(200)
        expected_release = [50.0, 100.0, 100.0, 100.0, 100.0]
        self.assertEqual(result.reach_flows["R2"].values, expected_release)

    def test_missing_inflow_observed_warns_and_fallback_to_simulated(self) -> None:
        engine = CalculationEngine()
        observed_flows = {}  # 缺失 IN1

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = engine.run(
                self.scheme,
                self.catchment_forcing,
                self.time_context,
                observed_flows=observed_flows,
            )

        expected_release = [50.0, 50.0, 50.0, 50.0, 50.0]
        self.assertEqual(result.reach_flows["R2"].values, expected_release)
        self.assertTrue(any("missing in observed_flows" in str(x.message) for x in w))


if __name__ == "__main__":
    unittest.main()

