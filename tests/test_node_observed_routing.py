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
from hydro_engine.domain.reach import RiverReach
from hydro_engine.engine.calculator import CalculationEngine
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.models.routing import DummyRoutingModel
from hydro_engine.models.runoff import DummyRunoffModel


class TestNodeObservedRouting(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 1, 1, 0, 0, 0)
        self.step = timedelta(hours=1)
        self.end = self.start + self.step * 5

        # forecast_start = start + 1step => blend picks observed for i=0..1
        self.time_context = ForecastTimeContext.from_absolute_times(
            warmup_start_time=self.start,
            correction_start_time=self.start,
            forecast_start_time=self.start + self.step,
            display_start_time=self.start,
            end_time=self.end,
            time_type=TimeType.HOUR,
            step_size=1,
        )

        self.sim_values = [1.0, 2.0, 3.0, 4.0, 5.0]
        self.obs_values = [10.0, 20.0, 30.0, 40.0, 50.0]

        self.ts_sim = TimeSeries(self.start, self.step, list(self.sim_values))
        self.ts_obs = TimeSeries(self.start, self.step, list(self.obs_values))

        self.scheme_base = ForecastingScheme()
        self.scheme_base.add_catchment(
            SubCatchment(
                id="CA",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(1.0),
                downstream_node_id="N1",
            )
        )
        self.scheme_base.add_node(
            CrossSectionNode(id="N0", incoming_reach_ids=[], outgoing_reach_ids=[], local_catchment_ids=["CA"])
        )

        self.scheme_base.add_node(
            CrossSectionNode(
                id="N1",
                outgoing_reach_ids=["R1"],
                local_catchment_ids=[],
                observed_station_id="ST1",
                use_observed_for_routing=False,  # default, will override in each test
            )
        )
        self.scheme_base.add_node(CrossSectionNode(id="N2", incoming_reach_ids=["R1"], outgoing_reach_ids=[]))
        self.scheme_base.add_reach(
            RiverReach(
                id="R1",
                upstream_node_id="N1",
                downstream_node_id="N2",
                routing_model=DummyRoutingModel(1.0),
            )
        )

        self.catchment_forcing = {
            "CA": ForcingData.single(ForcingKind.PRECIPITATION, self.ts_sim)
        }

        self.observed_flows = {"ST1": self.ts_obs}

    def _run(self, *, use_observed_for_routing: bool, observed_all_nan: bool = False):
        scheme = ForecastingScheme()
        # Rebuild scheme (since nodes are dataclasses; easiest is deep copy by re-adding)
        scheme.add_catchment(
            SubCatchment(
                id="CA",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(1.0),
                downstream_node_id="N1",
            )
        )
        scheme.add_node(
            CrossSectionNode(id="N0", incoming_reach_ids=[], outgoing_reach_ids=[], local_catchment_ids=["CA"])
        )
        scheme.add_node(
            CrossSectionNode(
                id="N1",
                outgoing_reach_ids=["R1"],
                local_catchment_ids=[],
                observed_station_id="ST1",
                use_observed_for_routing=use_observed_for_routing,
            )
        )
        scheme.add_node(CrossSectionNode(id="N2", incoming_reach_ids=["R1"], outgoing_reach_ids=[]))
        scheme.add_reach(
            RiverReach(
                id="R1",
                upstream_node_id="N1",
                downstream_node_id="N2",
                routing_model=DummyRoutingModel(1.0),
            )
        )

        obs = self.ts_obs
        if observed_all_nan:
            obs = TimeSeries(self.start, self.step, [float("nan")] * 5)
        observed_flows = {"ST1": obs}

        engine = CalculationEngine()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = engine.run(
                scheme,
                self.catchment_forcing,
                self.time_context,
                observed_flows=observed_flows,
            )
            return result, w

    def test_only_for_comparison_no_blend(self) -> None:
        result, w = self._run(use_observed_for_routing=False)
        self.assertEqual(result.reach_flows["R1"].values.tolist(), list(self.sim_values))
        self.assertIn("N1", result.node_observed_flows)
        self.assertEqual(result.node_observed_flows["N1"].values.tolist(), list(self.obs_values))

    def test_use_observed_for_routing_blend(self) -> None:
        result, w = self._run(use_observed_for_routing=True)
        expected = [10.0, 20.0, 3.0, 4.0, 5.0]
        self.assertEqual(result.reach_flows["R1"].values.tolist(), expected)
        self.assertIn("N1", result.node_observed_flows)
        self.assertEqual(result.node_observed_flows["N1"].values.tolist(), list(self.obs_values))

    def test_all_nan_observed_fallback_to_simulated(self) -> None:
        result, w = self._run(use_observed_for_routing=True, observed_all_nan=True)
        self.assertEqual(result.reach_flows["R1"].values.tolist(), list(self.sim_values))
        self.assertIn("N1", result.node_observed_flows)
        # should warn
        self.assertTrue(any("all NaN" in str(x.message) for x in w))


if __name__ == "__main__":
    unittest.main()

