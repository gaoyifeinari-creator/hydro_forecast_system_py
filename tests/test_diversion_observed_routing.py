from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
from datetime import datetime, timedelta

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.nodes.diversion import DiversionNode


class TestDiversionObservedRouting(unittest.TestCase):
    def test_observed_routing_splits_total_flow_instead_of_cloning_to_both_branches(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        end = start + step * 4
        time_context = ForecastTimeContext.from_absolute_times(
            warmup_start_time=start,
            correction_start_time=start,
            forecast_start_time=start + step,
            display_start_time=start,
            end_time=end,
            time_type=TimeType.HOUR,
            step_size=1,
        )

        node = DiversionNode(
            id="D1",
            outgoing_reach_ids=["R1", "R2"],
            main_channel_id="R1",
            bypass_channel_id="R2",
            main_channel_capacity=5.0,
            use_observed_for_routing=True,
        )

        total_inflow = TimeSeries(start, step, [3.0, 7.0, 8.0, 2.0])
        observed_total_outflow = TimeSeries(start, step, [10.0, 10.0, 10.0, 10.0])

        outflow_map = node.process_water(
            total_inflow=total_inflow,
            observed_series=observed_total_outflow,
            time_context=time_context,
        )

        self.assertEqual(outflow_map["R1"].values.tolist(), [5.0, 5.0, 5.0, 2.0])
        self.assertEqual(outflow_map["R2"].values.tolist(), [5.0, 5.0, 3.0, 0.0])


if __name__ == "__main__":
    unittest.main()
