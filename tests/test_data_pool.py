from __future__ import annotations

import unittest
from datetime import datetime, timedelta

import _sys_path  # noqa: F401

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.data_pool import DataPool
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries


class TestDataPool(unittest.TestCase):
    def test_get_combined_forcing_blend_at_forecast_start(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        # warmup 2 steps: [t0, t1] ; forecast_start is t2
        ctx = ForecastTimeContext.from_period_counts(
            warmup_start_time=start,
            time_type=TimeType.HOUR,
            step_size=1,
            warmup_period_steps=2,
            correction_period_steps=0,
            historical_display_period_steps=0,
            forecast_period_steps=3,
        )

        obs = TimeSeries(start, step, [1.0, 2.0, 3.0, 4.0, 5.0])
        fcst = TimeSeries(start, step, [10.0, 20.0, 30.0, 40.0, 50.0])

        pool = DataPool()
        pool.add_observed("P1", ForcingKind.PRECIPITATION, obs)
        pool.add_forecast("S1", "P1", ForcingKind.PRECIPITATION, fcst)

        combined = pool.get_combined_forcing(
            scenario_id="S1",
            station_id="P1",
            kind=ForcingKind.PRECIPITATION,
            context=ctx,
        )

        # boundary = forecast_start - 1step, so indices 0,1 take obs; 2..4 take fcst
        self.assertEqual(combined.values, [1.0, 2.0, 30.0, 40.0, 50.0])

    def test_add_get_catchment_forcing_pool(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        series = TimeSeries(start, step, [0.0, 1.0, 2.0])

        forcing = ForcingData.from_pairs(
            [
                (ForcingKind.PRECIPITATION, series),
                (ForcingKind.POTENTIAL_EVAPOTRANSPIRATION, series),
            ]
        )

        pool = DataPool()
        pool.add_catchment_forcing("S1", "CA", forcing)
        out = pool.get_catchment_forcing("S1", "CA")
        self.assertEqual(out.as_mapping(), forcing.as_mapping())


if __name__ == "__main__":
    unittest.main()

