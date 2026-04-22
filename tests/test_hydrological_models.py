from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
from datetime import datetime, timedelta

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.models.routing import MuskingumRoutingModel
from hydro_engine.models.runoff import SnowmeltRunoffModel, TankRunoffModel
from hydro_engine.models.runoff.tank import TankParams, TankState
from hydro_engine.models.runoff.xinanjiang import XinanjiangParams, XinanjiangRunoffModel, XinanjiangState


class TestHydrologicalModels(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 1, 1, 0, 0, 0)
        self.step = timedelta(hours=1)
        self.values_p = [10.0, 30.0, 50.0, 40.0, 20.0]
        self.series_p = TimeSeries(self.start, self.step, self.values_p)
        self.series_pet = TimeSeries(self.start, self.step, [2.0, 3.0, 4.0, 3.0, 2.0])

    def test_xinanjiang_runoff_model(self) -> None:
        model = XinanjiangRunoffModel(
            params=XinanjiangParams(
                wum=20.0,
                wlm=40.0,
                wdm=40.0,
                k=0.8,
                c=0.1,
                b=0.3,
                imp=0.02,
                sm=30.0,
                ex=1.2,
                kss=0.4,
                kg=0.3,
                kkss=0.9,
                kkg=0.95,
                area=100.0,
            ),
            state=XinanjiangState(
                wu=5.0,
                wl=10.0,
                wd=20.0,
                fr=0.01,
                s=6.0,
                qrss0=18.0,
                qrg0=20.0,
            ),
        )
        forcing = ForcingData.from_pairs(
            [
                (ForcingKind.PRECIPITATION, self.series_p),
                (ForcingKind.POTENTIAL_EVAPOTRANSPIRATION, self.series_pet),
            ]
        )
        output = model.run(forcing)
        self.assertEqual(output.time_steps, self.series_p.time_steps)
        self.assertTrue(bool((output.values >= 0).all()))

    def test_tank_runoff_model(self) -> None:
        model = TankRunoffModel(
            params=TankParams(
                upper_outflow_coeff=0.3,
                lower_outflow_coeff=0.1,
                percolation_coeff=0.2,
                evap_coeff=0.05,
            ),
            state=TankState(upper_storage=20.0, lower_storage=60.0),
        )
        forcing = ForcingData.single(ForcingKind.PRECIPITATION, self.series_p)
        output = model.run(forcing)
        self.assertEqual(output.time_steps, self.series_p.time_steps)
        self.assertTrue(bool((output.values >= 0).all()))

    def test_muskingum_routing_model(self) -> None:
        model = MuskingumRoutingModel(k_hours=3.0, x=0.2)
        forcing = ForcingData.single(ForcingKind.ROUTING_INFLOW, self.series_p)
        output = model.run(forcing)
        self.assertEqual(output.time_steps, self.series_p.time_steps)
        self.assertTrue(bool((output.values >= 0).all()))
        self.assertLessEqual(float(output.values.max()), max(self.series_p.values) * 1.05)

    def test_snowmelt_runoff_model(self) -> None:
        model = SnowmeltRunoffModel()
        t_air = TimeSeries(self.start, self.step, [-2.0, 1.0, 5.0, 3.0, 0.0])
        snow = TimeSeries(self.start, self.step, [100.0, 80.0, 50.0, 20.0, 0.0])
        forcing = ForcingData.from_pairs(
            [
                (ForcingKind.PRECIPITATION, self.series_p),
                (ForcingKind.AIR_TEMPERATURE, t_air),
                (ForcingKind.SNOW_DEPTH, snow),
            ]
        )
        out = model.run(forcing)
        self.assertEqual(out.time_steps, self.series_p.time_steps)


if __name__ == "__main__":
    unittest.main()
