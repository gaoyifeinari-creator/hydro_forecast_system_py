"""产流模型与 IHydrologicalModel / ForcingData 契约的适配性测试。"""

from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
from datetime import datetime, timedelta

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.io.json_config import _build_model
from hydro_engine.models.runoff import DummyRunoffModel
from hydro_engine.models.runoff.tank import TankRunoffModel
from hydro_engine.models.runoff.xinanjiang import XinanjiangParams, XinanjiangRunoffModel, XinanjiangState
from hydro_engine.models.runoff.xinanjiang_cs import (
    XinanjiangCSParams,
    XinanjiangCSRunoffModel,
    XinanjiangCSState,
)


class TestRunoffFrameworkAdaptation(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 1, 1, 0, 0, 0)
        self.step = timedelta(hours=1)
        self.n = 4
        self.p = TimeSeries(self.start, self.step, [10.0, 20.0, 5.0, 0.0])
        self.pet = TimeSeries(self.start, self.step, [2.0, 2.0, 2.0, 2.0])

    def test_dummy_tank_xaj_xajcs_subcatchment_contract(self) -> None:
        """不同产流模型通过同一 SubCatchment.generate_runoff 路径可运行。"""
        fd_pe = ForcingData.from_pairs(
            [
                (ForcingKind.PRECIPITATION, self.p),
                (ForcingKind.POTENTIAL_EVAPOTRANSPIRATION, self.pet),
            ]
        )
        fd_p = ForcingData.single(ForcingKind.PRECIPITATION, self.p)

        models: list[tuple[str, IHydrologicalModel, ForcingData]] = [
            (
                "dummy",
                DummyRunoffModel(1.0),
                fd_p,
            ),
            (
                "tank",
                TankRunoffModel(),
                fd_p,
            ),
            (
                "xaj",
                XinanjiangRunoffModel(
                    params=XinanjiangParams(area=50.0),
                    state=XinanjiangState(),
                ),
                fd_pe,
            ),
            (
                "xajcs",
                XinanjiangCSRunoffModel(
                    params=XinanjiangCSParams(lag=1, cs=0.85, area=50.0),
                    state=XinanjiangCSState(),
                ),
                fd_pe,
            ),
        ]

        kinds_seen: set[frozenset] = set()
        for name, model, fd in models:
            with self.subTest(model=name):
                kinds_seen.add(model.required_inputs())
                sc = SubCatchment(id="C", runoff_model=model)
                out = sc.generate_runoff(fd)
                self.assertEqual(len(out.values), self.n)

        self.assertEqual(len(kinds_seen), 2)

    def test_json_build_model_registers_xinanjiang_cs(self) -> None:
        m = _build_model(
            {
                "name": "XinanjiangCSRunoffModel",
                "params": {"lag": 2, "cs": 0.75, "area": 100.0},
                "state": {"qs0": 10.0},
            }
        )
        self.assertIsInstance(m, XinanjiangCSRunoffModel)
        self.assertEqual(m.params.lag, 2)
        self.assertEqual(m.state.qs0, 10.0)


if __name__ == "__main__":
    unittest.main()
