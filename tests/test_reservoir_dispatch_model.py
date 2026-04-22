from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
from datetime import datetime, timedelta

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.nodes.reservoir import ReservoirNode


class TestReservoirDispatchModel(unittest.TestCase):
    def test_inoutflow_balance_release_equals_inflow(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        inflow = TimeSeries(start, step, [100.0, 80.0, 120.0, 90.0])

        node = ReservoirNode(
            id="N3",
            outgoing_reach_ids=["R3"],
            inflow_attenuation=0.3,  # 在 InOutflowBalance 下应被忽略
            dispatch_model_alg_type="InOutflowBalance",
        )

        out = node._compute_simulated_outflows(inflow)
        self.assertTrue((out["R3"].values == inflow.values).all())


if __name__ == "__main__":
    unittest.main()

