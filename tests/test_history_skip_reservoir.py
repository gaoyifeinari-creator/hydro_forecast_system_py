from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
from datetime import datetime, timedelta

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.domain.nodes.reservoir import ReservoirNode
from hydro_engine.domain.reach import RiverReach
from hydro_engine.engine.calculator import CalculationEngine
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.models.routing import DummyRoutingModel
from hydro_engine.models.runoff import DummyRunoffModel


class CountingReservoirNode(ReservoirNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_input_start: datetime | None = None
        self.last_input_len: int | None = None

    def _compute_simulated_outflows(self, total_inflow: TimeSeries):
        self.last_input_start = total_inflow.start_time
        self.last_input_len = len(total_inflow.values)
        return super()._compute_simulated_outflows(total_inflow)


class TestHistorySkipReservoir(unittest.TestCase):
    def test_history_not_computed_when_observed_for_routing_no_correction(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        end = start + step * 5

        time_context = ForecastTimeContext.from_absolute_times(
            warmup_start_time=start,
            correction_start_time=start,
            forecast_start_time=start + step,  # t0
            display_start_time=start,
            end_time=end,
            time_type=TimeType.HOUR,
            step_size=1,
        )

        sim_in = TimeSeries(start, step, [1.0, 2.0, 3.0, 4.0, 5.0])
        obs_out = TimeSeries(start, step, [10.0, 20.0, 30.0, 40.0, 50.0])

        reservoir = CountingReservoirNode(
            id="N2",
            incoming_reach_ids=[],
            outgoing_reach_ids=["R2"],
            local_catchment_ids=[],
            inflow_attenuation=1.0,
            observed_station_id="ST_OUT",
            use_observed_for_routing=True,
            # correction_config 默认 None：触发 history skip
        )

        scheme = ForecastingScheme()
        scheme.add_catchment(
            SubCatchment(
                id="CA",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(attenuation=1.0),
                downstream_node_id="N2",
            )
        )
        scheme.add_node(
            __import__("hydro_engine.domain.nodes.cross_section", fromlist=["CrossSectionNode"]).CrossSectionNode(
                id="N1", incoming_reach_ids=[], outgoing_reach_ids=[], local_catchment_ids=["CA"]
            )
        )
        scheme.add_node(reservoir)
        scheme.add_node(
            # 下游节点不影响本测试
            # 使用 cross_section 简化，但此处用已存在节点类型会更稳定
            # 我们直接不加下游，让引擎只做 reservoir 输出即可不依赖下游
            # 这里仍需要 reach id 对应校验，故补一个空下游节点
            # 实际不参与计算逻辑（出流会路由）
            # 使用一个 cross_section
            # (在本文件中避免 import 链过深)
            # 直接用 CrossSectionNode 的最小输入方式
            # ---------------------------
            __import__("hydro_engine.domain.nodes.cross_section", fromlist=["CrossSectionNode"]).CrossSectionNode(
                id="N3", incoming_reach_ids=["R2"], outgoing_reach_ids=[]
            )
        )
        scheme.add_reach(
            RiverReach(
                id="R2",
                upstream_node_id="N2",
                downstream_node_id="N3",
                routing_model=DummyRoutingModel(attenuation=1.0),
            )
        )

        catchment_forcing = {
            "CA": ForcingData.single(ForcingKind.PRECIPITATION, sim_in)
        }
        observed_flows = {"ST_OUT": obs_out}

        engine = CalculationEngine()
        result = engine.run(
            scheme,
            catchment_forcing,
            time_context,
            observed_flows=observed_flows,
        )

        # forecast_start = start+1step，override_start = forecast_start+1step = start+2step
        self.assertEqual(reservoir.last_input_start, start + step * 2)
        self.assertEqual(reservoir.last_input_len, 3)  # [t2,t3,t4] 三步

        # 输出历史（<=forecast_start）使用 observed：[10,20,...]
        # 输出 forecast 后段（>forecast_start）使用 simulated：[3,4,5]
        expected = [10.0, 20.0, 3.0, 4.0, 5.0]
        self.assertEqual(result.reach_flows["R2"].values, expected)


if __name__ == "__main__":
    unittest.main()

