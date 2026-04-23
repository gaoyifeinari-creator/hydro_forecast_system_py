from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
import warnings
from datetime import datetime, timedelta
import json
import tempfile
from pathlib import Path

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.domain.nodes.cross_section import CrossSectionNode
from hydro_engine.domain.reach import RiverReach
from hydro_engine.engine.calculator import CalculationEngine
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.io.json_config import run_calculation_from_json
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

    def test_chain_diverges_from_second_node_when_historical_routing_enabled(self) -> None:
        """
        对应现场现象复核：
        - 第一个节点仅走计算，保持与基准一致
        - 第二个节点开启“历史模拟接力”（after_forecast=True）后，输出按 observed 缝合
        - 差异继续向下游节点传播
        """
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        end = start + step * 5
        tc = ForecastTimeContext.from_absolute_times(
            warmup_start_time=start,
            correction_start_time=start,
            forecast_start_time=start + step * 2,  # t0 = 第3个点
            display_start_time=start,
            end_time=end,
            time_type=TimeType.HOUR,
            step_size=1,
        )

        sim = TimeSeries(start, step, [1.0, 2.0, 3.0, 4.0, 5.0])
        # 预报时段开始后（t>=t0）与 sim 明显不同；历史段保持一致，便于定位“从预报段开始偏离”
        obs_n2 = TimeSeries(start, step, [1.0, 2.0, 3.0, 30.0, 50.0])

        scheme = ForecastingScheme()
        scheme.add_catchment(
            SubCatchment(
                id="CA",
                runoff_model=DummyRunoffModel(1.0),
                routing_model=DummyRoutingModel(1.0),
                downstream_node_id="N1",
            )
        )
        scheme.add_node(
            CrossSectionNode(
                id="N1",
                incoming_reach_ids=[],
                outgoing_reach_ids=["R12"],
                local_catchment_ids=["CA"],
                use_observed_for_routing=False,
            )
        )
        scheme.add_node(
            CrossSectionNode(
                id="N2",
                incoming_reach_ids=["R12"],
                outgoing_reach_ids=["R23"],
                local_catchment_ids=[],
                observed_station_id="ST_N2",
                use_observed_for_routing=True,
                # 这就是 historical_simulation 在 json_config 里设置的开关效果
                use_observed_for_routing_after_forecast=True,
            )
        )
        scheme.add_node(
            CrossSectionNode(
                id="N3",
                incoming_reach_ids=["R23"],
                outgoing_reach_ids=[],
                local_catchment_ids=[],
                use_observed_for_routing=False,
            )
        )
        scheme.add_reach(
            RiverReach(
                id="R12",
                upstream_node_id="N1",
                downstream_node_id="N2",
                routing_model=DummyRoutingModel(1.0),
            )
        )
        scheme.add_reach(
            RiverReach(
                id="R23",
                upstream_node_id="N2",
                downstream_node_id="N3",
                routing_model=DummyRoutingModel(1.0),
            )
        )

        result = CalculationEngine().run(
            scheme=scheme,
            catchment_forcing={"CA": ForcingData.single(ForcingKind.PRECIPITATION, sim)},
            time_context=tc,
            observed_flows={"ST_N2": obs_n2},
        )

        # 第一个节点与其下游 reach 仍是纯计算
        self.assertEqual(result.node_outflows["N1"].values.tolist(), [1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertEqual(result.reach_flows["R12"].values.tolist(), [1.0, 2.0, 3.0, 4.0, 5.0])

        # 第二个节点开启历史模拟接力后，输出按 observed 覆盖（此例在 t0 后明显偏离）
        self.assertEqual(result.node_outflows["N2"].values.tolist(), [1.0, 2.0, 3.0, 30.0, 50.0])
        self.assertEqual(result.reach_flows["R23"].values.tolist(), [1.0, 2.0, 3.0, 30.0, 50.0])

        # 第三个节点总入流来自 R23，因此偏离从第二节点向下游传播
        self.assertEqual(result.node_total_inflows["N3"].values.tolist(), [1.0, 2.0, 3.0, 30.0, 50.0])

    def test_historical_mode_only_reservoir_keeps_observed_after_forecast(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        end = start + step * 5
        cfg = {
            "time_axis": {
                "time_step_hours": 1,
                "warmup_start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                "correction_start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                "display_start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                "forecast_start_time": (start + step * 2).strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
            },
            "nodes": [
                {
                    "id": "N1",
                    "type": "cross_section",
                    "outgoing_reach_ids": ["R12"],
                    "observed_station_id": "ST_N1",
                    "use_observed_for_routing": True,
                    "local_catchment_ids": ["C1"],
                },
                {
                    "id": "N2",
                    "type": "reservoir",
                    "outgoing_reach_ids": ["R23"],
                    "observed_station_id": "ST_N2",
                    "use_observed_for_routing": True,
                    "params": {"inflow_attenuation": 1.0, "dispatch_model_alg_type": "Attenuation"},
                },
                {"id": "N3", "type": "cross_section"},
            ],
            "reaches": [
                {"id": "R12", "upstream_node_id": "N1", "downstream_node_id": "N2", "routing_model": {"name": "DummyRoutingModel", "params": {"attenuation": 1.0}}},
                {"id": "R23", "upstream_node_id": "N2", "downstream_node_id": "N3", "routing_model": {"name": "DummyRoutingModel", "params": {"attenuation": 1.0}}},
            ],
            "catchments": [
                {
                    "id": "C1",
                    "downstream_node_id": "N1",
                    "runoff_model": {"name": "DummyRunoffModel", "params": {"runoff_coefficient": 1.0}},
                    "routing_model": {"name": "DummyRoutingModel", "params": {"attenuation": 1.0}},
                }
            ],
            "catchment_forcing_bindings": [
                {
                    "catchment_id": "C1",
                    "bindings": [{"forcing_kind": "precipitation", "station_id": "STA_RAIN"}],
                }
            ],
        }

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
            config_path = f.name
        try:
            sim = TimeSeries(start, step, [1.0, 2.0, 3.0, 4.0, 5.0])
            obs_n1 = TimeSeries(start, step, [10.0, 20.0, 30.0, 40.0, 50.0])
            obs_n2 = TimeSeries(start, step, [11.0, 22.0, 33.0, 44.0, 55.0])
            out = run_calculation_from_json(
                config_path=config_path,
                station_packages={"STA_RAIN": ForcingData.single(ForcingKind.PRECIPITATION, sim)},
                time_type="Hour",
                step_size=1,
                warmup_start_time=start,
                observed_flows={"ST_N1": obs_n1, "ST_N2": obs_n2},
                forecast_mode="historical_simulation",
            )
            # cross_section: 仅到 forecast_start 所在步使用 observed，之后回到计算值
            self.assertEqual(out["node_outflows"]["N1"], [10.0, 20.0, 30.0, 4.0, 5.0])
            # reservoir: 历史模拟下预报段继续 observed 接力
            self.assertEqual(out["node_outflows"]["N2"], [11.0, 22.0, 33.0, 44.0, 55.0])
        finally:
            Path(config_path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()

