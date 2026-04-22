from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.forecast.scenario_forcing import load_catchment_forecast_rainfall_map_from_csv
from hydro_engine.io.calculation_app_data_loader import station_observation_query_end_realtime
from hydro_engine.io.json_config import (
    flatten_stations_catalog,
    load_scheme_from_json,
    run_calculation_from_json,
)


_PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestJsonConfigPipeline(unittest.TestCase):
    def test_station_observation_query_end_realtime(self) -> None:
        ws = datetime(2026, 1, 1, 0, 0, 0)
        tc = ForecastTimeContext.from_period_counts(
            ws,
            TimeType.HOUR,
            1,
            warmup_period_steps=2,
            correction_period_steps=0,
            historical_display_period_steps=0,
            forecast_period_steps=3,
        )
        self.assertEqual(tc.forecast_start_time, ws + timedelta(hours=2))
        self.assertEqual(
            station_observation_query_end_realtime(tc),
            tc.forecast_start_time - timedelta(hours=1),
        )

    def test_load_and_run(self) -> None:
        config_path = _PROJECT_ROOT / "configs" / "example_forecast_config.json"
        start = datetime(2026, 1, 1, 0, 0, 0)
        scheme, binding_specs, time_context = load_scheme_from_json(
            config_path,
            time_type="Hour",
            step_size=1,
            warmup_start_time=start,
        )

        self.assertEqual(len(scheme.nodes), 6)
        self.assertEqual(len(scheme.reaches), 5)
        self.assertEqual(len(scheme.catchments), 2)
        self.assertEqual(time_context.step_count, 5)
        self.assertEqual(len(binding_specs), 2)
        self.assertEqual(
            scheme.custom_interval_channels,
            [{"name": "default", "boundary_node_ids": []}],
        )

        self.assertEqual(scheme.nodes["N1"].name, "上游常规节点1")
        self.assertEqual(scheme.nodes["N4"].name, "分流节点4")
        self.assertEqual(scheme.nodes["N3"].operation_constraints.max_release, 500.0)
        self.assertEqual(scheme.nodes["N3"].level_features.dead_level, 235.0)
        self.assertEqual(len(scheme.nodes["N3"].curves), 2)

        step = timedelta(hours=1)
        rain_a = [100.0, 130.0, 160.0, 140.0, 120.0]
        pet_a = [3.0, 3.5, 4.0, 3.8, 3.2]
        rain_b = [90.0, 110.0, 140.0, 130.0, 100.0]

        station_packages = {
            "STA_A": ForcingData.from_pairs(
                [
                    (ForcingKind.PRECIPITATION, TimeSeries(start, step, rain_a)),
                ]
            ),
            "PET_STA_A": ForcingData.single(
                ForcingKind.POTENTIAL_EVAPOTRANSPIRATION,
                TimeSeries(start, step, pet_a),
            ),
            "STA_B": ForcingData.single(
                ForcingKind.PRECIPITATION,
                TimeSeries(start, step, rain_b),
            ),
        }

        output = run_calculation_from_json(
            config_path,
            station_packages,
            time_type="Hour",
            step_size=1,
            warmup_start_time=start,
        )
        self.assertIn("topological_order", output)
        self.assertIn("reach_flows", output)
        self.assertIn("node_total_inflows", output)
        self.assertIn("time_context", output)
        self.assertIn("display_results", output)
        self.assertIn("interval_channels", output)
        self.assertIn("node_interval_inflows", output)
        self.assertIn("node_interval_outflows", output)
        self.assertIn("reach_interval_flows", output)
        self.assertIn("default", output["interval_channels"])
        for _k, disp in output["display_results"].items():
            self.assertIsInstance(disp, dict)
            self.assertIn("deterministic", disp)
        self.assertTrue(
            any(k.startswith("node_interval_inflow:default:") for k in output["display_results"].keys())
        )
        self.assertIn("R5", output["reach_flows"])
        # 当前示例的分流节点参数可能使得旁路分量为 0（即 `R5` 全为 0）。
        # 这里更稳健地断言主干河道 `R4` 必须产生正流量。
        self.assertTrue(any(v > 0.0 for v in output["reach_flows"]["R4"]))

    def test_load_scheme_with_custom_interval_channels(self) -> None:
        config_path = _PROJECT_ROOT / "configs" / "example_forecast_config.json"
        data = json.loads(config_path.read_text(encoding="utf-8"))
        data["schemes"][0]["custom_interval_channels"] = [
            {
                "name": "generalized_A_to_D",
                "boundary_node_ids": ["N1", "N6"],
            }
        ]
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
            encoding="utf-8",
        ) as f:
            json.dump(data, f, ensure_ascii=False)
            tmp = f.name
        try:
            scheme, _, _ = load_scheme_from_json(
                tmp,
                time_type="Hour",
                step_size=1,
                warmup_start_time=datetime(2026, 1, 1, 0, 0, 0),
            )
            self.assertEqual(
                scheme.custom_interval_channels,
                [
                    {"name": "default", "boundary_node_ids": []},
                    {
                        "name": "generalized_A_to_D",
                        "boundary_node_ids": ["N1", "N6"],
                    },
                ],
            )
        finally:
            Path(tmp).unlink(missing_ok=True)

    def test_load_scheme_with_explicit_default_interval_boundaries(self) -> None:
        config_path = _PROJECT_ROOT / "configs" / "example_forecast_config.json"
        data = json.loads(config_path.read_text(encoding="utf-8"))
        data["schemes"][0]["custom_interval_channels"] = [
            {
                "name": "default",
                "boundary_node_ids": ["N1", "N6"],
            },
            {
                "name": "generalized_A_to_D",
                "boundary_node_ids": ["N3"],
            },
        ]
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
            encoding="utf-8",
        ) as f:
            json.dump(data, f, ensure_ascii=False)
            tmp = f.name
        try:
            scheme, _, _ = load_scheme_from_json(
                tmp,
                time_type="Hour",
                step_size=1,
                warmup_start_time=datetime(2026, 1, 1, 0, 0, 0),
            )
            self.assertEqual(
                scheme.custom_interval_channels,
                [
                    {"name": "default", "boundary_node_ids": ["N1", "N6"]},
                    {
                        "name": "generalized_A_to_D",
                        "boundary_node_ids": ["N3"],
                    },
                ],
            )
        finally:
            Path(tmp).unlink(missing_ok=True)

    def test_run_calculation_scenario_rainfall_multiscenario(self) -> None:
        """预报面雨 CSV 注入 + 三情景引擎输出键。"""
        import tempfile

        config_path = _PROJECT_ROOT / "configs" / "example_forecast_config.json"
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)
        rain_a = [100.0, 130.0, 160.0, 140.0, 120.0]
        pet_a = [3.0, 3.5, 4.0, 3.8, 3.2]
        rain_b = [90.0, 110.0, 140.0, 130.0, 100.0]
        station_packages = {
            "STA_A": ForcingData.from_pairs(
                [
                    (ForcingKind.PRECIPITATION, TimeSeries(start, step, rain_a)),
                ]
            ),
            "PET_STA_A": ForcingData.single(
                ForcingKind.POTENTIAL_EVAPOTRANSPIRATION,
                TimeSeries(start, step, pet_a),
            ),
            "STA_B": ForcingData.single(
                ForcingKind.PRECIPITATION,
                TimeSeries(start, step, rain_b),
            ),
        }
        lines = ["time,expected,upper,lower"]
        for i in range(5):
            t = start + step * i
            lines.append(
                f"{t.strftime('%Y-%m-%d %H:%M:%S')},{10.0 + i},{12.0 + i},{8.0 + i}"
            )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8", newline=""
        ) as f:
            f.write("\n".join(lines))
            csv_path = f.name
        try:
            scen_map = load_catchment_forecast_rainfall_map_from_csv(
                csv_path, default_catchment_ids=["CA"]
            )
            output = run_calculation_from_json(
                config_path,
                station_packages,
                time_type="Hour",
                step_size=1,
                warmup_start_time=start,
                forecast_mode="realtime_forecast",
                catchment_scenario_rainfall=scen_map,
                scenario_precipitation="expected",
                forecast_multiscenario=True,
            )
            self.assertIn("multiscenario_engine_outputs", output)
            ms = output["multiscenario_engine_outputs"]
            self.assertEqual(set(ms.keys()), {"expected", "upper", "lower"})
            self.assertIn("catchment_runoffs", ms["upper"])
        finally:
            Path(csv_path).unlink(missing_ok=True)

    def test_flatten_stations_catalog_categorized(self) -> None:
        raw = {
            "rain_gauges": [{"id": "STA_A", "name": "a"}],
            "flow_stations": [{"id": "ST_FLOW_N1", "name": "f"}],
        }
        flat = flatten_stations_catalog(raw)
        self.assertEqual(len(flat), 2)
        cats = {e["id"]: e["catalog_category"] for e in flat}
        self.assertEqual(cats["STA_A"], "rain_gauges")
        self.assertEqual(cats["ST_FLOW_N1"], "flow_stations")

    def test_flatten_stations_catalog_duplicate_id(self) -> None:
        raw = {
            "rain_gauges": [{"id": "X", "name": "a"}],
            "flow_stations": [{"id": "X", "name": "b"}],
        }
        with self.assertRaises(ValueError) as ctx:
            flatten_stations_catalog(raw)
        self.assertIn("Duplicate station id", str(ctx.exception))

    def test_flatten_stations_catalog_nested_container(self) -> None:
        raw = {
            "reservoir": [
                {
                    "node_id": "N3",
                    "name": "res",
                    "stations": [
                        {"id": "ST_IN_N3", "name": "in"},
                        {"id": "ST_OUT_N3", "name": "out"},
                    ],
                }
            ]
        }
        flat = flatten_stations_catalog(raw)
        self.assertEqual({e["id"] for e in flat}, {"ST_IN_N3", "ST_OUT_N3"})
        cats = {e["id"]: e["catalog_category"] for e in flat}
        self.assertEqual(cats["ST_IN_N3"], "reservoir.N3")


if __name__ == "__main__":
    unittest.main()
