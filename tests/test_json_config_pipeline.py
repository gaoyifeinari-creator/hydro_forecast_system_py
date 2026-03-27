from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
from datetime import datetime, timedelta
from pathlib import Path

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.io.json_config import (
    flatten_stations_catalog,
    load_scheme_from_json,
    run_calculation_from_json,
)


_PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestJsonConfigPipeline(unittest.TestCase):
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
        self.assertIn("R5", output["reach_flows"])
        self.assertTrue(any(v > 0.0 for v in output["reach_flows"]["R5"]))

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
