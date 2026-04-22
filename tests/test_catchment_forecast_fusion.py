from __future__ import annotations

import math
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import unittest

import _sys_path  # noqa: F401

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.io.calculation_app_data_builder import (
    apply_catchment_forecast_fusion_to_station_packages,
)
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.io.json_config import _apply_catchment_forecast_rules_to_binding_specs


class TestCatchmentForecastFusion(unittest.TestCase):
    def test_priority_fallback_elementwise(self) -> None:
        times = pd.date_range("2026-01-01 00:00:00", periods=5, freq="h")
        start = times[0].to_pydatetime()
        dt = timedelta(hours=1)

        src_a = "A_PCP_1"
        src_b = "B_PCP_1"
        virtual_id = "fusion_cma_ecmwf_PCP_1"

        values_a = [np.nan, 2.0, 3.0, np.nan, 5.0]
        values_b = [1.0, 20.0, 30.0, 40.0, 50.0]

        rows = []
        for t, va, vb in zip(times, values_a, values_b):
            rows.append({"SENID": src_a, "TIME_DT": t.to_pydatetime(), "V": va, "AVGV": va})
            rows.append({"SENID": src_b, "TIME_DT": t.to_pydatetime(), "V": vb, "AVGV": vb})
        rain_df = pd.DataFrame(rows)

        fusion_plan = {
            "virtual_bindings": {
                virtual_id: {
                    "kind": ForcingKind.PRECIPITATION,
                    "source_ids": [src_a, src_b],
                }
            }
        }

        station_packages: dict[str, ForcingData] = {}
        station_packages = apply_catchment_forecast_fusion_to_station_packages(
            station_packages=station_packages,
            fusion_plan=fusion_plan,
            rain_df=rain_df,
            times=times,
            start_time=start,
            time_step=dt,
        )

        self.assertIn(virtual_id, station_packages)
        ts = station_packages[virtual_id].require(ForcingKind.PRECIPITATION)

        expected = [1.0, 2.0, 3.0, 40.0, 5.0]
        for got, exp in zip(ts.values, expected):
            self.assertTrue((not math.isnan(got)) and got == exp)

    def test_missing_source_returns_nan_then_fallback(self) -> None:
        times = pd.date_range("2026-01-01 00:00:00", periods=3, freq="h")
        start = times[0].to_pydatetime()
        dt = timedelta(hours=1)

        src_a = "A_PCP_1"
        src_b = "B_PCP_1"
        virtual_id = "fusion_cma_ecmwf_PCP_1"

        values_b = [7.0, 8.0, 9.0]
        rows = []
        for t, vb in zip(times, values_b):
            rows.append({"SENID": src_b, "TIME_DT": t.to_pydatetime(), "V": vb, "AVGV": vb})
        rain_df = pd.DataFrame(rows)

        fusion_plan = {
            "virtual_bindings": {
                virtual_id: {
                    "kind": ForcingKind.PRECIPITATION,
                    "source_ids": [src_a, src_b],
                }
            }
        }

        station_packages: dict[str, ForcingData] = {}
        station_packages = apply_catchment_forecast_fusion_to_station_packages(
            station_packages=station_packages,
            fusion_plan=fusion_plan,
            rain_df=rain_df,
            times=times,
            start_time=start,
            time_step=dt,
        )

        ts = station_packages[virtual_id].require(ForcingKind.PRECIPITATION)
        self.assertEqual(ts.values.tolist(), values_b)

    def test_json_rule_to_binding_specs_virtual_ids(self) -> None:
        scheme = ForecastingScheme()
        scheme.catchments = {"4101": None, "4301": None}

        rules = {
            "precipitation": {
                "unit": "mm",
                "source_id_template": "{subtype}_PCP_{catchment_id}",
                "default_profile": "fusion_cma_ecmwf",
                "profiles": {"fusion_cma_ecmwf": ["CMA_MESO", "ECMWF_HRES"]},
            },
            "temperature": {
                "unit": "Celsius",
                "source_id_template": "{subtype}_TMP_{catchment_id}",
                "default_profile": "fusion_cma_ecmwf",
                "profiles": {"fusion_cma_ecmwf": ["CMA_MESO", "ECMWF_HRES"]},
            },
        }

        binding_specs, fusion_plan = _apply_catchment_forecast_rules_to_binding_specs(
            scheme=scheme,
            binding_specs=[],
            forecast_rules=rules,
        )

        self.assertTrue(fusion_plan["raw_senids"])
        self.assertIn("fusion_cma_ecmwf_PCP_4101", fusion_plan["virtual_bindings"])
        self.assertIn("fusion_cma_ecmwf_TMP_4101", fusion_plan["virtual_bindings"])

        # 两个 catchment 都应得到 precipitation + air_temperature 两个变量
        by_cid = {str(s["catchment_id"]): s for s in binding_specs}
        for cid in ["4101", "4301"]:
            spec = by_cid[cid]
            kinds = {v["kind"] for v in spec.get("variables", [])}
            self.assertIn(ForcingKind.PRECIPITATION.value, kinds)
            self.assertIn(ForcingKind.AIR_TEMPERATURE.value, kinds)


if __name__ == "__main__":
    unittest.main()

