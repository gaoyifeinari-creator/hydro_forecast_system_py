from __future__ import annotations

import _sys_path  # noqa: F401

import unittest

from hydro_engine.domain.nodes.cross_section import CrossSectionNode
from hydro_engine.domain.nodes.reservoir import ReservoirNode
from hydro_engine.io.forecast_mode_policy import (
    allow_node_observed_routing_after_forecast,
    allow_scenario_rainfall_injection,
    is_historical_simulation_mode,
    is_realtime_forecast_mode,
    normalize_forecast_mode,
)


class TestForecastModePolicy(unittest.TestCase):
    def test_normalize_and_validate(self) -> None:
        self.assertEqual(normalize_forecast_mode("realtime_forecast"), "realtime_forecast")
        self.assertEqual(normalize_forecast_mode(" historical_simulation "), "historical_simulation")
        with self.assertRaises(ValueError):
            normalize_forecast_mode("history")

    def test_allow_scenario_rainfall_injection(self) -> None:
        self.assertTrue(allow_scenario_rainfall_injection("realtime_forecast"))
        self.assertFalse(allow_scenario_rainfall_injection("historical_simulation"))
        self.assertTrue(is_realtime_forecast_mode("realtime_forecast"))
        self.assertTrue(is_historical_simulation_mode("historical_simulation"))

    def test_allow_node_observed_routing_after_forecast(self) -> None:
        rsv = ReservoirNode(id="R1")
        sec = CrossSectionNode(id="S1")
        self.assertTrue(
            allow_node_observed_routing_after_forecast("historical_simulation", rsv)
        )
        self.assertFalse(
            allow_node_observed_routing_after_forecast("historical_simulation", sec)
        )
        self.assertFalse(
            allow_node_observed_routing_after_forecast("realtime_forecast", rsv)
        )


if __name__ == "__main__":
    unittest.main()
