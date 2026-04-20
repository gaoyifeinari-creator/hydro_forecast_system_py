from __future__ import annotations

from tests import _sys_path  # noqa: F401

import unittest

from hydro_engine.forecast.multisource_areal_rainfall import (
    parse_forecast_rain_config_from_scheme,
)


class TestForecastRainConfigParser(unittest.TestCase):
    def test_parse_java_style_source_config(self) -> None:
        scheme = {
            "future_rainfall": {
                "sources": [
                    {
                        "name": "四川省气象局",
                        "isSelect": True,
                        "unitType": "3",
                        "timeSpan": "1,3",
                        "subRainSource": [
                            {"subSourceName": "24小时", "rank": 1, "subType": "24", "timeSpan": 1},
                            {"subSourceName": "72小时", "rank": 2, "subType": "72", "timeSpan": 1},
                            {"subSourceName": "240小时", "rank": 3, "subType": "240", "timeSpan": 3},
                        ],
                    }
                ],
                "rainDistributionParams": [
                    {"disName": "小雨", "disScaleMap": {"1": 10, "2": 90}},
                    {"disName": "中雨以上所有样本", "disScaleMap": {"1": 20, "2": 80}},
                ],
            }
        }
        bundle = parse_forecast_rain_config_from_scheme(scheme)
        self.assertEqual(bundle.selected_source.name, "四川省气象局")
        self.assertEqual(bundle.selected_source.unit_type, "3")
        self.assertEqual(list(bundle.selected_source.time_span_arr), [1, 3])
        self.assertEqual(len(bundle.selected_source.sub_sources), 3)
        self.assertEqual(bundle.selected_source.sub_sources[2].subtype, "240")
        self.assertEqual(len(bundle.distribution_params), 2)


if __name__ == "__main__":
    unittest.main()

