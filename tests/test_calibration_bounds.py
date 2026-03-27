"""新安江率定上下界与 Python 参数字段一致性的回归测试。"""

from __future__ import annotations

import _sys_path  # noqa: F401

import unittest
from dataclasses import fields

from hydro_engine.models.runoff.calibration_bounds import (
    XINANJIANG_CS_PARAM_BOUNDS,
    XINANJIANG_PARAM_BOUNDS,
    calibration_vector_bounds_xinanjiang,
    calibration_vector_bounds_xinanjiang_cs,
    clip_scalar,
)
from hydro_engine.models.runoff.xinanjiang import XinanjiangParams
from hydro_engine.models.runoff.xinanjiang_cs import XinanjiangCSParams


class TestCalibrationBounds(unittest.TestCase):
    def test_xinanjiang_param_keys_match_dataclass_except_unit_graph(self) -> None:
        names = {f.name for f in fields(XinanjiangParams)} - {"unit_graph"}
        self.assertEqual(set(XINANJIANG_PARAM_BOUNDS.keys()), names)

    def test_xinanjiang_cs_param_keys_match_dataclass_except_lag(self) -> None:
        names = {f.name for f in fields(XinanjiangCSParams)} - {"lag"}
        self.assertEqual(set(XINANJIANG_CS_PARAM_BOUNDS.keys()), names)

    def test_vector_helpers_length(self) -> None:
        lo, hi, inc = calibration_vector_bounds_xinanjiang()
        self.assertEqual(len(lo), 14)
        self.assertEqual(len(hi), 14)
        self.assertEqual(len(inc), 14)

        lo2, hi2, inc2 = calibration_vector_bounds_xinanjiang_cs()
        self.assertEqual(len(lo2), 15)
        self.assertEqual(len(hi2), 15)
        self.assertEqual(len(inc2), 15)

    def test_clip_scalar_respects_java_range(self) -> None:
        b = XINANJIANG_PARAM_BOUNDS["k"]
        self.assertEqual(clip_scalar(0.5, b), 0.5)
        self.assertEqual(clip_scalar(0.0, b), 0.2)
        self.assertEqual(clip_scalar(2.0, b), 1.5)


if __name__ == "__main__":
    unittest.main()
