from __future__ import annotations

from datetime import datetime
import unittest

from tests import _sys_path  # noqa: F401

from hydro_engine.forecast.daily_rainfall_compiler import (
    compile_multispan_rain_to_daily,
    RainSpanRecord,
)


class TestDailyRainfallCompiler(unittest.TestCase):
    def test_cross_day_split_on_hour_canvas(self) -> None:
        records = [
            # 10-31 23:00~11-01 02:00 共 3h，累计 0.8
            RainSpanRecord(start_time=datetime(2025, 10, 31, 23, 0, 0), span_hours=3, value=0.8),
            # 11-01 02:00~11-01 08:00 共 6h，累计 1.2（用于验证多步长混合）
            RainSpanRecord(start_time=datetime(2025, 11, 1, 2, 0, 0), span_hours=6, value=1.2),
        ]

        points = compile_multispan_rain_to_daily(records, timestamp_mode="forward", decimals=3)
        got = {(p.time.date().isoformat()): float(p.value) for p in points}

        # 10-31：只分到 0.8 的 1/3
        self.assertAlmostEqual(got["2025-10-31"], round(0.8 / 3.0, 3), places=9)
        # 11-01：分到 0.8 的 2/3 + 1.2
        self.assertAlmostEqual(got["2025-11-01"], round((0.8 * 2.0 / 3.0) + 1.2, 3), places=9)

    def test_backward_mode_shifts_daily_labels_by_one_day(self) -> None:
        records = [
            RainSpanRecord(start_time=datetime(2025, 11, 1, 0, 0, 0), span_hours=24, value=24.0),
            RainSpanRecord(start_time=datetime(2025, 11, 2, 0, 0, 0), span_hours=24, value=12.0),
        ]
        forward = compile_multispan_rain_to_daily(records, timestamp_mode="forward", decimals=2)
        backward = compile_multispan_rain_to_daily(records, timestamp_mode="backward", decimals=2)

        self.assertEqual([p.time.date().isoformat() for p in forward], ["2025-11-01", "2025-11-02"])
        self.assertEqual([p.time.date().isoformat() for p in backward], ["2025-11-02", "2025-11-03"])
        self.assertEqual([p.value for p in forward], [24.0, 12.0])
        self.assertEqual([p.value for p in backward], [24.0, 12.0])


if __name__ == "__main__":
    unittest.main()

