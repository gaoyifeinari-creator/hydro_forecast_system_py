from __future__ import annotations

from datetime import datetime
import unittest

from tests import _sys_path  # noqa: F401

from hydro_engine.forecast.multisource_areal_rainfall import (
    CompileRequest,
    ForecastRainPoint,
    ForecastRainRecord,
    ForecastRainSourceConfig,
    ForecastRainRepository,
    MultiSourceArealRainfallCompiler,
)


class _DummyRepo(ForecastRainRepository):
    def __init__(self, records: list[ForecastRainRecord]) -> None:
        self._records = list(records)

    def fetch_latest_records(
        self,
        *,
        reg_ids,
        subtype,
        time_span_hours,
        latest_ftime_begin,
        latest_ftime_end,
        read_begin,
        read_end,
    ):
        return list(self._records)


def _build_points(
    dbtype: int,
    *,
    target_time_type: str = "Hour",
    target_time_step: int = 1,
    forecast_begin: datetime = datetime(2025, 10, 31, 12, 0, 0),
    forecast_end: datetime = datetime(2025, 10, 31, 20, 0, 0),
) -> list[ForecastRainPoint]:
    record = ForecastRainRecord(
        reg_id="154034",
        subtype="ECMWF",
        time_span_hours=3,
        ftime=datetime(2025, 10, 30, 20, 0, 0),
        btime=datetime(2025, 10, 31, 14, 0, 0),
        aver_pre=2.1,
        min_pre=2.1,
        max_pre=2.1,
    )
    repo = _DummyRepo([record])
    compiler = MultiSourceArealRainfallCompiler(repo)
    cfg = ForecastRainSourceConfig(
        name="dummy",
        unit_type="ECMWF",
        time_span_arr=[3],
        sub_sources=[],
    )
    req = CompileRequest(
        forecast_begin=forecast_begin,
        forecast_end=forecast_end,
        target_time_type=str(target_time_type),
        target_time_step=int(target_time_step),
        dbtype=int(dbtype),
        reg_ids=["154034"],
        source_config=cfg,
        distribution_params=[],
    )
    return compiler.compile(req)


class TestForecastRainDbtypeMapping(unittest.TestCase):
    def test_front_label_anchor_stays_at_btime(self) -> None:
        points = _build_points(dbtype=-1)
        non_zero_times = [p.time.hour for p in points if abs(float(p.value)) > 1e-12]
        non_zero_vals = [float(p.value) for p in points if abs(float(p.value)) > 1e-12]
        self.assertEqual(non_zero_times, [14, 15, 16])
        for got in non_zero_vals:
            self.assertAlmostEqual(got, 0.7, places=9)

    def test_back_label_anchor_shifts_by_one_hour_only(self) -> None:
        points = _build_points(dbtype=0)
        non_zero_times = [p.time.hour for p in points if abs(float(p.value)) > 1e-12]
        non_zero_vals = [float(p.value) for p in points if abs(float(p.value)) > 1e-12]
        self.assertEqual(non_zero_times, [15, 16, 17])
        for got in non_zero_vals:
            self.assertAlmostEqual(got, 0.7, places=9)

    def test_daily_label_shift_for_backward_mode(self) -> None:
        points_front = _build_points(
            dbtype=-1,
            target_time_type="Day",
            target_time_step=1,
            forecast_begin=datetime(2025, 10, 31, 0, 0, 0),
            forecast_end=datetime(2025, 11, 2, 0, 0, 0),
        )
        points_back = _build_points(
            dbtype=0,
            target_time_type="Day",
            target_time_step=1,
            forecast_begin=datetime(2025, 10, 31, 0, 0, 0),
            forecast_end=datetime(2025, 11, 2, 0, 0, 0),
        )
        self.assertEqual(points_front[0].time.date().isoformat(), "2025-10-31")
        self.assertEqual(points_back[0].time.date().isoformat(), "2025-11-01")
        self.assertAlmostEqual(float(points_front[0].value), float(points_back[0].value), places=9)

    def test_daily_values_should_not_change_between_dbtypes(self) -> None:
        points_front = _build_points(
            dbtype=-1,
            target_time_type="Day",
            target_time_step=1,
            forecast_begin=datetime(2025, 10, 31, 0, 0, 0),
            forecast_end=datetime(2025, 11, 1, 0, 0, 0),
        )
        points_back = _build_points(
            dbtype=0,
            target_time_type="Day",
            target_time_step=1,
            forecast_begin=datetime(2025, 10, 31, 0, 0, 0),
            forecast_end=datetime(2025, 11, 1, 0, 0, 0),
        )
        self.assertEqual(len(points_front), len(points_back))
        self.assertAlmostEqual(float(points_front[0].value), float(points_back[0].value), places=9)

    def test_latest_ftime_end_for_hour_keeps_forecast_begin(self) -> None:
        t0 = datetime(2025, 10, 31, 0, 0, 0)
        got = MultiSourceArealRainfallCompiler._resolve_latest_ftime_end(
            forecast_begin=t0,
            target_time_type="Hour",
        )
        self.assertEqual(got, t0)

    def test_latest_ftime_end_for_day_uses_current_hour(self) -> None:
        t0 = datetime(2025, 10, 31, 0, 0, 0)
        got = MultiSourceArealRainfallCompiler._resolve_latest_ftime_end(
            forecast_begin=t0,
            target_time_type="Day",
        )
        self.assertEqual(got.year, 2025)
        self.assertEqual(got.month, 10)
        self.assertEqual(got.day, 31)
        self.assertEqual(got.minute, 0)
        self.assertEqual(got.second, 0)
        self.assertEqual(got.microsecond, 0)


if __name__ == "__main__":
    unittest.main()
