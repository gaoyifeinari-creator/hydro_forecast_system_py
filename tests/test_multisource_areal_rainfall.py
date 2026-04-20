from __future__ import annotations

from tests import _sys_path  # noqa: F401

import unittest
from datetime import datetime
from typing import List, Sequence

from hydro_engine.forecast.multisource_areal_rainfall import (
    CompileRequest,
    ForecastRainRecord,
    ForecastRainRepository,
    ForecastRainSourceConfig,
    MultiSourceArealRainfallCompiler,
    RainDistributionParam,
    SubRainSourceConfig,
)


class _FakeRepo(ForecastRainRepository):
    def __init__(self, records: List[ForecastRainRecord]):
        self._records = list(records)

    def fetch_latest_records(
        self,
        *,
        reg_ids: Sequence[str],
        subtype: str,
        time_span_hours: int,
        latest_ftime_begin: datetime,
        latest_ftime_end: datetime,
        read_begin: datetime,
        read_end: datetime,
    ) -> List[ForecastRainRecord]:
        out: List[ForecastRainRecord] = []
        for r in self._records:
            if r.reg_id not in reg_ids:
                continue
            if r.subtype != subtype:
                continue
            if int(r.time_span_hours) != int(time_span_hours):
                continue
            if not (latest_ftime_begin <= r.ftime <= latest_ftime_end):
                continue
            if not (read_begin <= r.btime <= read_end):
                continue
            out.append(r)
        return out


class TestMultiSourceArealRainfall(unittest.TestCase):
    def test_single_source_multiscale_overlay(self) -> None:
        # 24h 先写均摊 1mm/h，1h 再覆盖第一小时 5mm
        recs = [
            ForecastRainRecord(
                reg_id="R1",
                subtype="GFS",
                time_span_hours=24,
                ftime=datetime(2024, 8, 31, 12, 0, 0),
                btime=datetime(2024, 9, 1, 0, 0, 0),
                aver_pre=24.0,
            ),
            ForecastRainRecord(
                reg_id="R1",
                subtype="GFS",
                time_span_hours=1,
                ftime=datetime(2024, 8, 31, 12, 0, 0),
                btime=datetime(2024, 9, 1, 0, 0, 0),
                aver_pre=5.0,
            ),
        ]
        repo = _FakeRepo(recs)
        comp = MultiSourceArealRainfallCompiler(repo)
        req = CompileRequest(
            forecast_begin=datetime(2024, 9, 1, 0, 0, 0),
            forecast_end=datetime(2024, 9, 2, 0, 0, 0),
            target_time_type="Hour",
            target_time_step=1,
            dbtype=-1,
            reg_ids=["R1"],
            source_config=ForecastRainSourceConfig(
                name="GFS",
                unit_type="GFS",
                time_span_arr=[24, 1],
                sub_sources=[],
            ),
            distribution_params=[],
            fluctuate_range=0.0,
            use_min_max_from_db=False,
        )
        points = comp.compile(req)
        self.assertEqual(len(points), 24)
        self.assertAlmostEqual(points[0].value, 5.0)
        self.assertAlmostEqual(points[1].value, 1.0)

    def test_multi_source_rank_overlay(self) -> None:
        # 低优先级先写 2，高优先级覆盖成 8
        recs = [
            ForecastRainRecord(
                reg_id="R1",
                subtype="SRC_LOW",
                time_span_hours=1,
                ftime=datetime(2024, 8, 31, 12, 0, 0),
                btime=datetime(2024, 9, 1, 0, 0, 0),
                aver_pre=2.0,
            ),
            ForecastRainRecord(
                reg_id="R1",
                subtype="SRC_HIGH",
                time_span_hours=1,
                ftime=datetime(2024, 8, 31, 12, 0, 0),
                btime=datetime(2024, 9, 1, 0, 0, 0),
                aver_pre=8.0,
            ),
        ]
        repo = _FakeRepo(recs)
        comp = MultiSourceArealRainfallCompiler(repo)
        req = CompileRequest(
            forecast_begin=datetime(2024, 9, 1, 0, 0, 0),
            forecast_end=datetime(2024, 9, 1, 2, 0, 0),
            target_time_type="Hour",
            target_time_step=1,
            dbtype=-1,
            reg_ids=["R1"],
            source_config=ForecastRainSourceConfig(
                name="MIX",
                unit_type="UNUSED",
                time_span_arr=[],
                sub_sources=[
                    SubRainSourceConfig("low", rank=1, subtype="SRC_LOW", time_span_hours=1),
                    SubRainSourceConfig("high", rank=9, subtype="SRC_HIGH", time_span_hours=1),
                ],
            ),
            distribution_params=[],
            fluctuate_range=0.0,
            use_min_max_from_db=False,
        )
        points = comp.compile(req)
        self.assertEqual(len(points), 2)
        self.assertAlmostEqual(points[0].value, 8.0)

    def test_day_distribution_curve(self) -> None:
        # 24h + 分配曲线：全部雨量落到第 1 小时
        recs = [
            ForecastRainRecord(
                reg_id="R1",
                subtype="GFS",
                time_span_hours=24,
                ftime=datetime(2024, 8, 31, 12, 0, 0),
                btime=datetime(2024, 9, 1, 0, 0, 0),
                aver_pre=8.0,
            ),
        ]
        dist = RainDistributionParam(
            dis_name="小雨",
            dis_scale_map={i: (100.0 if i == 1 else 0.0) for i in range(1, 25)},
        )
        repo = _FakeRepo(recs)
        comp = MultiSourceArealRainfallCompiler(repo)
        req = CompileRequest(
            forecast_begin=datetime(2024, 9, 1, 0, 0, 0),
            forecast_end=datetime(2024, 9, 2, 0, 0, 0),
            target_time_type="Hour",
            target_time_step=1,
            dbtype=-1,
            reg_ids=["R1"],
            source_config=ForecastRainSourceConfig(
                name="GFS",
                unit_type="GFS",
                time_span_arr=[24],
                sub_sources=[],
            ),
            distribution_params=[dist],
            fluctuate_range=0.0,
            use_min_max_from_db=False,
        )
        points = comp.compile(req)
        self.assertAlmostEqual(points[0].value, 8.0)
        self.assertAlmostEqual(points[1].value, 0.0)


if __name__ == "__main__":
    unittest.main()

