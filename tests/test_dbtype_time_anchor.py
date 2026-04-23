from __future__ import annotations

from tests import _sys_path  # noqa: F401

import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from hydro_engine.io.scheme_config_utils import (
    resolve_scheme_for_time_scale,
    scheme_dbtype,
)
from hydro_engine.io.time_anchor_policy import (
    resolve_actual_forecast_start,
    resolve_forecast_rain_read_anchor_window,
    resolve_station_read_window_for_dbtype,
    shift_station_df_time_label_for_dbtype,
)


class TestDbtypeTimeAnchor(unittest.TestCase):
    def test_resolve_actual_forecast_start(self) -> None:
        t0 = datetime(2026, 4, 20, 8, 0, 0)
        step = timedelta(hours=1)
        self.assertEqual(
            resolve_actual_forecast_start(t0, time_delta=step, dbtype=-1),
            t0,
        )
        self.assertEqual(
            resolve_actual_forecast_start(t0, time_delta=step, dbtype=0),
            t0,
        )

    def test_resolve_forecast_rain_read_anchor_window(self) -> None:
        display_begin = datetime(2025, 11, 1, 15, 0, 0)
        display_end = datetime(2025, 11, 8, 15, 0, 0)
        day_step = timedelta(days=1)
        # 前时标：锚点不变
        b0, e0 = resolve_forecast_rain_read_anchor_window(
            forecast_start_time=display_begin,
            end_time=display_end,
            time_delta=day_step,
            dbtype=-1,
        )
        self.assertEqual((b0, e0), (display_begin, display_end))
        # 后时标：锚点整体回拨 1 步
        b1, e1 = resolve_forecast_rain_read_anchor_window(
            forecast_start_time=display_begin,
            end_time=display_end,
            time_delta=day_step,
            dbtype=0,
        )
        self.assertEqual(b1, datetime(2025, 10, 31, 15, 0, 0))
        self.assertEqual(e1, datetime(2025, 11, 7, 15, 0, 0))

    def test_resolve_station_read_window_for_dbtype(self) -> None:
        start = datetime(2025, 11, 1, 15, 0, 0)
        end = datetime(2025, 11, 2, 15, 0, 0)
        obs_end = datetime(2025, 11, 1, 14, 0, 0)
        step = timedelta(hours=1)

        s0, e0, o0 = resolve_station_read_window_for_dbtype(
            read_time_start=start,
            read_time_end=end,
            station_obs_end=obs_end,
            time_delta=step,
            dbtype=-1,
        )
        self.assertEqual((s0, e0, o0), (start + step, end + step, obs_end + step))

        s1, e1, o1 = resolve_station_read_window_for_dbtype(
            read_time_start=start,
            read_time_end=end,
            station_obs_end=obs_end,
            time_delta=step,
            dbtype=0,
        )
        self.assertEqual((s1, e1, o1), (start, end, obs_end))

    def test_resolve_station_read_window_for_dbtype_day_front_obs_end_relax(self) -> None:
        start = datetime(2025, 11, 1, 0, 0, 0)
        end = datetime(2025, 11, 8, 0, 0, 0)
        # realtime 基准上界通常是 forecast_start - 1day
        obs_end = datetime(2025, 10, 31, 0, 0, 0)
        day_step = timedelta(days=1)
        s0, e0, o0 = resolve_station_read_window_for_dbtype(
            read_time_start=start,
            read_time_end=end,
            station_obs_end=obs_end,
            time_delta=day_step,
            dbtype=-1,
        )
        self.assertEqual(s0, datetime(2025, 11, 2, 0, 0, 0))
        self.assertEqual(e0, datetime(2025, 11, 9, 0, 0, 0))
        # 日方案前时标：obs_end 额外 +1day，避免 daydb 白天时间戳导致尾日漏读
        self.assertEqual(o0, datetime(2025, 11, 2, 0, 0, 0))

    def test_shift_station_df_time_label_for_dbtype(self) -> None:
        df = pd.DataFrame(
            {
                "SENID": ["s1", "s1"],
                "TIME_DT": [datetime(2025, 11, 1, 5, 0, 0), datetime(2025, 11, 1, 6, 0, 0)],
                "V": [1.0, 2.0],
            }
        )
        step = timedelta(hours=1)
        shifted_front = shift_station_df_time_label_for_dbtype(
            df,
            time_delta=step,
            dbtype=-1,
        )
        self.assertEqual(
            [t.to_pydatetime() for t in pd.to_datetime(shifted_front["TIME_DT"]).tolist()],
            [datetime(2025, 11, 1, 4, 0, 0), datetime(2025, 11, 1, 5, 0, 0)],
        )
        shifted_back = shift_station_df_time_label_for_dbtype(
            df,
            time_delta=step,
            dbtype=0,
        )
        self.assertEqual(
            [t.to_pydatetime() for t in pd.to_datetime(shifted_back["TIME_DT"]).tolist()],
            [datetime(2025, 11, 1, 5, 0, 0), datetime(2025, 11, 1, 6, 0, 0)],
        )

    def test_read_scheme_dbtype_from_config(self) -> None:
        cfg = {
            "schemes": [
                {"time_type": "Hour", "step_size": 1, "dbtype": 0},
                {"time_type": "Day", "step_size": 1, "dbtype": -1},
            ]
        }
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
            encoding="utf-8",
        ) as f:
            json.dump(cfg, f, ensure_ascii=False)
            path = f.name
        try:
            self.assertEqual(
                scheme_dbtype(resolve_scheme_for_time_scale(path, time_type="Hour", step_size=1), default=-1),
                0,
            )
            self.assertEqual(
                scheme_dbtype(resolve_scheme_for_time_scale(path, time_type="Day", step_size=1), default=-1),
                -1,
            )
            # 未配置时默认前时标
            self.assertEqual(
                scheme_dbtype(resolve_scheme_for_time_scale(path, time_type="Minute", step_size=1), default=-1),
                -1,
            )
        finally:
            Path(path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()

