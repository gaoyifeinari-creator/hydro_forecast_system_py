from datetime import datetime, timedelta
from pathlib import Path

import _sys_path  # noqa: F401

import pandas as pd

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.forecast import (
    CatchmentForecastRainfall,
    ForecastDataManager,
    run_forecast_pipeline,
    run_forecast_pipeline_from_mock_csv,
)
from hydro_engine.models.runoff.dummy import DummyRunoffModel


def test_catchment_forecast_rainfall_validation():
    idx = pd.date_range("2025-01-01", periods=3, freq="h")
    CatchmentForecastRainfall.from_aligned_arrays(
        catchment_id="C1",
        time_index=idx,
        expected=[1, 2, 3],
        upper=[2, 3, 4],
        lower=[0, 1, 2],
        time_step=timedelta(hours=1),
    )
    try:
        CatchmentForecastRainfall.from_aligned_arrays(
            catchment_id="C1",
            time_index=idx,
            expected=[1, 2],
            upper=[2, 3, 4],
            lower=[0, 1, 2],
            time_step=timedelta(hours=1),
        )
        assert False, "expected length mismatch error"
    except ValueError as e:
        assert "不一致" in str(e)


def test_run_forecast_pipeline_dummy():
    m = DummyRunoffModel(runoff_coefficient=0.5)
    hist = ForcingData.single(
        ForcingKind.PRECIPITATION,
        TimeSeries(datetime(2025, 8, 31, 21), timedelta(hours=1), [1.0, 2.0, 3.0]),
    )
    idx = pd.date_range("2025-09-01", periods=2, freq="h")
    rain = CatchmentForecastRainfall.from_aligned_arrays(
        catchment_id="X",
        time_index=idx,
        expected=[4.0, 6.0],
        upper=[5.0, 7.0],
        lower=[3.0, 5.0],
        time_step=timedelta(hours=1),
    )
    df = run_forecast_pipeline(
        runoff_model=m,
        historical_forcing=hist,
        forecast_rainfall=rain,
    )
    assert list(df["Q_expected"]) == [2.0, 3.0]
    assert list(df["Q_upper"]) == [2.5, 3.5]
    assert list(df["Q_lower"]) == [1.5, 2.5]


def test_run_forecast_pipeline_from_mock_csv():
    root = Path(__file__).resolve().parents[1]
    csv_path = root / "tests" / "fixtures" / "forecast_rain_mock.csv"
    m = DummyRunoffModel(runoff_coefficient=1.0)
    hist = ForcingData.single(
        ForcingKind.PRECIPITATION,
        TimeSeries(datetime(2025, 8, 31, 22), timedelta(hours=1), [1.0, 1.0]),
    )
    df = run_forecast_pipeline_from_mock_csv(
        runoff_model=m,
        historical_forcing=hist,
        mock_csv=csv_path,
        catchment_id="MOCK",
    )
    assert len(df) == 3
    assert df["Q_expected"].iloc[0] == 2.0


def test_forecast_data_manager_stubs():
    mgr = ForecastDataManager()
    try:
        mgr.fetch_forecast_from_db()
    except NotImplementedError:
        pass
    else:
        assert False
    try:
        mgr.process_and_align_timeseries(None, target_time_index=pd.DatetimeIndex([]), time_step=timedelta(hours=1))
    except NotImplementedError:
        pass
    else:
        assert False
