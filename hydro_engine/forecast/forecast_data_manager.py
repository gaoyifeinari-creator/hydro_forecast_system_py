"""
预报数据获取管理器：预留读库/对齐接口，当前阶段用本地 Mock CSV。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from hydro_engine.forecast.catchment_forecast_rainfall import CatchmentForecastRainfall


class ForecastDataManager:
    """
    统一管理预报面雨量等数据的获取。

    - ``fetch_forecast_from_db`` / ``process_and_align_timeseries``：预留给后续读库与重采样对齐。
    - ``get_forecast_from_mock_file``：开发阶段从 CSV 加载三情景面雨。
    """

    def fetch_forecast_from_db(self, *args: Any, **kwargs: Any) -> CatchmentForecastRainfall:
        """
        预留：从业务库读取预报面雨量（多情景）。

        后续实现时需保证：时段长度、时间间隔与引擎预报网格一致，并完成缺测/质控。
        """
        raise NotImplementedError(
            "fetch_forecast_from_db 尚未实现：请接入实际库表/SQL 后在此填充逻辑。"
        )

    def process_and_align_timeseries(
        self,
        raw: Any,
        *,
        target_time_index: pd.DatetimeIndex,
        time_step: timedelta,
    ) -> CatchmentForecastRainfall:
        """
        预留：将原始预报序列处理并对齐到目标 ``target_time_index``。

        典型工作包括：频率转换、重采样、边界裁剪、与实况网格对齐校验等。
        """
        raise NotImplementedError(
            "process_and_align_timeseries 尚未实现：请在接入真实预报产品后实现重采样与对齐。"
        )

    def get_forecast_from_mock_file(
        self,
        filepath: str | Path,
        *,
        catchment_id: str,
        time_column: str = "time",
        encoding: str = "utf-8",
    ) -> CatchmentForecastRainfall:
        """
        从本地 CSV 读取 Mock 预报面雨量，返回 :class:`CatchmentForecastRainfall`。

        CSV 约定列（表头不区分大小写，但读取后按下列名匹配）：
        - ``time``：预报时刻（可被 pandas 解析）
        - ``expected`` / ``upper`` / ``lower``：三情景面雨量
        可选列：
        - ``pet``：若存在，可在管线中作为预报期蒸发能力（当前由串联函数读取，见 skeleton_pipeline）

        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 缺列、长度不一致或时间轴非均匀
        """
        path = Path(filepath)
        if not path.is_file():
            raise FileNotFoundError(f"Mock 预报文件不存在: {path}")

        df = pd.read_csv(path, encoding=encoding)
        return self.forecast_rainfall_from_dataframe(df, catchment_id=catchment_id, time_column=time_column)

    def forecast_rainfall_from_dataframe(
        self,
        df: pd.DataFrame,
        *,
        catchment_id: str,
        time_column: str = "time",
    ) -> CatchmentForecastRainfall:
        """
        与 :meth:`get_forecast_from_mock_file` 相同列约定，但直接接受已加载的 ``DataFrame``（便于多子流域分组）。
        """
        if df.empty:
            raise ValueError("预报面雨 DataFrame 为空")

        cols = {c.lower(): c for c in df.columns}

        def _col(name: str) -> str:
            key = name.lower()
            if key not in cols:
                raise ValueError(f"预报面雨表缺少列 {name!r}，当前列: {list(df.columns)}")
            return cols[key]

        tcol = _col(time_column)
        time_series = pd.to_datetime(df[tcol], errors="coerce")
        if time_series.isna().any():
            raise ValueError("预报面雨表存在无法解析的时间戳")

        time_index = pd.DatetimeIndex(time_series)
        ec = _col("expected")
        uc = _col("upper")
        lc = _col("lower")

        expected = df[ec].astype(float).tolist()
        upper = df[uc].astype(float).tolist()
        lower = df[lc].astype(float).tolist()

        pet_list = None
        if "pet" in cols:
            pet_list = [float(x) for x in df[cols["pet"]].astype(float).tolist()]

        time_step: timedelta
        if len(time_index) >= 2:
            deltas = time_index[1:] - time_index[:-1]
            uniq = set(deltas.to_pytimedelta().tolist())
            if len(uniq) != 1:
                raise ValueError(
                    f"预报面雨表时间步长不唯一: {sorted({str(x) for x in uniq})[:5]}..."
                )
            time_step = next(iter(uniq))
        else:
            raise ValueError("预报面雨表至少需要 2 行以推断预报时间步长")

        return CatchmentForecastRainfall.from_aligned_arrays(
            catchment_id=catchment_id,
            time_index=time_index,
            expected=expected,
            upper=upper,
            lower=lower,
            time_step=time_step,
            pet=pet_list,
        )

    def read_optional_pet_column(self, filepath: str | Path, *, encoding: str = "utf-8") -> Optional[list[float]]:
        """若 Mock CSV 含 ``pet`` 列则返回其浮点列表，否则 ``None``。"""
        path = Path(filepath)
        df = pd.read_csv(path, encoding=encoding)
        cols = {c.lower(): c for c in df.columns}
        if "pet" not in cols:
            return None
        return [float(x) for x in df[cols["pet"]].astype(float).tolist()]
