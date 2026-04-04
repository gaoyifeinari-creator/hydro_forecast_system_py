from __future__ import annotations

import pandas as pd

from .types import DataReadSpec, IDataReader


def normalize_station_dataframe(df: pd.DataFrame, source: str = "") -> pd.DataFrame:
    """Normalize station dataframe and validate mandatory columns."""
    out = df.copy()
    out.columns = [
        str(c).strip().strip('"').strip("'").strip().upper()
        for c in out.columns
    ]
    if "SENID" not in out.columns or "TIME" not in out.columns:
        hint = f" ({source})" if source else ""
        raise ValueError(f"Input data{hint} must include SENID and TIME columns")
    out["SENID"] = out["SENID"].astype(str).str.strip().str.strip('"').str.strip("'")
    time_text = out["TIME"].astype(str).str.strip().str.strip('"').str.strip("'")
    out["TIME_DT"] = pd.to_datetime(time_text, errors="coerce")
    return out


def apply_daily_time_midnight_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """
    日表（DAYDB）读数后处理：将每条记录的日历日对齐到当日 00:00:00。

    库内部分日数据 TIME 存为 08:00 等时刻，与引擎日方案 ``times`` 网格（常为 00:00）不一致时，
    会导致 ``reindex`` 后缺测被填 0。开启本选项后统一把 ``TIME_DT``（及 ``TIME`` 字符串）归一到 0 点。
    """
    out = df.copy()
    if "TIME_DT" not in out.columns:
        return out
    ts = pd.to_datetime(out["TIME_DT"], errors="coerce")
    out["TIME_DT"] = ts.dt.normalize()
    if "TIME" in out.columns:
        out["TIME"] = out["TIME_DT"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return out


class FileDataReader(IDataReader):
    """Read station data from local files."""

    def read(self, spec: DataReadSpec) -> pd.DataFrame:
        df = pd.read_csv(spec.source, **dict(spec.options or {}))
        return normalize_station_dataframe(df, source=spec.source)
