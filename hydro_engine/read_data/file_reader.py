from __future__ import annotations

import pandas as pd

from .types import DataReadSpec, IDataReader


def normalize_station_dataframe(df: pd.DataFrame, source: str = "") -> pd.DataFrame:
    """Normalize station dataframe and validate mandatory columns."""
    out = df.copy()
    out.columns = [str(c).strip().strip('"').strip("'") for c in out.columns]
    if "SENID" not in out.columns or "TIME" not in out.columns:
        hint = f" ({source})" if source else ""
        raise ValueError(f"Input data{hint} must include SENID and TIME columns")
    out["SENID"] = out["SENID"].astype(str).str.strip().str.strip('"').str.strip("'")
    time_text = out["TIME"].astype(str).str.strip().str.strip('"').str.strip("'")
    out["TIME_DT"] = pd.to_datetime(time_text, errors="coerce")
    return out


class FileDataReader(IDataReader):
    """Read station data from local files."""

    def read(self, spec: DataReadSpec) -> pd.DataFrame:
        df = pd.read_csv(spec.source, **dict(spec.options or {}))
        return normalize_station_dataframe(df, source=spec.source)
