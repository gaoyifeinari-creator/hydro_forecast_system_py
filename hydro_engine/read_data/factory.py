from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from .api_reader import ApiDataReader
from .database_reader import DatabaseDataReader
from .file_reader import FileDataReader
from .types import DataReadSpec, IDataReader


def build_data_reader(source_type: str) -> IDataReader:
    st = str(source_type or "").strip().lower()
    if st == "file":
        return FileDataReader()
    if st == "database":
        return DatabaseDataReader()
    if st == "api":
        return ApiDataReader()
    raise ValueError("source_type must be one of: file, database, api")


def read_station_data(
    source: str,
    *,
    source_type: str = "file",
    options: Optional[Dict[str, object]] = None,
) -> pd.DataFrame:
    """Convenience helper used by current apps (default file backend)."""
    reader = build_data_reader(source_type)
    spec = DataReadSpec(source_type=source_type, source=source, options=dict(options or {}))
    return reader.read(spec)
