from __future__ import annotations

import pandas as pd

from .types import DataReadSpec, IDataReader


class DatabaseDataReader(IDataReader):
    """Reserved backend for database reads."""

    def read(self, spec: DataReadSpec) -> pd.DataFrame:
        raise NotImplementedError(
            "DatabaseDataReader is reserved for future use. "
            "Please provide a DB connector/query implementation."
        )
