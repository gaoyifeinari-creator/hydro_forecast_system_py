from __future__ import annotations

import pandas as pd

from .types import DataReadSpec, IDataReader


class ApiDataReader(IDataReader):
    """Reserved backend for external API reads."""

    def read(self, spec: DataReadSpec) -> pd.DataFrame:
        raise NotImplementedError(
            "ApiDataReader is reserved for future use. "
            "Please provide API request/parsing implementation."
        )
