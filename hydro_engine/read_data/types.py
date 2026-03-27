from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Protocol

import pandas as pd


@dataclass(frozen=True)
class DataReadSpec:
    """Describe where and how to read source data."""

    source_type: str  # file | database | api
    source: str
    options: Dict[str, Any] = field(default_factory=dict)


class IDataReader(Protocol):
    def read(self, spec: DataReadSpec) -> pd.DataFrame:
        """Read a tabular dataset into a DataFrame."""
