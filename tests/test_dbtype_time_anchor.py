from __future__ import annotations

from tests import _sys_path  # noqa: F401

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS = _ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from calculation_pipeline_runner import (
    _read_scheme_dbtype,
    _resolve_actual_forecast_start,
)


class TestDbtypeTimeAnchor(unittest.TestCase):
    def test_resolve_actual_forecast_start(self) -> None:
        t0 = datetime(2026, 4, 20, 8, 0, 0)
        step = timedelta(hours=1)
        self.assertEqual(
            _resolve_actual_forecast_start(t0, time_delta=step, dbtype=-1),
            t0,
        )
        self.assertEqual(
            _resolve_actual_forecast_start(t0, time_delta=step, dbtype=0),
            t0 + step,
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
            self.assertEqual(_read_scheme_dbtype(path, "Hour", 1), 0)
            self.assertEqual(_read_scheme_dbtype(path, "Day", 1), -1)
            # 未配置时默认前时标
            self.assertEqual(_read_scheme_dbtype(path, "Minute", 1), -1)
        finally:
            Path(path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()

