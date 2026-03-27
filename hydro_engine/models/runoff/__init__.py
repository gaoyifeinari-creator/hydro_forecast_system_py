"""Runoff model implementations for catchments/nodes."""

from .dummy import DummyRunoffModel
from .snowmelt import SnowmeltRunoffModel
from .tank import TankParams, TankRunoffModel, TankState
from .xinanjiang import XinanjiangParams, XinanjiangRunoffModel, XinanjiangState
from .xinanjiang_cs import (
    XinanjiangCSParams,
    XinanjiangCSRunoffModel,
    XinanjiangCSState,
)
from . import calibration_bounds
from .calibration_bounds import (
    CalibrationBounds,
    IntCalibrationBounds,
    XINANJIANG_CS_LAG_BOUNDS,
    XINANJIANG_CS_PARAM_BOUNDS,
    XINANJIANG_CS_STATE_BOUNDS,
    XINANJIANG_PARAM_BOUNDS,
    XINANJIANG_STATE_BOUNDS,
    XINANJIANG_UNIT_GRAPH_BOUNDS,
    calibration_vector_bounds_xinanjiang,
    calibration_vector_bounds_xinanjiang_cs,
    clip_int,
    clip_scalar,
)

__all__ = [
    "DummyRunoffModel",
    "SnowmeltRunoffModel",
    "XinanjiangRunoffModel",
    "XinanjiangCSRunoffModel",
    "TankRunoffModel",
    "XinanjiangParams",
    "XinanjiangState",
    "XinanjiangCSParams",
    "XinanjiangCSState",
    "TankParams",
    "TankState",
    "calibration_bounds",
    "CalibrationBounds",
    "IntCalibrationBounds",
    "XINANJIANG_PARAM_BOUNDS",
    "XINANJIANG_STATE_BOUNDS",
    "XINANJIANG_UNIT_GRAPH_BOUNDS",
    "XINANJIANG_CS_PARAM_BOUNDS",
    "XINANJIANG_CS_STATE_BOUNDS",
    "XINANJIANG_CS_LAG_BOUNDS",
    "calibration_vector_bounds_xinanjiang",
    "calibration_vector_bounds_xinanjiang_cs",
    "clip_scalar",
    "clip_int",
]
