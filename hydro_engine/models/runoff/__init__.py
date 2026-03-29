"""Runoff model implementations for catchments/nodes.

每个模型文件在末尾通过 ``register_model`` 自注册到全局 MODEL_REGISTRY。
"""

from hydro_engine.models import register_model

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


# ---------------------------------------------------------------------------
# 自注册（触发 models.__init__ 中导入本包）
# ---------------------------------------------------------------------------

# Dummy / Snowmelt：直接注册，__init__ 接受 **params
register_model("DummyRunoffModel", DummyRunoffModel)
register_model("SnowmeltRunoffModel", SnowmeltRunoffModel)


# Tank：params/state 结构需要工厂函数
def _make_tank(model_data) -> TankRunoffModel:
    params_raw = model_data.get("params", {})
    state_raw = model_data.get("state", {})
    return TankRunoffModel(
        params=TankParams(
            upper_outflow_coeff=float(params_raw.get("upper_outflow_coeff", 0.30)),
            lower_outflow_coeff=float(params_raw.get("lower_outflow_coeff", 0.10)),
            percolation_coeff=float(params_raw.get("percolation_coeff", 0.20)),
            evap_coeff=float(params_raw.get("evap_coeff", 0.05)),
        ),
        state=TankState(
            upper_storage=float(
                state_raw.get(
                    "upper_storage",
                    params_raw.get("upper_initial_storage", 20.0),
                )
            ),
            lower_storage=float(
                state_raw.get(
                    "lower_storage",
                    params_raw.get("lower_initial_storage", 60.0),
                )
            ),
        ),
    )


register_model("TankRunoffModel", _make_tank)


# Xinanjiang：params/state/unit_graph 需要工厂函数
def _make_xinanjiang(model_data) -> XinanjiangRunoffModel:
    params_raw = model_data.get("params", {})
    state_raw = model_data.get("state", {})
    ug = params_raw.get("unit_graph")
    unit_graph = (
        (float(ug[0]), float(ug[1]), float(ug[2]))
        if isinstance(ug, (list, tuple)) and len(ug) == 3
        else (0.2, 0.7, 0.1)
    )
    return XinanjiangRunoffModel(
        params=XinanjiangParams(
            wum=float(params_raw.get("wum", 20.0)),
            wlm=float(params_raw.get("wlm", 40.0)),
            wdm=float(params_raw.get("wdm", 40.0)),
            k=float(params_raw.get("k", 0.8)),
            c=float(params_raw.get("c", 0.1)),
            b=float(params_raw.get("b", 0.3)),
            imp=float(params_raw.get("imp", 0.02)),
            sm=float(params_raw.get("sm", 30.0)),
            ex=float(params_raw.get("ex", 1.2)),
            kss=float(params_raw.get("kss", 0.4)),
            kg=float(params_raw.get("kg", 0.3)),
            kkss=float(params_raw.get("kkss", 0.9)),
            kkg=float(params_raw.get("kkg", 0.95)),
            area=float(params_raw.get("area", 0.0)),
            unit_graph=unit_graph,
        ),
        state=XinanjiangState(
            wu=float(state_raw.get("wu", params_raw.get("wu0", 5.0))),
            wl=float(state_raw.get("wl", params_raw.get("wl0", 10.0))),
            wd=float(state_raw.get("wd", params_raw.get("wd0", 20.0))),
            fr=float(state_raw.get("fr", params_raw.get("fr0", 0.01))),
            s=float(state_raw.get("s", params_raw.get("s0", 6.0))),
            qrss0=float(state_raw.get("qrss0", params_raw.get("qrss0", 18.0))),
            qrg0=float(state_raw.get("qrg0", params_raw.get("qrg0", 20.0))),
        ),
    )


register_model("XinanjiangRunoffModel", _make_xinanjiang)


# XinanjiangCS：params/state/lag/cs/debug_trace 需要工厂函数
def _make_xinanjiang_cs(model_data) -> XinanjiangCSRunoffModel:
    params_raw = model_data.get("params", {})
    state_raw = model_data.get("state", {})
    return XinanjiangCSRunoffModel(
        params=XinanjiangCSParams(
            lag=int(params_raw.get("lag", 1)),
            wum=float(params_raw.get("wum", 20.0)),
            wlm=float(params_raw.get("wlm", 40.0)),
            wdm=float(params_raw.get("wdm", 40.0)),
            k=float(params_raw.get("k", 0.8)),
            c=float(params_raw.get("c", 0.1)),
            b=float(params_raw.get("b", 0.3)),
            imp=float(params_raw.get("imp", 0.02)),
            sm=float(params_raw.get("sm", 30.0)),
            ex=float(params_raw.get("ex", 1.2)),
            kss=float(params_raw.get("kss", 0.4)),
            kg=float(params_raw.get("kg", 0.3)),
            kkss=float(params_raw.get("kkss", 0.9)),
            kkg=float(params_raw.get("kkg", 0.95)),
            cs=float(params_raw.get("cs", 0.8)),
            area=float(params_raw.get("area", 0.0)),
        ),
        state=XinanjiangCSState(
            wu=float(state_raw.get("wu", params_raw.get("wu0", 5.0))),
            wl=float(state_raw.get("wl", params_raw.get("wl0", 10.0))),
            wd=float(state_raw.get("wd", params_raw.get("wd0", 20.0))),
            fr=float(state_raw.get("fr", params_raw.get("fr0", 0.01))),
            s=float(state_raw.get("s", params_raw.get("s0", 6.0))),
            qrss0=float(state_raw.get("qrss0", params_raw.get("qrss0", 18.0))),
            qrg0=float(state_raw.get("qrg0", params_raw.get("qrg0", 20.0))),
            qs0=float(state_raw.get("qs0", params_raw.get("qrs0", 20.0))),
        ),
        debug_trace=bool(params_raw.get("debug_trace", False)),
    )


register_model("XinanjiangCSRunoffModel", _make_xinanjiang_cs)


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
