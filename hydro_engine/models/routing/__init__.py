"""Routing model implementations for river reaches.

每个模型文件在末尾通过 ``register_model`` 自注册到全局 MODEL_REGISTRY。
"""

from hydro_engine.models import register_model

from .dummy import DummyRoutingModel
from .muskingum import MuskingumRoutingModel


# ---------------------------------------------------------------------------
# 自注册（触发 models.__init__ 中导入本包）
# ---------------------------------------------------------------------------
register_model("DummyRoutingModel", DummyRoutingModel)


def _make_muskingum(model_data) -> MuskingumRoutingModel:
    """工厂函数：将 JSON params（ne/dt/x1/x2）与 Muskingum 构造函数（k_hours/x/n_segments）对齐。"""
    params = model_data.get("params", {})
    # JSON 中习惯字段名：ne / dt / x1 / x2
    # Muskingum 构造函数：k_hours / x / initial_outflow / n_segments
    # 注意：Java 旧版 JSON 用 ne=2, dt, x1, x2；k_hours 由 dt 与 x1/x2 推导
    dt = float(params.get("dt", 1.0))
    x1 = float(params.get("x1", 0.0))
    x2 = float(params.get("x2", 1.0))
    ne = int(params.get("ne", params.get("n_segments", 2)))
    # 若直接提供 k_hours 则优先使用；否则由 x1/x2 推导（兼容旧配置）
    k_hours = float(params.get("k_hours", params.get("k", (x1 + x2) / 2.0 if x2 > 0 else 1.0)))
    x = float(params.get("x", params.get("xe", 0.2)))
    return MuskingumRoutingModel(
        k_hours=k_hours,
        x=x,
        n_segments=ne,
        initial_outflow=params.get("initial_outflow"),
    )


register_model("MuskingumRoutingModel", _make_muskingum)

__all__ = ["DummyRoutingModel", "MuskingumRoutingModel"]
