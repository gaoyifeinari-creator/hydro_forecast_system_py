"""实时误差校正实现。

每个校正模型通过 ``register_model`` 自注册到全局 MODEL_REGISTRY。
"""

from hydro_engine.models import register_model

from .ar1_updater import AR1ErrorUpdater


# ---------------------------------------------------------------------------
# 自注册（触发 models.__init__ 中导入本包）
# ---------------------------------------------------------------------------

def _make_ar1(model_data) -> AR1ErrorUpdater:
    params = model_data.get("params", {})
    return AR1ErrorUpdater(decay_factor=float(params.get("decay_factor", 0.8)))


register_model("AR1ErrorUpdater", _make_ar1)

__all__ = ["AR1ErrorUpdater"]
