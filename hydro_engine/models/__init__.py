"""Built-in model implementations and model registry.

模型注册表
-----------
所有产流、河道演进、误差校正模型均通过 ``register_model`` 自注册到
``MODEL_REGISTRY``。加载 JSON 配置时，``_build_model`` 只需::

    MODEL_REGISTRY[name](model_data)

即可完成实例化，新增模型无需修改核心加载逻辑。

注册方式（两选一）：

1. 直接注册（适用于 ``__init__(self, **params)`` 的模型）::

       register_model("MuskingumRoutingModel", MuskingumRoutingModel)

2. 工厂函数注册（适用于有特殊 params/state 构造逻辑的模型）::

       register_model("XinanjiangRunoffModel", _make_xinanjiang)

   其中 ``_make_xinanjiang(model_data)`` 返回已实例化的模型对象。
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Type

# Registry: model_name -> factory (class or callable)
# Value can be a class (instantiated with **params) or a custom factory function.
MODEL_REGISTRY: Dict[str, Any] = {}
_MODEL_FACTORIES: Dict[str, Callable[[Dict[str, Any]], Any]] = {}


def register_model(name: str, factory: Any) -> None:
    """
    将模型注册到全局注册表。

    Args:
        name:           JSON 配置中的 ``model.name``，如 ``"XinanjiangRunoffModel"``。
        factory:        模型类，或签名如 ``(model_data: dict) -> IHydrologicalModel`` 的工厂函数。
                       若传入类且该类的 ``__init__`` 接受 ``**params``，则可直接用注册表实例化；
                       若有特殊构造逻辑，请传入工厂函数。
    """
    if name in _MODEL_FACTORIES:
        import warnings
        warnings.warn(
            f"Model '{name}' is already registered. "
            f"Overwriting previous registration (likely from multiple imports).",
            RuntimeWarning,
        )
    _MODEL_FACTORIES[name] = factory
    MODEL_REGISTRY[name] = factory


def _make_model_from_registry(name: str, model_data: Dict[str, Any]) -> Any:
    """
    通过注册表实例化模型。

    优先使用注册时登记的工厂函数；若注册的是类，则用 ``model_data["params"]``
    作为 ``**params`` 实例化。

    Args:
        name:        模型名称（须与 JSON 中的 ``name`` 一致）
        model_data:  JSON 中 ``model`` 节点的完整 dict（含 ``name`` / ``params`` / ``state`` 等）

    Returns:
        已实例化的水文模型对象。
    """
    if name not in _MODEL_FACTORIES:
        available = ", ".join(sorted(_MODEL_FACTORIES.keys()))
        raise ValueError(
            f"Model '{name}' is not registered. "
            f"Available models: {available}. "
            f"Did you forget to import the model's module?"
        )

    factory = _MODEL_FACTORIES[name]

    # 如果注册的是类（而非工厂函数），用 params 实例化
    if isinstance(factory, type):
        params = model_data.get("params", {})
        return factory(**params)

    # 工厂函数：传入完整 model_data（可自行解析 params / state）
    return factory(model_data)


# ---------------------------------------------------------------------------
# 导入子包，触发各模型的 self-register
# ---------------------------------------------------------------------------
from .routing import DummyRoutingModel, MuskingumRoutingModel
from .runoff import (
    DummyRunoffModel,
    SnowmeltRunoffModel,
    TankRunoffModel,
    XinanjiangCSRunoffModel,
    XinanjiangRunoffModel,
)
from .correction import AR1ErrorUpdater

__all__ = [
    "MODEL_REGISTRY",
    "register_model",
    "_make_model_from_registry",
    # 导出所有模型类（供外部 type 注解等使用）
    "DummyRunoffModel",
    "DummyRoutingModel",
    "SnowmeltRunoffModel",
    "XinanjiangRunoffModel",
    "XinanjiangCSRunoffModel",
    "TankRunoffModel",
    "MuskingumRoutingModel",
    "AR1ErrorUpdater",
]
