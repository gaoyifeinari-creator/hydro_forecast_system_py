"""
对外接口层：面向 UI、脚本与外部系统的稳定入口。

后续新增门面类、会话对象或薄封装时，优先放在本包下，与 `core` / `engine` / `io` 等内部实现分离。
"""

from .forecast_session import ForecastSession

__all__ = ["ForecastSession"]
