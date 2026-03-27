from __future__ import annotations

from abc import ABC, abstractmethod

from hydro_engine.core.context import ForecastTimeContext
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries


class IErrorUpdater(ABC):
    """
    实时误差校正接口。

    实现类应仅使用「校正时段」内的模拟与实测残差估计校正量（与
    :class:`~hydro_engine.core.context.ForecastTimeContext` 中
    ``correction_start_time`` 至 ``display_start_time`` 对齐），
    再应用于预报期（``forecast_start_time`` / T0 及之后）的模拟序列。
    """

    @abstractmethod
    def correct(
        self,
        simulated: TimeSeries,
        observed: TimeSeries,
        time_context: ForecastTimeContext,
    ) -> TimeSeries:
        """返回与 simulated 同网格的校正后序列。"""


class IHydrologicalModel(ABC):
    """
    水文数学模型统一接口。

    - required_inputs：声明运行所需的强迫量种类（契约）。
    - run：输入统一为 ForcingData 容器；河道模型使用 ROUTING_INFLOW 键承载入流序列。
    """

    @classmethod
    @abstractmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        """模型运行所必需的强迫量键集合。"""

    @abstractmethod
    def run(self, forcing: ForcingData) -> TimeSeries:
        """执行模型计算，返回输出时间序列。"""
