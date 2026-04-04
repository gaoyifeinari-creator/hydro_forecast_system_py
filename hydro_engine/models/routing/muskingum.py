from __future__ import annotations

from dataclasses import dataclass
from typing import List
import warnings

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class MuskingumRoutingModel(IHydrologicalModel):
    """
    马斯京根（Muskingum）河道演进模型（与 Java ``HFMSKAlg`` 对齐）。

    **时段与输出网格**：返回的 :class:`~hydro_engine.core.timeseries.TimeSeries` 与入流**共用**
    ``start_time``、``time_step``、``len(values)``，**不对时间戳做平移或插值挪位**；
    ``outflow[i]`` 与 ``inflow[i]`` 对应同一离散时刻 ``start_time + i·Δt``。

    **递推**：``i≥1`` 时用 ``inflow[i-1]``、``inflow[i]`` 与上一时刻状态算 ``outflow[i]``（离散马斯京根），
    物理上有滞蓄，但**索引 i 与时段对齐**，不会产生「整列相对入流错位一步」的实现错误。

    **Δt**：由 ``input_series.time_step`` 换算为小时参与系数 ``c0,c1,c2``；日方案（每步 1 日）下为 24h。
    **K**：``k_hours`` 始终按**小时**解释，与 ``Δt`` 一致即可。
    """

    k_hours: float
    x: float = 0.2
    initial_outflow: float | None = None
    # 与 Java `HFMSKAlg` 一致：IntParaArr[0] = 2（分段数）
    n_segments: int = 2

    @classmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        return frozenset({ForcingKind.ROUTING_INFLOW})

    def run(self, forcing: ForcingData) -> TimeSeries:
        input_series = forcing.require(ForcingKind.ROUTING_INFLOW)
        return self._route_series(input_series)

    def _route_series(self, input_series: TimeSeries) -> TimeSeries:
        self._validate_parameters()
        # 与 Java HFMSKAlg：Dt 以小时计
        dt_hours = input_series.time_step.total_seconds() / 3600.0

        # Java 默认参数：NE = 2；当 NE == 0 则直接返回入流
        ne = int(self.n_segments)
        inflow = input_series.values
        nt = len(inflow)
        if ne == 0:
            return TimeSeries(input_series.start_time, input_series.time_step, list(inflow))

        ke = self.k_hours
        xe = self.x
        # 允许 k_hours == 0：按“无滞时”处理，直接透传入流。
        if ke == 0:
            return TimeSeries(input_series.start_time, input_series.time_step, list(inflow))

        # Java：x1 = 2 * KE * XE；x2 = 2 * KE - x1
        x1 = 2.0 * ke * xe
        x2 = 2.0 * ke - x1
        if dt_hours < x1 or dt_hours > x2:
            # Java 为 false（调用方可能会中止/报错），但本项目路由模型更偏向“不中断计算”。
            # 因此在这里仅发出警告并继续使用系数计算。
            warnings.warn(
                f"Muskingum constraint violated (proceed anyway): Dt={dt_hours}, "
                f"required_range=[{x1}, {x2}], k_hours={ke}, x={xe}",
                RuntimeWarning,
            )

        # Java：重算 x1 = KE - KE*XE + 0.5*Dt
        x1 = ke - ke * xe + 0.5 * dt_hours
        c0 = (0.5 * dt_hours - ke * xe) / x1
        c1 = (ke * xe + 0.5 * dt_hours) / x1
        c2 = (ke - ke * xe - 0.5 * dt_hours) / x1

        # Java：QC[j] 全初始化为 UpQInput[0]
        qc = [inflow[0]] * ne

        # Java：m_runRes 初始全 0；若 m_runRes[0] < 1e-6 则置为 QC[NE-1] (= inflow[0])
        outflow: List[float] = [0.0] * nt
        if self.initial_outflow is None:
            outflow[0] = inflow[0]
        else:
            outflow[0] = float(self.initial_outflow)
            if outflow[0] < 1e-6:
                outflow[0] = inflow[0]

        # Java：QC 与 m_runRes[0] 没有直接耦合；内层循环只依赖 QC
        qi1 = 0.0
        qi2 = 0.0
        qo1 = 0.0
        qo2 = 0.0

        for i in range(1, nt):
            for j in range(ne):
                qo1 = qc[j]
                if j == 0:
                    qi1 = inflow[i - 1]
                    qi2 = inflow[i]
                qo2 = c0 * qi2 + c1 * qi1 + c2 * qo1
                qi1 = qo1
                qi2 = qo2
                qc[j] = qo2
            outflow[i] = qo2

        # Java 不做 clamp；这里保持与 Java 一致（若输入含负值，输出也可能为负）
        # 显式复用入流时间轴：与 HFMSKAlg 一致，输出与入流逐索引同一时刻。
        return TimeSeries(input_series.start_time, input_series.time_step, outflow)

    def _validate_parameters(self) -> None:
        if self.k_hours < 0:
            raise ValueError("k_hours must be >= 0")
        if not -0.5 <= self.x <= 0.5:
            # Java 默认 XE 可能取负（示例：m_douParaArr[1] = -0.1）
            raise ValueError("x must be in [-0.5, 0.5]")
        if int(self.n_segments) < 0:
            raise ValueError("n_segments must be >= 0")
