from __future__ import annotations

from dataclasses import dataclass
import warnings

import numpy as np

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class MuskingumRoutingModel(IHydrologicalModel):
    """
    马斯京根（Muskingum）河道演进模型（与 Java ``HFMSKAlg`` 对齐）。

    **时段与输出网格**：返回的 :class:`~hydro_engine.core.timeseries.TimeSeries` 与入流**共用**
    ``start_time``、``time_step``、时间长度，**不对时间戳做平移或插值挪位**；
    ``outflow[i]`` 与 ``inflow[i]`` 对应同一离散时刻 ``start_time + i·Δt``。

    支持 **1D** ``(T,)`` 与 **2D 集合** ``(S, T)``：集合维 ``S`` 上完全向量化（同一参数 K/X 对所有情景成立）。
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
        dt_hours = input_series.time_step.total_seconds() / 3600.0

        ne = int(self.n_segments)
        raw = np.asarray(input_series.values, dtype=np.float64, order="C")
        was_1d = raw.ndim == 1
        inflow = np.atleast_2d(raw)
        s_count, nt = inflow.shape

        if ne == 0:
            out = inflow.copy()
            return self._pack_output(input_series, out, was_1d)

        ke = self.k_hours
        xe = self.x
        if ke == 0:
            return self._pack_output(input_series, inflow.copy(), was_1d)

        x1 = 2.0 * ke * xe
        x2 = 2.0 * ke - x1
        if dt_hours < x1 or dt_hours > x2:
            warnings.warn(
                f"Muskingum constraint violated (proceed anyway): Dt={dt_hours}, "
                f"required_range=[{x1}, {x2}], k_hours={ke}, x={xe}",
                RuntimeWarning,
            )

        x1 = ke - ke * xe + 0.5 * dt_hours
        c0 = (0.5 * dt_hours - ke * xe) / x1
        c1 = (ke * xe + 0.5 * dt_hours) / x1
        c2 = (ke - ke * xe - 0.5 * dt_hours) / x1

        qc = np.zeros((s_count, ne), dtype=np.float64)
        qc[:] = inflow[:, 0:1]

        outflow = np.zeros((s_count, nt), dtype=np.float64)
        if self.initial_outflow is None:
            outflow[:, 0] = inflow[:, 0]
        else:
            init = float(self.initial_outflow)
            outflow[:, 0] = np.where(init < 1e-6, inflow[:, 0], init)

        qi1 = np.zeros(s_count, dtype=np.float64)
        qi2 = np.zeros(s_count, dtype=np.float64)
        qo1 = np.zeros(s_count, dtype=np.float64)
        qo2 = np.zeros(s_count, dtype=np.float64)

        for i in range(1, nt):
            for j in range(ne):
                qo1 = qc[:, j].copy()
                if j == 0:
                    qi1 = inflow[:, i - 1].copy()
                    qi2 = inflow[:, i].copy()
                qo2 = c0 * qi2 + c1 * qi1 + c2 * qo1
                qi1 = qo1
                qi2 = qo2
                qc[:, j] = qo2
            outflow[:, i] = qo2

        return self._pack_output(input_series, outflow, was_1d)

    @staticmethod
    def _pack_output(input_series: TimeSeries, outflow: np.ndarray, was_1d: bool) -> TimeSeries:
        arr = outflow[0, :] if was_1d else outflow
        return TimeSeries(input_series.start_time, input_series.time_step, arr)

    def _validate_parameters(self) -> None:
        if self.k_hours < 0:
            raise ValueError("k_hours must be >= 0")
        if not -0.5 <= self.x <= 0.5:
            raise ValueError("x must be in [-0.5, 0.5]")
        if int(self.n_segments) < 0:
            raise ValueError("n_segments must be >= 0")
