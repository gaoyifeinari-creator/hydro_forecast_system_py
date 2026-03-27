from __future__ import annotations

import math
from dataclasses import dataclass

from hydro_engine.core.context import ForecastTimeContext
from hydro_engine.core.interfaces import IErrorUpdater
from hydro_engine.core.timeseries import TimeSeries


@dataclass
class AR1ErrorUpdater(IErrorUpdater):
    """
    示例校正器：在校正区间 [correction_start, display_start)（即「校正时段」步）上估计加性偏差，
    并将该偏差加到预报段（严格晚于 forecast_start / T0 的时刻）的模拟值上。

    （完整 AR(1) 残差模型可在此基础上扩展，不修改水文动力核。）
    """

    decay_factor: float = 0.8

    def correct(
        self,
        simulated: TimeSeries,
        observed: TimeSeries,
        time_context: ForecastTimeContext,
    ) -> TimeSeries:
        simulated._assert_compatible(observed)
        tc = time_context
        i_corr = simulated.get_index_by_time(tc.correction_start_time)
        i_disp = simulated.get_index_by_time(tc.display_start_time)
        i_t0 = simulated.get_index_by_time(tc.forecast_start_time)
        if i_corr > i_disp:
            raise ValueError("correction window invalid: correction_start after display_start")

        residuals: list[float] = []
        for i in range(i_corr, i_disp):
            o = observed.values[i]
            s = simulated.values[i]
            if isinstance(o, float) and math.isnan(o):
                continue
            residuals.append(o - s)

        bias = sum(residuals) / len(residuals) if residuals else 0.0
        bias *= self.decay_factor

        out = list(simulated.values)
        for i in range(i_t0, len(out)):
            out[i] = out[i] + bias

        return TimeSeries(simulated.start_time, simulated.time_step, out)
