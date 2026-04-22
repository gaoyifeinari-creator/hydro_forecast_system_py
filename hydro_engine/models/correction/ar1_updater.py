from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

from hydro_engine.core.context import ForecastTimeContext
from hydro_engine.core.interfaces import IErrorUpdater
from hydro_engine.core.timeseries import TimeSeries


@dataclass
class AR1ErrorUpdater(IErrorUpdater):
    """
    示例校正器：在 T0 前「校正尾段」[correction_start, forecast_start) 上估计加性偏差
    （步数 = correction_period_steps，自 T0 向历史回溯），
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
        if simulated.values.ndim != 1 or observed.values.ndim != 1:
            raise ValueError("AR1ErrorUpdater currently supports 1-D deterministic series only")
        tc = time_context
        i_corr = simulated.get_index_by_time(tc.correction_start_time)
        i_t0 = simulated.get_index_by_time(tc.forecast_start_time)
        if i_corr > i_t0:
            raise ValueError("correction window invalid: correction_start after forecast_start")

        residuals: list[float] = []
        obs_arr = np.asarray(observed.values, dtype=np.float64)
        sim_arr = np.asarray(simulated.values, dtype=np.float64)
        for i in range(i_corr, i_t0):
            o = float(obs_arr[i])
            s = float(sim_arr[i])
            if math.isnan(o):
                continue
            residuals.append(o - s)

        bias = sum(residuals) / len(residuals) if residuals else 0.0
        bias *= self.decay_factor

        out = np.array(sim_arr, dtype=np.float64, copy=True)
        out[i_t0 :] = out[i_t0 :] + bias

        return TimeSeries(simulated.start_time, simulated.time_step, out)
