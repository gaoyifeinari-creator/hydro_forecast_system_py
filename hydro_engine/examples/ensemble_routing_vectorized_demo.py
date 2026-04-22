"""
演示：同一马斯京根河段上，对 ``(num_scenarios, time_steps)`` 入流张量一次性演进。

拓扑仍按时间步递推，但 **情景维** 在 NumPy 中向量化（等价于 50 个情景同步算完每个时刻）。
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.models.routing.muskingum import MuskingumRoutingModel


def demo_muskingum_ensemble_vectorized(
    *,
    num_scenarios: int = 50,
    time_steps: int = 120,
    k_hours: float = 6.0,
    x: float = 0.2,
) -> TimeSeries:
    start = datetime(2026, 1, 1, 0, 0, 0)
    step = timedelta(hours=1)
    t = np.arange(time_steps, dtype=np.float64)
    # 情景维：不同情景仅缩放幅值，便于检查输出形状
    scales = np.linspace(0.8, 1.2, num=num_scenarios, dtype=np.float64)[:, np.newaxis]
    base = (np.sin(t / 12.0) + 1.1) * 80.0
    inflow = scales * base[np.newaxis, :]

    forcing = ForcingData.single(
        ForcingKind.ROUTING_INFLOW,
        TimeSeries(start, step, inflow),
    )
    routed = MuskingumRoutingModel(k_hours=k_hours, x=x).run(forcing)
    assert routed.values.shape == (num_scenarios, time_steps)
    return routed


if __name__ == "__main__":
    out = demo_muskingum_ensemble_vectorized()
    print("shape", out.values.shape, "mean(last_step)=", float(np.mean(out.values[:, -1])))
