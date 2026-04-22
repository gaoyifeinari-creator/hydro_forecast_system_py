from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import numpy as np

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class SpatialWeights:
    """station_id -> weight 的轻量容器。"""

    weights: Dict[str, float]

    def get(self, station_id: str, default: float = 1.0) -> float:
        return float(self.weights.get(station_id, default))


class SpatialAggregator:
    """
    空间汇聚：把多个站点的 TimeSeries 汇聚到单个子流域 TimeSeries。

    注意：本实现假设多个输入序列已在时间网格上严格对齐。
    """

    @staticmethod
    def _assert_compatible_reference(series_list: Iterable[TimeSeries]) -> None:
        series_list = list(series_list)
        if not series_list:
            raise ValueError("series_list must not be empty")
        ref = series_list[0]
        for s in series_list[1:]:
            if s.start_time != ref.start_time or s.time_step != ref.time_step or s.time_steps != ref.time_steps:
                raise ValueError("TimeSeries inputs must align for spatial aggregation")
            if s.values.ndim != 1 or ref.values.ndim != 1:
                raise ValueError("SpatialAggregator currently requires 1-D station series")

    @classmethod
    def aggregate_time_series(
        cls,
        *,
        series_by_station: Dict[str, TimeSeries],
        weights: Dict[str, float] | None,
        kind: ForcingKind,
        method: str,
    ) -> TimeSeries:
        if not series_by_station:
            raise ValueError(f"series_by_station must not be empty (kind={kind.value})")

        cls._assert_compatible_reference(series_by_station.values())

        # 以任一序列的网格为输出网格（已通过断言保证一致）
        ref = next(iter(series_by_station.values()))
        out: List[float] = []
        ws = weights or {}

        # 内部默认把 NaN 当作缺测：聚合时忽略该站点该时刻
        for i in range(ref.time_steps):
            numer = 0.0
            denom = 0.0
            any_valid = False
            for sid, ts in series_by_station.items():
                v = float(ts.values[i])
                if np.isnan(v):
                    continue
                any_valid = True
                w = float(ws.get(sid, 1.0))

                if method == "sum":
                    # 对 sum：权重直接作为系数（调用方可传站点面积分数等）
                    numer += w * v
                elif method in ("weighted_average", "weighted_mean"):
                    numer += w * v
                    denom += w
                elif method in ("arithmetic_mean", "mean"):
                    numer += v
                    denom += 1.0
                else:
                    raise ValueError(f"Unsupported aggregation method: {method}")

            if not any_valid:
                out.append(float("nan"))
                continue

            if method == "sum":
                out.append(numer)
            else:
                out.append(numer / denom if denom != 0.0 else float("nan"))

        return TimeSeries(ref.start_time, ref.time_step, out)

