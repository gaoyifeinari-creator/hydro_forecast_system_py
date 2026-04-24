from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Optional

import numpy as np

from hydro_engine.core.context import ForecastTimeContext
from hydro_engine.core.interfaces import IErrorUpdater
from hydro_engine.core.timeseries import TimeSeries


@dataclass
class NodeCorrectionConfig:
    """节点算法相关的误差校正配置。"""

    updater_model: Optional[IErrorUpdater] = None


@dataclass
class AbstractNode(ABC):
    """节点抽象基类。"""

    id: str
    name: str = ""
    incoming_reach_ids: list[str] = field(default_factory=list)
    outgoing_reach_ids: list[str] = field(default_factory=list)
    local_catchment_ids: list[str] = field(default_factory=list)
    # 节点关联的实测站点（用于展示/比对/接力缝合）
    observed_station_id: str = ""
    use_observed_for_routing: bool = False
    # 历史模拟模式可启用：预报起报时刻之后也继续使用实测出流接力。
    use_observed_for_routing_after_forecast: bool = False
    # 节点输入（如水库“入库流量”）的实测/预报注入：用于驱动调度模型计算未来出库
    observed_inflow_station_id: str = ""
    use_observed_inflow_for_simulation: bool = False
    correction_config: Optional[NodeCorrectionConfig] = None

    def _build_observed_outflow_map(self, observed_series: TimeSeries) -> Dict[str, TimeSeries]:
        """
        将节点总出流实测序列映射到各下游河段。

        - 单出口节点：该实测序列就是该河段出流
        - 多出口节点：沿用节点自身的分流/调度规则，把“总出流”拆分到各河段，
          避免将同一条实测序列整段复制到多条支路上造成水量重复
        """
        if not self.outgoing_reach_ids:
            return {}
        if len(self.outgoing_reach_ids) == 1:
            return {self.outgoing_reach_ids[0]: observed_series}
        return self._compute_simulated_outflows(observed_series)

    def process_water(
        self,
        total_inflow: TimeSeries,
        observed_series: Optional[TimeSeries],
        time_context: ForecastTimeContext,
        observed_inflow_series: Optional[TimeSeries] = None,
    ) -> Dict[str, TimeSeries]:
        """
        模板方法：先物理模拟，再可选误差校正，再可选实测缝合（接力）。

        observed_series 由引擎在运行时按 context 切片后传入；
        当 observed_series 缺失或全为 NaN 时，由引擎回退到纯模拟，不在此抛异常。

        observed_inflow_series：用于把“节点输入”未来段替换成外部序列（例如水库入库预报），以便调度模型生成未来出库。
        """
        # 0) 缓存实测序列的可用性：后续用来决定是否需要做接力缝合/优化跳过计算。
        obs = observed_series
        obs_available = obs is not None and not self._is_all_nan(obs)

        # 1) 可选：对节点输入 total_inflow 注入外部序列（通常用于水库“入库流量”驱动未来出库）
        effective_inflow = total_inflow
        if self.use_observed_inflow_for_simulation:
            inflow_obs = observed_inflow_series
            inflow_obs_available = inflow_obs is not None and not self._is_all_nan(inflow_obs)
            if inflow_obs_available:
                # 规则：<= forecast_start_time - 1step 用模拟，> 该时刻用外部序列
                boundary = time_context.forecast_start_time - effective_inflow.time_step
                effective_inflow = inflow_obs.blend(effective_inflow, boundary)

        # 2) 优化：当启用“实测期演进（接力）”且没有误差校正器时，
        #    历史段（t <= forecast_start_time）输出直接取 observed，
        #    只需要对 forecast 之后的输入切片做调度计算，避免历史时段不必要的水库调度计算。
        cfg = self.correction_config
        if self.use_observed_for_routing and obs_available and (cfg is None or cfg.updater_model is None):
            # 默认：t < forecast_start_time 用实测，forecast 起用计算值。
            # 历史模拟模式（显式开关）下：全时段都可使用实测接力。
            if self.use_observed_for_routing_after_forecast:
                override_start = time_context.end_time
            else:
                # 与 ``blend(..., forecast_start - step)`` 一致：起报时刻所在步仍用实测，模拟从下一格点起算。
                override_start = time_context.forecast_start_time + time_context.time_delta

            # forecast 段为空：输出完全使用观测值（不需要任何调度计算）
            if override_start >= time_context.end_time:
                # 历史段不做模拟；多出口节点仍需按自身分流规则拆分 observed 总出流。
                return self._build_observed_outflow_map(obs)

            # 仅对 forecast 后段计算出流
            sim_in = effective_inflow.slice(override_start, time_context.end_time)
            sim_map = self._compute_simulated_outflows(sim_in)

            # 历史段输出直接取 observed；forecast 段用模拟结果覆盖
            out: Dict[str, TimeSeries] = self._build_observed_outflow_map(obs)
            for rid, sim_ts in sim_map.items():
                start_idx = obs.get_index_by_time(sim_ts.start_time)
                new_values = np.array(out[rid].values, dtype=np.float64, copy=True).reshape(-1)
                seg = np.asarray(sim_ts.values, dtype=np.float64).reshape(-1)
                new_values[start_idx : start_idx + sim_ts.time_steps] = seg
                out[rid] = TimeSeries(
                    start_time=obs.start_time,
                    time_step=obs.time_step,
                    values=new_values,
                )

            return out

        # 3) 默认路径：先对全段做模拟，然后（如启用）对输出做误差校正与接力缝合。
        simulated = self._compute_simulated_outflows(effective_inflow)
        out: Dict[str, TimeSeries] = {}
        for rid, series in simulated.items():
            corrected = series

            # 误差校正：只有在存在有效实测时才执行
            if cfg is not None and cfg.updater_model is not None and obs_available:
                corrected = cfg.updater_model.correct(series, obs, time_context)

            # 实测接力缝合：仅当 use_observed_for_routing 为真且实测有效
            if self.use_observed_for_routing and obs_available:
                boundary = (
                    time_context.end_time
                    if self.use_observed_for_routing_after_forecast
                    else (time_context.forecast_start_time - corrected.time_step)
                )
                observed_outflow = self._build_observed_outflow_map(obs).get(rid)
                if observed_outflow is None:
                    raise ValueError(f"Observed outflow mapping missing reach id: {rid}")
                out[rid] = corrected.blend(observed_outflow, boundary)
            else:
                out[rid] = corrected

        return out

    @staticmethod
    def _is_all_nan(ts: TimeSeries) -> bool:
        return bool(np.all(np.isnan(np.asarray(ts.values, dtype=np.float64))))

    @abstractmethod
    def _compute_simulated_outflows(self, total_inflow: TimeSeries) -> Dict[str, TimeSeries]:
        """子类实现纯物理/调度模拟，返回各下游河道 ID -> 出流序列。"""
