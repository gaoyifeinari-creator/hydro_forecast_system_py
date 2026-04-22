from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import warnings

from hydro_engine.core.context import ForecastTimeContext
from hydro_engine.core.forcing import ForcingData, ForcingKind, validate_forcing_contract
from hydro_engine.core.timeseries import TimeSeries, add_timeseries_list
from hydro_engine.domain.nodes.reservoir import ReservoirNode
from hydro_engine.engine.scheme import ForecastingScheme


def _validate_station_data_native_scale(
    time_context: ForecastTimeContext,
    catchment_forcing: Dict[str, ForcingData],
    observed_flows: Dict[str, TimeSeries],
) -> None:
    """
    运行前校验：外部序列须与方案原生 ``time_delta`` 一致，不做跨尺度换算。

    日方案下雨量、流量等序列的每一步即代表一个日尺度；步长与方案不一致则拒绝运行。
    """
    expected = time_context.time_delta
    for cid, fd in catchment_forcing.items():
        for kind, series in fd.items():
            if series.time_step != expected:
                raise ValueError(
                    f"Catchment {cid} forcing {kind.value}: time_step {series.time_step!r} "
                    f"must equal scheme native time_delta {expected!r}"
                )
            if series.start_time != time_context.warmup_start_time:
                raise ValueError(
                    f"Catchment {cid} forcing {kind.value}: start_time must equal "
                    f"warmup_start_time {time_context.warmup_start_time!r}"
                )
            if series.time_steps != time_context.step_count:
                raise ValueError(
                    f"Catchment {cid} forcing {kind.value}: length must equal step_count "
                    f"{time_context.step_count}"
                )
    for sid, series in observed_flows.items():
        if series.time_step != expected:
            raise ValueError(
                f"Observed flow {sid}: time_step {series.time_step!r} must equal "
                f"scheme native time_delta {expected!r}"
            )
        # observed_flows 可能覆盖更长区间；引擎将按 context 切片到 [warmup_start_time, end_time)
        try:
            sliced = series.slice(time_context.warmup_start_time, time_context.end_time)
        except Exception as exc:
            raise ValueError(
                f"Observed flow {sid}: cannot slice to context "
                f"[{time_context.warmup_start_time!r}, {time_context.end_time!r}). "
                f"Original start={series.start_time!r}, step={series.time_step!r}, len={series.time_steps}."
            ) from exc
        if sliced.time_steps != time_context.step_count:
            raise ValueError(
                f"Observed flow {sid}: sliced length {sliced.time_steps} != step_count {time_context.step_count}"
            )


@dataclass
class CalculationResult:
    """计算结果容器。"""

    time_context: Optional[ForecastTimeContext] = None
    node_total_inflows: Dict[str, TimeSeries] = field(default_factory=dict)
    node_outflows: Dict[str, TimeSeries] = field(default_factory=dict)
    node_observed_flows: Dict[str, TimeSeries] = field(default_factory=dict)
    catchment_runoffs: Dict[str, TimeSeries] = field(default_factory=dict)
    catchment_routed_flows: Dict[str, TimeSeries] = field(default_factory=dict)
    catchment_debug_traces: Dict[str, List[Dict[str, float]]] = field(default_factory=dict)
    reach_flows: Dict[str, TimeSeries] = field(default_factory=dict)
    interval_channels: List[str] = field(default_factory=list)
    node_interval_inflows: Dict[str, Dict[str, TimeSeries]] = field(default_factory=dict)
    node_interval_outflows: Dict[str, Dict[str, TimeSeries]] = field(default_factory=dict)
    reach_interval_flows: Dict[str, Dict[str, TimeSeries]] = field(default_factory=dict)

    def get_display_results(self) -> Dict[str, TimeSeries]:
        """
        从 display_start_time 起至 end_time，截取各节点入流与河道流量，供前端展示（滤除预热期爬升等）。
        """
        if self.time_context is None:
            raise ValueError("time_context is required for get_display_results")
        tc = self.time_context
        out: Dict[str, TimeSeries] = {}
        for nid, s in self.node_total_inflows.items():
            out[f"node:{nid}"] = s.slice(tc.display_start_time, tc.end_time)
        for rid, s in self.reach_flows.items():
            out[f"reach:{rid}"] = s.slice(tc.display_start_time, tc.end_time)
        # 区间通道也下沉到统一 display_results 接口，便于调度/导出复用。
        for nid, cmap in self.node_interval_inflows.items():
            for ch, s in cmap.items():
                out[f"node_interval_inflow:{ch}:{nid}"] = s.slice(
                    tc.display_start_time, tc.end_time
                )
        for nid, cmap in self.node_interval_outflows.items():
            for ch, s in cmap.items():
                out[f"node_interval_outflow:{ch}:{nid}"] = s.slice(
                    tc.display_start_time, tc.end_time
                )
        for ch, rmap in self.reach_interval_flows.items():
            for rid, s in rmap.items():
                out[f"reach_interval:{ch}:{rid}"] = s.slice(tc.display_start_time, tc.end_time)
        return out


class CalculationEngine:
    """
    核心执行器：按拓扑序推演流量。

    时间推进：每个时间步对应 ``ForecastTimeContext.time_delta`` 的一跳，与输入序列步长一一对应，
    不在引擎内做时间尺度换算。
    """

    def run(
        self,
        scheme: ForecastingScheme,
        catchment_forcing: Dict[str, ForcingData],
        time_context: ForecastTimeContext,
        observed_flows: Optional[Dict[str, TimeSeries]] = None,
        catchment_workers: Optional[int] = None,
    ) -> CalculationResult:
        time_context.validate()
        observed_flows = observed_flows or {}
        _validate_station_data_native_scale(time_context, catchment_forcing, observed_flows)

        topo_order = scheme.topological_order()
        reservoir_node_ids: Set[str] = {
            str(nid) for nid, n in scheme.nodes.items() if isinstance(n, ReservoirNode)
        }
        channel_cfgs = list(scheme.custom_interval_channels or [])
        if not channel_cfgs:
            channel_cfgs = [{"name": "default", "boundary_node_ids": []}]
        if not any(str(c.get("name", "")).strip() == "default" for c in channel_cfgs):
            channel_cfgs = [{"name": "default", "boundary_node_ids": []}] + channel_cfgs
        channel_names: List[str] = []
        channel_boundaries: Dict[str, Set[str]] = {}
        for c in channel_cfgs:
            name = str(c.get("name", "")).strip()
            if not name:
                continue
            if name in channel_boundaries:
                continue
            channel_names.append(name)
            if name == "default":
                # 标准相邻水库区间：default 通道默认在所有水库节点清零；
                # 同时允许配置显式 boundary_node_ids（用于“水库在当前方案被建成非 ReservoirNode”场景）。
                configured = {
                    str(x).strip()
                    for x in (c.get("boundary_node_ids") or [])
                    if str(x).strip()
                }
                channel_boundaries[name] = set(reservoir_node_ids) | configured
            else:
                channel_boundaries[name] = {
                    str(x).strip()
                    for x in (c.get("boundary_node_ids") or [])
                    if str(x).strip()
                }

        for catchment_id, catchment in scheme.catchments.items():
            if catchment_id not in catchment_forcing:
                raise ValueError(f"Missing forcing package for catchment: {catchment_id}")
            validate_forcing_contract(catchment.runoff_model, catchment_forcing[catchment_id])
            if catchment.routing_model is None:
                raise ValueError(
                    f"Catchment '{catchment_id}' must configure routing_model."
                )

        result = CalculationResult(time_context=time_context, interval_channels=channel_names)
        local_runoff: Dict[str, TimeSeries] = {}
        catchments = list(scheme.catchments.items())
        workers = int(catchment_workers or 0)
        if workers <= 0:
            workers = min(32, max(1, len(catchments)))

        def _run_one(catchment_id: str) -> tuple[str, TimeSeries, List[Dict[str, float]]]:
            c = scheme.catchments[catchment_id]
            runoff = c.generate_runoff(catchment_forcing[catchment_id])
            rows: List[Dict[str, float]] = []
            get_rows = getattr(c.runoff_model, "get_debug_rows", None)
            if callable(get_rows):
                try:
                    rows = get_rows()
                except Exception:
                    rows = []
            return catchment_id, runoff, rows

        if len(catchments) <= 1 or workers == 1:
            for catchment_id, _ in catchments:
                cid, runoff, rows = _run_one(catchment_id)
                local_runoff[cid] = runoff
                if rows:
                    result.catchment_debug_traces[cid] = rows
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(_run_one, catchment_id) for catchment_id, _ in catchments]
                for fut in futures:
                    cid, runoff, rows = fut.result()
                    local_runoff[cid] = runoff
                    if rows:
                        result.catchment_debug_traces[cid] = rows

        reach_cache: Dict[str, TimeSeries] = {}
        reach_interval_cache: Dict[str, Dict[str, TimeSeries]] = {
            ch: {} for ch in channel_names
        }
        catchment_routed_to_node: Dict[str, List[TimeSeries]] = {}
        catchment_routed_to_node_interval: Dict[str, Dict[str, List[TimeSeries]]] = {
            ch: {} for ch in channel_names
        }
        catchment_owner_node: Dict[str, str] = {}
        for nid, node in scheme.nodes.items():
            for cid in node.local_catchment_ids:
                if cid in catchment_owner_node and catchment_owner_node[cid] != nid:
                    raise ValueError(
                        f"Catchment '{cid}' is mounted on multiple nodes: "
                        f"{catchment_owner_node[cid]} and {nid}"
                    )
                catchment_owner_node[cid] = nid

        for catchment_id, catchment in scheme.catchments.items():
            owner_node_id = catchment_owner_node.get(catchment_id, "")
            if not owner_node_id:
                raise ValueError(f"Catchment '{catchment_id}' is not mounted on any node.local_catchment_ids")
            target_node_id = str(catchment.downstream_node_id or "").strip()
            if not target_node_id:
                raise ValueError(
                    f"Catchment '{catchment_id}' must configure downstream_node_id."
                )
            if target_node_id not in scheme.nodes:
                raise ValueError(
                    f"Catchment '{catchment_id}' downstream_node_id='{target_node_id}' not found in scheme nodes"
                )
            runoff = local_runoff[catchment_id]
            routing_input = ForcingData.single(ForcingKind.ROUTING_INFLOW, runoff)
            validate_forcing_contract(catchment.routing_model, routing_input)
            routed = catchment.route_runoff(runoff)
            result.catchment_runoffs[catchment_id] = runoff
            result.catchment_routed_flows[catchment_id] = routed
            catchment_routed_to_node.setdefault(target_node_id, []).append(routed)
            for ch in channel_names:
                catchment_routed_to_node_interval[ch].setdefault(target_node_id, []).append(routed)

        for node_id in topo_order:
            node = scheme.nodes[node_id]
            # 将“节点实测站”从 observed_flows 中按 context 对齐后，传给 node.process_water。
            # 当实测缺失/全 NaN 时，node.process_water 内部会自动回退到纯模拟。
            observed_series_for_node: Optional[TimeSeries] = None
            observed_inflow_series_for_node: Optional[TimeSeries] = None

            # 1) 节点输出实测：用于展示/比对，以及在 use_observed_for_routing=true 时参与输出缝合
            if getattr(node, "observed_station_id", ""):
                station_id = str(node.observed_station_id)
                if station_id in observed_flows:
                    try:
                        sliced = observed_flows[station_id].slice(
                            time_context.warmup_start_time, time_context.end_time
                        )
                    except Exception as exc:
                        if getattr(node, "use_observed_for_routing", False):
                            warnings.warn(
                                f"Node '{node_id}' observed_station_id='{station_id}' slicing failed; "
                                f"fallback to simulated. reason={exc!s}",
                                RuntimeWarning,
                            )
                    else:
                        # 用于展示/比对
                        result.node_observed_flows[node_id] = sliced
                        # 全 NaN：接力缝合回退
                        if node._is_all_nan(sliced):
                            if getattr(node, "use_observed_for_routing", False):
                                warnings.warn(
                                    f"Node '{node_id}' observed_station_id='{station_id}' is all NaN; "
                                    "fallback to simulated.",
                                    RuntimeWarning,
                                )
                        else:
                            observed_series_for_node = sliced
                else:
                    # 缺失实测：仅当启用接力缝合才警告
                    if getattr(node, "use_observed_for_routing", False):
                        warnings.warn(
                            f"Node '{node_id}' observed_station_id='{station_id}' missing in observed_flows; "
                            "fallback to simulated.",
                            RuntimeWarning,
                        )

            # 2) 节点输入注入：用于把 reservoir “入库流量/预报入库”注入调度模型计算未来出库
            if getattr(node, "observed_inflow_station_id", "") and getattr(
                node, "use_observed_inflow_for_simulation", False
            ):
                inflow_station_id = str(node.observed_inflow_station_id)
                if inflow_station_id in observed_flows:
                    try:
                        observed_inflow_series_for_node = observed_flows[inflow_station_id].slice(
                            time_context.warmup_start_time, time_context.end_time
                        )
                    except Exception as exc:
                        warnings.warn(
                            f"Node '{node_id}' observed_inflow_station_id='{inflow_station_id}' "
                            f"inflow slice failed; fallback to simulated inflow. reason={exc!s}",
                            RuntimeWarning,
                        )
                        observed_inflow_series_for_node = None
                    else:
                        if node._is_all_nan(observed_inflow_series_for_node):
                            warnings.warn(
                                f"Node '{node_id}' observed_inflow_station_id='{inflow_station_id}' is all NaN; "
                                "fallback to simulated inflow.",
                                RuntimeWarning,
                            )
                            observed_inflow_series_for_node = None
                else:
                    warnings.warn(
                        f"Node '{node_id}' observed_inflow_station_id='{inflow_station_id}' missing in observed_flows; "
                        "fallback to simulated inflow.",
                        RuntimeWarning,
                    )
            inflows: List[TimeSeries] = []
            interval_inflows: Dict[str, List[TimeSeries]] = {
                ch: [] for ch in channel_names
            }

            for reach_id in node.incoming_reach_ids:
                if reach_id in reach_cache:
                    inflows.append(reach_cache[reach_id])
                for ch in channel_names:
                    ch_cache = reach_interval_cache[ch]
                    if reach_id in ch_cache:
                        interval_inflows[ch].append(ch_cache[reach_id])

            for s in catchment_routed_to_node.get(node_id, []):
                inflows.append(s)
            for ch in channel_names:
                for s in catchment_routed_to_node_interval[ch].get(node_id, []):
                    interval_inflows[ch].append(s)

            for catchment_id in node.local_catchment_ids:
                if catchment_id not in local_runoff:
                    raise ValueError(f"Unknown catchment id: {catchment_id}")

            if inflows:
                result.node_total_inflows[node_id] = add_timeseries_list(inflows)

            if inflows:
                total_inflow = add_timeseries_list(inflows)
            else:
                total_inflow = time_context.build_uniform_series(0.0)
            node_interval_inflow_map: Dict[str, TimeSeries] = {}
            for ch in channel_names:
                ch_arr = interval_inflows[ch]
                node_interval_inflow_map[ch] = (
                    add_timeseries_list(ch_arr) if ch_arr else time_context.build_uniform_series(0.0)
                )
            result.node_interval_inflows[node_id] = node_interval_inflow_map

            outflow_map = node.process_water(
                total_inflow,
                observed_series_for_node,
                time_context,
                observed_inflow_series=observed_inflow_series_for_node,
            )
            if outflow_map:
                # 节点总出流（所有出流河段求和），用于前端直接核对“出库/出流”演进效果。
                result.node_outflows[node_id] = add_timeseries_list(list(outflow_map.values()))
            node_interval_outflow_sum: Dict[str, TimeSeries] = {}
            for ch in channel_names:
                ch_outflow_map = self._compute_interval_outflow_map_for_node(
                    node=node,
                    interval_inflow=node_interval_inflow_map[ch],
                    channel_name=ch,
                    boundary_node_ids=channel_boundaries.get(ch, set()),
                    time_context=time_context,
                )
                if ch_outflow_map:
                    node_interval_outflow_sum[ch] = add_timeseries_list(list(ch_outflow_map.values()))
                else:
                    node_interval_outflow_sum[ch] = time_context.build_uniform_series(0.0)
                for out_reach_id, outflow in ch_outflow_map.items():
                    if out_reach_id not in scheme.reaches:
                        raise ValueError(f"Unknown reach id: {out_reach_id}")
                    routing_input = ForcingData.single(ForcingKind.ROUTING_INFLOW, outflow)
                    validate_forcing_contract(scheme.reaches[out_reach_id].routing_model, routing_input)
                    routed = scheme.reaches[out_reach_id].route(routing_input)
                    if out_reach_id in reach_interval_cache[ch]:
                        reach_interval_cache[ch][out_reach_id] = (
                            reach_interval_cache[ch][out_reach_id] + routed
                        )
                    else:
                        reach_interval_cache[ch][out_reach_id] = routed
            result.node_interval_outflows[node_id] = node_interval_outflow_sum
            for out_reach_id, outflow in outflow_map.items():
                if out_reach_id not in scheme.reaches:
                    raise ValueError(f"Unknown reach id: {out_reach_id}")

                routing_input = ForcingData.single(ForcingKind.ROUTING_INFLOW, outflow)
                validate_forcing_contract(scheme.reaches[out_reach_id].routing_model, routing_input)
                routed = scheme.reaches[out_reach_id].route(routing_input)
                if out_reach_id in reach_cache:
                    reach_cache[out_reach_id] = reach_cache[out_reach_id] + routed
                else:
                    reach_cache[out_reach_id] = routed

        result.reach_flows = reach_cache
        result.reach_interval_flows = reach_interval_cache
        return result

    @staticmethod
    def _compute_interval_outflow_map_for_node(
        *,
        node,
        interval_inflow: TimeSeries,
        channel_name: str,
        boundary_node_ids: Set[str],
        time_context: ForecastTimeContext,
    ) -> Dict[str, TimeSeries]:
        if not node.outgoing_reach_ids:
            return {}
        if str(node.id) in boundary_node_ids:
            z = time_context.build_uniform_series(0.0)
            return {rid: z for rid in node.outgoing_reach_ids}
        if isinstance(node, ReservoirNode):
            return {rid: interval_inflow for rid in node.outgoing_reach_ids}
        _ = channel_name
        return node._compute_simulated_outflows(interval_inflow)
