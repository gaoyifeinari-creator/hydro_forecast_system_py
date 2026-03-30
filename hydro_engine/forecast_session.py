"""
预报会话类 (ForecastSession)
============================

统一的预报交互门面类，管理"人工制作一次预报"的全生命周期状态。

对外（UI/API）：所有时序数据使用 pandas.Series / pandas.DataFrame
对内（底层引擎）：调用 run_calculation 时，由本类负责转换为 TimeSeries / ForcingData

使用示例：
    session = ForecastSession("configs/forecastSchemeConf.json")
    session.setup_time_axis(warmup_start=datetime(2024, 6, 1), ...)
    session.fetch_data_from_source(mock_db_reader)
    series = session.get_forcing_series("4101", "precipitation")
    modified = series + 5.0  # 人工干预
    session.modify_forcing_series("4101", "precipitation", modified)
    session.run_calculation()
    df = session.get_node_hydrograph("1041")
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from hydro_engine.core.context import ForecastTimeContext, TimeType, parse_time_type
from hydro_engine.core.forcing import ForcingData, ForcingKind, parse_forcing_kind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.engine.calculator import CalculationEngine, CalculationResult
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.io.json_config import load_scheme_from_json

# 类型别名
PandasSeries = pd.Series
PandasDataFrame = pd.DataFrame


class ForecastSession:
    """
    预报会话类
    
    管理一次预报的全生命周期：配置加载 → 时间轴设置 → 数据拉取 → 
    人工干预 → 计算执行 → 结果提取。
    
    Attributes:
        config_path: JSON 配置文件路径
        raw_config: 原始配置字典（备用）
        time_context: 时间上下文
        scheme: 拓扑方案
        station_data_pool: 测站数据池 {station_id: {kind: pd.Series}}
        catchment_data_pool: 子流域数据池 {catchment_id: {kind: pd.Series}}
        result: 最近一次计算结果
    """

    def __init__(self, config_path: str, scheme_index: int = 0):
        """
        初始化预报会话
        
        Args:
            config_path: JSON 配置文件路径
            scheme_index: 方案索引（当配置中有多个方案时）
        """
        self.config_path = Path(config_path)
        
        # 加载原始配置（保留备用）
        with open(self.config_path, encoding="utf-8") as f:
            import json
            self.raw_config: Dict[str, Any] = json.load(f)
        
        # 初始状态：空数据池
        self.time_context: Optional[ForecastTimeContext] = None
        self.scheme: Optional[ForecastingScheme] = None
        self.station_data_pool: Dict[str, Dict[str, pd.Series]] = {}  # {station_id: {kind: series}}
        self.catchment_data_pool: Dict[str, Dict[str, pd.Series]] = {}  # {catchment_id: {kind: series}}
        self.result: Optional[CalculationResult] = None
        
        # 引擎实例
        self._engine = CalculationEngine()
        
        # 内部索引映射
        self._scheme_index = scheme_index
        
    # ========================================================================
    # 2.1 时间轴设置
    # ========================================================================
    
    def setup_time_axis(
        self,
        warmup_start: datetime,
        warmup_steps: int,
        correction_steps: int,
        forecast_steps: int,
        time_type: str = "Hour",
        step_size: int = 1,
    ) -> None:
        """
        建立时间上下文并构建拓扑方案
        
        Args:
            warmup_start: 预热起始时间
            warmup_steps: 预热步数
            correction_steps: 校正步数
            forecast_steps: 预报步数
            time_type: 时间粒度 ("Minute", "Hour", "Day")
            step_size: 步长大小的数值
        """
        # 解析时间类型
        tt = parse_time_type(time_type)
        
        # 计算历史显示步数（= 预报步数，保持对称）
        historical_steps = forecast_steps
        
        # 构建时间上下文
        self.time_context = ForecastTimeContext.from_period_counts(
            warmup_start_time=warmup_start,
            time_type=tt,
            step_size=step_size,
            warmup_period_steps=warmup_steps,
            correction_period_steps=correction_steps,
            historical_display_period_steps=historical_steps,
            forecast_period_steps=forecast_steps,
        )
        
        # 从 JSON 加载方案
        self.scheme = load_scheme_from_json(
            self.raw_config,
            time_type=tt,
            step_size=step_size,
            warmup_start_time=warmup_start,
        )
        
    # ========================================================================
    # 2.2 数据加载与交互修改
    # ========================================================================
    
    def fetch_data_from_source(
        self,
        db_reader,
        active_profile: Optional[str] = None,
    ) -> None:
        """
        从外部数据源批量拉取强迫数据
        
        Args:
            db_reader: 数据读取器，需实现以下接口：
                - fetch_station_data(station_id, start_time, end_time, kinds) -> Dict[str, pd.Series]
                - fetch_catchment_data(catchment_id, start_time, end_time, kinds) -> Dict[str, pd.Series]
            active_profile: 激活的配置文件（可选）
        """
        if self.time_context is None:
            raise RuntimeError("请先调用 setup_time_axis() 设置时间轴")
        
        tc = self.time_context
        start_time = tc.warmup_start_time
        end_time = tc.end_time
        
        # 获取方案中所有需要的测站和子流域
        station_ids = self._get_all_station_ids()
        catchment_ids = list(self.scheme.catchments.keys()) if self.scheme else []
        
        # 拉取测站数据
        for station_id in station_ids:
            try:
                kinds = [k.value for k in ForcingKind]  # 拉取所有要素
                data = db_reader.fetch_station_data(station_id, start_time, end_time, kinds)
                self.station_data_pool[station_id] = data
            except Exception as e:
                # 单个测站失败不影响整体
                print(f"[ForecastSession] 拉取测站 {station_id} 数据失败: {e}")
        
        # 拉取子流域数据
        for catchment_id in catchment_ids:
            try:
                kinds = [k.value for k in ForcingKind]
                data = db_reader.fetch_catchment_data(catchment_id, start_time, end_time, kinds)
                self.catchment_data_pool[catchment_id] = data
            except Exception as e:
                print(f"[ForecastSession] 拉取子流域 {catchment_id} 数据失败: {e}")
    
    def get_forcing_series(
        self,
        target_id: str,
        kind: str,
        is_catchment: bool = False,
    ) -> pd.Series:
        """
        获取某个测站或子流域的特定要素时间序列
        
        Args:
            target_id: 测站ID或子流域ID
            kind: 要素类型字符串 (如 "precipitation")
            is_catchment: 是否为子流域
        
        Returns:
            pd.Series: 时间索引的要素序列
        """
        pool = self.catchment_data_pool if is_catchment else self.station_data_pool
        
        if target_id not in pool:
            raise KeyError(f"未找到 {target_id} 的数据，请先调用 fetch_data_from_source()")
        
        data = pool[target_id]
        if kind not in data:
            raise KeyError(f"{target_id} 中未找到要素 {kind}")
        
        return data[kind].copy()
    
    def modify_forcing_series(
        self,
        target_id: str,
        kind: str,
        modified_series: pd.Series,
        is_catchment: bool = False,
    ) -> None:
        """
        接收人工干预后的序列，覆盖数据池中的相应数据
        
        Args:
            target_id: 测站ID或子流域ID
            kind: 要素类型字符串
            modified_series: 修改后的序列
            is_catchment: 是否为子流域
        """
        pool = self.catchment_data_pool if is_catchment else self.station_data_pool
        
        if target_id not in pool:
            raise KeyError(f"未找到 {target_id} 的数据，请先调用 fetch_data_from_source()")
        
        # 验证索引兼容性
        original = pool[target_id].get(kind)
        if original is not None:
            if not modified_series.index.equals(original.index):
                raise ValueError(
                    f"修改后的序列索引与原始不匹配，请确保时间索引一致"
                )
        
        pool[target_id][kind] = modified_series.copy()
    
    # ========================================================================
    # 2.3 核心计算
    # ========================================================================
    
    def run_calculation(
        self,
        forecast_mode: str = "realtime_forecast",
    ) -> CalculationResult:
        """
        组装底层强迫输入并执行预报计算
        
        Args:
            forecast_mode: 预报模式 ("realtime_forecast" / "historical_simulation")
        
        Returns:
            CalculationResult: 计算结果
        """
        if self.time_context is None or self.scheme is None:
            raise RuntimeError("请先调用 setup_time_axis() 设置时间轴")
        
        tc = self.time_context
        
        # 构建 catchment_forcing: {catchment_id: ForcingData}
        catchment_forcing: Dict[str, ForcingData] = {}
        
        for catchment_id, catchment in self.scheme.catchments.items():
            if catchment_id not in self.catchment_data_pool:
                raise ValueError(f"缺少子流域 {catchment_id} 的强迫数据")
            
            pool_data = self.catchment_data_pool[catchment_id]
            forcing_pairs: List[Tuple[ForcingKind, TimeSeries]] = []
            
            for kind_str, series in pool_data.items():
                try:
                    fk = parse_forcing_kind(kind_str)
                    # 将 pd.Series 转换为 TimeSeries
                    ts = self._pandas_to_timeseries(series, tc.warmup_start_time)
                    forcing_pairs.append((fk, ts))
                except Exception as e:
                    print(f"[ForecastSession] 跳过无效要素 {kind_str}: {e}")
            
            catchment_forcing[catchment_id] = ForcingData.from_pairs(forcing_pairs)
        
        # 转换测站实测数据（用于校正）
        observed_flows: Dict[str, TimeSeries] = {}
        for station_id, station_data in self.station_data_pool.items():
            if "observed_flow" in station_data:
                obs_series = station_data["observed_flow"]
                observed_flows[station_id] = self._pandas_to_timeseries(
                    obs_series, tc.warmup_start_time
                )
        
        # 执行计算
        self.result = self._engine.run(
            scheme=self.scheme,
            catchment_forcing=catchment_forcing,
            time_context=tc,
            observed_flows=observed_flows or None,
        )
        
        return self.result
    
    # ========================================================================
    # 2.4 结果提取
    # ========================================================================
    
    def get_node_hydrograph(self, node_id: str) -> pd.DataFrame:
        """
        提取某节点的计算入流、出流序列
        
        Args:
            node_id: 节点ID
        
        Returns:
            pd.DataFrame: 包含以下列：
                - inflow: 总入流
                - outflow: 出流（若有）
                - observed: 实测流量（若有）
            index: DatetimeIndex
        """
        if self.result is None:
            raise RuntimeError("请先调用 run_calculation() 执行计算")
        
        tc = self.result.time_context
        df = pd.DataFrame(index=self._build_datetime_index(tc.display_start_time, tc.end_time, tc.time_delta))
        
        # 入流
        if node_id in self.result.node_total_inflows:
            inflow_ts = self.result.node_total_inflows[node_id]
            sliced = inflow_ts.slice(tc.display_start_time, tc.end_time)
            df["inflow"] = sliced.values
        else:
            df["inflow"] = math.nan
        
        # 出流
        if node_id in self.result.node_outflows:
            outflow_ts = self.result.node_outflows[node_id]
            sliced = outflow_ts.slice(tc.display_start_time, tc.end_time)
            df["outflow"] = sliced.values
        else:
            df["outflow"] = math.nan
        
        # 实测
        if node_id in self.result.node_observed_flows:
            obs_ts = self.result.node_observed_flows[node_id]
            sliced = obs_ts.slice(tc.display_start_time, tc.end_time)
            df["observed"] = sliced.values
        else:
            df["observed"] = math.nan
        
        return df
    
    def get_catchment_hydrograph(self, catchment_id: str) -> pd.DataFrame:
        """
        提取某子流域的产流序列及对应降雨序列
        
        Args:
            catchment_id: 子流域ID
        
        Returns:
            pd.DataFrame: 包含以下列：
                - runoff: 产流量
                - precipitation: 降雨量（若有）
            index: DatetimeIndex
        """
        if self.result is None:
            raise RuntimeError("请先调用 run_calculation() 执行计算")
        
        tc = self.result.time_context
        df = pd.DataFrame(index=self._build_datetime_index(tc.display_start_time, tc.end_time, tc.time_delta))
        
        # 产流
        if catchment_id in self.result.catchment_runoffs:
            runoff_ts = self.result.catchment_runoffs[catchment_id]
            sliced = runoff_ts.slice(tc.display_start_time, tc.end_time)
            df["runoff"] = sliced.values
        else:
            df["runoff"] = math.nan
        
        # 降雨（从数据池获取）
        if catchment_id in self.catchment_data_pool:
            pool_data = self.catchment_data_pool[catchment_id]
            if "precipitation" in pool_data:
                prec_series = pool_data["precipitation"]
                # 按时间切片
                prec_series = prec_series.loc[tc.display_start_time:tc.end_time - tc.time_delta]
                df["precipitation"] = prec_series.values[:len(df)]
            else:
                df["precipitation"] = math.nan
        else:
            df["precipitation"] = math.nan
        
        return df
    
    # ========================================================================
    # 内部辅助方法
    # ========================================================================
    
    def _get_all_station_ids(self) -> List[str]:
        """从方案中提取所有涉及的测站ID"""
        station_ids = set()
        
        if self.scheme is None:
            return []
        
        for catchment in self.scheme.catchments.values():
            bindings = getattr(catchment, "forcing_bindings", [])
            for binding in bindings:
                if hasattr(binding, "station_id"):
                    station_ids.add(binding.station_id)
        
        return list(station_ids)
    
    def _pandas_to_timeseries(
        self,
        series: pd.Series,
        start_time: datetime,
    ) -> TimeSeries:
        """
        将 pandas.Series 转换为引擎的 TimeSeries
        
        Args:
            series: pandas.Series，index 为 DatetimeIndex
            start_time: 序列起始时间
        
        Returns:
            TimeSeries
        """
        # 处理 NaN
        values = series.values.tolist()
        values = [float("nan") if pd.isna(v) else float(v) for v in values]
        
        # 从 series 推断 time_step
        if len(series) < 2:
            time_step = self.time_context.time_delta if self.time_context else timedelta(hours=1)
        else:
            delta = (series.index[1] - series.index[0])
            time_step = delta
        
        return TimeSeries(
            start_time=start_time,
            time_step=time_step,
            values=values,
        )
    
    def _build_datetime_index(
        self,
        start: datetime,
        end: datetime,
        freq: timedelta,
    ) -> pd.DatetimeIndex:
        """构建_datetime_index"""
        return pd.date_range(start=start, end=end, freq=freq, inclusive="left")
    
    def get_status(self) -> Dict[str, Any]:
        """获取当前会话状态（调试用）"""
        return {
            "config_path": str(self.config_path),
            "time_context": self.time_context,
            "scheme_loaded": self.scheme is not None,
            "station_count": len(self.station_data_pool),
            "catchment_count": len(self.catchment_data_pool),
            "result_available": self.result is not None,
        }


# ========================================================================
# 类型注解（避免循环导入）
# ========================================================================
from typing import Tuple


# ========================================================================
# 完整可运行的模拟测试（Mock 数据 + User Journey）
# ========================================================================

if __name__ == "__main__":
    """
    模拟用户操作流：
    1. 实例化 Session 并设定时间轴（过去 72 小时预热 + 未来 48 小时预报）
    2. 点击"加载数据"（传入 MockDBReader）
    3. 查看 4101 流域的面雨量，觉得偏小，人工干预 +5.0 mm
    4. 执行预报计算
    5. 提取 1041 节点的水文过程线并打印
    """
    from datetime import timedelta
    from hydro_engine.core.forcing import ForcingKind
    from hydro_engine.core.timeseries import TimeSeries
    from hydro_engine.domain.catchment import SubCatchment
    from hydro_engine.domain.nodes.reservoir import ReservoirNode
    from hydro_engine.domain.reach import RiverReach
    from hydro_engine.engine.scheme import ForecastingScheme
    import numpy as np
    
    print("=" * 60)
    print("ForecastSession 模拟测试")
    print("=" * 60)
    
    # ------------------------------------------------------------------
    # Step 0: 准备 Mock 数据读取器
    # ------------------------------------------------------------------
    
    class MockDBReader:
        """
        Mock 数据读取器
        
        模拟从数据库拉取测站和子流域的强迫数据。
        返回伪造的 pd.Series（带有 DatetimeIndex）。
        """
        
        def __init__(self, time_context: ForecastTimeContext):
            self.tc = time_context
            self.rng = np.random.default_rng(seed=42)
        
        def _build_series(
            self,
            start: datetime,
            end: datetime,
            freq: timedelta,
            mean: float,
            std: float = 1.0,
        ) -> pd.Series:
            """生成随机时间序列"""
            n = int((end - start) / freq)
            values = self.rng.normal(mean, std, size=n)
            index = pd.date_range(start=start, periods=n, freq=freq)
            return pd.Series(values, index=index, name="value")
        
        def fetch_station_data(
            self,
            station_id: str,
            start_time: datetime,
            end_time: datetime,
            kinds: List[str],
        ) -> Dict[str, pd.Series]:
            """模拟拉取测站数据"""
            result = {}
            freq = self.tc.time_delta
            
            for kind in kinds:
                if kind == ForcingKind.PRECIPITATION.value:
                    # 降雨：随机值 + 一些较大值
                    values = self.rng.exponential(2.0, size=int((end_time - start_time) / freq))
                    index = pd.date_range(start=start_time, periods=len(values), freq=freq)
                    result[kind] = pd.Series(values, index=index, name=kind)
                    
                elif kind == ForcingKind.OBSERVED_FLOW.value if hasattr(ForcingKind, 'OBSERVED_FLOW') else None:
                    # 忽略
                    pass
                    
                elif kind == ForcingKind.POTENTIAL_EVAPOTRANSPIRATION.value:
                    values = self.rng.normal(3.0, 0.5, size=int((end_time - start_time) / freq))
                    index = pd.date_range(start=start_time, periods=len(values), freq=freq)
                    result[kind] = pd.Series(values, index=index, name=kind)
            
            return result
        
        def fetch_catchment_data(
            self,
            catchment_id: str,
            start_time: datetime,
            end_time: datetime,
            kinds: List[str],
        ) -> Dict[str, pd.Series]:
            """模拟拉取子流域数据"""
            result = {}
            freq = self.tc.time_delta
            
            for kind in kinds:
                if kind == ForcingKind.PRECIPITATION.value:
                    # 面雨量：随机值
                    values = self.rng.exponential(3.0, size=int((end_time - start_time) / freq))
                    index = pd.date_range(start=start_time, periods=len(values), freq=freq)
                    result[kind] = pd.Series(values, index=index, name=kind)
                    
                elif kind == ForcingKind.POTENTIAL_EVAPOTRANSPIRATION.value:
                    values = self.rng.normal(2.5, 0.3, size=int((end_time - start_time) / freq))
                    index = pd.date_range(start=start_time, periods=len(values), freq=freq)
                    result[kind] = pd.Series(values, index=index, name=kind)
            
            return result
    
    # ------------------------------------------------------------------
    # Step 1: 实例化 Session 并设定时间轴
    # ------------------------------------------------------------------
    
    print("\n[Step 1] 设定时间轴...")
    
    # 计算时间：过去 72 小时预热 + 未来 48 小时预报
    base_time = datetime(2024, 6, 15, 8, 0, 0)
    warmup_steps = 72   # 72 小时预热
    forecast_steps = 48  # 48 小时预报
    
    # 由于 ForecastSession 需要从 JSON 加载配置，
    # 这里我们创建一个临时的内存配置来模拟
    import json
    from io import StringIO
    
    # 创建模拟配置（简化版）
    mock_config = {
        "schemes": [
            {
                "name": "日方案",
                "time_type": "Hour",
                "step_size": 1,
                "catchments": [
                    {
                        "id": "4101",
                        "name": "流域A",
                        "runoff_model": {"type": "xinanjiang"},
                        "routing_model": {"type": "muskingum"},
                        "forcing_bindings": [],
                    },
                    {
                        "id": "4102", 
                        "name": "流域B",
                        "runoff_model": {"type": "xinanjiang"},
                        "routing_model": {"type": "muskingum"},
                        "forcing_bindings": [],
                    },
                ],
                "nodes": [
                    {"id": "1041", "name": "节点1", "type": "reservoir"},
                    {"id": "1042", "name": "节点2", "type": "cross_section"},
                ],
                "reaches": [
                    {"id": "R1", "upstream": "4101", "downstream": "1041"},
                    {"id": "R2", "upstream": "4102", "downstream": "1042"},
                ],
                "time_axis": {
                    "warmup_period_steps": warmup_steps,
                    "correction_period_steps": 24,
                    "historical_display_period_steps": forecast_steps,
                    "forecast_period_steps": forecast_steps,
                },
            }
        ]
    }
    
    # 保存临时配置文件
    temp_config_path = "/tmp/mock_forecast_config.json"
    with open(temp_config_path, "w") as f:
        json.dump(mock_config, f)
    
    # 实例化（注意：实际需要真实配置文件，这里演示接口设计）
    # session = ForecastSession(temp_config_path)
    # session.setup_time_axis(base_time, warmup_steps, 24, forecast_steps, "Hour", 1)
    
    print(f"  - 预热起始: {base_time}")
    print(f"  - 预热步数: {warmup_steps}")
    print(f"  - 预报步数: {forecast_steps}")
    print("  ✓ 时间轴设定完成")
    
    # ------------------------------------------------------------------
    # Step 2: 加载数据（使用 Mock）
    # ------------------------------------------------------------------
    
    print("\n[Step 2] 加载数据...")
    
    # 构建时间上下文（模拟）
    tc = ForecastTimeContext.from_period_counts(
        warmup_start_time=base_time,
        time_type=TimeType.HOUR,
        step_size=1,
        warmup_period_steps=warmup_steps,
        correction_period_steps=24,
        historical_display_period_steps=forecast_steps,
        forecast_period_steps=forecast_steps,
    )
    
    mock_reader = MockDBReader(tc)
    
    # 模拟数据池
    station_pool = {}
    catchment_pool = {}
    
    # 拉取子流域数据
    for catchment_id in ["4101", "4102"]:
        catchment_pool[catchment_id] = mock_reader.fetch_catchment_data(
            catchment_id, tc.warmup_start_time, tc.end_time,
            [ForcingKind.PRECIPITATION.value, ForcingKind.POTENTIAL_EVAPOTRANSPIRATION.value]
        )
    
    print(f"  - 已加载 {len(catchment_pool)} 个子流域数据")
    print("  ✓ 数据加载完成")
    
    # ------------------------------------------------------------------
    # Step 3: 查看并修改面雨量（人工干预）
    # ------------------------------------------------------------------
    
    print("\n[Step 3] 查看并修改面雨量...")
    
    # 查看 4101 流域的面雨量
    prec_4101 = catchment_pool["4101"][ForcingKind.PRECIPITATION.value]
    print(f"  - 4101 原始面雨量统计:")
    print(f"      均值: {prec_4101.mean():.2f} mm")
    print(f"      最大: {prec_4101.max():.2f} mm")
    
    # 人工干预：未来几个时刻雨量 +5.0 mm
    # 找到预报起始时间的位置
    forecast_start_idx = prec_4101.index.get_loc(tc.forecast_start_time)
    
    # 将预报期的雨量 +5.0 mm
    modified_prec = prec_4101.copy()
    future_slice = modified_prec.iloc[forecast_start_idx:]
    modified_prec.iloc[forecast_start_idx:] = future_slice + 5.0
    
    # 写回数据池
    catchment_pool["4101"][ForcingKind.PRECIPITATION.value] = modified_prec
    
    print(f"  - 已将预报期雨量 +5.0 mm")
    print(f"  - 修改后均值: {modified_prec.mean():.2f} mm")
    print("  ✓ 人工干预完成")
    
    # ------------------------------------------------------------------
    # Step 4: 执行预报计算（模拟）
    # ------------------------------------------------------------------
    
    print("\n[Step 4] 执行预报计算...")
    
    # 由于底层引擎需要完整的拓扑配置，这里演示计算流程
    # 实际使用时会在 run_calculation() 中调用 CalculationEngine
    
    print("  - 构建 ForcingData...")
    print("  - 转换 pandas.Series -> TimeSeries...")
    print("  - 调用 CalculationEngine.run()...")
    print("  - 计算完成!")
    print("  ✓ 预报计算完成（模拟）")
    
    # ------------------------------------------------------------------
    # Step 5: 提取水文过程线
    # ------------------------------------------------------------------
    
    print("\n[Step 5] 提取水文过程线...")
    
    # 模拟计算结果
    # 实际使用 session.get_node_hydrograph("1041")
    
    # 构建模拟的 DataFrame
    display_index = pd.date_range(
        start=tc.display_start_time,
        end=tc.end_time,
        freq=tc.time_delta,
        inclusive="left",
    )
    
    # 模拟水文过程线数据
    n = len(display_index)
    mock_hydrograph = pd.DataFrame({
        "inflow": np.random.exponential(100, n),
        "outflow": np.random.exponential(95, n),
        "observed": np.random.exponential(90, n),
    }, index=display_index)
    
    print("\n  1041 节点水文过程线 (DataFrame.head()):")
    print("-" * 50)
    print(mock_hydrograph.head(10).to_string())
    print("-" * 50)
    
    print("\n  DataFrame 信息:")
    print(f"  - 时间范围: {mock_hydrograph.index[0]} ~ {mock_hydrograph.index[-1]}")
    print(f"  - 总步数: {len(mock_hydrograph)}")
    print(f"  - 列: {list(mock_hydrograph.columns)}")
    
    print("\n" + "=" * 60)
    print("✓ ForecastSession 模拟测试完成！")
    print("=" * 60)
    
    # 清理临时文件
    import os
    os.remove(temp_config_path)
