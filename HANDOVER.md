# 洪水预报引擎（hydro_project）开发交接手册

| 项目 | 内容 |
|------|------|
| **工程路径** | `d:\floodForecastSystem\hydro_project` |
| **交接文档编写日期** | **2026-03-25**（星期三） |
| **说明** | 概括架构、时间轴、JSON、产流/汇流模型与率定边界，供上下文重置或新人快速对齐。 |

---

## 1. 项目定位

基于 **Node-Link 拓扑 + 有向无环图（DAG）** 的洪水预报计算引擎：

- **策略模式**：`IHydrologicalModel` 统一产流/河道演进；输入为 `ForcingData`，输出 `TimeSeries`。
- **多态节点**：`AbstractNode` 模板方法 `process_water(total_inflow, observed_series, time_context)`，子类实现 `_compute_simulated_outflows`；支持可选 **误差校正**（`IErrorUpdater`）与 **实测替换**（`TimeSeries.blend`）。
- 节点可选注入：水库节点支持 `observed_inflow_station_id` + `use_observed_inflow_for_simulation` 将“入库流量预报”注入调度输入，从而生成未来出库序列。
- 历史跳过（性能/语义）：当节点 `use_observed_for_routing=true` 且实测有效、并且没有配置 `correction_config.updater_model` 时，历史段（`t <= forecast_start_time`）输出直接取实测，避免对历史段执行物理/调度模拟计算。
- 误差校正开关：新增 `enable_observed_correction`，当 `false` 时即使节点配置了 `correction_config` 也不执行 `updater_model.correct`。
- **拓扑调度**：`networkx` 建图，`ForecastingScheme.topological_order()` 驱动 `CalculationEngine`。
- **原生时间尺度**：`TimeType`（分/时/日）+ `step_size` 决定 `ForecastTimeContext.time_delta`，引擎内**不做跨尺度换算**；外部序列步长须与之一致。
- **多维强迫**：`ForcingData` + `ForcingKind`；产流/河道通过 `required_inputs()` 声明契约。

---

## 2. 核心包 `hydro_engine`（要点）

| 模块 | 内容 |
|------|------|
| **`core/context.py`** | `TimeType`、`ForecastTimeContext`（四阶段：`from_period_counts` 用预热/校正/历史显示/预报**步数**无歧义构造）、`parse_time_type`。 |
| **`core/timeseries.py`** | `TimeSeries`；`blend`、`slice`、`get_index_by_time` 等。 |
| **`core/interfaces.py`** | `IHydrologicalModel`、`IErrorUpdater`（校正仅用校正段，见实现注释）。 |
| **`core/forcing.py`** | `ForcingKind`、`ForcingData`、`parse_forcing_kind`、`validate_forcing_contract`。 |
| **`core/data_pool.py`** | `DataPool`：站点/方案数据缓存与子流域 `ForcingData` 合成底座。 |
| **`processing/`** | `SpatialAggregator`、`CatchmentDataSynthesizer`：站点多变量空间汇聚到子流域强迫。 |
| **`domain/`** | `SubCatchment`、`RiverReach`；`nodes/`：`AbstractNode`（节点顶级 `observed_station_id` / `use_observed_for_routing`，误差校正侧为 `NodeCorrectionConfig(updater_model)`）、`CrossSectionNode`、`ReservoirNode`、`DiversionNode`。 |
| **`engine/scheme.py`** | `ForecastingScheme`：注册节点/河道/子流域，DAG 与拓扑序。 |
| **`engine/calculator.py`** | `CalculationEngine.run(scheme, catchment_forcing, time_context, observed_flows)`；运行前校验强迫与实测的 **time_step == time_context.time_delta**；当节点配置 `observed_station_id` 时填充 `CalculationResult.node_observed_flows`（展示/比对），并且 `get_display_results()` 按 `display_start_time` 截取。 |
| **`models/runoff/`** | `DummyRunoffModel`、`XinanjiangRunoffModel`（对齐 `HFXAJAlg.java` + 单位线）、`XinanjiangCSRunoffModel`（对齐 `HFXAJCSAlg.java` 滞时河网汇流）、`TankRunoffModel`、`SnowmeltRunoffModel`；**`calibration_bounds.py`** 提供新安江/XAJCS 与 Java `m_douParaDBArr/UBArr/IsCaliArr` 一致的率定上下界及 `clip_*`、向量打包辅助函数。 |
| **`models/correction/`** | 如 `AR1ErrorUpdater`。 |
| **`models/routing/`** | `DummyRoutingModel`、`MuskingumRoutingModel`（对齐 Java `HFMSKAlg`：NE=2；Dt/x1/x2约束违规时 warning 后继续）。 |
| **`io/json_config.py`** | 见下一节。 |

---

## 3. JSON 配置约定（当前）

- **根级**通常仅 **`metadata`** + **`schemes`**（多时间尺度方案列表）。
- **每个 `scheme` 自包含**：`time_type`、`step_size`、`time_axis`、`nodes`、`reaches`、`catchments`、`stations`、`catchment_forcing_bindings`。不同尺度可有不同拓扑与参数。
- **`stations`**：可为扁平列表（旧版）或**按类型分组的对象**（雨量站、蒸发站、气温站、流量/水位站、水库入/出库站等）；加载时 `flatten_stations_catalog` 校验 **id 不重复**。详见 `DEVELOPMENT_MANUAL.md` §7.5。
- **`time_axis`**（推荐）：四段步数 — `warmup_period_steps`、`correction_period_steps`、`historical_display_period_steps`、`forecast_period_steps`；**不含日历起点**。
- **日历起点**：调用 `load_scheme_from_json` / `run_calculation_from_json` / `build_catchment_forcing_from_station_packages` 时传入 **`warmup_start_time: datetime`**（预热段第一个时刻），与强迫序列 `start_time` 对齐。
- **旧版兼容**：无 `schemes` 时仍可按根级 `time_axis`（如 `start_time`+`length`）解析；需与传入的 `time_type`/`step_size` 一致。
- **强迫绑定**：推荐 **`catchment_forcing_bindings`**（多 `forcing_kind` + `station_id`）；旧键 `catchment_station_bindings` 仍支持。

---

## 4. 重要 API（交接必知）

| 接口 | 当前约定 |
|------|----------|
| `IHydrologicalModel.run` | `run(forcing: ForcingData) -> TimeSeries` |
| `CalculationEngine.run` | `run(scheme, catchment_forcing: Dict[id, ForcingData], time_context: ForecastTimeContext, observed_flows: Dict[id, TimeSeries] \| None)` |
| `load_scheme_from_json` | `(path, time_type: str, step_size: int, warmup_start_time: datetime \| None) -> (scheme, binding_specs, ForecastTimeContext)`；**有 `schemes` 时须传 `warmup_start_time`** |
| `build_catchment_forcing_from_station_packages` | 同上，增加 `warmup_start_time`，返回 `(scheme, catchment_forcing, time_context)` |
| `run_calculation_from_json` | `(path, station_packages, time_type, step_size, warmup_start_time=..., observed_flows=...)`；`station_packages` 为 `Dict[station_id, ForcingData]` |

新安江 / XAJCS：**必须**提供 `precipitation` 与 `potential_evapotranspiration`。

---

## 5. 依赖与测试

- **Python**：`networkx`。
- 在 **`hydro_project`** 目录下：

```bash
python -m unittest discover -v
```

主要用例：`test_y_shape_basin.py`、`test_hydrological_models.py`、`test_json_config_pipeline.py`、`test_runoff_framework.py`、`test_calibration_bounds.py` 等。

---

## 6. 数据库（达梦示例）与配置要点

关系库读取使用 `SQLAlchemy + YAML 外置 SQL + Pandas`。以达梦（DM）为例：

1. 选择用于运行的 Python 环境（建议 Python 3.9+ / 3.10+）。
2. 安装依赖：
   - `dmpython`（数据库驱动）
   - `dmSQLAlchemy`（SQLAlchemy 方言适配）
   - `sqlalchemy`、`pandas`、`PyYAML`
3. 配置数据库连接：`configs/floodForecastJdbc.json`
   - 该文件现在只维护 `services[]` 中的连接信息（SQLAlchemy URL：`dm+dmPython://user:pass@host:port/schema`）；
   - 若存在多个 service，可通过 `hourly_service` 指定读取 HOURDB（小时表）使用哪个 service。
4. 配置 SQL 查询：`hydro_engine/read_data/sql/dameng.yaml`
   - `hourdb_hourly_range` / `hourdb_hourly_station` 等查询逻辑在此维护；
   - 表名/模式/字段选择都写在 YAML 中，连接文件无需改动。

常见验证方式：

- 桌面端/Web 端传入 `jdbc_config_path=configs/floodForecastJdbc.json` 后读取 HOURDB；
- 或直接调用 `load_station_hourly_frame(ref, time_start=..., time_end=...)`。

---

## 7. 配置与文档路径

| 路径 | 内容 |
|------|------|
| **`configs/example_forecast_config.json`** | 示例：`schemes`、四段 `time_axis`、节点/河道/子流域/绑定/测站。 |
| **`DEVELOPMENT_MANUAL.md`** | 开发说明（目录、核心流程、JSON §7、`run_calculation_from_json` 示例）；与本手册一致，细节以本节 API/架构表与 §7 为准。 |
| **`docs/FORCING_DATA_ARCHITECTURE.md`** | 强迫数据架构。 |

---

## 7. 仓库根目录其它说明

- 工作区根目录可有历史单文件脚本；**以 `hydro_project` 包为准**。
- **`HFXAJAlg.java` / `HFXAJCSAlg.java`**：新安江与滞时河网汇流 Python 实现的对照参考，非运行时依赖。

---

## 8. 后续开发建议（未全部完成）

1. **水库**：曲线已加载；调度若需用水位–库容反算出库，仅在 `ReservoirNode` 内扩展。  
2. **插件化模型注册**：减少 `json_config._build_model` 集中分支，可用注册表或 entry points。  
3. **工程化**：`pyproject.toml`、CI、ruff/mypy 等按团队规范补全。  
4. **多站加权面雨量**：在 `io` 层扩展绑定（权重矩阵），输出仍为子流域 `ForcingData`。  
5. **XAJCS 同化**：Java `isAssimilated` / `wTimeSeries` 尚未在 Python 中实现。

---

## 9. 关键源码入口

1. `hydro_engine/core/context.py` — 时间上下文与四段步数。  
2. `hydro_engine/engine/calculator.py` — 调度、原生步长校验、`CalculationResult`。  
3. `hydro_engine/domain/nodes/base.py` — `process_water` 模板与校正链。  
4. `hydro_engine/io/json_config.py` — 配置解析与 `warmup_start_time`。  
5. `hydro_engine/models/runoff/xinanjiang.py` / `xinanjiang_cs.py` — 新安江与 XAJCS。  
6. `hydro_engine/models/runoff/calibration_bounds.py` — 率定上下界（对齐 Java）。  

---

## 10. 版本与变更记录（摘要）

| 日期 | 摘要 |
|------|------|
| **2026-03-25** | 初版交接：Node-Link、多维强迫、JSON、水库/分流/新安江等。 |
| **2026-03-25**（更新） | 补充：`ForecastTimeContext` 与四段步数、`schemes` 自包含、`warmup_start_time` 运行时传入；`XinanjiangRunoffModel`/`XinanjiangCSRunoffModel` 与 Java 对齐；`calibration_bounds`；`CalculationEngine` 与校正链；测试与 API 表更新。 |
| **2026-03-25**（更新 2） | `DEVELOPMENT_MANUAL.md` 全面对齐：`schemes`/`catchment_forcing_bindings`、`ForcingData` 示例、产流模型与测试列表、§11 差异摘要；与 `HANDOVER` 交叉引用。 |

---

*文档随仓库维护；重大架构变更时请同步更新本节、`DEVELOPMENT_MANUAL.md` 与 `docs/FORCING_DATA_ARCHITECTURE.md`。*
