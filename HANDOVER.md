# 洪水预报引擎（hydro_project）开发交接手册

| 项目 | 内容 |
|------|------|
| **工程路径** | `d:\floodForecastSystem\hydro_project` |
| **交接文档编写日期** | **2026-03-25**（星期三）；**2026-04-03** 补充实时预报 T0 与 Web aux 对齐（见 §10.6、§11） |
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
| **`io/json_config.py`** | 方案解析、`run_calculation_from_json`、`apply_realtime_forecast_observed_meteorology_cutoff`（实时预报 T0 起气象截断）；详见 §4、§10.6。 |

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
| `apply_realtime_forecast_observed_meteorology_cutoff` | `(station_packages, *, time_context)`：**原地**将各站 **P / PET / 气温** 从 **预报起点 T0（索引含 T0）** 至末尾置 **0**；业务含义为实时预报下 T0 起不使用“实测”气象。 |
| `run_calculation_from_json` | `(path, station_packages, time_type, step_size, warmup_start_time=..., observed_flows=..., forecast_mode=..., catchment_workers=...)`；`station_packages` 为 `Dict[station_id, ForcingData]`。`forecast_mode="realtime_forecast"` 时**在入池计算前**调用上述截断；`historical_simulation` 不截断。 |

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

## 10. 本次修改交接（数据库读取效率 + 结构拆分）

本轮主要目标是提升“多测站数据库读取”的效率，避免真实库（几百万 `SENID`）场景下的全量扫描；同时做职责拆分，让 `hydro_engine` 不再长期依赖 `scripts/` 层的公共逻辑。

### 10.1 测站读取效率：`SENID IN` + 分块

在数据库读取层（`hydro_engine/read_data/database_reader.py`）新增对 `params["senids"]` 的支持：
- YAML SQL 中使用 `SENID IN :senids`
- SQLAlchemy 使用 `expanding=True` 将列表参数展开
- 当 `senids` 很长时，按 `senid_chunk_size` 自动分块查询并汇总结果

达梦/各方言 YAML 新增 `hourdb_hourly_range_in`（用于 `IN` 查询）：
- `hydro_engine/read_data/sql/dameng.yaml`
- `hydro_engine/read_data/sql/mysql.yaml`
- `hydro_engine/read_data/sql/oracle.yaml`
- `hydro_engine/read_data/sql/postgresql.yaml`
- `hydro_engine/read_data/sql/sqlserver.yaml`

### 10.2 应用如何传入“本次需要的 SENID”

桌面端/Web 入口在加载 `scheme` 后，会先统计本次计算真正用到的测站集合，再传给数据库读取：
- 雨量/PET：从 `binding_specs` 扫描 `PRECIPITATION` 以及 `POTENTIAL_EVAPOTRANSPIRATION`（仅当 `use_station_pet=true`）
- 实测流量：从 `scheme.nodes` 聚合 `observed_station_id` 与 `observed_inflow_station_id`

相关实现入口在新增模块：
- `hydro_engine/io/calculation_app_data_loader.py`

数据库读取函数新增参数：
- `load_rain_flow_for_calculation(..., rain_senids=..., flow_senids=..., senid_chunk_size=...)`
- `load_station_hourly_frame(..., senids=..., senid_chunk_size=...)`

### 10.3 结构拆分（方案B：阶段1+2）

新增引擎层模块替代 `scripts/calculation_app_common.py` 中的“读数/拼装”职责：
- `hydro_engine/io/calculation_app_data_loader.py`：读数/连库/测站集合统计/`IN` 策略
- `hydro_engine/io/calculation_app_data_builder.py`：df -> `ForcingData` / `TimeSeries` 的拼装

入口依赖关系已更新：
- `hydro_engine/calibration/calibrator.py`：不再依赖 `scripts/calculation_app_common.py` 的读数/拼装逻辑
- `scripts/calculation_pipeline_runner.py`：改为依赖 `hydro_engine/io/*`（作为通用计算入口）
- `scripts/web_calculation_app.py`：改为依赖 `hydro_engine/io/*`

### 10.4 注意事项（给后续智能体）

1. `scripts/calculation_app_common.py` 已瘦身为“薄封装/重导出”，不再包含读数与拼装的重复实现；如果后续需要继续清理，可以逐步减少对其的历史性依赖（测试/入口已尽量迁移到 `hydro_engine/io/*`）。
2. 如果遇到 YAML key not found（如 `*_in` 没配）：
   - 确认对应方言 YAML 是否新增了 `hourdb_hourly_range_in`
   - 可暂时不传 `senids` 触发回退（用于排障，但不建议用于生产）
3. 若 `SENID/TIME` 列名大小写不一致：
   - 依赖 `normalize_station_dataframe` 的 `.upper()` 机制进行归一化；如仍失败，需要调整 SQL 返回列别名。

### 10.5 拓扑冗余清理：`catchments[].downstream_node_id` 变为可选

为避免配置员维护遗漏导致“拓扑矛盾”（双向冗余），现在推荐配置层面只保留单向关联：

- `nodes[].local_catchment_ids`：子流域归属到哪个节点（单向真相）
- `reaches[].upstream_node_id/downstream_node_id`：水系拓扑（河道演进用）

配置加载时的规则：

- 若 `catchments[].downstream_node_id` 缺失，则自动回退为其所属的 owner 节点（即对应 `nodes[].local_catchment_ids` 里的 node id）。
- 若 `catchments[].downstream_node_id` 存在，则要求与 owner 节点一致（不一致会在加载阶段直接报错）。

另外，前端应允许“序列缺失 => 显示空值（None）”，避免把空序列误判为 `0.0`。

---

### 10.6 实时预报：T0 气象截断与 Web `aux` 与引擎对齐

**背景**：预报起点 **T0** 定义为**第一个预报时间步**。实时业务上，T0 及之后的**雨量/蒸发/气温“实测”**通常尚未到达，引擎在 `run_calculation_from_json` 中已对 `forecast_mode=realtime_forecast` 将 `station_packages` 内对应序列从 **T0（含）** 起置 **0** 后再 `DataPool` 合成与计算。

**曾有问题**：`scripts/calculation_pipeline_runner.py` 在组装 **`aux`**（`station_precip`、`catchment_rain` 等）时早于或未同步该截断：测站图仍用库表值，**面雨量**仍从 `rain_df` 加权，导致 **T0 仍显示“实测”雨量**，与引擎实际强迫不一致。

**当前约定（代码已实现）**：

1. **`run_calculation_pipeline`**：在 `forecast_mode=realtime_forecast` 时，于生成 aux **之前**调用 `apply_realtime_forecast_observed_meteorology_cutoff(station_packages, time_context)`；**`catchment_rain`** 改为 `build_catchment_precip_series_from_station_packages`（与 `station_packages` 同权、同源），从而 **“仅读取数据”**（`compute_forecast=False`）时图表也与引擎输入一致。
2. **`runtime_cache`**：增加 **`binding_specs`**，供分步 UI（先读数、再点预报）使用。
3. **`run_forecast_from_runtime_cache`**：在 `run_calculation_from_json` **之后**，对实时模式用**已修补**的 `station_packages` 重建 `station_precip` / `station_pet` / `station_temp` / `catchment_rain`，并回写 `runtime_cache["aux_base"]`，避免 aux 仍为读数阶段的未截断副本。

**实现落点**：`hydro_engine/io/json_config.py`（截断函数 + `run_calculation_from_json` 内调用）、`hydro_engine/io/calculation_app_data_builder.py`（`build_catchment_precip_series_from_station_packages`）、`scripts/calculation_pipeline_runner.py`。

**率定等路径**：若仍直接使用 `build_catchment_precip_series(rain_df, ...)` 且不经上述截断，则保留“原始库表语义”，与 Web 实时预报入口的差异为**预期**。

---

## 11. 版本与变更记录（摘要）

| 日期 | 摘要 |
|------|------|
| **2026-03-25** | 初版交接：Node-Link、多维强迫、JSON、水库/分流/新安江等。 |
| **2026-03-25**（更新） | 补充：`ForecastTimeContext` 与四段步数、`schemes` 自包含、`warmup_start_time` 运行时传入；`XinanjiangRunoffModel`/`XinanjiangCSRunoffModel` 与 Java 对齐；`calibration_bounds`；`CalculationEngine` 与校正链；测试与 API 表更新。 |
| **2026-03-25**（更新 2） | `DEVELOPMENT_MANUAL.md` 全面对齐：`schemes`/`catchment_forcing_bindings`、`ForcingData` 示例、产流模型与测试列表、§11 差异摘要；与 `HANDOVER` 交叉引用。 |
| **2026-03-30**（更新 3） | 解决拓扑冗余导致的 `downstream_node_id` 推导问题：缺失时回退为 owner 节点；并修复桌面端“节点全 0 误判”的展示逻辑。 |
| **2026-04-03**（更新 4） | **实时预报 T0 气象**：抽出 `apply_realtime_forecast_observed_meteorology_cutoff`；Web pipeline 在 aux 前截断；`catchment_rain` 与测站序列改由 `station_packages` 派生；`runtime_cache` 增加 `binding_specs`；`run_forecast_from_runtime_cache` 计算后重写雨量相关 aux。详见 `DEVELOPMENT_MANUAL.md` §3.6、§3.8、§5 与本文 §10.6。 |

---

*文档随仓库维护；重大架构变更时请同步更新本节、`DEVELOPMENT_MANUAL.md` 与 `docs/FORCING_DATA_ARCHITECTURE.md`。*
