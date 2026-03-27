# 洪水预报引擎开发说明

本文档描述 `hydro_project` 的**代码目录结构**、**核心调用关系**、**JSON 配置文件约定**及扩展要点，便于团队协作与二次开发。

**多维强迫数据（降雨、气温、PET、雪深等）**的容器、模型契约、配置绑定与引擎校验，见专文：[`docs/FORCING_DATA_ARCHITECTURE.md`](docs/FORCING_DATA_ARCHITECTURE.md)。

---

## 1. 项目定位与架构原则

- **Node-Link + DAG**：流域抽象为有向无环图；边为河道 `RiverReach`，点为各类 `AbstractNode` 子类。
- **策略模式**：产流、河道演进均实现 `IHydrologicalModel`，由 `SubCatchment` / `RiverReach` 持有。
- **多态（OCP）**：`CalculationEngine` 只调用 `node.process_water(...)`，不按节点类型写分支。
- **图与拓扑**：`ForecastingScheme` 用 `networkx` 建图并做拓扑排序。
- **强类型**：核心对象使用 `dataclass` 与类型注解。
- **配置与数据分离**：JSON 描述拓扑、模型参数/状态、测站绑定及**相对时间轴**（四段步数等）；**日历起点与降雨时序由运行时传入**（`warmup_start_time`、测站 `ForcingData`），不在配置中写死起算日期。

---

## 2. 代码目录结构（当前）

```
hydro_project/
├── configs/
│   └── example_forecast_config.json   # 示例：拓扑 + 模型 + 测站绑定（无降雨序列）
├── hydro_engine/
│   ├── __init__.py
│   ├── core/                          # 基础类型与接口
│   │   ├── __init__.py
│   │   ├── context.py                 # ForecastTimeContext、TimeType、四段步数
│   │   ├── timeseries.py              # TimeSeries、序列相加、聚合、blend
│   │   ├── interfaces.py              # IHydrologicalModel、IErrorUpdater
│   │   ├── forcing.py                 # ForcingKind、ForcingData
│   │   └── data_pool.py              # DataPool（observed/forecast cache + 子流域 Forcing 合成底座）
│   ├── processing/                  # 空间汇聚、子流域强迫合成流水线
│   │   ├── aggregator.py
│   │   └── pipeline.py
│   ├── domain/                        # 领域实体
│   │   ├── __init__.py
│   │   ├── catchment.py               # SubCatchment（挂产流模型）
│   │   ├── reach.py                   # RiverReach（挂河道模型）
│   │   └── nodes/
│   │       ├── __init__.py
│   │       ├── base.py                # AbstractNode（process_water 模板、NodeCorrectionConfig）
│   │       ├── cross_section.py       # CrossSectionNode
│   │       ├── reservoir.py           # ReservoirNode + 特征水位/约束/曲线定义
│   │       └── diversion.py           # DiversionNode（主槽容量分洪）
│   ├── engine/
│   │   ├── __init__.py
│   │   ├── scheme.py                  # ForecastingScheme（注册 + DAG + 拓扑序）
│   │   └── calculator.py              # CalculationEngine（time_context、observed_flows）、CalculationResult
│   ├── models/                        # 数学模型（按用途分子包）
│   │   ├── __init__.py                # 聚合导出
│   │   ├── runoff/                    # 子流域产流模型
│   │   │   ├── __init__.py
│   │   │   ├── dummy.py
│   │   │   ├── xinanjiang.py          # 新安江（HFXAJAlg）+ 单位线
│   │   │   ├── xinanjiang_cs.py      # 新安江滞时河网汇流（HFXAJCSAlg）
│   │   │   ├── calibration_bounds.py  # 新安江/XAJCS 率定上下界（对齐 Java）
│   │   │   └── tank.py                # TankParams + TankState + 模型
│   │   ├── correction/               # 如 AR1ErrorUpdater
│   │   └── routing/                   # 河道演进模型
│   │       ├── __init__.py
│   │       ├── dummy.py
│   │       └── muskingum.py          # 对齐 Java HFMSKAlg（NE=2）；Dt/x1/x2约束仅 warning 后继续
│   └── io/
│       ├── __init__.py
│       └── json_config.py             # 从 JSON 加载方案、映射测站降雨、一键计算
├── tests/
│   ├── __init__.py
│   ├── test_y_shape_basin.py          # Y 型 + 分洪集成测试（代码组网）
│   ├── test_hydrological_models.py    # 新安江 / Tank / Muskingum 等
│   ├── test_json_config_pipeline.py   # JSON 加载 + 强迫 + 计算
│   ├── test_runoff_framework.py       # 多产流模型与 SubCatchment 契约
│   └── test_calibration_bounds.py     # 新安江率定边界与字段一致性
└── DEVELOPMENT_MANUAL.md              # 本文件
```

**依赖**：需安装 `networkx`（拓扑与 DAG 校验）。

引擎层新增数据读取模块：`hydro_engine/read_data/`（与 `processing` 同级），
按 `file / database / api` 三类方式分别由独立文件管理。当前已实现文件读取，
数据库与外部接口为预留占位实现（`NotImplementedError`）。

---

## 2.1 文件级详细说明（按目录）

本节给出“主要代码文件 -> 主要内容 -> 关键入口/类”，用于快速定位代码。

### `hydro_engine/core/`

- `hydro_engine/core/context.py`
  - 主要内容：时间轴定义与校验（`ForecastTimeContext`、`TimeType`），支持四段步数转绝对时刻。
  - 关键点：`validate()`、`from_period_counts(...)`、`time_delta` 推导。
- `hydro_engine/core/timeseries.py`
  - 主要内容：统一时间序列对象 `TimeSeries`，以及加法、切片、拼接/缝合（`blend`）等基础运算。
  - 关键点：对齐检查（起点/步长/长度一致）是引擎稳定性的核心约束。
- `hydro_engine/core/forcing.py`
  - 主要内容：强迫数据枚举与容器（`ForcingKind`、`ForcingData`），模型输入契约校验。
  - 关键点：`validate_forcing_contract(...)` 在运行前做 fail-fast。
- `hydro_engine/core/interfaces.py`
  - 主要内容：模型与校正器抽象接口（`IHydrologicalModel`、`IErrorUpdater`）。
  - 关键点：所有产流/汇流模型均通过统一接口接入引擎。
- `hydro_engine/core/data_pool.py`
  - 主要内容：观测/预报数据池，负责在时间维上融合 observed 与 forecast，并向处理流水线供数。
  - 关键点：`_blend_observed_and_forecast(...)` 使用 `TimeSeries.blend(...)` 统一拼接语义。

### `hydro_engine/processing/`

- `hydro_engine/processing/aggregator.py`
  - 主要内容：多站聚合算法（加权平均、均值、求和等）与插补/预处理工具。
- `hydro_engine/processing/pipeline.py`
  - 主要内容：`CatchmentDataSynthesizer`，按 `catchment_forcing_bindings` 把站点数据合成为子流域强迫。
  - 关键点：PET 可按 `monthly_values` 回退；结果直接供 `CalculationEngine` 使用。

### `hydro_engine/read_data/`（新）

- `hydro_engine/read_data/types.py`
  - 主要内容：读取规范定义（`DataReadSpec`）与读取器协议（`IDataReader`）。
- `hydro_engine/read_data/file_reader.py`
  - 主要内容：文件读取实现（`FileDataReader`）与标准化（`normalize_station_dataframe`）。
  - 关键点：统一约束输入必须有 `SENID`、`TIME`，并生成 `TIME_DT`。
- `hydro_engine/read_data/database_reader.py`
  - 主要内容：数据库读取占位实现（后续接 DB connector/query）。
- `hydro_engine/read_data/api_reader.py`
  - 主要内容：外部 API 读取占位实现（后续接 HTTP 请求/鉴权/解析）。
- `hydro_engine/read_data/factory.py`
  - 主要内容：读取器工厂（`build_data_reader`）与统一入口（`read_station_data`）。
- `hydro_engine/read_data/__init__.py`
  - 主要内容：对外导出读取层公共 API，供脚本层/服务层调用。

### `hydro_engine/domain/`

- `hydro_engine/domain/catchment.py`
  - 主要内容：子流域实体 `SubCatchment`，封装产流与子流域内汇流（runoff + routing）。
- `hydro_engine/domain/reach.py`
  - 主要内容：河道实体 `RiverReach`，封装河道演进模型调用。
- `hydro_engine/domain/nodes/base.py`
  - 主要内容：节点模板方法 `process_water(...)`，包含实测接力、可选误差校正、入流注入逻辑。
  - 关键点：
    - `use_observed_for_routing`
    - `use_observed_inflow_for_simulation`
    - `use_observed_for_routing_after_forecast`
- `hydro_engine/domain/nodes/cross_section.py`
  - 主要内容：断面节点实现（通常输出等于输入，供演进链路传递）。
- `hydro_engine/domain/nodes/reservoir.py`
  - 主要内容：水库节点，包含约束、特征水位、曲线等结构，执行水库出流计算流程。
- `hydro_engine/domain/nodes/diversion.py`
  - 主要内容：分洪节点，按主槽能力分配主槽/分洪道流量。

### `hydro_engine/models/`

- `hydro_engine/models/runoff/dummy.py`
  - 主要内容：占位产流模型，便于调试链路。
- `hydro_engine/models/runoff/xinanjiang.py`
  - 主要内容：新安江产流模型（HFXAJAlg 对齐），含单位线汇流。
- `hydro_engine/models/runoff/xinanjiang_cs.py`
  - 主要内容：新安江滞时河网汇流模型（HFXAJCSAlg 对齐），支持 `debug_trace`。
- `hydro_engine/models/runoff/tank.py`
  - 主要内容：Tank 产流模型（参数/状态分离）。
- `hydro_engine/models/runoff/calibration_bounds.py`
  - 主要内容：新安江/XAJCS 参数率定上下界。
- `hydro_engine/models/routing/dummy.py`
  - 主要内容：占位河道模型。
- `hydro_engine/models/routing/muskingum.py`
  - 主要内容：Muskingum 演进模型（含 `k_hours=0` 即时演进语义）。
- `hydro_engine/models/correction/ar1_updater.py`
  - 主要内容：AR(1) 误差校正器实现。
- `hydro_engine/models/__init__.py`
  - 主要内容：模型聚合导出（统一导出 runoff/routing 下常用模型）。

### `hydro_engine/engine/`

- `hydro_engine/engine/scheme.py`
  - 主要内容：`ForecastingScheme`，负责实体注册与 DAG 拓扑排序。
- `hydro_engine/engine/calculator.py`
  - 主要内容：核心执行器 `CalculationEngine` 与结果容器 `CalculationResult`。
  - 关键点：
    - 运行前强校验（时间步/长度/契约）
    - 子流域产流并行（`catchment_workers`）
    - 节点/河道按拓扑顺序演进
    - 输出 `node_total_inflows`、`node_outflows`、`reach_flows` 等

### `hydro_engine/io/`

- `hydro_engine/io/json_config.py`
  - 主要内容：JSON 解析与运行入口。
  - 关键函数：
    - `load_scheme_from_json(...)`：加载配置、构建 scheme/bindings/time_context
    - `run_calculation_from_json(...)`：一键计算入口（支持 `forecast_mode`、`catchment_workers`）
    - `_build_node(...)` / `_build_model(...)`：配置映射到实体/模型
  - 关键点：兼容新旧 JSON 结构，统一输出序列化结果。

### `scripts/`（应用层）

- `scripts/calculation_app_common.py`
  - 主要内容：桌面端与网页端共用辅助函数（配置读写、CSV 到 `ForcingData` 转换、图表辅助数据）。
  - 关键点：读取入口已接入 `hydro_engine.read_data.read_station_data(...)`。
- `scripts/desktop_calculation_app.py`
  - 主要内容：Tkinter 桌面客户端，包含参数面板、多标签图表、debug 表格、异步计算线程。
  - 关键点：
    - `forecast_mode`（实时预报/历史模拟）
    - 单线程开关（映射 `catchment_workers=1`）
    - 节点三线同图（实测入库/实测出库/预报入库）
- `scripts/web_calculation_app.py`
  - 主要内容：Streamlit 调试界面，快速验证配置与计算输出。
- `scripts/convert_legacy_config.py`
  - 主要内容：旧配置到新配置结构转换工具。
- `scripts/debug_client.py`
  - 主要内容：命令行级联调脚本，用于快速跑通和打印关键输出。

### `tests/`

- `tests/test_json_config_pipeline.py`
  - 主要内容：配置加载与整链路回归测试。
- `tests/test_hydrological_models.py`
  - 主要内容：产流/河道模型行为测试。
- `tests/test_y_shape_basin.py`
  - 主要内容：Y 型网络拓扑与演进集成测试。
- `tests/test_runoff_framework.py`
  - 主要内容：多模型接入契约测试。
- `tests/test_calibration_bounds.py`
  - 主要内容：率定边界字段一致性测试。

---

## 3. 核心执行流程

1. **组网**：向 `ForecastingScheme` 注册 `nodes`、`reaches`、`catchments`（或通过 `load_scheme_from_json` 并传入 `time_type`、`step_size`、`warmup_start_time`）。
2. **构造 `ForecastTimeContext`**：与 JSON 中 `time_axis`（四段步数）及原生 `time_delta` 一致；**强迫序列** `start_time` 须等于 `warmup_start_time`，`time_step` 须等于 `time_context.time_delta`。
3. **准备子流域强迫**：`Dict[catchment_id, ForcingData]`；若使用 JSON，则通过 `catchment_forcing_bindings` 将测站 `ForcingData` 合并到各子流域。
4. **计算**：`CalculationEngine().run(scheme, catchment_forcing, time_context, observed_flows)`  
   - 校验各强迫与可选实测流量与 `time_delta` 对齐；  
   - 各子流域 `generate_runoff`；  
   - 按拓扑序：汇总入流 → `process_water`（引擎从 `observed_flows` 按节点 `observed_station_id` 提取并按需 blend/接力）→ 河道 `route(ForcingData)`。
   - 当节点配置了 `observed_station_id` 时，`CalculationResult` 会额外填充 `node_observed_flows`（用于前端展示/比对）。

---

## 4. 关键类职责速查

| 模块 | 类 | 职责 |
|------|----|------|
| `core` | `ForecastTimeContext` | 四阶段时间轴、`time_delta`（`TimeType`+`step_size`）、`from_period_counts` |
| `core` | `TimeSeries` | 时间序列；`__add__` 要求起报时刻、步长、长度一致 |
| `core` | `IHydrologicalModel` | `required_inputs()`；`run(ForcingData) -> TimeSeries` |
| `domain` | `SubCatchment` | `runoff_model`；`generate_runoff(ForcingData)` |
| `domain` | `RiverReach` | `routing_model`；`route(ForcingData)`（入流键 `ROUTING_INFLOW`） |
| `domain` | `AbstractNode` | `process_water(total_inflow, observed_series, time_context)`；节点顶级可配置 `observed_station_id` / `use_observed_for_routing`；可选 `NodeCorrectionConfig(updater_model)` |
| `domain` | `CrossSectionNode` / `ReservoirNode` / `DiversionNode` | 见各文件 |
| `engine` | `ForecastingScheme` | 注册实体、`topological_order()` |
| `engine` | `CalculationEngine` | `run(scheme, catchment_forcing, time_context, observed_flows)` |
| `io` | `load_scheme_from_json` | 返回 `(scheme, binding_specs, ForecastTimeContext)` |
| `io` | `run_calculation_from_json` | 传入 `station_packages: Dict[str, ForcingData]`、`time_type`、`step_size`、`warmup_start_time`、`forecast_mode` 等 |

---

## 5. 水库节点（ReservoirNode）扩展说明

除 `inflow_attenuation` 外，当前还支持（均由 JSON `params` 解析）：

- **特征水位** `ReservoirLevelFeatures`：`dead_level`、`normal_level`、`flood_limit_level`、`check_flood_level`。
- **运行约束** `ReservoirOperationConstraints`：`min_release`、`max_release`（对衰减后出流逐时段截断）。
- **关系曲线** `List[ReservoirCurve]`：每条含 `name`、`direction`、`points[{x,y}, ...]`。

`direction` 取值（用于描述曲线物理含义，便于后续插值与调度逻辑扩展）：

- `level_to_storage` / `storage_to_level`
- `tailwater_to_discharge` / `discharge_to_tailwater`

> 说明：曲线数据已加载到节点对象；**当前 `process_water` 尚未用水位–库容等曲线反算出库**，后续可在不改动引擎的前提下，仅在 `ReservoirNode.process_water` 内接入插值与平衡计算。

---

## 6. 产流模型：参数与状态分离

`XinanjiangRunoffModel`、`XinanjiangCSRunoffModel`、`TankRunoffModel` 等采用：

- **`params`**：常参（新安江系列字段名与 Java `HFXAJAlg` / `HFXAJCSAlg` 一致，如 `wum`、`k`、`sm`、`area`；XAJCS 另有 `lag`、`cs`）。
- **`state`**：初值及运行后可回写的状态（如 `wu`/`wl`/`wd`、`qrss0` 等）。

**率定上下界**（与 Java `m_douParaDBArr` / `m_douParaUBArr` / `m_douParaIsCaliArr` 对齐）见 `hydro_engine/models/runoff/calibration_bounds.py`。

JSON 示例：

```json
"runoff_model": {
  "name": "XinanjiangRunoffModel",
  "params": { "wum": 20.0, "k": 0.8, "area": 500.0 },
  "state": { "wu": 5.0, "wl": 10.0, "wd": 20.0 }
}
```

解析逻辑见 `hydro_engine/io/json_config.py` 中 `_build_model`。

---

## 7. JSON 配置文件说明

配置文件**不包含降雨数值与日历时点**，描述拓扑、模型参数、测站与子流域关系及**相对时间轴**（四段步数）；**日历起点**在加载/计算 API 中通过 `warmup_start_time` 传入。

### 7.1 顶层结构（推荐：`schemes`）

| 键 | 说明 |
|----|------|
| `metadata` | 可选：`name`、`description` |
| `schemes` | **推荐**：多时间尺度方案列表；每项含 `time_type`、`step_size`、`time_axis`、`nodes`、`reaches`、`catchments`、`stations`、`catchment_forcing_bindings`（**每方案自包含**） |
| `time_axis`（在 scheme 内） | 四段步数：`warmup_period_steps`、`correction_period_steps`、`historical_display_period_steps`、`forecast_period_steps` |
| `catchment_forcing_bindings` | 每子流域多条 `{ forcing_kind, station_id }` |

旧版无 `schemes` 时仍支持根级扁平结构（见 `io/json_config.py` 兼容分支）。

### 7.2 `nodes[]` 通用字段

| 字段 | 必填 | 说明 |
|------|------|------|
| `id` | 是 | 节点唯一标识 |
| `name` | 否 | 显示名称；省略时读取逻辑可用 `id` 作为默认名 |
| `type` | 是 | `cross_section` \| `reservoir` \| `diversion` |
| `incoming_reach_ids` | 否 | 入流河道 ID 列表 |
| `outgoing_reach_ids` | 否 | 出流河道 ID 列表 |
| `local_catchment_ids` | 否 | 本节点汇入的子流域 ID |
| `observed_station_id` | 否 | 断面关联的实测站 ID（用于展示/比对；可选用于接力缝合） |
| `use_observed_for_routing` | 否 | `true` 时使用实测序列参与 `process_water` 的缝合接力（缺测/全 NaN 自动回退模拟并告警）。若未配置误差校正器（`correction_config.updater_model` 为空），则历史时段（`t <= forecast_start_time`）跳过物理/调度模拟计算，输出直接取实测；仅对 `forecast_start_time + 1step` 之后做模拟覆盖。 |
| `use_observed_for_routing_after_forecast` | 否 | 预报起报后是否继续使用节点实测出流接力。通常由接口参数 `forecast_mode` 自动控制：`historical_simulation` 打开，`realtime_forecast` 关闭。 |
| `observed_inflow_station_id` | 否 | 节点输入注入的站 ID（通常水库的“入库流量/预报入库”） |
| `use_observed_inflow_for_simulation` | 否 | `true` 时将输入 `total_inflow` 的未来段替换为 `observed_inflow_station_id` 序列，驱动调度模型计算未来出库；缺测/全 NaN 自动回退模拟并告警 |
| `params` | 视类型 | 见下 |
| `correction_config` | 否 | 误差校正配置，仅保留算法侧字段，如 `updater_model` |
| `enable_observed_correction` | 否 | 若为 `false` 则即使配置了 `correction_config` 也不执行 `updater_model.correct`（仅做接力缝合/展示）。字段缺省时保持旧行为。 |

> 向后兼容：旧版允许把 `observed_station_id` / `use_observed_for_routing` 写在 `correction` 内；新结构建议改为节点顶级字段。

> `station_binding`（可选，配置便利字段）：用于统一管理各节点关联的站点 ID。
> 目前的解析逻辑会把它映射到节点运行所需的字段：
> - 普通断面/分流节点：`station_binding.flow_station_id` 可用于补齐 `observed_station_id`（`stage_station_id` 当前未被物理/调度逻辑直接消费）。
> - 水库节点：`station_binding.inflow_station_id` -> `observed_inflow_station_id`，`station_binding.outflow_station_id`（或 `flow_station_id`）-> `observed_station_id`。

**`cross_section`**：`params` 通常为空。

**`reservoir`**：`params` 可含：

- `inflow_attenuation`：入流衰减系数（默认 0.8）
- `level_features`：`dead_level`, `normal_level`, `flood_limit_level`, `check_flood_level`
- `operation_constraints`：`min_release`, `max_release`
- `curves`：数组，元素为 `{ "name", "direction", "points": [{"x","y"}, ...] }`

**`diversion`**：`params` 必填：

- `main_channel_id`、`bypass_channel_id`、`main_channel_capacity`

### 7.3 `reaches[]`

| 字段 | 说明 |
|------|------|
| `id` | 河道 ID |
| `upstream_node_id` / `downstream_node_id` | 上下游节点 |
| `routing_model` | `{ "name": "...", "params": { ... } }` |

支持的 `routing_model.name`（与代码注册表一致）：`MuskingumRoutingModel`、`DummyRoutingModel` 等。

### 7.4 `catchments[]`

| 字段 | 说明 |
|------|------|
| `id` | 子流域 ID（与节点 `local_catchment_ids` 对应） |
| `runoff_model` | `name` + `params` + 可选 `state` |

支持的产流模型名（注册表）：`XinanjiangRunoffModel`、`XinanjiangCSRunoffModel`、`TankRunoffModel`、`DummyRunoffModel`、`SnowmeltRunoffModel` 等。

### 7.5 测站与绑定

- `stations`：测站目录（元数据），**两种写法**：
  - **旧版**：扁平数组 `[{ "id", "name", ... }, ...]`。
  - **推荐**：按类型分组的对象，便于与模型要素对应，例如：
    - `rain_gauges`：雨量站 → 通常供给 `precipitation`（新安江等产流模型面雨量加权）。
    - `evapotranspiration_stations`：蒸发/PET 站 → `potential_evapotranspiration`（可与 `monthly_values` 回退配合）。
    - `air_temperature_stations`：气温站（预留）→ `air_temperature` 等。
    - `flow_stations` / `stage_stations`：河道流量、水位 → 对应节点 `station_binding` 的 `flow_station_id`、`stage_station_id`。
    - `reservoir`：按“水库节点”分组的测站目录：`[{ node_id, name, stations:[{id,name,...}, ...] }]`。推荐把水库的入库流量/出库流量/水位等都挂在同一个水库节点下，避免平铺重复分类。
  - 分类下每条可含 `unit`、`notes`、`typical_forcing_kind` 等**说明性字段**（引擎不强制解析，仅供配置管理与前端展示）。加载时 `flatten_stations_catalog()` 会展开并校验 **id 全局唯一**。
- `catchment_forcing_bindings`：强迫绑定。
  - 旧式：`bindings: [{forcing_kind, station_id}, ...]`（默认权重 1.0，适合单站点）。
  - 新式（推荐）：`variables: [{kind, method, stations:[{id, weight}], ...}]`。
    - 对 `potential_evapotranspiration`：支持 `monthly_values`（长度为 12，Jan..Dec）。当绑定的 PET 站点在输入 `station_packages` 中缺失（或均无法取到）时，将回退使用 `monthly_values` 并按 `warmup_start_time + i * time_delta` 的月份展开成 `TimeSeries`。

运行时需提供 **`Dict[station_id, ForcingData]`**，且各序列与 `ForecastTimeContext` 对齐（`warmup_start_time`、`time_delta`、`step_count`）。

默认约定的项目配置文件名为 `configs/forecastSchemeConf.json`（调试脚本默认读取该文件；也可通过命令行参数覆盖）。

### 7.6 对外接口：`forecast_mode`（实时预报 / 历史模拟）

`run_calculation_from_json(...)` 支持入参：

- `forecast_mode="realtime_forecast"`（默认）
  - `forecast_start_time` 之后不使用实测气象驱动产流（当前对 `precipitation` / `potential_evapotranspiration` / `air_temperature` 置零）。
  - 节点 `use_observed_for_routing=true` 时，仅 `t < forecast_start_time` 使用实测接力，`t >= forecast_start_time` 使用计算值。
- `forecast_mode="historical_simulation"`
  - `forecast_start_time` 之后继续使用实测气象驱动产流。
  - 对 `use_observed_for_routing=true` 的节点，起报后也继续使用实测流量接力（便于分析“本断面产流误差”）。

若传入非法值，将抛出 `ValueError`。

### 7.7 从配置运行计算（示例）

```python
from datetime import datetime, timedelta

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.io.json_config import run_calculation_from_json

start = datetime(2026, 1, 1, 0, 0, 0)
step = timedelta(hours=1)
station_packages = {
    "STA_A": ForcingData.from_pairs(
        [
            (ForcingKind.PRECIPITATION, TimeSeries(start, step, [100.0, 130.0, 160.0, 140.0, 120.0])),
            (ForcingKind.POTENTIAL_EVAPOTRANSPIRATION, TimeSeries(start, step, [3.0, 3.5, 4.0, 3.8, 3.2])),
        ]
    ),
    # ...
}
result = run_calculation_from_json(
    "configs/example_forecast_config.json",
    station_packages,
    time_type="Hour",
    step_size=1,
    warmup_start_time=start,
    forecast_mode="realtime_forecast",  # 或 "historical_simulation"
)
# result 含 topological_order、node_total_inflows、reach_flows、time_context、display_results、forecast_mode
```

完整示例见仓库内 `configs/example_forecast_config.json`。

---

## 8. 测试与运行

在项目根目录 `hydro_project/` 下执行：

```bash
python -m unittest
```

主要用例：

- `test_y_shape_basin.py`：代码组装 Y 型网络 + 分洪。
- `test_hydrological_models.py`：新安江、Tank、Muskingum 等。
- `test_json_config_pipeline.py`：JSON 加载、`warmup_start_time`、整链计算。
- `test_runoff_framework.py`、`test_calibration_bounds.py`：多模型契约与率定边界。

---

## 9. 扩展开发要点

- **新节点类型**：在 `domain/nodes/` 新增文件，继承 `AbstractNode`，实现 `process_water`；在 `json_config._build_node` 中增加 `type` 分支（仅此一处按类型分发，引擎内仍无类型判断）。
- **新产流模型**：在 `models/runoff/` 实现 `IHydrologicalModel`，导出后在 `json_config._build_model` 注册 `name`。
- **新河道模型**：在 `models/routing/` 同上。
- **多站加权面雨量**：可在 `io` 层扩展绑定结构（如多站 + 权重），再生成子流域 `TimeSeries`，无需改核心引擎。

---

## 10. 文档维护

新增节点类型、JSON 字段或模型名时，请同步更新：

- 本文件第 7 节（配置约定）；
- `json_config.py` 内解析与错误提示；
- 对应 `tests/` 用例。

---

## 11. 与旧版手册的差异摘要

- 模型已拆分为 `models/runoff`、`models/routing`、`models/correction`；已移除旧的顶层兼容 re-export 文件。
- 配置文件**不含降雨与日历时点**；强迫由 `station_packages: Dict[str, ForcingData]` 与 **`warmup_start_time`** 注入。
- 推荐 **`schemes`** 自包含多尺度方案；`time_axis` 为四段**步数**。
- 产流模型支持 **params / state**；新安江与 Java 参数名对齐；**`calibration_bounds`** 提供率定区间。
- 节点支持 **`process_water` 校正链**与 **`name`**；水库支持 **特征水位、出流约束、关系曲线**。

更完整的交接摘要见 **`HANDOVER.md`**。
