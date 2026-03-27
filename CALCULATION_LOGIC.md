# 计算逻辑说明（当前版本）

本文档说明当前项目的预报计算主流程，覆盖从配置加载、站点强迫合成，到 `catchment` 产流与演进、节点汇流、河道演进的端到端逻辑。

## 1. 总体流程

当前计算链路如下：

1. 读取配置并构建方案（`scheme`）
2. 由站点数据合成 `catchment` 强迫（`ForcingData`）
3. 每个 `catchment` 先执行产流模型（`runoff_model`）
4. 每个 `catchment` 再执行其洪水演进模型（`routing_model`），并把结果注入 `downstream_node_id`
5. 节点按拓扑序计算：累加上游 `reach` 演进流量 + 注入到该节点的 `catchment` 演进流量
6. 节点产出出流后，进入 `reach` 的洪水演进模型，传递到下游
7. 输出 `node_total_inflows`、`node_observed_flows`、`reach_flows`

---

## 2. 配置与建模阶段

核心入口：

- `load_scheme_from_json(...)`
- `run_calculation_from_json(...)`

其中 `run_calculation_from_json(...)` 新增接口参数：

- `forecast_mode: "realtime_forecast" | "historical_simulation"`（默认 `realtime_forecast`）

构建对象：

- `nodes`
- `reaches`
- `catchments`
- `ForecastTimeContext`
- `catchment_forcing_bindings`

### 2.1 catchment 配置约束（当前强制）

每个 `catchment` 必须配置：

- `runoff_model`
- `routing_model`
- `downstream_node_id`

否则会在加载或计算校验阶段报错。

---

## 3. 站点强迫 -> catchment 强迫

由 `CatchmentDataSynthesizer.synthesize(...)` 完成，输入为：

- `station_packages: Dict[station_id, ForcingData]`
- `catchment_forcing_bindings`
- `time_context`

输出为每个 `catchment` 的 `ForcingData`。

### 3.1 聚合规则

按 `variables` 中配置执行：

- `kind`（如 `precipitation`、`potential_evapotranspiration`）
- `method`（如 `weighted_average`、`arithmetic_mean`、`sum`）
- `stations`（含 `id` 与 `weight`）

### 3.2 预处理规则

按要素做异常值处理与插补：

- 降雨/PET 等：下界裁剪后插补
- 气温：上下界裁剪后插补
- 其他要素：插补

### 3.3 PET 特殊规则

`kind = potential_evapotranspiration` 支持：

- `use_station_pet = true`：优先测站；缺站时可回退 `monthly_values`
- `use_station_pet = false`：直接使用 `monthly_values`

---

## 4. 计算引擎主流程

主入口：`CalculationEngine.run(...)`

### 4.1 运行前校验

- 强迫序列与 `time_context` 对齐（起点、步长、长度）
- 强迫满足模型输入契约（`required_inputs()`）
- 每个 `catchment` 必须有 `routing_model`

### 4.2 catchment 产流与演进

对每个 `catchment`：

1. `runoff = catchment.generate_runoff(forcing)`
2. `routed = catchment.route_runoff(runoff)`（使用 catchment 的 `routing_model`）
3. 将 `routed` 放入 `catchment_routed_to_node[downstream_node_id]`

这一步在节点循环前统一完成，确保 `catchment` 的贡献独立、可累加。

### 4.3 节点汇流与节点过程

对每个节点（拓扑序）：

节点总入流由两部分构成并求和：

1. 上游 `reach` 演进结果（`reach_cache`）
2. 注入本节点的 `catchment` 演进结果（`catchment_routed_to_node[node_id]`）

随后执行：

- `node.process_water(total_inflow, observed_series, time_context, observed_inflow_series=...)`

### 4.4 节点实测相关逻辑

节点可选配置：

- `observed_station_id` + `use_observed_for_routing`
- `observed_inflow_station_id` + `use_observed_inflow_for_simulation`（常用于水库）
- `enable_observed_correction` + `correction_config`

当实测缺失或全 NaN 时，系统会告警并回退到模拟结果。

### 4.6 预报模式分支（接口控制）

通过 `run_calculation_from_json(..., forecast_mode=...)` 控制：

- `realtime_forecast`
  - `forecast_start_time` 之后，站点实测气象不再用于产流（当前对 `precipitation` / `potential_evapotranspiration` / `air_temperature` 置零）。
  - 节点 `use_observed_for_routing=true` 时，仅在 `t < forecast_start_time` 使用实测流量接力。
- `historical_simulation`
  - `forecast_start_time` 之后继续使用实测气象参与产流计算。
  - 节点 `use_observed_for_routing=true` 时，预报起报后也继续使用实测流量接力（等价于节点侧启用 `use_observed_for_routing_after_forecast`）。

### 4.5 节点出流 -> reach 演进

节点对每个 `outgoing_reach_id` 的出流，按以下流程处理：

1. 构造 `ROUTING_INFLOW`
2. 调用 `reach.routing_model`
3. 演进结果写入/累加 `reach_cache[reach_id]`

---

## 5. 结果输出

`CalculationResult` 主要字段：

- `node_total_inflows`
- `node_observed_flows`
- `reach_flows`
- `time_context`

展示接口：

- `get_display_results()`：按 `display_start_time ~ end_time` 截取结果

---

## 6. 与旧逻辑的关键区别

旧逻辑中，`catchment` 产流可直接作为挂载节点本地入流参与节点计算。  
当前逻辑改为强制：

- `catchment` 必须先经过自身 `routing_model` 演进
- 再注入 `downstream_node_id` 对应节点参与汇流

因此配置中 `catchments[]` 的 `routing_model` 和 `downstream_node_id` 为必填项。
