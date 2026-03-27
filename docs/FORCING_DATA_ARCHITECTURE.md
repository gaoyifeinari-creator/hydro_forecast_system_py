# 多维强迫数据（Forcing Data）接入架构说明

本文档说明 `hydro_project` 中强迫数据的领域模型、扩展方式、配置约定及引擎校验策略，与实现代码一致。

---

## 1. 设计目标

| 目标 | 实现要点 |
|------|----------|
| **OCP** | 新增气象/水文要素时：在 `ForcingKind` 枚举中增加成员；测站包与 JSON 绑定使用字符串键；`ForcingData` 为通用字典容器，**类体不随要素种类变化**。 |
| **契约（Contract）** | 各模型实现 `IHydrologicalModel.required_inputs()` → `frozenset[ForcingKind]`；计算前对子流域强迫包做 **Fail-Fast** 校验。 |
| **统一容器** | `ForcingData`：内部 `Dict[ForcingKind, TimeSeries]`，禁止 `self.rainfall` 等硬编码属性。 |
| **多源绑定** | JSON 中 `catchment_forcing_bindings`：同一子流域可绑定多个 `(forcing_kind, station_id)`，从不同测站取不同要素。 |

---

## 2. 核心类型

### 2.1 `ForcingKind`（注册表）

定义位置：`hydro_engine/core/forcing.py`。

采用 `str, Enum`，JSON 与配置中使用 **枚举值字符串**（如 `"precipitation"`），与业务文档对齐。

当前标准成员包括但不限于：

- `precipitation`：面雨量  
- `potential_evapotranspiration`：潜在蒸散（PET）  
- `air_temperature`：气温  
- `soil_moisture`：墒情  
- `snow_depth`：雪深  
- `solar_radiation` / `wind_speed`：预留  
- `routing_inflow`：**河道演进专用**，由引擎将节点汇流后的入流封装后传入，**非观测要素**

新增要素时：**仅扩展枚举**；`ForcingData` 实现不变。

### 2.2 `ForcingData`

- 构造：`empty()`、`single(kind, series)`、`from_pairs(...)`、`with_series(kind, series)`（不可变合并语义）、`merge(other)`。  
- 访问：`require(kind)`（缺失则 `KeyError`）、`get`、`keys`、`items`。  
- 校验辅助：`validate_forcing_contract(model, forcing)`、`validate_station_package_covers_binding(...)`。

### 2.3 `IHydrologicalModel`

```text
@classmethod
def required_inputs(cls) -> frozenset[ForcingKind]: ...

def run(self, forcing: ForcingData) -> TimeSeries: ...
```

- **产流模型**：通常需要 `PRECIPITATION`、可选 `POTENTIAL_EVAPOTRANSPIRATION` 等。  
- **河道模型**：仅声明 `ROUTING_INFLOW`；引擎在调用 `route()` 前构造 `ForcingData.single(ROUTING_INFLOW, outflow_series)`。

---

## 3. 引擎与数据流

1. **输入**：`CalculationEngine.run(scheme, catchment_forcing: Dict[str, ForcingData])`  
   - `catchment_forcing[catchment_id]` 为该子流域组装后的多维强迫集合。

2. **子流域产流前**：对每个子流域调用 `validate_forcing_contract(catchment.runoff_model, forcing)`，缺失任一 `required_inputs` 中的键则 **立即报错**。

3. **产流**：`SubCatchment.generate_runoff(forcing)` → `runoff_model.run(forcing)`。

4. **河道**：`RiverReach.route(ForcingData)` → `routing_model.run(forcing)`；引擎内在汇流得到 `TimeSeries` 后封装为含 `ROUTING_INFLOW` 的 `ForcingData`，并在路由前再次做契约校验。

---

## 4. JSON 配置

### 4.1 推荐结构：`catchment_forcing_bindings`

```json
"catchment_forcing_bindings": [
  {
    "catchment_id": "CA",
    "bindings": [
      { "forcing_kind": "precipitation", "station_id": "STA_A" },
      { "forcing_kind": "potential_evapotranspiration", "station_id": "STA_A" }
    ]
  },
  {
    "catchment_id": "CB",
    "bindings": [
      { "forcing_kind": "precipitation", "station_id": "STA_B" }
    ]
  }
]
```

含义：子流域 `CA` 的降水与 PET 均来自 `STA_A`（同一站可提供多个要素）；`CB` 仅降水来自 `STA_B`。

### 4.2 向后兼容

若配置中仍使用旧键 `catchment_station_bindings`（仅 `catchment_id` + `station_id`），解析时等价于该子流域只绑定 **`precipitation`** 到对应测站。解析逻辑见 `json_config._normalize_catchment_forcing_bindings`。

### 4.3 运行时测站数据

调用方提供：

```python
station_packages: Dict[str, ForcingData]
```

每个测站一个 `ForcingData`，包含该站可提供的全部要素序列（键为 `ForcingKind`）。

组装 API：`build_catchment_forcing_from_station_packages(config_path, station_packages)`。

兼容旧仅降水字典：

```python
legacy_rainfall_dict_to_station_packages({"STA_A": ts_rain})
# -> {"STA_A": ForcingData.single(PRECIPITATION, ts_rain)}
```

---

## 5. 模型示例

### 5.1 多要素产流：`SnowmeltRunoffModel`

文件：`hydro_engine/models/runoff/snowmelt.py`。

- `required_inputs`：`PRECIPITATION`、`AIR_TEMPERATURE`、`SNOW_DEPTH`。  
- `run(forcing)`：通过 `forcing.require(...)` 取序列并计算（示意公式）。

### 5.2 新安江简化版：`XinanjiangRunoffModel`

- `required_inputs`：`PRECIPITATION`、`POTENTIAL_EVAPOTRANSPIRATION`。  
- 有效降雨按步长使用 `P` 与 `PET` 及参数 `evap_coeff` 计算。

---

## 6. 与「零修改核心」的边界说明

- **枚举扩展**：新增 `ForcingKind` 成员属于**有意识的注册表变更**，集中在一处，不扩散到 `ForcingData` 容器实现。  
- **JSON 解析**：`_normalize_catchment_forcing_bindings` 与 `build_catchment_forcing_from_station_packages` 为 **通用循环**（按 `forcing_kind` 字符串解析为枚举、按站取序列、合并），不因新增要素类型而增加 `if kind == "rain"` 等分支。  
- **新模型**：新建类并实现 `required_inputs` + `run`；在 `_build_model` 的 `model_map` 中注册 **模型名**（一次性登记，与要素种类无关）。

若未来希望 **连 `model_map` 都不改**，可再引入插件注册表或入口点（entry points），作为下一阶段演进。

---

## 7. 相关源码路径

| 路径 | 说明 |
|------|------|
| `hydro_engine/core/forcing.py` | `ForcingKind`、`ForcingData`、校验函数 |
| `hydro_engine/core/interfaces.py` | `IHydrologicalModel` |
| `hydro_engine/engine/calculator.py` | 子流域与河道强迫校验与调度 |
| `hydro_engine/io/json_config.py` | JSON 加载、绑定归一化、组装 `catchment_forcing` |
| `hydro_engine/models/runoff/snowmelt.py` | 多要素示例模型 |
