# 区间通道（多通道伴随流）使用说明

本文说明如何在方案中启用“区间通道”，用于同时输出：

- `total_flow`：总流量（现有主流程）
- `interval_flows`：区间流量（按通道拆分）

## 1. 核心规则

- 始终存在隐式通道 `default`。
- `default` 通道在计算时会把**所有水库节点**当作边界节点（自动清零）。
  - 含义：得到“相邻水库之间”的标准区间流。
- 自定义通道通过 `boundary_node_ids` 指定“在哪些节点清零”。
  - 常用于“跨若干水库”的广义区间流。

## 2. 最小配置片段

把以下字段加到对应 `schemes[i]`（与 `time_type + step_size` 匹配的那一项）中：

```json
{
  "custom_interval_channels": [
    {
      "name": "generalized_A_to_C",
      "boundary_node_ids": ["A", "C"]
    }
  ]
}
```

说明：

- 不需要手动写 `default`，系统会自动补齐。
- `boundary_node_ids` 里的节点 ID 必须已存在于当前方案 `nodes` 中。

## 3. 结果字段（标准接口）

计算输出（`run_calculation_from_json` / Web 侧 `out`）包含：

- `interval_channels`: 通道名列表（例如 `["default", "generalized_A_to_C"]`）
- `node_interval_inflows[node_id][channel]`: 节点区间入流序列
- `node_interval_outflows[node_id][channel]`: 节点区间出流序列
- `reach_interval_flows[channel][reach_id]`: 河段区间流量序列

另外，`display_results` 也已统一包含区间通道序列，键名格式为：

- `node_interval_inflow:<channel>:<node_id>`
- `node_interval_outflow:<channel>:<node_id>`
- `reach_interval:<channel>:<reach_id>`

这样可直接复用现有导出/摘要链路，无需单独走区间专用接口。

## 4. Web 调试建议

在 `scripts/web_calculation_app.py` 页面：

- 打开「区间通道」标签
- 先选通道（`default` 或自定义）
- 再选展示类型（河段区间流量 / 节点区间入流 / 节点区间出流）

推荐先看 `default` 通道，快速确认“水库边界清零”是否符合预期，再切到自定义通道核对广义区间结果。
