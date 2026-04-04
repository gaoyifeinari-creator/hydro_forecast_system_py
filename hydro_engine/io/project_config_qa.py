"""
新项目配置（schemes）的静态检查 + 引擎侧试加载，用于转换工具与人工校对。

与 `json_config.load_scheme_from_json` 对齐：试加载能捕获模型名、拓扑、绑定等大部分错误。
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass(frozen=True)
class ConfigIssue:
    severity: str  # "error" | "warning"
    message: str
    path: str = ""


def _static_check_one_scheme(scheme: Dict[str, Any], px: str) -> List[ConfigIssue]:
    """对单个 scheme 对象做静态检查，`px` 为路径前缀，如 `schemes[0]`。"""
    issues: List[ConfigIssue] = []

    req = ("time_axis", "nodes", "reaches", "catchments", "stations")
    for k in req:
        if k not in scheme:
            issues.append(ConfigIssue("error", f"方案缺少必填键 `{k}`。", f"{px}.{k}"))
    has_forecast_rules = bool(scheme.get("catchment_forecast_rules"))
    if "catchment_forcing_bindings" not in scheme and not has_forecast_rules:
        issues.append(
            ConfigIssue(
                "error",
                "方案需包含 `catchment_forcing_bindings` 或 `catchment_forecast_rules` 之一。",
                px,
            )
        )

    nodes = scheme.get("nodes") if isinstance(scheme.get("nodes"), list) else []
    reaches = scheme.get("reaches") if isinstance(scheme.get("reaches"), list) else []
    catchments = scheme.get("catchments") if isinstance(scheme.get("catchments"), list) else []

    node_ids: Set[str] = set()
    for i, n in enumerate(nodes):
        if not isinstance(n, dict):
            issues.append(ConfigIssue("error", f"nodes[{i}] 不是对象", f"{px}.nodes[{i}]"))
            continue
        nid = str(n.get("id", "")).strip()
        if not nid:
            issues.append(ConfigIssue("error", f"nodes[{i}] 缺少 id", f"{px}.nodes[{i}].id"))
        elif nid in node_ids:
            issues.append(ConfigIssue("error", f"重复节点 id: {nid!r}", f"{px}.nodes id={nid}"))
        else:
            node_ids.add(nid)

        ntype = str(n.get("type", "")).strip().lower()
        sb = n.get("station_binding") if isinstance(n.get("station_binding"), dict) else {}
        if ntype == "reservoir":
            for key, label in (
                ("inflow_station_id", "入库流量站"),
                ("outflow_station_id", "出库流量站"),
            ):
                v = str(sb.get(key, "")).strip()
                if not v:
                    issues.append(
                        ConfigIssue(
                            "warning",
                            f"节点 {nid or i}（水库）未配置 {label} `{key}`，可能影响计算或展示。",
                            f"{px}.nodes[{nid or i}].station_binding.{key}",
                        )
                    )
        elif ntype == "cross_section":
            if not str(sb.get("flow_station_id", "")).strip():
                issues.append(
                    ConfigIssue(
                        "warning",
                        f"节点 {nid or i}（断面）未配置 `flow_station_id`。",
                        f"{px}.nodes[{nid or i}].station_binding.flow_station_id",
                    )
                )

    catchment_ids: Set[str] = set()
    for c in catchments:
        if isinstance(c, dict):
            cid = str(c.get("id", "")).strip()
            if cid:
                catchment_ids.add(cid)

    for i, r in enumerate(reaches):
        if not isinstance(r, dict):
            issues.append(ConfigIssue("error", f"reaches[{i}] 不是对象", f"{px}.reaches[{i}]"))
            continue
        uid = str(r.get("upstream_node_id", "")).strip()
        did = str(r.get("downstream_node_id", "")).strip()
        rid = str(r.get("id", "")).strip()
        if uid and uid not in node_ids:
            issues.append(
                ConfigIssue(
                    "error",
                    f"河段 {rid or i} 的上游节点 {uid!r} 不在 nodes 中。",
                    f"{px}.reaches[{rid or i}].upstream_node_id",
                )
            )
        if did and did not in node_ids:
            issues.append(
                ConfigIssue(
                    "error",
                    f"河段 {rid or i} 的下游节点 {did!r} 不在 nodes 中。",
                    f"{px}.reaches[{rid or i}].downstream_node_id",
                )
            )

    for n in nodes:
        if not isinstance(n, dict):
            continue
        nid = str(n.get("id", "")).strip()
        for cid in list(n.get("local_catchment_ids") or []):
            cs = str(cid).strip()
            if cs and cs not in catchment_ids:
                issues.append(
                    ConfigIssue(
                        "warning",
                        f"节点 {nid} 的 local_catchment_ids 含 {cs!r}，但 catchments 中无此 id。",
                        f"{px}.nodes[{nid}].local_catchment_ids",
                    )
                )

    bindings = scheme.get("catchment_forcing_bindings")
    if isinstance(bindings, list) and bindings:
        bound_c: Set[str] = set()
        for j, spec in enumerate(bindings):
            if not isinstance(spec, dict):
                continue
            cid = str(spec.get("catchment_id", "")).strip()
            if cid:
                bound_c.add(cid)
            vars_ = spec.get("variables")
            if isinstance(vars_, list) and len(vars_) == 0:
                issues.append(
                    ConfigIssue(
                        "warning",
                        f"子流域 {cid or j} 的 forcing 变量列表为空，通常需至少配置降水等要素。",
                        f"{px}.catchment_forcing_bindings[{j}]",
                    )
                )
            elif isinstance(vars_, list):
                kinds = {str(v.get("kind", "")).lower() for v in vars_ if isinstance(v, dict)}
                if "precipitation" not in kinds:
                    issues.append(
                        ConfigIssue(
                            "warning",
                            f"子流域 {cid or j} 未包含 kind=precipitation 的变量。",
                            f"{px}.catchment_forcing_bindings[{j}]",
                        )
                    )
        for cid in catchment_ids:
            if cid not in bound_c:
                issues.append(
                    ConfigIssue(
                        "error",
                        f"子流域 {cid!r} 在 catchments 中已定义，但 catchment_forcing_bindings 中无对应项。",
                        f"{px}.catchment_forcing_bindings",
                    )
                )
    elif catchment_ids and not has_forecast_rules:
        raw_b = scheme.get("catchment_forcing_bindings")
        if raw_b is None or (isinstance(raw_b, list) and len(raw_b) == 0):
            issues.append(
                ConfigIssue(
                    "error",
                    "存在子流域定义，但 `catchment_forcing_bindings` 为空或缺失，且未配置 `catchment_forecast_rules`。",
                    f"{px}.catchment_forcing_bindings",
                )
            )

    return issues


def static_check_project_config(data: Dict[str, Any]) -> List[ConfigIssue]:
    """不访问文件系统，仅做结构与拓扑层面的提醒（支持 `schemes` 多项）。"""
    issues: List[ConfigIssue] = []

    if not isinstance(data, dict):
        return [ConfigIssue("error", "根对象必须是 JSON 对象", "root")]

    schemes = data.get("schemes")
    if not isinstance(schemes, list) or not schemes:
        issues.append(
            ConfigIssue(
                "error",
                "缺少 `schemes` 非空数组，或需使用根级旧版结构（本工具主要面向 `schemes` 新项目配置）。",
                "schemes",
            )
        )
        return issues

    for si, scheme in enumerate(schemes):
        if not isinstance(scheme, dict):
            issues.append(ConfigIssue("error", f"schemes[{si}] 不是对象", f"schemes[{si}]"))
            continue
        px = f"schemes[{si}]"
        issues.extend(_static_check_one_scheme(scheme, px))

    return issues


def try_load_scheme_from_dict(
    data: Dict[str, Any],
    *,
    time_type: str,
    step_size: int,
    warmup_start_time: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    将配置写入临时文件并调用 `load_scheme_from_json`，与正式计算使用同一套校验逻辑。

    返回 (是否成功, 错误信息或空字符串)。
    """
    from hydro_engine.io.json_config import load_scheme_from_json

    t0 = warmup_start_time or datetime(2020, 1, 1, 0, 0, 0)
    path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
            encoding="utf-8",
        ) as f:
            json.dump(data, f, ensure_ascii=False)
            path = Path(f.name)
        load_scheme_from_json(path, time_type, step_size, warmup_start_time=t0)
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    finally:
        if path is not None and path.exists():
            try:
                path.unlink()
            except OSError:
                pass


def analyze_project_config(
    data: Dict[str, Any],
    *,
    time_type: str,
    step_size: int,
    warmup_start_time: Optional[datetime] = None,
) -> List[ConfigIssue]:
    """合并静态检查与引擎试加载（与 `load_scheme_from_json` 一致；多 scheme 时逐个试加载）。"""
    issues = static_check_project_config(data)
    schemes = data.get("schemes") if isinstance(data, dict) else None
    t0 = warmup_start_time or datetime(2020, 1, 1, 0, 0, 0)

    if isinstance(schemes, list) and len(schemes) > 1:
        for i, sch in enumerate(schemes):
            if not isinstance(sch, dict):
                continue
            tt = str(sch.get("time_type", time_type))
            sz = int(sch.get("step_size", step_size))
            ok, err = try_load_scheme_from_dict(
                data,
                time_type=tt,
                step_size=sz,
                warmup_start_time=t0,
            )
            if not ok:
                issues.insert(
                    0,
                    ConfigIssue(
                        "error",
                        f"引擎试加载失败（schemes[{i}] time_type={tt!r} step_size={sz}）：{err}",
                        "load_scheme_from_json",
                    ),
                )
    else:
        ok, err = try_load_scheme_from_dict(
            data,
            time_type=time_type,
            step_size=step_size,
            warmup_start_time=t0,
        )
        if not ok:
            issues.insert(0, ConfigIssue("error", f"引擎试加载失败：{err}", "load_scheme_from_json"))
    return issues
