"""
从 JSON 方案文件中读取 ``schemes``、按 time_type / step_size 选取子方案。

供 ``calculation_pipeline_runner``、Streamlit Web、``write_temp_config_with_periods`` 等复用，
避免各处手写 ``for s in schemes`` 循环。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from hydro_engine.io.calculation_app_data_loader import read_config
from hydro_engine.io.json_config import flatten_stations_catalog


def read_schemes_list(config_path: str | Path) -> List[Dict[str, Any]]:
    """读取配置文件并返回 ``schemes`` 中的 dict 项列表；文件不存在或解析失败时返回空列表。"""
    path = Path(config_path)
    if not path.is_file():
        return []
    try:
        data = read_config(str(path))
    except Exception:
        return []
    raw = data.get("schemes")
    if not isinstance(raw, list):
        return []
    return [s for s in raw if isinstance(s, dict)]


def select_scheme_dict_exact(
    schemes: Sequence[Dict[str, Any]],
    *,
    time_type: str,
    step_size: int,
) -> Optional[Dict[str, Any]]:
    """``time_type`` + ``step_size`` 精确匹配一条 scheme；无匹配返回 ``None``。"""
    tt = str(time_type).strip()
    try:
        sz = int(step_size)
    except (TypeError, ValueError):
        return None
    for s in schemes:
        if str((s.get("time_type") or "")).strip() != tt:
            continue
        try:
            if int(s.get("step_size", -999999)) != sz:
                continue
        except (TypeError, ValueError):
            continue
        return s
    return None


def select_scheme_dict_smallest_step(
    schemes: Sequence[Dict[str, Any]],
    *,
    time_type: str,
) -> Optional[Dict[str, Any]]:
    """
    同一 ``time_type`` 下取 ``step_size`` 最小的 scheme（须 ``step_size >= 1``）。
    用于 UI 默认回填与桌面端一致。
    """
    tt = str(time_type).strip()
    candidates: List[Dict[str, Any]] = []
    for s in schemes:
        if str((s.get("time_type") or "")).strip() != tt:
            continue
        try:
            step = int(s.get("step_size", 999999))
        except (TypeError, ValueError):
            step = 999999
        if step < 1:
            continue
        candidates.append(s)
    if not candidates:
        return None
    return min(candidates, key=lambda x: int(x.get("step_size", 999999)))


def scheme_dbtype(scheme: Optional[Dict[str, Any]], *, default: int = -1) -> int:
    """读取 scheme 的 ``dbtype``；缺失或非法时返回 ``default``。"""
    if not scheme:
        return default
    try:
        return int(scheme.get("dbtype", default))
    except (TypeError, ValueError):
        return default


def station_catalog_names_from_scheme(scheme: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """当前 scheme 的 stations 目录展平为 id -> 配置名称（仅包含有名称的项）。"""
    if not scheme:
        return {}
    raw = scheme.get("stations")
    if raw is None:
        return {}
    try:
        flat = flatten_stations_catalog(raw)
    except Exception:
        return {}
    out: Dict[str, str] = {}
    for e in flat:
        sid = str(e.get("id", "")).strip()
        if not sid:
            continue
        nm = str(e.get("name", "") or "").strip()
        if nm:
            out[sid] = nm
    return out


def catchment_catalog_names_from_scheme(scheme: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """当前 scheme 的 ``catchments[]`` 得到 id -> name。"""
    if not scheme:
        return {}
    out: Dict[str, str] = {}
    for c in scheme.get("catchments") or []:
        if not isinstance(c, dict):
            continue
        cid = str(c.get("id", "")).strip()
        if not cid:
            continue
        nm = str(c.get("name", "") or "").strip()
        if nm:
            out[cid] = nm
    return out
