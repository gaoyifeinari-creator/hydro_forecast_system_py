from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

def _ensure_hydro_on_path() -> None:
    # 当前文件位于 hydro_project/scripts/debug_client.py
    hydro_project_root = Path(__file__).resolve().parent.parent
    if str(hydro_project_root) not in sys.path:
        sys.path.insert(0, str(hydro_project_root))


_ensure_hydro_on_path()

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.io.json_config import load_scheme_from_json, run_calculation_from_json


def _parse_datetime(value: str) -> datetime:
    # 支持 "YYYY-mm-ddTHH:MM:SS" 或 "YYYY-mm-dd HH:MM:SS"
    v = value.replace("T", " ")
    return datetime.fromisoformat(v)


def _repeat_to_len(base: List[float], length: int) -> List[float]:
    if length <= 0:
        return []
    if not base:
        raise ValueError("base series must not be empty")
    out: List[float] = []
    i = 0
    while len(out) < length:
        out.append(float(base[i % len(base)]))
        i += 1
    return out


def _extract_station_ids_from_binding_specs(binding_specs: List[Dict[str, Any]]) -> List[str]:
    station_ids: List[str] = []
    for spec in binding_specs:
        if "variables" in spec and spec["variables"] is not None:
            for v in spec["variables"]:
                for st in v.get("stations", []) or []:
                    sid = st.get("id") or st.get("station_id")
                    if sid and str(sid) not in station_ids:
                        station_ids.append(str(sid))
        else:
            for b in spec.get("bindings", []) or []:
                sid = b.get("station_id")
                if sid and str(sid) not in station_ids:
                    station_ids.append(str(sid))
    return station_ids


def main() -> None:
    _ensure_hydro_on_path()

    parser = argparse.ArgumentParser(description="hydro_project debug client")
    parser.add_argument(
        "--config",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "configs" / "forecastSchemeConf.json"),
        help="Path to json config (default: configs/forecastSchemeConf.json)",
    )
    parser.add_argument("--time-type", type=str, default="Hour", help="TimeType: Minute/Hour/Day")
    parser.add_argument("--step-size", type=int, default=1, help="step size in the given time type")
    parser.add_argument(
        "--warmup-start-time",
        type=str,
        default="2026-01-01 00:00:00",
        help="Warmup start time (ISO or 'YYYY-mm-dd HH:MM:SS')",
    )
    parser.add_argument(
        "--observed",
        action="store_true",
        help="Provide observed flows for nodes with observed_station_id (if present in config).",
    )
    parser.add_argument(
        "--omit-pet-station",
        action="store_true",
        help="Omit PET station time series to force monthly_values PET fallback (if configured).",
    )

    args = parser.parse_args()

    config_arg = Path(args.config)
    if not config_arg.is_absolute():
        # 默认相对路径以 hydro_project 目录为基准（便于在任意工作目录运行）
        config_path = Path(__file__).resolve().parent.parent / config_arg
    else:
        config_path = config_arg
    warmup_start_time = _parse_datetime(args.warmup_start_time)
    time_type_str = str(args.time_type)
    step_size = int(args.step_size)

    # 先加载以得到 time_context.step_count
    scheme, binding_specs, time_context = load_scheme_from_json(
        file_path=config_path,
        time_type=time_type_str,
        step_size=step_size,
        warmup_start_time=warmup_start_time,
    )

    step_count = time_context.step_count
    dt = time_context.time_delta
    end_time = time_context.end_time
    print(f"[debug] time_context.step_count={step_count}, dt={dt}, warmup={time_context.warmup_start_time}, end={end_time}")

    # 构造 station_packages：按示例配置默认生成
    # 如需更复杂数据输入，可在这里接入 CSV/数据库/你自己的数据源。
    # precipitation 基准形状（与测试用例一致）
    rain_a = [100.0, 130.0, 160.0, 140.0, 120.0]
    pet_a = [3.0, 3.5, 4.0, 3.8, 3.2]
    rain_b = [90.0, 110.0, 140.0, 130.0, 100.0]

    # 发现 binding_specs 里用到了哪些 station_id（仅用于打印提示）
    used_station_ids = _extract_station_ids_from_binding_specs(binding_specs)
    print(f"[debug] station_ids referenced in bindings: {used_station_ids}")

    start = time_context.warmup_start_time
    station_packages: Dict[str, ForcingData] = {}

    # STA_A：降雨 +（可选）PET
    sta_a_series_p = TimeSeries(start, dt, _repeat_to_len(rain_a, step_count))
    if not args.omit_pet_station:
        sta_a_series_pet = TimeSeries(start, dt, _repeat_to_len(pet_a, step_count))
        station_packages["STA_A"] = ForcingData.from_pairs(
            [
                (ForcingKind.PRECIPITATION, sta_a_series_p),
                (ForcingKind.POTENTIAL_EVAPOTRANSPIRATION, sta_a_series_pet),
            ]
        )
    else:
        station_packages["STA_A"] = ForcingData.single(ForcingKind.PRECIPITATION, sta_a_series_p)

    # PET_STA_A：PET 站点（对应示例配置）
    if not args.omit_pet_station:
        station_packages["PET_STA_A"] = ForcingData.single(
            ForcingKind.POTENTIAL_EVAPOTRANSPIRATION,
            TimeSeries(start, dt, _repeat_to_len(pet_a, step_count)),
        )

    # STA_B：降雨
    station_packages["STA_B"] = ForcingData.single(
        ForcingKind.PRECIPITATION,
        TimeSeries(start, dt, _repeat_to_len(rain_b, step_count)),
    )

    # observed_flows：仅用于节点 output 缝合（如果配置了 observed_station_id）
    observed_flows: Dict[str, TimeSeries] | None = None
    if args.observed:
        observed_flows = {}
        # 示例配置里 N1 的 observed_station_id 是 ST_FLOW_N1；为了通用，这里按 nodes 扫一遍
        for node_id, node in scheme.nodes.items():
            station_id = getattr(node, "observed_station_id", "") or ""
            if station_id:
                # 默认用某个与模拟值不同的形状，便于肉眼验证 blend/缝合效果
                obs_values = [v * 10.0 for v in _repeat_to_len(rain_a, step_count)]
                observed_flows[station_id] = TimeSeries(start, dt, obs_values)

        print(f"[debug] provided observed_flows keys: {list(observed_flows.keys())}")

    # 跑计算
    output = run_calculation_from_json(
        config_path=config_path,
        station_packages=station_packages,
        time_type=time_type_str,
        step_size=step_size,
        warmup_start_time=warmup_start_time,
        observed_flows=observed_flows,
    )

    # 打印关键输出
    print("[debug] topological_order:", output.get("topological_order"))
    print("[debug] reach_flows keys:", list((output.get("reach_flows") or {}).keys()))
    # 重点打印 R5（示例里有 bypass）
    rf = output.get("reach_flows", {})
    if "R5" in rf:
        print("[debug] R5 reach_flows:", rf["R5"])

    if "node_observed_flows" in output:
        print("[debug] node_observed_flows keys:", list((output.get("node_observed_flows") or {}).keys()))


if __name__ == "__main__":
    main()

