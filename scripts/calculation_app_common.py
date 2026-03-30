"""Shared calculation helpers for web (Streamlit) and desktop (tkinter) test UIs."""

from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd

import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hydro_engine.read_data import read_station_data
from hydro_engine.core.forcing import ForcingData, ForcingKind, parse_forcing_kind
from hydro_engine.core.timeseries import TimeSeries


def read_config(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _sqlalchemy_dm_url_from_jdbc(jdbc_url: str, user: str, password: str) -> str:
    """将 ``jdbc:dm://host:port/dbname`` 转为 ``dm+dmPython://user:pass@host:port/dbname``。"""
    m = re.match(r"jdbc:dm://([^:/]+):(\d+)/(.+)$", jdbc_url.strip())
    if not m:
        raise ValueError(
            f"达梦 JDBC URL 格式应为 jdbc:dm://host:port/schema，当前: {jdbc_url!r}"
        )
    host, port, db = m.group(1), m.group(2), m.group(3)
    return (
        f"dm+dmPython://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db}"
    )


DEFAULT_FLOOD_JDBC_CONFIG = PROJECT_ROOT / "configs" / "floodForecastJdbc.json"


def _service_entry_to_url(
    found: Dict[str, Any],
    blob: Dict[str, Any],
    spec: Dict[str, Any],
) -> Tuple[str, int, Optional[str], int, int]:
    raw_url = str(found.get("url") or "").strip()
    if not raw_url:
        raise ValueError(f'服务 "{found.get("service")}" 缺少 url')

    # Java 习惯里的 minCon/maxCon -> SQLAlchemy 的 pool_min/max_overflow
    pool_min = int(found.get("minCon", spec.get("pool_min", spec.get("pool_max", 1))))
    pool_max = int(found.get("maxCon", spec.get("pool_max", pool_min)))
    if pool_max < pool_min:
        pool_max = pool_min
    max_overflow = max(0, pool_max - pool_min)

    dialect = found.get("dialect") or blob.get("dialect") or spec.get("dialect")
    if dialect is not None:
        dialect = str(dialect)

    # 兼容：仍支持 jdbc:dm://...，也允许直接使用 dm+dmPython://...（SQLAlchemy 标准）
    if raw_url.startswith("jdbc:dm://"):
        user = str(found.get("user") or "").strip()
        if not user:
            raise ValueError(f'服务 "{found.get("service")}" 使用 jdbc URL 时需要配置 user')
        pwd_env = str(found.get("password_env") or "").strip()
        if pwd_env:
            password = os.environ.get(pwd_env, "")
        else:
            password = str(
                found.get("password") if found.get("password") is not None else ""
            )
        url = _sqlalchemy_dm_url_from_jdbc(raw_url, user, password)
    else:
        # SQLAlchemy URL 直接透传（账号密码可在 URL 内，也可不提供 user/password 字段）
        url = raw_url

    return url, pool_max, dialect, pool_min, max_overflow


def _merge_database_service_spec(spec: Dict[str, Any], ref_path: Path) -> Dict[str, Any]:
    """
    支持：
    1) 顶层 ``url`` / ``pool_max``（旧版）；
    2) ``_embedded_services`` + ``service``（``floodForecastJdbc.json`` 内联 services）；
    3) ``service`` + ``services_file``（默认 ``floodForecastJdbc.json``，兼容 ``dameng_services.json``）。
    """
    if spec.get("url"):
        out = dict(spec)
        out.pop("_embedded_services", None)
        return out
    svc_name = str(spec.get("service") or "").strip()
    if not svc_name:
        raise ValueError('数据库 JSON 需包含顶层 "url"，或 "service" 与 JDBC 服务列表')
    embedded = spec.get("_embedded_services")
    if isinstance(embedded, list):
        blob: Dict[str, Any] = {"services": embedded, "dialect": spec.get("dialect")}
        services = embedded
        svc_path: Optional[Path] = None
    else:
        rel = str(spec.get("services_file") or "floodForecastJdbc.json").strip()
        candidates = [
            ref_path.parent / rel,
            PROJECT_ROOT / "configs" / Path(rel).name,
            PROJECT_ROOT / rel,
        ]
        svc_path = next((p for p in candidates if p.is_file()), None)
        if svc_path is None:
            raise FileNotFoundError(
                f"未找到 JDBC 服务配置文件: {rel}（已尝试: {[str(c) for c in candidates]}）"
            )
        blob = json.loads(svc_path.read_text(encoding="utf-8"))
        services = blob.get("services")
    if not isinstance(services, list):
        raise ValueError("JDBC 配置须包含 services 数组")
    found = None
    for s in services:
        if str(s.get("service", "")).strip().upper() == svc_name.upper():
            found = s
            break
    if found is None:
        loc = str(svc_path) if svc_path else "embedded"
        raise KeyError(f'服务 "{svc_name}" 未在 {loc} 中定义')
    url, pool_max, dialect, pool_min, max_overflow = _service_entry_to_url(found, blob, spec)
    out = dict(spec)
    out.pop("_embedded_services", None)
    out["url"] = url
    out["pool_max"] = pool_max
    out["pool_min"] = pool_min
    out["max_overflow"] = max_overflow
    if dialect:
        out["dialect"] = dialect
    return out


def _coerce_hourdb_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """库表返回列名大小写不一致时，统一为 SENID / TIME / V / AVGV。"""
    out = df.copy()
    rename = {}
    for c in list(out.columns):
        u = str(c).strip().upper()
        if u in ("SENID", "TIME", "V", "AVGV") and str(c).strip() != u:
            rename[c] = u
    return out.rename(columns=rename)


# 测站小时表：表名/模式写在方言 YAML（如 ``sql/dameng.yaml``）；此处仅默认选用的查询键与日志标签。
_DEFAULT_STATION_HOURLY_SQL_KEY = "hourdb_hourly_range"
_DEFAULT_STATION_HOURLY_LABEL = "hourdb"


def load_station_hourly_frame(
    ref: str,
    *,
    time_start: Optional[datetime] = None,
    time_end: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    读取测站小时数据：CSV 文件，或 JSON 数据库配置（见 ``configs/station_hourly_database.example.json``）。

    * `HOURDB` 表字段：`SENID`, `TIME`, `V`（雨量/水位/蒸发等瞬时值或累计量）, `AVGV`（流量等时段平均）。
    * JSON 配置时必须在一次计算的时间窗内传入 ``time_start`` / ``time_end``，绑定到 SQL 的 ``:t_start`` / ``t_end``。
    * **floodForecastJdbc**：仅需 ``services[]``；可选 ``hourly_service`` 指定连哪个服务（多服务时），缺省为 ``services[0]``。
    * **旧版**：仍可嵌套 ``station_hourly`` 以覆盖 ``sql_key`` / ``label`` 等。
    """
    ref = str(ref).strip()
    p = Path(ref)
    if p.suffix.lower() == ".json" and p.is_file():
        raw = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(raw.get("services"), list):
            services_list = raw["services"]
            if not services_list:
                raise ValueError(f"{ref} 中 services 须为非空数组")
            if isinstance(raw.get("station_hourly"), dict):
                sh = dict(raw["station_hourly"])
                spec: Dict[str, Any] = {
                    "type": "database",
                    "_embedded_services": raw["services"],
                    "service": str(sh.get("service") or "").strip(),
                    "label": str(sh.get("label", _DEFAULT_STATION_HOURLY_LABEL)),
                    "sql_key": str(sh.get("sql_key", _DEFAULT_STATION_HOURLY_SQL_KEY)),
                    "params": dict(sh.get("params") or {}),
                }
                if not spec["service"]:
                    raise ValueError(f"{ref} 中 station_hourly.service 不能为空")
                if sh.get("pool_max") is not None:
                    spec["pool_max"] = int(sh["pool_max"])
                if sh.get("sql_yaml_path"):
                    spec["sql_yaml_path"] = str(sh["sql_yaml_path"])
                dialect = sh.get("dialect") or raw.get("dialect")
                if dialect:
                    spec["dialect"] = str(dialect)
            else:
                svc_name = str(raw.get("hourly_service") or "").strip()
                if not svc_name:
                    svc_name = str(services_list[0].get("service") or "").strip()
                if not svc_name:
                    raise ValueError(
                        f"{ref} 需设置 hourly_service，或保证 services[0].service 非空"
                    )
                spec = {
                    "type": "database",
                    "_embedded_services": raw["services"],
                    "service": svc_name,
                    "label": _DEFAULT_STATION_HOURLY_LABEL,
                    "sql_key": _DEFAULT_STATION_HOURLY_SQL_KEY,
                    "params": {},
                }
                if raw.get("pool_max") is not None:
                    spec["pool_max"] = int(raw["pool_max"])
                if raw.get("sql_yaml_path"):
                    spec["sql_yaml_path"] = str(raw["sql_yaml_path"])
                dialect = raw.get("dialect")
                if dialect:
                    spec["dialect"] = str(dialect)
        elif raw.get("type") == "database":
            spec = raw
        else:
            raise ValueError(
                f"JSON {ref} 须为 floodForecastJdbc（services[]，可选 hourly_service）或 type=database"
            )
        spec = _merge_database_service_spec(spec, p.resolve())
        if time_start is None or time_end is None:
            raise ValueError(
                "数据库小时表读取必须在本次计算时间窗内提供 time_start 与 time_end（注入 :t_start / :t_end）"
            )
        params = dict(spec.get("params") or {})
        params["t_start"] = time_start.strftime("%Y-%m-%d %H:%M:%S")
        params["t_end"] = time_end.strftime("%Y-%m-%d %H:%M:%S")
        opts: Dict[str, Any] = {
            "url": spec["url"],
            "dialect": spec["dialect"],
            "sql_key": spec.get("sql_key", _DEFAULT_STATION_HOURLY_SQL_KEY),
            "pool_max": int(spec.get("pool_max", 5)),
            "pool_min": int(spec.get("pool_min", spec.get("pool_max", 5))),
            "max_overflow": int(
                spec.get(
                    "max_overflow",
                    max(0, int(spec.get("pool_max", 5)) - int(spec.get("pool_min", spec.get("pool_max", 5)))),
                )
            ),
            "params": params,
            "normalize": True,
        }
        if spec.get("sql_yaml_path"):
            opts["sql_yaml_path"] = str(spec["sql_yaml_path"])
        df = read_station_data(
            source=str(spec.get("label", _DEFAULT_STATION_HOURLY_LABEL)),
            source_type="database",
            options=opts,
        )
        return _coerce_hourdb_column_names(df)
    return read_station_data(ref, source_type="file")


def load_rain_flow_for_calculation(
    *,
    jdbc_config_path: str = "",
    rain_csv: str = "",
    flow_csv: str = "",
    time_start: datetime,
    time_end: datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """
    优先使用 ``floodForecastJdbc.json`` 连库读取 HOURDB（雨量 V + 流量 AVGV 同表）；
    若未配置或文件不存在，则回退为 CSV / 旧版 JSON 路径（与原先逻辑一致）。
    """
    warns: List[str] = []
    jc = str(jdbc_config_path).strip()
    if jc and Path(jc).is_file():
        shared = load_station_hourly_frame(jc, time_start=time_start, time_end=time_end)
        return shared, shared, warns
    if jc and not Path(jc).is_file():
        warns.append(f"JDBC 配置不存在，已回退 CSV：{jc}")
    rp = str(rain_csv).strip()
    fp = str(flow_csv).strip()
    if rp and rp == fp and Path(rp).suffix.lower() == ".json":
        shared = load_station_hourly_frame(rp, time_start=time_start, time_end=time_end)
        return shared, shared, warns
    if not rp or not fp:
        raise ValueError(
            "请配置 floodForecastJdbc.json 路径，或同时提供雨量 CSV 与流量 CSV（备用）"
        )
    rain_df = load_station_hourly_frame(rp, time_start=time_start, time_end=time_end)
    flow_df = load_station_hourly_frame(fp, time_start=time_start, time_end=time_end)
    return rain_df, flow_df, warns


def load_csv(path: str) -> pd.DataFrame:
    """仅从本地 CSV 读取（兼容旧调用）。若路径为 JSON 数据库配置，请改用 ``load_station_hourly_frame``。"""
    p = Path(str(path).strip())
    if p.suffix.lower() == ".json":
        raise ValueError(
            "数据库小时表请在计算流程中使用 load_station_hourly_frame(ref, time_start=..., time_end=...)，"
            "主配置见 configs/floodForecastJdbc.example.json"
        )
    return read_station_data(str(path), source_type="file")


def build_times(context_start: datetime, step, count: int) -> pd.DatetimeIndex:
    return pd.DatetimeIndex([context_start + step * i for i in range(count)])


def extract_station_series(
    df: pd.DataFrame,
    station_id: str,
    times: pd.DatetimeIndex,
    *,
    value_col: str,
    fill_mode: str,
) -> List[float]:
    if value_col not in df.columns:
        raise ValueError(f"CSV missing value column '{value_col}'")

    sub = df[df["SENID"] == str(station_id)].copy()
    if sub.empty:
        if fill_mode == "zero":
            return [0.0] * len(times)
        raise ValueError(f"Station {station_id} not found in CSV")

    sub[value_col] = pd.to_numeric(sub[value_col], errors="coerce")
    sub = sub.dropna(subset=["TIME_DT"]).sort_values("TIME_DT")
    if sub.empty:
        if fill_mode == "zero":
            return [0.0] * len(times)
        raise ValueError(f"Station {station_id} has no valid time rows")

    s = sub.groupby("TIME_DT")[value_col].mean().sort_index()
    s = s.reindex(times)

    if fill_mode == "zero":
        s = s.fillna(0.0)
    elif fill_mode == "interp":
        s = s.interpolate(method="time").ffill().bfill().fillna(0.0)
    else:
        raise ValueError(f"Unsupported fill_mode: {fill_mode}")

    return [float(x) for x in s.to_list()]


def iter_variable_specs(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    if spec.get("variables") is not None:
        return list(spec["variables"])
    return []


def build_station_packages(
    binding_specs: List[Dict[str, Any]],
    rain_df: pd.DataFrame,
    times: pd.DatetimeIndex,
    start_time: datetime,
    time_step,
) -> Tuple[Dict[str, ForcingData], List[str]]:
    station_kind_values: Dict[str, Dict[ForcingKind, List[float]]] = {}
    warnings: List[str] = []

    for spec in binding_specs:
        for var in iter_variable_specs(spec):
            kind = parse_forcing_kind(str(var.get("kind") or var.get("forcing_kind")))
            use_station = True
            if kind is ForcingKind.POTENTIAL_EVAPOTRANSPIRATION:
                use_station = bool(var.get("use_station_pet", True))
            if not use_station:
                continue

            stations = var.get("stations") or []
            for st_item in stations:
                sid = str(st_item.get("id") or st_item.get("station_id") or "").strip()
                if not sid:
                    continue

                value_col = "V" if kind is ForcingKind.PRECIPITATION else "V"
                try:
                    values = extract_station_series(
                        rain_df,
                        sid,
                        times,
                        value_col=value_col,
                        fill_mode="zero",
                    )
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"站点 {sid} 读取失败（{kind.value}）: {exc}")
                    values = [0.0] * len(times)

                station_kind_values.setdefault(sid, {})[kind] = values

    station_packages: Dict[str, ForcingData] = {}
    for sid, kv in station_kind_values.items():
        pairs = [
            (k, TimeSeries(start_time=start_time, time_step=time_step, values=v))
            for k, v in kv.items()
        ]
        station_packages[sid] = ForcingData.from_pairs(pairs)
    return station_packages, warnings


def build_observed_flows(
    scheme,
    flow_df: pd.DataFrame,
    times: pd.DatetimeIndex,
    start_time: datetime,
    time_step,
) -> Tuple[Dict[str, TimeSeries], List[str]]:
    station_ids = set()
    for node in scheme.nodes.values():
        for sid in (
            getattr(node, "observed_station_id", ""),
            getattr(node, "observed_inflow_station_id", ""),
        ):
            if str(sid).strip():
                station_ids.add(str(sid).strip())

    out: Dict[str, TimeSeries] = {}
    warnings: List[str] = []
    for sid in sorted(station_ids):
        try:
            vals = extract_station_series(
                flow_df,
                sid,
                times,
                value_col="AVGV",
                fill_mode="interp",
            )
            out[sid] = TimeSeries(start_time=start_time, time_step=time_step, values=vals)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"流量站 {sid} 读取失败: {exc}")
    return out, warnings


def write_temp_config_with_periods(
    config_path: str,
    *,
    time_type: str,
    step_size: int,
    warmup_steps: int,
    correction_steps: int,
    historical_steps: int,
    forecast_steps: int,
) -> str:
    data = read_config(config_path)
    if not data.get("schemes"):
        raise ValueError("配置缺少 schemes")
    target = None
    for s in data["schemes"]:
        if str(s.get("time_type")) == str(time_type) and int(s.get("step_size")) == int(step_size):
            target = s
            break
    if target is None:
        raise ValueError(f"未找到匹配方案：time_type={time_type}, step_size={step_size}")

    target["time_axis"] = {
        "warmup_period_steps": int(warmup_steps),
        "correction_period_steps": int(correction_steps),
        "historical_display_period_steps": int(historical_steps),
        "forecast_period_steps": int(forecast_steps),
    }

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
    json.dump(data, tmp, ensure_ascii=False, indent=2)
    tmp.close()
    return tmp.name


def build_catchment_precip_series(
    binding_specs: List[Dict[str, Any]],
    rain_df: pd.DataFrame,
    times: pd.DatetimeIndex,
) -> Tuple[Dict[str, List[float]], List[str]]:
    """Build catchment areal precipitation from weighted station bindings."""
    out: Dict[str, List[float]] = {}
    warnings: List[str] = []

    for spec in binding_specs:
        catchment_id = str(spec.get("catchment_id") or "").strip()
        if not catchment_id:
            continue
        precip_var = None
        for var in iter_variable_specs(spec):
            kind = parse_forcing_kind(str(var.get("kind") or var.get("forcing_kind")))
            if kind is ForcingKind.PRECIPITATION:
                precip_var = var
                break
        if precip_var is None:
            continue

        stations = precip_var.get("stations") or []
        if not stations:
            out[catchment_id] = [0.0] * len(times)
            continue

        weighted_sum = [0.0] * len(times)
        weight_total = 0.0
        valid_station_count = 0
        for st_item in stations:
            sid = str(st_item.get("id") or st_item.get("station_id") or "").strip()
            if not sid:
                continue
            try:
                series = extract_station_series(
                    rain_df,
                    sid,
                    times,
                    value_col="V",
                    fill_mode="zero",
                )
                w = float(st_item.get("weight", 1.0))
                valid_station_count += 1
                if w > 0:
                    weight_total += w
                    for i, v in enumerate(series):
                        weighted_sum[i] += w * v
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"流域 {catchment_id} 雨量站 {sid} 读取失败: {exc}")

        if weight_total > 0:
            out[catchment_id] = [v / weight_total for v in weighted_sum]
        elif valid_station_count > 0:
            # fallback: arithmetic mean when all weights are missing/invalid
            # (re-read with equal weights to keep logic explicit and stable)
            eq_sum = [0.0] * len(times)
            count = 0
            for st_item in stations:
                sid = str(st_item.get("id") or st_item.get("station_id") or "").strip()
                if not sid:
                    continue
                try:
                    series = extract_station_series(
                        rain_df,
                        sid,
                        times,
                        value_col="V",
                        fill_mode="zero",
                    )
                    count += 1
                    for i, v in enumerate(series):
                        eq_sum[i] += v
                except Exception:
                    pass
            out[catchment_id] = [v / max(count, 1) for v in eq_sum]
        else:
            out[catchment_id] = [0.0] * len(times)
    return out, warnings


def _guess_catchment_area(catchment_obj: Any) -> float:
    runoff_model = getattr(catchment_obj, "runoff_model", None)
    if runoff_model is None:
        return 1.0
    for attr in ("area",):
        v = getattr(runoff_model, attr, None)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except Exception:
                pass
    params = getattr(runoff_model, "params", None)
    if isinstance(params, dict):
        try:
            fv = float(params.get("area", 1.0))
            if fv > 0:
                return fv
        except Exception:
            pass
    return 1.0


def build_node_precip_series(
    scheme: Any,
    catchment_precip: Dict[str, List[float]],
) -> Dict[str, List[float]]:
    """Aggregate catchment precipitation to node precipitation (area-weighted)."""
    out: Dict[str, List[float]] = {}
    for node_id, node in scheme.nodes.items():
        cids = list(getattr(node, "local_catchment_ids", []) or [])
        if not cids:
            continue
        length = 0
        for cid in cids:
            if cid in catchment_precip:
                length = len(catchment_precip[cid])
                break
        if length == 0:
            continue

        acc = [0.0] * length
        area_total = 0.0
        for cid in cids:
            if cid not in catchment_precip:
                continue
            catchment_obj = scheme.catchments.get(cid)
            area = _guess_catchment_area(catchment_obj)
            area_total += area
            vals = catchment_precip[cid]
            for i, v in enumerate(vals):
                acc[i] += area * v
        if area_total > 0:
            out[str(node_id)] = [v / area_total for v in acc]
        else:
            out[str(node_id)] = [0.0] * length
    return out


def build_node_observed_flow_series(
    scheme: Any,
    observed_flows: Dict[str, TimeSeries],
) -> Dict[str, List[float]]:
    out: Dict[str, List[float]] = {}
    for node_id, node in scheme.nodes.items():
        sid_candidates = [
            str(getattr(node, "observed_inflow_station_id", "")).strip(),
            str(getattr(node, "observed_station_id", "")).strip(),
            str(getattr(node, "inflow_station_id", "")).strip(),
        ]
        sid = next((x for x in sid_candidates if x), "")
        if sid and sid in observed_flows:
            out[str(node_id)] = list(observed_flows[sid].values)
    return out


def build_catchment_observed_flow_series(
    scheme: Any,
    node_observed: Dict[str, List[float]],
) -> Dict[str, List[float]]:
    """Map catchment observed flow to its downstream node observed inflow."""
    out: Dict[str, List[float]] = {}
    for cid, catchment in scheme.catchments.items():
        dnid = str(getattr(catchment, "downstream_node_id", "")).strip()
        if dnid and dnid in node_observed:
            out[str(cid)] = list(node_observed[dnid])
    return out
