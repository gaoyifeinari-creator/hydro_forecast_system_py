"""
Application-level data loader helpers for the calculation apps.

目标：把原先 `scripts/calculation_app_common.py` 中的“读数/连库逻辑”下沉到引擎层，
避免 `hydro_engine` 依赖 `scripts`。
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus

import pandas as pd

from hydro_engine.core.context import ForecastTimeContext
from hydro_engine.core.forcing import ForcingData, ForcingKind, parse_forcing_kind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.read_data import read_station_data


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_FLOOD_JDBC_CONFIG = PROJECT_ROOT / "configs" / "floodForecastJdbc.json"

_DEFAULT_STATION_HOURLY_SQL_KEY = "hourdb_hourly_range"
_DEFAULT_STATION_HOURLY_LABEL = "hourdb"
_DEFAULT_STATION_DAILY_SQL_KEY = "daydb_daily_range"
_DEFAULT_STATION_DAILY_LABEL = "daydb"


def read_config(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def build_times(context_start: datetime, step, count: int) -> pd.DatetimeIndex:
    return pd.DatetimeIndex([context_start + step * i for i in range(count)])


def _sqlalchemy_dm_url_from_jdbc(jdbc_url: str, user: str, password: str) -> str:
    """将 ``jdbc:dm://host:port/dbname`` 转为 ``dm+dmPython://user:pass@host:port/dbname``。"""
    m = re.match(r"jdbc:dm://([^:/]+):(\d+)/(.+)$", jdbc_url.strip())
    if not m:
        raise ValueError(f"达梦 JDBC URL 格式应为 jdbc:dm://host:port/schema，当前: {jdbc_url!r}")
    host, port, db = m.group(1), m.group(2), m.group(3)
    return f"dm+dmPython://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db}"


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
            password = str(found.get("password") if found.get("password") is not None else "")
        url = _sqlalchemy_dm_url_from_jdbc(raw_url, user, password)
    else:
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


def read_jdbc_daydb_normalize_time_to_midnight_from_path(path: str) -> bool:
    """读取 ``floodForecastJdbc.json`` 路径，返回 ``daydb.normalize_time_to_midnight``（缺省 False）。"""
    p = Path(str(path).strip())
    if not p.is_file():
        return False
    return _jdbc_daydb_normalize_time_to_midnight(read_config(str(p)))


def _jdbc_daydb_normalize_time_to_midnight(raw: Dict[str, Any]) -> bool:
    """
    从 ``floodForecastJdbc.json``（或同类 JSON）读取：日表读数后是否将时刻归一到当日 0 点。

    支持两种写法（任选其一）：
    - ``"daydb": { "normalize_time_to_midnight": true }``（推荐，字段集中、易读）
    - ``"daydb_normalize_time_to_midnight": true``（扁平兼容）
    """
    if isinstance(raw.get("daydb"), dict):
        return bool(raw["daydb"].get("normalize_time_to_midnight", False))
    v = raw.get("daydb_normalize_time_to_midnight")
    if v is not None:
        return bool(v)
    return False


def _coerce_hourdb_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """库表返回列名大小写不一致时，统一为 SENID / TIME / V / AVGV。"""
    out = df.copy()
    rename = {}
    for c in list(out.columns):
        u = str(c).strip().upper()
        if u in ("SENID", "TIME", "V", "AVGV") and str(c).strip() != u:
            rename[c] = u
    return out.rename(columns=rename)


def load_station_hourly_frame(
    ref: str,
    *,
    time_start: Optional[datetime] = None,
    time_end: Optional[datetime] = None,
    senids: Optional[List[str]] = None,
    senid_chunk_size: int = 1000,
    db_sql_key: Optional[str] = None,
    db_label: Optional[str] = None,
) -> pd.DataFrame:
    """
    读取测站小时数据：CSV 文件，或 JSON 数据库配置（见 ``configs/station_hourly_database.example.json``）。

    对数据库后端：支持传入 `senids`，将 SQL 切到 `*_range_in`（以 `SENID IN :senids` 降低全表扫描）。
    """
    ref = str(ref).strip()
    preferred_sql_key = str(db_sql_key or _DEFAULT_STATION_HOURLY_SQL_KEY)
    preferred_label = str(db_label or _DEFAULT_STATION_HOURLY_LABEL)
    p = Path(ref)
    if p.suffix.lower() == ".json" and p.is_file():
        raw = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(raw.get("services"), list):
            services_list = raw["services"]
            if not services_list:
                raise ValueError(f"{ref} 中 services 须为非空数组")

            if isinstance(raw.get("station_hourly"), dict):
                # 兼容旧结构（仍支持 station_hourly 嵌套块覆盖 sql_key/label）
                sh = dict(raw["station_hourly"])
                spec: Dict[str, Any] = {
                    "type": "database",
                    "_embedded_services": raw["services"],
                    "service": str(sh.get("service") or "").strip(),
                    "label": str(sh.get("label", preferred_label)),
                    "sql_key": str(sh.get("sql_key", preferred_sql_key)),
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
                # 新结构：只维护 services[] + 可选 hourly_service
                svc_name = str(raw.get("hourly_service") or "").strip()
                if not svc_name:
                    svc_name = str(services_list[0].get("service") or "").strip()
                if not svc_name:
                    raise ValueError(f"{ref} 需设置 hourly_service，或保证 services[0].service 非空")
                spec = {
                    "type": "database",
                    "_embedded_services": raw["services"],
                    "service": svc_name,
                    "label": preferred_label,
                    "sql_key": preferred_sql_key,
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
            raise ValueError(f"JSON {ref} 须为 floodForecastJdbc（services[]，可选 hourly_service）或 type=database")

        spec = _merge_database_service_spec(spec, p.resolve())
        if time_start is None or time_end is None:
            raise ValueError("数据库小时表读取必须提供 time_start 与 time_end（注入 :t_start / :t_end）")

        params = dict(spec.get("params") or {})
        params["t_start"] = time_start.strftime("%Y-%m-%d %H:%M:%S")
        params["t_end"] = time_end.strftime("%Y-%m-%d %H:%M:%S")

        base_sql_key = spec.get("sql_key", preferred_sql_key)
        pool_max_int = int(spec.get("pool_max", 5))
        pool_min_int = int(spec.get("pool_min", spec.get("pool_max", 5)))
        max_overflow_int = int(spec.get("max_overflow", max(0, pool_max_int - pool_min_int)))

        is_daydb_sql = str(base_sql_key).strip().lower().startswith("daydb")
        day_midnight = _jdbc_daydb_normalize_time_to_midnight(raw) if is_daydb_sql else False

        if senids is not None:
            unique = sorted({str(s).strip() for s in senids if str(s).strip()})
            if not unique:
                df = pd.DataFrame(columns=["SENID", "TIME", "V", "AVGV"])
                df["SENID"] = df["SENID"].astype(str)
                return _coerce_hourdb_column_names(df)

            sql_key_in = f"{base_sql_key}_in" if not str(base_sql_key).endswith("_in") else str(base_sql_key)
            params["senids"] = unique
            opts = {
                "url": spec["url"],
                "dialect": spec["dialect"],
                "sql_key": sql_key_in,
                "senid_chunk_size": int(senid_chunk_size),
                "pool_max": pool_max_int,
                "pool_min": pool_min_int,
                "max_overflow": max_overflow_int,
                "params": params,
                "normalize": True,
            }
        else:
            opts = {
                "url": spec["url"],
                "dialect": spec["dialect"],
                "sql_key": base_sql_key,
                "pool_max": pool_max_int,
                "pool_min": pool_min_int,
                "max_overflow": max_overflow_int,
                "params": params,
                "normalize": True,
            }

        if is_daydb_sql:
            opts["normalize_daily_times_to_midnight"] = day_midnight

        if spec.get("sql_yaml_path"):
            opts["sql_yaml_path"] = str(spec["sql_yaml_path"])

        df = read_station_data(
            source=str(spec.get("label", preferred_label)),
            source_type="database",
            options=opts,
        )
        return _coerce_hourdb_column_names(df)

    # CSV
    return read_station_data(ref, source_type="file")


def station_observation_query_end_realtime(time_context: ForecastTimeContext) -> datetime:
    """
    实时预报：测站「实况」库表查询上界 ``t_end``（与 SQL ``TIME <= :t_end`` 配合）。

    取 ``forecast_start_time - time_delta``，使不在库中读取 T0 及之后的记录。
    雨量、流量、气温等凡从同一测站时序表读取的变量，均应使用该上界，便于在历史 T0 复现实时预报读数。
    """
    return time_context.forecast_start_time - time_context.time_delta


def meteorology_station_query_end_realtime(time_context: ForecastTimeContext) -> datetime:
    """兼容旧名，等价于 :func:`station_observation_query_end_realtime`。"""
    return station_observation_query_end_realtime(time_context)


def clip_station_dataframe_rows_before_forecast_start(
    df: pd.DataFrame,
    *,
    forecast_start: datetime,
) -> Tuple[pd.DataFrame, int]:
    """
    去掉 ``TIME_DT``/``TIME`` 不早于 ``forecast_start`` 的行（严格 ``< forecast_start``）。

    用于实时预报下 CSV/文件读入后兜底，避免文件中含 T0 之后「伪实况」。
    返回 ``(新 DataFrame, 删除行数)``。
    """
    if df is None or df.empty:
        return df, 0
    tcol = "TIME_DT" if "TIME_DT" in df.columns else ("TIME" if "TIME" in df.columns else None)
    if not tcol:
        return df, 0
    t0 = pd.Timestamp(forecast_start)
    n0 = len(df)
    tseries = pd.to_datetime(df[tcol], errors="coerce")
    out = df[tseries < t0].copy()
    return out, n0 - len(out)


def _empty_station_hourly_frame() -> pd.DataFrame:
    """与 ``load_station_hourly_frame(..., senids=[])`` 相同列结构，且不发库。"""
    df = pd.DataFrame(columns=["SENID", "TIME", "V", "AVGV"])
    df["SENID"] = df["SENID"].astype(str)
    return _coerce_hourdb_column_names(df)


def _resolve_db_source_for_time_type(time_type: str) -> Tuple[str, str]:
    """
    根据计算时间尺度选择数据库读取配置。
    - Day -> DAYDB
    - Hour/Minute/其他 -> HOURDB（保持现有行为）
    """
    t = str(time_type or "").strip().lower()
    if t == "day":
        return _DEFAULT_STATION_DAILY_SQL_KEY, _DEFAULT_STATION_DAILY_LABEL
    return _DEFAULT_STATION_HOURLY_SQL_KEY, _DEFAULT_STATION_HOURLY_LABEL


def _union_station_senids_for_load(
    *,
    unified_station_senids: Optional[List[str]],
    rain_senids: Optional[List[str]],
    flow_senids: Optional[List[str]],
) -> Optional[List[str]]:
    """合并测站 id 列表，供单次库表 ``IN`` 查询。"""
    if unified_station_senids is not None:
        u = sorted({str(x).strip() for x in unified_station_senids if str(x).strip()})
        return u if u else None
    sset: Set[str] = set()
    if rain_senids:
        sset.update(str(x).strip() for x in rain_senids if str(x).strip())
    if flow_senids:
        sset.update(str(x).strip() for x in flow_senids if str(x).strip())
    return sorted(sset) if sset else None


def load_rain_flow_for_calculation(
    *,
    jdbc_config_path: str = "",
    rain_csv: str = "",
    flow_csv: str = "",
    time_start: datetime,
    time_end: datetime,
    rain_senids: Optional[List[str]] = None,
    flow_senids: Optional[List[str]] = None,
    senid_chunk_size: int = 1000,
    time_type: str = "Hour",
    rain_meteorology_time_end: Optional[datetime] = None,
    station_table_query_end: Optional[datetime] = None,
    unified_station_senids: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """
    读取测站时序表（雨量 V、流量 AVGV 等，库表通常同一张）。

    - **JDBC** 或 **雨量/流量指向同一 JSON 库配置**：按 ``unified_station_senids``（若提供）或
      ``rain_senids|flow_senids`` 合并后的列表 **一次** ``IN`` 查询，减少库交互。
    - **双 CSV 文件**：仍各读一次，但共用同一 ``query_end``。

    ``station_table_query_end`` / ``rain_meteorology_time_end``（后者为兼容别名）：
    实时预报时传入 ``station_observation_query_end_realtime``，使 **所有测站类型** 的 ``t_end``
    截断到预报起点之前；为 ``None`` 时读到 ``time_end``（历史模拟）。
    """
    warns: List[str] = []
    sql_key, label = _resolve_db_source_for_time_type(time_type)
    jc = str(jdbc_config_path).strip()
    cap = station_table_query_end if station_table_query_end is not None else rain_meteorology_time_end
    query_end = time_end if cap is None else min(time_end, cap)
    if cap is not None and query_end < time_end:
        warns.append(
            "[data] 实时预报：测站表 t_end 截断至预报起点之前（雨/流/温等共用，单次查询）"
        )

    union = _union_station_senids_for_load(
        unified_station_senids=unified_station_senids,
        rain_senids=rain_senids,
        flow_senids=flow_senids,
    )

    if jc and Path(jc).is_file():
        shared = load_station_hourly_frame(
            jc,
            time_start=time_start,
            time_end=query_end,
            senids=union,
            senid_chunk_size=senid_chunk_size,
            db_sql_key=sql_key,
            db_label=label,
        )
        return shared, shared, warns

    if jc and not Path(jc).is_file():
        warns.append(f"JDBC 配置不存在，已回退 CSV：{jc}")

    rp = str(rain_csv).strip()
    fp = str(flow_csv).strip()
    if rp and rp == fp and Path(rp).suffix.lower() == ".json":
        shared = load_station_hourly_frame(
            rp,
            time_start=time_start,
            time_end=query_end,
            senids=union,
            senid_chunk_size=senid_chunk_size,
            db_sql_key=sql_key,
            db_label=label,
        )
        return shared, shared, warns

    if not rp or not fp:
        raise ValueError("请配置 floodForecastJdbc.json 路径，或同时提供雨量 CSV 与流量 CSV（备用）")

    rain_df = load_station_hourly_frame(
        rp,
        time_start=time_start,
        time_end=query_end,
        senids=rain_senids,
        senid_chunk_size=senid_chunk_size,
        db_sql_key=sql_key if Path(rp).suffix.lower() == ".json" else None,
        db_label=label if Path(rp).suffix.lower() == ".json" else None,
    )
    flow_df = load_station_hourly_frame(
        fp,
        time_start=time_start,
        time_end=query_end,
        senids=flow_senids,
        senid_chunk_size=senid_chunk_size,
        db_sql_key=sql_key if Path(fp).suffix.lower() == ".json" else None,
        db_label=label if Path(fp).suffix.lower() == ".json" else None,
    )
    return rain_df, flow_df, warns


def load_csv(path: str) -> pd.DataFrame:
    """仅从本地 CSV 读取（兼容旧调用）。若路径为 JSON 数据库配置，请改用 load_station_hourly_frame。"""
    p = Path(str(path).strip())
    if p.suffix.lower() == ".json":
        raise ValueError(
            "数据库小时表请用 load_station_hourly_frame(ref, time_start=..., time_end=...) "
            "读取（主配置见 configs/floodForecastJdbc.example.json）"
        )
    return read_station_data(str(p), source_type="file")


def collect_rain_station_ids(binding_specs: List[Dict[str, Any]]) -> Set[str]:
    """
    收集 ``build_station_packages`` 用到的测站 id：降水、潜在蒸发（可选关闭）、**气温**。
    """
    out: Set[str] = set()
    for spec in binding_specs:
        for var in list(spec.get("variables") or []):
            kind = parse_forcing_kind(str(var.get("kind") or var.get("forcing_kind")))

            if kind is ForcingKind.PRECIPITATION:
                pass
            elif kind is ForcingKind.POTENTIAL_EVAPOTRANSPIRATION:
                if not bool(var.get("use_station_pet", True)):
                    continue
            elif kind is ForcingKind.AIR_TEMPERATURE:
                pass
            else:
                continue

            stations = var.get("stations") or []
            for st_item in stations:
                sid = str(st_item.get("id") or st_item.get("station_id") or "").strip()
                if sid:
                    out.add(sid)
    return out


def collect_all_station_ids_for_calculation(binding_specs: List[Dict[str, Any]], scheme: Any) -> List[str]:
    """
    计算管线一次读库所需的全部测站 id（子流域绑定：雨/PET/气温；节点：流量实测/入流等）。

    与 :func:`load_rain_flow_for_calculation` 的 ``unified_station_senids`` 配合，实现单表单次查询。
    """
    s = set(collect_rain_station_ids(binding_specs))
    s.update(collect_observed_flow_station_ids(scheme))
    return sorted(s)


def collect_observed_flow_station_ids(scheme: Any) -> Set[str]:
    """Collect flow station IDs used by build_observed_flows."""
    out: Set[str] = set()
    for node in getattr(scheme, "nodes", {}).values():
        for sid in (
            getattr(node, "observed_station_id", ""),
            getattr(node, "observed_inflow_station_id", ""),
        ):
            if str(sid).strip():
                out.add(str(sid).strip())
    return out

