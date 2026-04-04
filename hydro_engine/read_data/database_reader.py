"""
数据库读取：SQLAlchemy Core + 外置 YAML SQL，连接池进程内单例缓存。

依赖（需单独安装）::

    pip install sqlalchemy pyyaml pandas
    # 按需再安装驱动，例如: pymysql psycopg2-binary oracledb pyodbc dmPython

``DataReadSpec`` 约定（``options`` 内）::

    url:        SQLAlchemy 连接 URL（必填）
    dialect:    mysql | postgresql | oracle | sqlserver | dameng（必填，用于选择 sql/*.yaml）
    sql_key:    当前 YAML 中的查询逻辑名（必填；默认 ``hourdb_hourly_range`` 对应 HOURDB 全站时段）
    params:     绑定到 :name 占位符的字典（选填，默认 {}）
    pool_max:   连接池大小，对应 create_engine(pool_size=...)（选填，默认 5）
    sql_yaml_path: 覆盖内置 sql/<dialect>.yaml 的绝对/相对路径（选填）
    normalize:  是否按站点表规范列名（选填，默认 True，需 SENID/TIME）
    normalize_daily_times_to_midnight:  日表读数后是否将 ``TIME_DT`` 归一到当日 00:00（选填，默认 False；由 ``floodForecastJdbc.json`` 的 ``daydb.normalize_time_to_midnight`` 控制）

``spec.source`` 不参与 SQL，仅作日志/报错中的数据源说明，可填库名或业务标签。
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
from functools import lru_cache

import pandas as pd

from .types import DataReadSpec, IDataReader

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "DatabaseDataReader requires PyYAML. Install with: pip install pyyaml"
    ) from exc

try:
    from sqlalchemy import create_engine, text, bindparam
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.url import URL
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "DatabaseDataReader requires SQLAlchemy. Install with: pip install sqlalchemy"
    ) from exc

_SQL_DIR = Path(__file__).resolve().parent / "sql"

# 方言 -> 外置 SQL 文件名（仅做路径映射，SQL 文本内无任何分支）
DIALECT_SQL_FILE: Dict[str, str] = {
    "mysql": "mysql.yaml",
    "postgresql": "postgresql.yaml",
    "oracle": "oracle.yaml",
    "sqlserver": "sqlserver.yaml",
    "mssql": "sqlserver.yaml",
    "dameng": "dameng.yaml",
}

_engine_lock = threading.Lock()
_engine_cache: Dict[Tuple[str, int, int], Engine] = {}


def _resolve_yaml_path(dialect: str, options: Dict[str, Any]) -> Path:
    custom = options.get("sql_yaml_path")
    if custom:
        p = Path(str(custom))
        if not p.is_file():
            raise FileNotFoundError(f"sql_yaml_path not found: {p}")
        return p
    key = dialect.strip().lower()
    fname = DIALECT_SQL_FILE.get(key)
    if not fname:
        known = ", ".join(sorted(DIALECT_SQL_FILE.keys()))
        raise ValueError(f"Unknown dialect {dialect!r}. Expected one of: {known}")
    return _SQL_DIR / fname


def _load_sql_map(yaml_path: Path) -> Dict[str, Any]:
    # 通过文件 mtime 让缓存具备“修改即失效”的能力
    mtime_ns = yaml_path.stat().st_mtime_ns
    return _load_sql_map_cached(str(yaml_path), mtime_ns)


@lru_cache(maxsize=32)
def _load_sql_map_cached(yaml_path_str: str, mtime_ns: int) -> Dict[str, Any]:
    # mtime_ns 只用于参与 key，避免 YAML 变化后仍命中旧缓存
    _ = mtime_ns
    yaml_path = Path(yaml_path_str)
    with yaml_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping: {yaml_path}")
    return data


def _pick_sql(data: Dict[str, Any], sql_key: str) -> str:
    if sql_key in data and isinstance(data[sql_key], str):
        return data[sql_key].strip()
    nested = data.get("queries")
    if isinstance(nested, dict) and sql_key in nested and isinstance(nested[sql_key], str):
        return nested[sql_key].strip()
    raise KeyError(
        f"sql_key {sql_key!r} not found in YAML (top-level or under 'queries')"
    )


def _dm_dmpython_url_and_connect_args(
    raw_url: str,
) -> Tuple[Union[str, URL], Dict[str, Any]]:
    """
    dmSQLAlchemy 会把 URL 路径映射为 ``database=`` 传给 dmPython，而 dmPython 只接受 ``schema=``。
    将 ``dm+dmPython://.../SCHEMA`` 规范为无库名路径，并把 SCHEMA 放入 ``connect_args``。

    返回的 URL 必须是 :class:`sqlalchemy.engine.url.URL` 对象（而非 ``str()``），否则密码中的
    ``@`` 等字符经字符串往返后会损坏，导致登录失败。
    """
    from sqlalchemy.engine.url import make_url

    u = make_url(raw_url)
    driver = str(u.drivername or "").lower()
    if not driver.startswith("dm") or not u.database:
        return raw_url, {}
    schema = u.database
    # 兼容不同 SQLAlchemy 版本：
    # - 1.4/2.x: URL.set(...)
    # - 部分旧版本: namedtuple-like URL，使用 _replace(...)
    # 对 dmSQLAlchemy + dmPython，传入 URL 对象时部分版本仍会把 database 透传给 DBAPI，
    # 导致 “database is an invalid keyword argument”。这里改为返回“去掉路径”的字符串 URL，
    # 并通过 connect_args["schema"] 显式指定模式。
    raw = str(raw_url).strip()
    qpos = raw.find("?")
    base = raw if qpos < 0 else raw[:qpos]
    query = "" if qpos < 0 else raw[qpos:]
    slash = base.rfind("/")
    if slash > raw.lower().find("://") + 2:
        cleaned = base[:slash] + query
    else:
        cleaned = raw
    return cleaned, {"schema": schema}


def get_shared_engine(url: str, pool_min: int, max_overflow: int = 0) -> Engine:
    """
    进程内按 (url, pool_min, max_overflow) 复用单个 Engine。
    pool_min/max_overflow 对应 SQLAlchemy 的连接池配置：
    - ``pool_size`` == ``pool_min``
    - ``max_overflow`` == 允许超出 ``pool_size`` 的额外连接数
    """
    if pool_min < 1:
        raise ValueError("pool_min must be >= 1")
    if max_overflow < 0:
        raise ValueError("max_overflow must be >= 0")

    key = (url, pool_min, max_overflow)
    with _engine_lock:
        eng = _engine_cache.get(key)
        if eng is None:
            engine_url, connect_args = _dm_dmpython_url_and_connect_args(url)
            eng = create_engine(
                engine_url,
                connect_args=connect_args,
                pool_size=pool_min,
                max_overflow=max_overflow,
                pool_pre_ping=True,
                pool_timeout=30,
            )
            _engine_cache[key] = eng
        return eng


def dispose_all_engines() -> None:
    """测试或进程退出前释放全部缓存池（一般业务无需调用）。"""
    global _engine_cache
    with _engine_lock:
        for eng in _engine_cache.values():
            eng.dispose()
        _engine_cache.clear()


class DatabaseDataReader(IDataReader):
    """通过 SQLAlchemy + 外置 YAML 从关系库读取为 DataFrame。"""

    def read(self, spec: DataReadSpec) -> pd.DataFrame:
        opts = dict(spec.options or {})
        url = opts.get("url")
        if not url or not str(url).strip():
            raise ValueError('options["url"] is required for database reads')
        dialect = str(opts.get("dialect", "")).strip().lower()
        if not dialect:
            raise ValueError('options["dialect"] is required (e.g. mysql, postgresql)')
        sql_key = str(opts.get("sql_key", "")).strip()
        if not sql_key:
            raise ValueError('options["sql_key"] is required to select SQL from YAML')

        pool_max = int(opts.get("pool_max", opts.get("pool_size", 5)))
        pool_min = int(opts.get("pool_min", pool_max))
        max_overflow = int(opts.get("max_overflow", max(0, pool_max - pool_min)))
        params: Dict[str, Any] = dict(opts.get("params") or {})
        normalize = bool(opts.get("normalize", True))

        yaml_path = _resolve_yaml_path(dialect, opts)
        sql_map = _load_sql_map(yaml_path)
        try:
            sql_text = _pick_sql(sql_map, sql_key)
        except KeyError:
            # 兼容：如果请求的是 *_in，但 YAML 没配，就回退到原 key
            if sql_key.endswith("_in"):
                sql_text = _pick_sql(sql_map, sql_key[: -len("_in")])
            else:
                raise

        stmt = text(sql_text)
        engine = get_shared_engine(str(url).strip(), pool_min=pool_min, max_overflow=max_overflow)

        with engine.connect() as conn:
            # 用 SQLAlchemy 自己执行 + 手动落到 DataFrame，规避部分 DBAPI/驱动对
            # ``pandas.read_sql(..., params=...)`` 的兼容性问题。
            if params:
                # IN 查询列表参数：让 SQLAlchemy 展开为 (..., ..., ...)
                if "senids" in params:
                    stmt = stmt.bindparams(bindparam("senids", expanding=True))

                senids = params.get("senids")
                chunk_size = int(opts.get("senid_chunk_size", 0) or 0)
                if (
                    senids is not None
                    and isinstance(senids, (list, tuple, set))
                    and chunk_size > 0
                    and len(senids) > chunk_size
                ):
                    senids_list = list(senids)
                    dfs: List[pd.DataFrame] = []
                    keys = None
                    for i in range(0, len(senids_list), chunk_size):
                        chunk = senids_list[i : i + chunk_size]
                        p2 = dict(params)
                        p2["senids"] = chunk
                        result = conn.execute(stmt, p2)
                        rows = result.fetchall()
                        if keys is None:
                            keys = result.keys()
                        dfs.append(pd.DataFrame(rows, columns=keys))
                    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=keys or [])
                else:
                    result = conn.execute(stmt, params)
                    rows = result.fetchall()
                    df = pd.DataFrame(rows, columns=result.keys())
            else:
                result = conn.execute(stmt)
                rows = result.fetchall()
                df = pd.DataFrame(rows, columns=result.keys())

        if normalize:
            from .file_reader import (
                apply_daily_time_midnight_normalization,
                normalize_station_dataframe,
            )

            tag = spec.source or str(yaml_path)
            df = normalize_station_dataframe(df, source=tag)
            if bool(opts.get("normalize_daily_times_to_midnight")):
                df = apply_daily_time_midnight_normalization(df)
            return df
        return df


__all__ = [
    "DatabaseDataReader",
    "get_shared_engine",
    "dispose_all_engines",
    "DIALECT_SQL_FILE",
]
