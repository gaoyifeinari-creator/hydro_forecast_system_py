"""
预报面雨量多源多尺度整编拼接模块（时间轴画布覆盖法）。

核心流程：
1) 先降维到 1 小时基准画布（baseline array）
2) 按优先级写入并覆盖（后写覆盖前写）
3) 再升维整编到目标步长（Hour/Day）
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

from sqlalchemy import bindparam, text

from hydro_engine.read_data.database_reader import get_shared_engine


@dataclass(frozen=True)
class ForecastRainRecord:
    """单条气象预报降雨记录（对应 WEA_GFSFORERAIN 一行概念）。"""

    reg_id: str
    subtype: str
    time_span_hours: int
    ftime: datetime
    btime: datetime
    aver_pre: float
    min_pre: Optional[float] = None
    max_pre: Optional[float] = None


@dataclass(frozen=True)
class RainDistributionParam:
    """
    日内降雨分配参数。

    - dis_name: 曲线名称（如“小雨”“中雨以上所有样本”）
    - dis_scale_map: key=第几个小时(1..24), value=百分比(0..100)
    """

    dis_name: str
    dis_scale_map: Mapping[int, float]


@dataclass(frozen=True)
class SubRainSourceConfig:
    """
    子预报源配置（多源融合时使用）。

    rank 约定：数值越大优先级越高（处理顺序：低 -> 高，后写覆盖前写）。
    """

    sub_source_name: str
    rank: int
    subtype: str
    time_span_hours: int


@dataclass(frozen=True)
class ForecastRainSourceConfig:
    """
    预报源配置。

    - 无 sub_sources：单源多尺度（按 time_span_arr 降序处理）
    - 有 sub_sources：多源融合（按 rank 升序处理）
    """

    name: str
    unit_type: str
    time_span_arr: Sequence[int]
    sub_sources: Sequence[SubRainSourceConfig]


@dataclass(frozen=True)
class ForecastRainConfigBundle:
    """
    面雨预报配置集合：
    - selected_source: 当前选中的预报源
    - all_sources: 所有可选预报源
    - distribution_params: 日内分配参数集合
    """

    selected_source: ForecastRainSourceConfig
    all_sources: Sequence[ForecastRainSourceConfig]
    distribution_params: Sequence[RainDistributionParam]


@dataclass(frozen=True)
class ForecastRainPoint:
    """最终整编输出点。"""

    reg_id: str
    time: datetime
    value: float
    min_value: float
    max_value: float


class ForecastRainRepository(Protocol):
    """
    读库协议：返回给定 source/time_span 的记录列表。

    说明：
    - 需在仓储实现中保证“取最新 FTIME”策略（可子查询或分步查询）。
    - 需返回处于读取窗口内的有效记录。
    """

    def fetch_latest_records(
        self,
        *,
        reg_ids: Sequence[str],
        subtype: str,
        time_span_hours: int,
        latest_ftime_begin: datetime,
        latest_ftime_end: datetime,
        read_begin: datetime,
        read_end: datetime,
    ) -> List[ForecastRainRecord]:
        ...


@dataclass(frozen=True)
class CompileRequest:
    """整编请求参数。"""

    forecast_begin: datetime
    forecast_end: datetime
    target_time_type: str  # "Hour" / "Day"
    target_time_step: int
    dbtype: int  # -1 前时标; 0 后时标
    reg_ids: Sequence[str]
    source_config: ForecastRainSourceConfig
    distribution_params: Sequence[RainDistributionParam]
    fluctuate_range: float = 0.0  # 0.1 = +/-10%
    use_min_max_from_db: bool = True
    latest_ftime_lookback_days: int = 6


class MultiSourceArealRainfallCompiler:
    """预报面雨量多源多尺度整编器。"""

    SMALL_RAIN_NAME = "小雨"
    MEDIUM_PLUS_NAME = "中雨以上所有样本"

    def __init__(self, repository: ForecastRainRepository):
        self._repo = repository

    def compile(self, req: CompileRequest) -> List[ForecastRainPoint]:
        if req.target_time_step < 1:
            raise ValueError("target_time_step must be >= 1")
        if req.forecast_end <= req.forecast_begin:
            raise ValueError("forecast_end must be later than forecast_begin")
        if not req.reg_ids:
            return []

        # 1) 降维：初始化 1h 画布（每个 reg_id 一条画布，三组值）
        hour_count = self._hours_between(req.forecast_begin, req.forecast_end)
        data_map: Dict[str, Dict[str, List[float]]] = {
            str(reg): {
                "expected": [0.0] * hour_count,
                "min": [0.0] * hour_count,
                "max": [0.0] * hour_count,
            }
            for reg in req.reg_ids
        }

        latest_begin = req.forecast_begin - timedelta(days=req.latest_ftime_lookback_days)

        # 2) 覆盖写入：先低优先级后高优先级
        source_steps = self._build_source_steps(req.source_config)
        for subtype, span in source_steps:
            records = self._repo.fetch_latest_records(
                reg_ids=[str(x) for x in req.reg_ids],
                subtype=subtype,
                time_span_hours=int(span),
                latest_ftime_begin=latest_begin,
                latest_ftime_end=req.forecast_begin,
                read_begin=req.forecast_begin - timedelta(hours=span),
                read_end=req.forecast_end,
            )
            self._write_records_to_hour_canvas(
                data_map=data_map,
                records=records,
                forecast_begin=req.forecast_begin,
                hour_count=hour_count,
                dbtype=req.dbtype,
                distribution_params=req.distribution_params,
                fluctuate_range=req.fluctuate_range,
                use_min_max_from_db=req.use_min_max_from_db,
            )

        # 3) 升维：按目标步长整编
        if str(req.target_time_type).strip().lower() == "day":
            daily_map = self._hour_to_day_map(data_map, req.forecast_begin, req.forecast_end)
            return self._compile_points(
                value_map=daily_map,
                begin=req.forecast_begin,
                step_unit="day",
                step_size=req.target_time_step,
                dbtype=req.dbtype,
            )
        return self._compile_points(
            value_map=data_map,
            begin=req.forecast_begin,
            step_unit="hour",
            step_size=req.target_time_step,
            dbtype=req.dbtype,
        )

    def _build_source_steps(self, cfg: ForecastRainSourceConfig) -> List[Tuple[str, int]]:
        # 多源融合：按 rank 升序，后写覆盖前写
        if cfg.sub_sources:
            ordered = sorted(cfg.sub_sources, key=lambda x: int(x.rank))
            return [(str(s.subtype), int(s.time_span_hours)) for s in ordered]
        # 单源多尺度：按时间跨度降序，后续小跨度精细覆盖
        spans = sorted((int(x) for x in cfg.time_span_arr), reverse=True)
        return [(str(cfg.unit_type), s) for s in spans]

    def _write_records_to_hour_canvas(
        self,
        *,
        data_map: Dict[str, Dict[str, List[float]]],
        records: Sequence[ForecastRainRecord],
        forecast_begin: datetime,
        hour_count: int,
        dbtype: int,
        distribution_params: Sequence[RainDistributionParam],
        fluctuate_range: float,
        use_min_max_from_db: bool,
    ) -> None:
        for r in records:
            reg = str(r.reg_id)
            if reg not in data_map:
                continue

            wea_begin = r.btime
            wea_end = r.btime + timedelta(hours=int(r.time_span_hours))

            # 过滤无效记录（超窗口 / 预报发布时间异常）
            if r.ftime > wea_end:
                continue
            if forecast_begin > wea_end:
                continue

            span = int(r.time_span_hours)
            # 前后时标对齐：
            # - 前时标（-1）：以时段起点 BTIME 落槽
            # - 后时标（0）：以时段终点（BTIME + span）落槽
            anchor_time = wea_begin if int(dbtype) == -1 else (wea_begin + timedelta(hours=span))
            idx = self._hours_between(forecast_begin, anchor_time)

            avgp = float(r.aver_pre)
            minp = avgp * (1.0 - float(fluctuate_range))
            maxp = avgp * (1.0 + float(fluctuate_range))
            if use_min_max_from_db:
                if r.min_pre is not None:
                    minp = float(r.min_pre)
                if r.max_pre is not None:
                    maxp = float(r.max_pre)
            # 达梦库里常见 -99 作为缺测哨兵；并做物理/区间约束，避免 lower>expected 或 upper<expected。
            if minp < 0:
                minp = 0.0
            if maxp < 0:
                maxp = max(avgp, 0.0)
            avgp = max(avgp, 0.0)
            if minp > avgp:
                minp = avgp
            if maxp < avgp:
                maxp = avgp
            if maxp < minp:
                maxp = minp

            # 降维分配：规则 A（24h + 日内分配）/ 规则 B（均摊）
            distr = self._select_distribution_map(avgp, span, distribution_params)
            for i in range(span):
                pos = idx + i
                if pos < 0:
                    continue
                if pos >= hour_count:
                    break
                if distr is not None:
                    rate = float(distr.get(i + 1, 0.0)) / 100.0
                    v_min = minp * rate
                    v_avg = avgp * rate
                    v_max = maxp * rate
                else:
                    v_min = minp / span
                    v_avg = avgp / span
                    v_max = maxp / span
                # 后发覆盖：直接赋值（不是累加）
                data_map[reg]["min"][pos] = v_min
                data_map[reg]["expected"][pos] = v_avg
                data_map[reg]["max"][pos] = v_max

    def _select_distribution_map(
        self,
        avgp: float,
        span: int,
        params: Sequence[RainDistributionParam],
    ) -> Optional[Mapping[int, float]]:
        if span != 24 or not params:
            return None
        # 与历史逻辑一致：<=9.9 选小雨；>10 选中雨以上样本
        if avgp <= 9.9:
            for p in params:
                if str(p.dis_name).strip() == self.SMALL_RAIN_NAME:
                    return p.dis_scale_map
        if avgp > 10.0:
            for p in params:
                if str(p.dis_name).strip() == self.MEDIUM_PLUS_NAME:
                    return p.dis_scale_map
        # 回退：使用第一个分配曲线
        return params[0].dis_scale_map if params else None

    def _hour_to_day_map(
        self,
        data_map: Dict[str, Dict[str, List[float]]],
        begin: datetime,
        end: datetime,
    ) -> Dict[str, Dict[str, List[float]]]:
        # 按自然日聚合（begin 到 end 覆盖小时）
        hours = self._hours_between(begin, end)
        day_index: Dict[str, int] = {}
        day_order: List[str] = []
        for i in range(hours):
            t = begin + timedelta(hours=i)
            key = t.strftime("%Y-%m-%d")
            if key not in day_index:
                day_index[key] = len(day_order)
                day_order.append(key)
        out: Dict[str, Dict[str, List[float]]] = {}
        for reg, bands in data_map.items():
            n_days = len(day_order)
            exp = [0.0] * n_days
            mn = [0.0] * n_days
            mx = [0.0] * n_days
            for i in range(hours):
                t = begin + timedelta(hours=i)
                di = day_index[t.strftime("%Y-%m-%d")]
                exp[di] += bands["expected"][i]
                mn[di] += bands["min"][i]
                mx[di] += bands["max"][i]
            out[reg] = {"expected": exp, "min": mn, "max": mx}
        return out

    def _compile_points(
        self,
        *,
        value_map: Dict[str, Dict[str, List[float]]],
        begin: datetime,
        step_unit: str,
        step_size: int,
        dbtype: int,
    ) -> List[ForecastRainPoint]:
        points: List[ForecastRainPoint] = []
        if step_size < 1:
            raise ValueError("step_size must be >= 1")
        for reg, bands in value_map.items():
            arr = bands["expected"]
            arr_min = bands["min"]
            arr_max = bands["max"]
            n = len(arr)
            # 前后时标已在写画布阶段完成锚点转换，这里统一按 begin 输出，避免二次平移。
            real_begin = begin
            for j in range(0, n, step_size):
                s0 = 0.0
                s1 = 0.0
                s2 = 0.0
                for k in range(j, min(j + step_size, n)):
                    s0 += arr[k]
                    s1 += arr_min[k]
                    s2 += arr_max[k]
                t = self._add_step(real_begin, step_unit, j)
                points.append(
                    ForecastRainPoint(
                        reg_id=reg,
                        time=t,
                        value=s0,
                        min_value=s1,
                        max_value=s2,
                    )
                )
        return points

    @staticmethod
    def _hours_between(beg: datetime, end: datetime) -> int:
        return int((end - beg).total_seconds() // 3600)

    @staticmethod
    def _add_step(t: datetime, unit: str, step: int) -> datetime:
        if unit == "day":
            return t + timedelta(days=int(step))
        return t + timedelta(hours=int(step))


def parse_forecast_rain_config_from_scheme(scheme_dict: Mapping[str, Any]) -> ForecastRainConfigBundle:
    """
    从 scheme 配置解析“预报面雨量多源配置”。

    支持两种结构：
    1) 推荐 Python 结构（future_rainfall）
    2) Java 迁移兼容结构（future_rain_sources / HFWinFutureRainSource 风格键）
    """
    block = scheme_dict.get("future_rainfall")
    if not isinstance(block, Mapping):
        # 兼容：直接把 source 列表挂在 scheme 下
        block = {
            "sources": scheme_dict.get("future_rain_sources") or [],
            "distribution_params": scheme_dict.get("rain_distribution_params") or [],
        }

    sources_raw = block.get("sources") or []
    if not isinstance(sources_raw, list) or not sources_raw:
        raise ValueError("future_rainfall.sources must be a non-empty list")

    all_sources: List[ForecastRainSourceConfig] = []
    selected_idx: Optional[int] = None

    for i, item in enumerate(sources_raw):
        if not isinstance(item, Mapping):
            raise ValueError(f"future_rainfall.sources[{i}] must be an object")
        name = str(item.get("name") or "").strip()
        if not name:
            raise ValueError(f"future_rainfall.sources[{i}] missing name")

        # 兼容 Java 键名：unitType/timeSpan/subRainSource/isSelect/subType/rank
        unit_type = str(item.get("unit_type", item.get("unitType", "")) or "").strip()
        if not unit_type:
            raise ValueError(f"future_rainfall.sources[{i}] missing unit_type/unitType")

        time_span_raw = item.get("time_span_arr", item.get("timeSpan", []))
        time_span_arr = _parse_int_list(time_span_raw)

        sub_raw = item.get("sub_sources", item.get("subRainSource", [])) or []
        if not isinstance(sub_raw, list):
            raise ValueError(f"future_rainfall.sources[{i}].sub_sources must be a list")
        sub_sources: List[SubRainSourceConfig] = []
        for j, s in enumerate(sub_raw):
            if not isinstance(s, Mapping):
                raise ValueError(f"future_rainfall.sources[{i}].sub_sources[{j}] must be an object")
            sub_sources.append(
                SubRainSourceConfig(
                    sub_source_name=str(s.get("sub_source_name", s.get("subSourceName", "")) or "").strip(),
                    rank=int(s.get("rank", 0)),
                    subtype=str(s.get("subtype", s.get("subType", "")) or "").strip(),
                    time_span_hours=int(s.get("time_span_hours", s.get("timeSpan", 1))),
                )
            )

        source = ForecastRainSourceConfig(
            name=name,
            unit_type=unit_type,
            time_span_arr=time_span_arr,
            sub_sources=sub_sources,
        )
        all_sources.append(source)

        selected = item.get("is_select", item.get("isSelect", False))
        if bool(selected):
            selected_idx = i

    if selected_idx is None:
        selected_name = str(block.get("selected_source_name", "") or "").strip()
        if selected_name:
            for i, s in enumerate(all_sources):
                if s.name == selected_name:
                    selected_idx = i
                    break
    if selected_idx is None:
        selected_idx = 0

    dist_raw = block.get("distribution_params", block.get("rainDistributionParams", [])) or []
    if not isinstance(dist_raw, list):
        raise ValueError("future_rainfall.distribution_params must be a list")
    dist_list: List[RainDistributionParam] = []
    for i, d in enumerate(dist_raw):
        if not isinstance(d, Mapping):
            raise ValueError(f"future_rainfall.distribution_params[{i}] must be an object")
        dis_name = str(d.get("dis_name", d.get("disName", "")) or "").strip()
        dis_map_raw = d.get("dis_scale_map", d.get("disScaleMap", {})) or {}
        if not isinstance(dis_map_raw, Mapping):
            raise ValueError(f"distribution_params[{i}].dis_scale_map must be a mapping")
        dis_map = {int(k): float(v) for k, v in dis_map_raw.items()}
        dist_list.append(RainDistributionParam(dis_name=dis_name, dis_scale_map=dis_map))

    return ForecastRainConfigBundle(
        selected_source=all_sources[selected_idx],
        all_sources=all_sources,
        distribution_params=dist_list,
    )


def _parse_int_list(raw: Any) -> List[int]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [int(x) for x in raw]
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return [int(p) for p in parts]
    return [int(raw)]


@dataclass(frozen=True)
class ForecastDbConfig:
    """
    预报降雨数据库查询配置。

    table_name: 预报降雨表名（默认对应 WEA_GFSFORRAIN）
    """

    url: str
    dialect: str
    table_name: str = "WEA_GFSFORRAIN"
    pool_min: int = 3
    pool_max: int = 7
    prefer_two_step_latest: bool = True


def load_forecast_db_config_from_jdbc_json(
    jdbc_json_path: str | Path,
    *,
    service_name: Optional[str] = None,
    table_name: str = "WEA_GFSFORRAIN",
    prefer_two_step_latest: bool = True,
) -> ForecastDbConfig:
    """
    从 floodForecastJdbc 风格配置解析预报降雨读库参数。
    """
    p = Path(jdbc_json_path)
    if not p.is_file():
        raise FileNotFoundError(f"JDBC config not found: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    services = data.get("services") or []
    if not isinstance(services, list) or not services:
        raise ValueError("jdbc json missing services array")

    svc = str(service_name or data.get("hourly_service") or "").strip()
    if not svc:
        svc = str((services[0] or {}).get("service") or "").strip()
    if not svc:
        raise ValueError("cannot resolve service name from jdbc config")

    found = None
    for item in services:
        if str(item.get("service") or "").strip().upper() == svc.upper():
            found = item
            break
    if found is None:
        raise KeyError(f"service {svc!r} not found in jdbc config")

    url = str(found.get("url") or "").strip()
    if not url:
        raise ValueError(f"service {svc!r} missing url")

    pool_max = int(found.get("maxCon", 7))
    pool_min = int(found.get("minCon", 3))
    if pool_max < pool_min:
        pool_max = pool_min
    dialect = str(data.get("dialect") or "dameng").strip().lower()
    return ForecastDbConfig(
        url=url,
        dialect=dialect,
        table_name=table_name,
        pool_min=pool_min,
        pool_max=pool_max,
        prefer_two_step_latest=bool(prefer_two_step_latest),
    )


class SqlAlchemyForecastRainRepository(ForecastRainRepository):
    """
    基于 SQLAlchemy 的预报降雨仓储。

    支持两种“最新 FTIME”策略：
    - 两步策略（默认）：先查 max(FTIME)，再按 FTIME + 条件查明细
    - 单 SQL 子查询策略：在明细 SQL 中嵌套 max(FTIME) 子查询
    """

    def __init__(self, cfg: ForecastDbConfig):
        self._cfg = cfg
        self._table = cfg.table_name
        self._max_overflow = max(0, int(cfg.pool_max) - int(cfg.pool_min))
        self._engine = get_shared_engine(
            cfg.url,
            pool_min=int(cfg.pool_min),
            max_overflow=self._max_overflow,
        )

    def fetch_latest_records(
        self,
        *,
        reg_ids: Sequence[str],
        subtype: str,
        time_span_hours: int,
        latest_ftime_begin: datetime,
        latest_ftime_end: datetime,
        read_begin: datetime,
        read_end: datetime,
    ) -> List[ForecastRainRecord]:
        ids = [str(x).strip() for x in reg_ids if str(x).strip()]
        if not ids:
            return []
        if self._cfg.prefer_two_step_latest:
            return self._fetch_two_step(
                reg_ids=ids,
                subtype=subtype,
                time_span_hours=time_span_hours,
                latest_ftime_begin=latest_ftime_begin,
                latest_ftime_end=latest_ftime_end,
                read_begin=read_begin,
                read_end=read_end,
            )
        return self._fetch_single_sql(
            reg_ids=ids,
            subtype=subtype,
            time_span_hours=time_span_hours,
            latest_ftime_begin=latest_ftime_begin,
            latest_ftime_end=latest_ftime_end,
            read_begin=read_begin,
            read_end=read_end,
        )

    def _fetch_two_step(
        self,
        *,
        reg_ids: Sequence[str],
        subtype: str,
        time_span_hours: int,
        latest_ftime_begin: datetime,
        latest_ftime_end: datetime,
        read_begin: datetime,
        read_end: datetime,
    ) -> List[ForecastRainRecord]:
        sql_max = text(
            f"""
            SELECT MAX(FTIME) AS FTIME
            FROM {self._table}
            WHERE FTIME >= :ftime_begin
              AND FTIME < :ftime_end
              AND SUBTYPE = :subtype
              AND TIMESPAN = :timespan
            """
        )
        sql_rows = text(
            f"""
            SELECT REGID, SUBTYPE, TIMESPAN, FTIME, BTIME, AVERPRE, MINPRE, MAXPRE
            FROM {self._table}
            WHERE REGID IN :regids
              AND SUBTYPE = :subtype
              AND TIMESPAN = :timespan
              AND FTIME = :max_ftime
              AND BTIME >= :read_begin
              AND BTIME <= :read_end
            ORDER BY REGID, BTIME
            """
        ).bindparams(bindparam("regids", expanding=True))
        with self._engine.connect() as conn:
            max_row = conn.execute(
                sql_max,
                {
                    "ftime_begin": latest_ftime_begin,
                    "ftime_end": latest_ftime_end,
                    "subtype": subtype,
                    "timespan": int(time_span_hours),
                },
            ).first()
            max_ftime = None if max_row is None else max_row[0]
            if max_ftime is None:
                return []
            rows = conn.execute(
                sql_rows,
                {
                    "regids": list(reg_ids),
                    "subtype": subtype,
                    "timespan": int(time_span_hours),
                    "max_ftime": max_ftime,
                    "read_begin": read_begin,
                    "read_end": read_end,
                },
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def _fetch_single_sql(
        self,
        *,
        reg_ids: Sequence[str],
        subtype: str,
        time_span_hours: int,
        latest_ftime_begin: datetime,
        latest_ftime_end: datetime,
        read_begin: datetime,
        read_end: datetime,
    ) -> List[ForecastRainRecord]:
        sql = text(
            f"""
            SELECT REGID, SUBTYPE, TIMESPAN, FTIME, BTIME, AVERPRE, MINPRE, MAXPRE
            FROM {self._table}
            WHERE REGID IN :regids
              AND SUBTYPE = :subtype
              AND TIMESPAN = :timespan
              AND FTIME = (
                  SELECT MAX(FTIME)
                  FROM {self._table}
                  WHERE FTIME >= :ftime_begin
                    AND FTIME < :ftime_end
                    AND SUBTYPE = :subtype
                    AND TIMESPAN = :timespan
              )
              AND BTIME >= :read_begin
              AND BTIME <= :read_end
            ORDER BY REGID, BTIME
            """
        ).bindparams(bindparam("regids", expanding=True))
        with self._engine.connect() as conn:
            rows = conn.execute(
                sql,
                {
                    "regids": list(reg_ids),
                    "subtype": subtype,
                    "timespan": int(time_span_hours),
                    "ftime_begin": latest_ftime_begin,
                    "ftime_end": latest_ftime_end,
                    "read_begin": read_begin,
                    "read_end": read_end,
                },
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    @staticmethod
    def _row_to_record(row: Mapping[str, object] | Tuple[object, ...]) -> ForecastRainRecord:
        # SQLAlchemy Row 同时支持键访问与位置访问
        def _v(name: str, idx: int) -> object:
            try:
                return row[name]  # type: ignore[index]
            except Exception:
                return row[idx]  # type: ignore[index]

        return ForecastRainRecord(
            reg_id=str(_v("REGID", 0)),
            subtype=str(_v("SUBTYPE", 1)),
            time_span_hours=int(_v("TIMESPAN", 2)),
            ftime=_v("FTIME", 3),  # type: ignore[arg-type]
            btime=_v("BTIME", 4),  # type: ignore[arg-type]
            aver_pre=float(_v("AVERPRE", 5)),
            min_pre=None if _v("MINPRE", 6) is None else float(_v("MINPRE", 6)),
            max_pre=None if _v("MAXPRE", 7) is None else float(_v("MAXPRE", 7)),
        )


__all__ = [
    "ForecastRainRecord",
    "RainDistributionParam",
    "SubRainSourceConfig",
    "ForecastRainSourceConfig",
    "ForecastRainPoint",
    "ForecastRainRepository",
    "CompileRequest",
    "MultiSourceArealRainfallCompiler",
    "ForecastRainConfigBundle",
    "parse_forecast_rain_config_from_scheme",
    "ForecastDbConfig",
    "load_forecast_db_config_from_jdbc_json",
    "SqlAlchemyForecastRainRepository",
]

