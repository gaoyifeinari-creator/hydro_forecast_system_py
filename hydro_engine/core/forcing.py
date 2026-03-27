from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, Iterator, Mapping, Set, Tuple

from hydro_engine.core.timeseries import TimeSeries


class ForcingKind(str, Enum):
    """
    标准强迫量标识（注册表）。

    新增要素：仅在此枚举中增加成员，并在测站/绑定 JSON 中使用相同字符串值。
    ForcingData 本身为通用字典容器，不随要素种类扩展而修改类体。
    """

    PRECIPITATION = "precipitation"
    POTENTIAL_EVAPOTRANSPIRATION = "potential_evapotranspiration"
    AIR_TEMPERATURE = "air_temperature"
    SOIL_MOISTURE = "soil_moisture"
    SNOW_DEPTH = "snow_depth"
    SOLAR_RADIATION = "solar_radiation"
    WIND_SPEED = "wind_speed"
    # 河道演进：节点汇流后的入流序列（由引擎注入，非气象观测）
    ROUTING_INFLOW = "routing_inflow"


def parse_forcing_kind(value: str) -> ForcingKind:
    try:
        return ForcingKind(value)
    except ValueError as exc:
        known = ", ".join(sorted(k.value for k in ForcingKind))
        raise ValueError(f"Unknown forcing_kind '{value}'. Known kinds: {known}") from exc


@dataclass
class ForcingData:
    """
    多维强迫数据容器：内部为 ForcingKind -> TimeSeries 映射，禁止为每种要素硬编码属性。
    """

    _series: Dict[ForcingKind, TimeSeries] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_series", dict(self._series))

    @classmethod
    def empty(cls) -> ForcingData:
        return cls({})

    @classmethod
    def single(cls, kind: ForcingKind, series: TimeSeries) -> ForcingData:
        return cls({kind: series})

    @classmethod
    def from_pairs(cls, pairs: Iterable[Tuple[ForcingKind, TimeSeries]]) -> ForcingData:
        data: Dict[ForcingKind, TimeSeries] = {}
        for kind, series in pairs:
            if kind in data:
                raise ValueError(f"Duplicate forcing kind in same package: {kind}")
            data[kind] = series
        return cls(data)

    def with_series(self, kind: ForcingKind, series: TimeSeries) -> ForcingData:
        """返回不可变语义的新容器（拷贝并覆盖/新增一个键）。"""
        new_map = dict(self._series)
        new_map[kind] = series
        return ForcingData(new_map)

    def merge(self, other: ForcingData) -> ForcingData:
        """合并两个容器；重复键以 other 为准。"""
        new_map = dict(self._series)
        new_map.update(other._series)
        return ForcingData(new_map)

    def require(self, kind: ForcingKind) -> TimeSeries:
        if kind not in self._series:
            raise KeyError(f"Missing forcing series for kind: {kind.value}")
        return self._series[kind]

    def get(self, kind: ForcingKind, default: TimeSeries | None = None) -> TimeSeries | None:
        return self._series.get(kind, default)

    def keys(self) -> Set[ForcingKind]:
        return set(self._series.keys())

    def items(self) -> Iterator[Tuple[ForcingKind, TimeSeries]]:
        return iter(self._series.items())

    def __contains__(self, kind: ForcingKind) -> bool:
        return kind in self._series

    def __len__(self) -> int:
        return len(self._series)

    def as_mapping(self) -> Mapping[ForcingKind, TimeSeries]:
        return self._series


def validate_forcing_contract(model: Any, forcing: ForcingData) -> None:
    """
    Fail-fast：检查强迫数据是否满足模型契约（required_inputs）。
    """
    required = model.required_inputs()
    present = forcing.keys()
    missing = required - present
    if missing:
        missing_str = ", ".join(sorted(k.value for k in missing))
        raise ValueError(
            f"Model {type(model).__name__} missing required forcing kinds: {missing_str}"
        )


def validate_station_package_covers_binding(
    station_id: str,
    kind: ForcingKind,
    package: ForcingData,
) -> None:
    if kind not in package:
        raise ValueError(
            f"Station '{station_id}' does not provide forcing '{kind.value}' in its package"
        )
