"""
新安江系列模型率定上下界 — 与 ``HFXAJAlg.java`` / ``HFXAJCSAlg.java`` 中
``m_douParaDBArr``（下界）、``m_douParaUBArr``（上界）、``m_douParaIsCaliArr``（是否参与率定）一致。

说明：Java 中对部分初值与面积给 ``0,0`` 上下界，表示界面中不按区间率定；自动率定时可跳过这些量或另行设定物理范围。
滞时 ``lag`` 在 Java 中未显式写入 ``intParaDBArr/UBArr``，此处给出常用整数步长范围供率定使用。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class CalibrationBounds:
    """单参数率定区间与是否参与率定（对齐 Java ``m_douParaIsCaliArr``）。"""

    lower: float
    upper: float
    included_in_calibration: bool = False


@dataclass(frozen=True)
class IntCalibrationBounds:
    """整型参数（如滞时步数）率定区间。"""

    lower: int
    upper: int
    included_in_calibration: bool = False


def clip_scalar(value: float, bounds: CalibrationBounds) -> float:
    """将标量限制在 ``[lower, upper]``；若 Java 为 ``0,0`` 无有效区间则原样返回。"""
    if bounds.lower == 0.0 and bounds.upper == 0.0:
        return value
    return min(max(value, bounds.lower), bounds.upper)


def clip_int(value: int, bounds: IntCalibrationBounds) -> int:
    if bounds.lower > bounds.upper:
        return value
    return min(max(value, bounds.lower), bounds.upper)


# --- HFXAJAlg：与 m_douParaArr[0..12] + [20] 及初值 [13..19] 对齐 ---

XINANJIANG_PARAM_BOUNDS: Dict[str, CalibrationBounds] = {
    "wum": CalibrationBounds(0.0, 100.0, False),
    "wlm": CalibrationBounds(5.0, 150.0, False),
    "wdm": CalibrationBounds(10.0, 200.0, False),
    "k": CalibrationBounds(0.2, 1.5, True),
    "c": CalibrationBounds(0.0, 0.3, False),
    "b": CalibrationBounds(0.1, 2.0, False),
    "imp": CalibrationBounds(0.0, 0.5, False),
    "sm": CalibrationBounds(0.0, 100.0, True),
    "ex": CalibrationBounds(0.5, 2.0, False),
    "kss": CalibrationBounds(0.1, 0.7, True),
    "kg": CalibrationBounds(0.1, 0.7, True),
    "kkss": CalibrationBounds(0.5, 1.0, True),
    "kkg": CalibrationBounds(0.6, 1.0, True),
    "area": CalibrationBounds(0.0, 0.0, False),
}

XINANJIANG_STATE_BOUNDS: Dict[str, CalibrationBounds] = {
    "wu": CalibrationBounds(0.0, 0.0, False),
    "wl": CalibrationBounds(0.0, 0.0, False),
    "wd": CalibrationBounds(0.0, 0.0, False),
    "fr": CalibrationBounds(0.0, 0.0, False),
    "s": CalibrationBounds(0.0, 0.0, False),
    "qrss0": CalibrationBounds(0.0, 0.0, False),
    "qrg0": CalibrationBounds(0.0, 0.0, False),
}

# 单位线三段：Java 默认 [0.2,0.7,0.1]，原类中无单独 DB/UB，率定时常取 [0,1] 且和为 1 约束在目标函数中处理
XINANJIANG_UNIT_GRAPH_BOUNDS: tuple[CalibrationBounds, CalibrationBounds, CalibrationBounds] = (
    CalibrationBounds(0.0, 1.0, False),
    CalibrationBounds(0.0, 1.0, False),
    CalibrationBounds(0.0, 1.0, False),
)


# --- HFXAJCSAlg：m_douParaArr[0..13] + [22] 及初值 [14..21]；滞时 int ---

XINANJIANG_CS_LAG_BOUNDS: IntCalibrationBounds = IntCalibrationBounds(
    lower=0,
    upper=48,
    included_in_calibration=True,
)
"""Java 未写入 int 上下界；此处为「步数」常用率定范围，可按流域修改。"""

XINANJIANG_CS_PARAM_BOUNDS: Dict[str, CalibrationBounds] = {
    "wum": CalibrationBounds(0.0, 100.0, False),
    "wlm": CalibrationBounds(5.0, 150.0, False),
    "wdm": CalibrationBounds(10.0, 200.0, False),
    "k": CalibrationBounds(0.2, 1.5, True),
    "c": CalibrationBounds(0.0, 0.3, False),
    "b": CalibrationBounds(0.1, 2.0, False),
    "imp": CalibrationBounds(0.0, 0.5, False),
    "sm": CalibrationBounds(0.0, 100.0, True),
    "ex": CalibrationBounds(0.5, 2.0, False),
    "kss": CalibrationBounds(0.1, 0.7, True),
    "kg": CalibrationBounds(0.1, 0.7, True),
    "kkss": CalibrationBounds(0.5, 1.0, True),
    "kkg": CalibrationBounds(0.6, 1.0, True),
    "cs": CalibrationBounds(0.4, 1.0, False),
    "area": CalibrationBounds(0.0, 0.0, False),
}

XINANJIANG_CS_STATE_BOUNDS: Dict[str, CalibrationBounds] = {
    "wu": CalibrationBounds(0.0, 0.0, False),
    "wl": CalibrationBounds(0.0, 0.0, False),
    "wd": CalibrationBounds(0.0, 0.0, False),
    "fr": CalibrationBounds(0.0, 0.0, False),
    "s": CalibrationBounds(0.0, 0.0, False),
    "qrss0": CalibrationBounds(0.0, 0.0, False),
    "qrg0": CalibrationBounds(0.0, 0.0, False),
    "qs0": CalibrationBounds(0.0, 0.0, False),
}


def calibration_vector_bounds_xinanjiang() -> tuple[list[float], list[float], list[bool]]:
    """
    按固定字段顺序（wum, wlm, wdm, k, c, b, imp, sm, ex, kss, kg, kkss, kkg, area）
    返回 (lower_list, upper_list, included_list)，供优化器打包使用。
    """
    order = (
        "wum",
        "wlm",
        "wdm",
        "k",
        "c",
        "b",
        "imp",
        "sm",
        "ex",
        "kss",
        "kg",
        "kkss",
        "kkg",
        "area",
    )
    lowers: list[float] = []
    uppers: list[float] = []
    flags: list[bool] = []
    for k in order:
        b = XINANJIANG_PARAM_BOUNDS[k]
        lowers.append(b.lower)
        uppers.append(b.upper)
        flags.append(b.included_in_calibration)
    return lowers, uppers, flags


def calibration_vector_bounds_xinanjiang_cs() -> tuple[list[float], list[float], list[bool]]:
    """顺序：wum, wlm, wdm, k, c, b, imp, sm, ex, kss, kg, kkss, kkg, cs, area（不含 lag，lag 见 ``XINANJIANG_CS_LAG_BOUNDS``）。"""
    order = (
        "wum",
        "wlm",
        "wdm",
        "k",
        "c",
        "b",
        "imp",
        "sm",
        "ex",
        "kss",
        "kg",
        "kkss",
        "kkg",
        "cs",
        "area",
    )
    lowers: list[float] = []
    uppers: list[float] = []
    flags: list[bool] = []
    for k in order:
        b = XINANJIANG_CS_PARAM_BOUNDS[k]
        lowers.append(b.lower)
        uppers.append(b.upper)
        flags.append(b.included_in_calibration)
    return lowers, uppers, flags
