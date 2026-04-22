"""
实况末态传递 -> 预报面雨多情景 -> 预报流量多情景 的核心骨架串联。

说明：
- 产流模型若带可热启动状态（如 :class:`~hydro_engine.models.runoff.xinanjiang.XinanjiangRunoffModel`），
  在实况期 ``run`` 结束后会快照 ``state``，预报期每个情景在**克隆模型 + 恢复快照**后独立演算。
- 无状态模型（如 :class:`~hydro_engine.models.runoff.dummy.DummyRunoffModel`）则快照为空，各情景使用相同参数克隆即可。
"""

from __future__ import annotations

import copy
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.forecast.catchment_forecast_rainfall import CatchmentForecastRainfall
from hydro_engine.forecast.forecast_data_manager import ForecastDataManager
from hydro_engine.models.runoff.xinanjiang import XinanjiangRunoffModel, XinanjiangState
from hydro_engine.models.runoff.xinanjiang_cs import XinanjiangCSRunoffModel, XinanjiangCSState


@dataclass(frozen=True)
class RunoffWarmstartSnapshot:
    """
    实况期结束时的产流模型末态（可序列化字典）。

    ``variant``:
    - ``xinanjiang`` / ``xinanjiang_cs``：对应模型 ``state`` 字段
    - ``none``：无状态或未识别模型
    """

    variant: str
    payload: Dict[str, Any]


def capture_runoff_warmstart(model: IHydrologicalModel) -> RunoffWarmstartSnapshot:
    """在实况 ``run`` 之后调用，抽取末态。"""
    if isinstance(model, XinanjiangRunoffModel):
        return RunoffWarmstartSnapshot("xinanjiang", {"state": asdict(model.state)})
    if isinstance(model, XinanjiangCSRunoffModel):
        return RunoffWarmstartSnapshot("xinanjiang_cs", {"state": asdict(model.state)})
    return RunoffWarmstartSnapshot("none", {})


def apply_runoff_warmstart(model: IHydrologicalModel, snap: RunoffWarmstartSnapshot) -> None:
    """将 ``snap`` 写回模型（用于预报情景分支前的初始化）。"""
    if snap.variant == "none":
        return
    if snap.variant == "xinanjiang" and isinstance(model, XinanjiangRunoffModel):
        # 产流模型本体为 frozen dataclass：不能整体替换 ``state``，逐字段写回末态。
        src = XinanjiangState(**snap.payload["state"])
        for fld in fields(src):
            setattr(model.state, fld.name, getattr(src, fld.name))
        return
    if snap.variant == "xinanjiang_cs" and isinstance(model, XinanjiangCSRunoffModel):
        src_cs = XinanjiangCSState(**snap.payload["state"])
        for fld in fields(src_cs):
            setattr(model.state, fld.name, getattr(src_cs, fld.name))
        return
    raise TypeError(
        f"末态 variant={snap.variant!r} 与模型类型 {type(model).__name__} 不匹配，无法热启动"
    )


def _build_forecast_forcing(
    *,
    rain_values: List[float],
    forecast_start: datetime,
    time_step,
    model: IHydrologicalModel,
    forecast_pet: Optional[TimeSeries],
    last_hist_pet: Optional[float],
) -> ForcingData:
    """根据模型契约组装预报段强迫（当前骨架仅处理降雨 + 可选 PET）。"""
    req = model.required_inputs()
    p_ts = TimeSeries(forecast_start, time_step, [float(x) for x in rain_values])

    if ForcingKind.POTENTIAL_EVAPOTRANSPIRATION in req:
        if forecast_pet is not None:
            pet_ts = forecast_pet
        elif last_hist_pet is not None:
            pet_ts = TimeSeries(
                forecast_start,
                time_step,
                [float(last_hist_pet)] * len(rain_values),
            )
        else:
            raise ValueError(
                "预报期需要 PET：请提供 forecast_pet（TimeSeries）、"
                "或在历史强迫中包含 PET 以便用末值常数延拓"
            )
        if pet_ts.time_steps != len(rain_values):
            raise ValueError(
                f"预报 PET 长度 {pet_ts.time_steps} 与降雨长度 {len(rain_values)} 不一致"
            )
        if pet_ts.start_time != forecast_start or pet_ts.time_step != time_step:
            raise ValueError("预报 PET 的 start_time / time_step 必须与预报降雨轴一致")
        return ForcingData.from_pairs(
            [
                (ForcingKind.PRECIPITATION, p_ts),
                (ForcingKind.POTENTIAL_EVAPOTRANSPIRATION, pet_ts),
            ]
        )

    return ForcingData.single(ForcingKind.PRECIPITATION, p_ts)


def _last_hist_pet_value(historical_forcing: ForcingData) -> Optional[float]:
    pet = historical_forcing.get(ForcingKind.POTENTIAL_EVAPOTRANSPIRATION)
    if pet is None:
        return None
    return float(np.asarray(pet.values, dtype=np.float64).reshape(-1)[-1])


def run_forecast_pipeline(
    *,
    runoff_model: IHydrologicalModel,
    historical_forcing: ForcingData,
    forecast_rainfall: CatchmentForecastRainfall,
    forecast_pet: Optional[TimeSeries] = None,
) -> pd.DataFrame:
    """
    三阶段骨架：

    1. 实况期：在 ``runoff_model`` 的独立克隆上运行 ``historical_forcing``，捕获末态 ``final_state``。
    2. 预报期：对 ``expected`` / ``upper`` / ``lower`` 分别克隆**参数初值**模型、恢复快照、运行预报强迫。
    3. 组装：返回以预报时间索引为 index 的多情景流量列。

    Args:
        runoff_model: 产流模型模板（不会被本函数就地改写；内部使用 ``deepcopy``）。
        historical_forcing: 实况期强迫（长度与网格由调用方保证与模型一致）。
        forecast_rainfall: 预报面雨三情景（时间轴与三条序列已校验一致）。
        forecast_pet: 可选，预报期 PET 序列；缺省则用实况末时刻 PET 常数延拓。

    Returns:
        DataFrame，索引为预报 ``time_index``，列 ``Q_expected`` / ``Q_upper`` / ``Q_lower``（m³/s 或模型输出单位）。
    """
    prototype = copy.deepcopy(runoff_model)
    hist_model = copy.deepcopy(prototype)
    hist_model.run(historical_forcing)
    final_state: RunoffWarmstartSnapshot = capture_runoff_warmstart(hist_model)

    last_pet = _last_hist_pet_value(historical_forcing)
    f_start = forecast_rainfall.time_index[0].to_pydatetime()
    step = forecast_rainfall.time_step

    scenarios: Dict[str, List[float]] = {
        "expected": list(forecast_rainfall.expected),
        "upper": list(forecast_rainfall.upper),
        "lower": list(forecast_rainfall.lower),
    }
    flows: Dict[str, List[float]] = {}

    for name, rseries in scenarios.items():
        branch = copy.deepcopy(prototype)
        apply_runoff_warmstart(branch, final_state)
        frc = _build_forecast_forcing(
            rain_values=rseries,
            forecast_start=f_start,
            time_step=step,
            model=branch,
            forecast_pet=forecast_pet,
            last_hist_pet=last_pet,
        )
        out_ts = branch.run(frc)
        if out_ts.time_steps != len(rseries):
            raise RuntimeError(
                f"情景 {name}: 模型输出长度 {out_ts.time_steps} 与预报步数 {len(rseries)} 不一致"
            )
        flows[name] = [float(x) for x in out_ts.values.tolist()]

    return pd.DataFrame(
        {
            "Q_expected": flows["expected"],
            "Q_upper": flows["upper"],
            "Q_lower": flows["lower"],
        },
        index=forecast_rainfall.time_index,
    )


def run_forecast_pipeline_from_mock_csv(
    *,
    runoff_model: IHydrologicalModel,
    historical_forcing: ForcingData,
    mock_csv: str | Path,
    catchment_id: str,
    data_manager: Optional[ForecastDataManager] = None,
) -> pd.DataFrame:
    """
    便捷入口：用 :class:`ForecastDataManager.get_forecast_from_mock_file` 读本地 CSV，
    再调用 :func:`run_forecast_pipeline`。

    Mock CSV 可含可选列 ``pet``；若存在则作为预报期 PET（长度须与预报步数一致）。
    """
    mgr = data_manager or ForecastDataManager()
    rain_pkg = mgr.get_forecast_from_mock_file(mock_csv, catchment_id=catchment_id)
    pet_list = mgr.read_optional_pet_column(mock_csv)
    forecast_pet: Optional[TimeSeries] = None
    if pet_list is not None:
        if len(pet_list) != len(rain_pkg.time_index):
            raise ValueError("Mock CSV 的 pet 列长度与预报时间步数不一致")
        forecast_pet = TimeSeries(
            rain_pkg.time_index[0].to_pydatetime(),
            rain_pkg.time_step,
            [float(x) for x in pet_list],
        )
    return run_forecast_pipeline(
        runoff_model=runoff_model,
        historical_forcing=historical_forcing,
        forecast_rainfall=rain_pkg,
        forecast_pet=forecast_pet,
    )
