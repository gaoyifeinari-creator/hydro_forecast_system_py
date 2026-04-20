"""
将「预报面雨三情景 CSV」注入引擎已合成的子流域强迫（仅改预报段）。

供 :func:`hydro_engine.io.json_config.run_calculation_from_json` 在合成 ``catchment_forcing`` 之后调用。
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Literal, Optional

import pandas as pd

from hydro_engine.core.context import ForecastTimeContext
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.forecast.catchment_forecast_rainfall import CatchmentForecastRainfall
from hydro_engine.forecast.forecast_data_manager import ForecastDataManager


ScenarioName = Literal["expected", "upper", "lower"]


def _forecast_start_index(time_context: ForecastTimeContext) -> int:
    td = time_context.time_delta
    delta = time_context.forecast_start_time - time_context.warmup_start_time
    return int(delta / td)


def patch_catchment_scenario_precipitation(
    catchment_forcing: Dict[str, ForcingData],
    *,
    time_context: ForecastTimeContext,
    catchment_id: str,
    rainfall: CatchmentForecastRainfall,
    scenario: ScenarioName,
) -> None:
    """
    将 ``rainfall`` 中指定情景（``expected``/``upper``/``lower``）的预报面雨量
    写入 ``catchment_forcing[catchment_id]`` 的 **预报段**（自 ``forecast_start_time`` 起至序列末），
    历史段保持合成结果不变。

    若 ``rainfall.pet`` 非空，则同步覆写预报段的 ``POTENTIAL_EVAPOTRANSPIRATION``。
    """
    cid = str(catchment_id).strip()
    if cid not in catchment_forcing:
        raise KeyError(f"子流域 {cid!r} 不在 catchment_forcing 中，无法注入情景面雨")

    fd = catchment_forcing[cid]
    p_ts = fd.get(ForcingKind.PRECIPITATION)
    if p_ts is None:
        raise ValueError(f"子流域 {cid} 强迫中缺少 precipitation，无法注入情景面雨")

    vals = list(p_ts.values)
    fs_idx = _forecast_start_index(time_context)
    n_fore = len(vals) - fs_idx
    if n_fore <= 0:
        raise ValueError("预报段步数为 0：请检查 time_context 与强迫序列长度")

    scen_vals: List[float] = list(getattr(rainfall, scenario))
    if len(scen_vals) != n_fore:
        raise ValueError(
            f"子流域 {cid} 情景 {scenario!r} 步数 {len(scen_vals)} 与引擎预报段步数 {n_fore} 不一致"
        )

    t0_engine = pd.Timestamp(time_context.forecast_start_time)
    t0_csv = pd.Timestamp(rainfall.time_index[0])
    if t0_engine != t0_csv:
        raise ValueError(
            f"子流域 {cid}: 情景 CSV 首时刻 {t0_csv} 须等于引擎 forecast_start_time {t0_engine}"
        )

    new_p_vals = vals[:fs_idx] + [float(x) for x in scen_vals]
    new_fd = fd.with_series(
        ForcingKind.PRECIPITATION,
        TimeSeries(p_ts.start_time, p_ts.time_step, new_p_vals),
    )

    if rainfall.pet is not None:
        pet_list = list(rainfall.pet)
        if len(pet_list) != n_fore:
            raise ValueError(
                f"子流域 {cid}: pet 列长度 {len(pet_list)} 与预报段步数 {n_fore} 不一致"
            )
        pet_ts = new_fd.get(ForcingKind.POTENTIAL_EVAPOTRANSPIRATION)
        if pet_ts is None:
            raise ValueError(
                f"子流域 {cid}: 情景数据含 pet，但合成强迫中缺少 potential_evapotranspiration，无法覆写"
            )
        pet_full = list(pet_ts.values)
        if len(pet_full) != len(new_p_vals):
            raise ValueError("PET 序列长度与降水序列长度不一致")
        new_pet_vals = pet_full[:fs_idx] + [float(x) for x in pet_list]
        new_fd = new_fd.with_series(
            ForcingKind.POTENTIAL_EVAPOTRANSPIRATION,
            TimeSeries(pet_ts.start_time, pet_ts.time_step, new_pet_vals),
        )

    catchment_forcing[cid] = new_fd


def load_catchment_forecast_rainfall_map_from_csv(
    filepath: str | Path,
    *,
    default_catchment_ids: Optional[List[str]] = None,
    encoding: str = "utf-8",
) -> Dict[str, CatchmentForecastRainfall]:
    """
    从 CSV 加载一个或多个子流域的情景面雨。

    - 若存在 ``catchment_id`` 列（不区分大小写）：按列分组，每组构造一个
      :class:`~hydro_engine.forecast.catchment_forecast_rainfall.CatchmentForecastRainfall`。
    - 否则：必须在 ``default_catchment_ids`` 中提供**恰好一个**子流域 id，整表对应该流域。
    """
    path = Path(filepath)
    if not path.is_file():
        raise FileNotFoundError(f"预报面雨 CSV 不存在: {path}")

    df = pd.read_csv(path, encoding=encoding)
    if df.empty:
        raise ValueError(f"预报面雨 CSV 为空: {path}")

    cols = {c.lower(): c for c in df.columns}
    mgr = ForecastDataManager()

    if "catchment_id" in cols:
        cid_col = cols["catchment_id"]
        out: Dict[str, CatchmentForecastRainfall] = {}
        for raw_cid, sub in df.groupby(cid_col, sort=False):
            cid = str(raw_cid).strip()
            if not cid:
                continue
            sub2 = sub.drop(columns=[cid_col]).reset_index(drop=True)
            out[cid] = mgr.forecast_rainfall_from_dataframe(sub2, catchment_id=cid)
        if not out:
            raise ValueError("按 catchment_id 分组后未得到有效子流域数据")
        return out

    ids = [x.strip() for x in (default_catchment_ids or []) if str(x).strip()]
    if len(ids) != 1:
        raise ValueError(
            "CSV 未包含 catchment_id 列时，必须在参数 default_catchment_ids 中指定且仅指定一个子流域 id"
        )
    cid = ids[0]
    return {cid: mgr.forecast_rainfall_from_dataframe(df, catchment_id=cid)}
