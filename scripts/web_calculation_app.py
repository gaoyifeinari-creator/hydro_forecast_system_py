from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from calculation_app_common import (
    PROJECT_ROOT,
    build_observed_flows,
    build_station_packages,
    build_times,
    load_csv as _load_csv_base,
    write_temp_config_with_periods,
)
from hydro_engine.io.json_config import load_scheme_from_json, run_calculation_from_json


@st.cache_data(show_spinner=False)
def _load_csv(path: str) -> pd.DataFrame:
    return _load_csv_base(path)


def _plot_series_group(title: str, data: Dict[str, List[float]], times: pd.DatetimeIndex) -> None:
    st.subheader(title)
    if not data:
        st.info("无数据")
        return
    for key, values in data.items():
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=times, y=values, mode="lines", name=str(key)))
        fig.update_layout(height=260, margin=dict(l=10, r=10, t=30, b=10), title=str(key))
        st.plotly_chart(fig, use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="Hydro Calculation Test", layout="wide")
    st.title("计算测试网页")

    default_cfg = str(PROJECT_ROOT / "configs" / "forecastSchemeConf.json")
    default_rain = str(PROJECT_ROOT / "tests" / "佛子岭雨量.csv")
    default_flow = str(PROJECT_ROOT / "tests" / "佛子岭流量.csv")

    with st.sidebar:
        st.header("输入设置")
        config_path = st.text_input("预报方案配置文件", value=default_cfg)
        rain_csv = st.text_input("雨量 CSV（V 字段）", value=default_rain)
        flow_csv = st.text_input("流量 CSV（AVGV 字段）", value=default_flow)

        warmup_start = st.text_input("计算开始时间", value="2024-01-01 01:00:00")
        time_type = st.selectbox("时间类型", options=["Hour", "Day", "Minute"], index=0)
        step_size = st.number_input("步长", min_value=1, value=1, step=1)

        warmup_steps = st.number_input("预热步数", min_value=0, value=0, step=1)
        correction_steps = st.number_input("校正步数", min_value=0, value=0, step=1)
        historical_steps = st.number_input("历史展示步数", min_value=0, value=0, step=1)
        forecast_steps = st.number_input("预报步数", min_value=1, value=24, step=1)

        run_btn = st.button("开始计算", type="primary")

    if not run_btn:
        st.info("请在左侧设置参数后点击“开始计算”。")
        return

    try:
        warmup_start_dt = datetime.fromisoformat(warmup_start.replace("T", " "))
        temp_cfg = write_temp_config_with_periods(
            config_path,
            time_type=time_type,
            step_size=int(step_size),
            warmup_steps=int(warmup_steps),
            correction_steps=int(correction_steps),
            historical_steps=int(historical_steps),
            forecast_steps=int(forecast_steps),
        )

        scheme, binding_specs, time_context = load_scheme_from_json(
            temp_cfg,
            time_type=time_type,
            step_size=int(step_size),
            warmup_start_time=warmup_start_dt,
        )

        times = build_times(
            context_start=time_context.warmup_start_time,
            step=time_context.time_delta,
            count=time_context.step_count,
        )

        rain_df = _load_csv(rain_csv)
        flow_df = _load_csv(flow_csv)

        station_packages, warn_a = build_station_packages(
            binding_specs,
            rain_df,
            times,
            time_context.warmup_start_time,
            time_context.time_delta,
        )
        observed_flows, warn_b = build_observed_flows(
            scheme,
            flow_df,
            times,
            time_context.warmup_start_time,
            time_context.time_delta,
        )

        output = run_calculation_from_json(
            config_path=temp_cfg,
            station_packages=station_packages,
            time_type=time_type,
            step_size=int(step_size),
            warmup_start_time=warmup_start_dt,
            observed_flows=observed_flows,
        )

        st.success("计算完成")
        for w in warn_a + warn_b:
            st.warning(w)

        tab1, tab2, tab3 = st.tabs(["Node 流量过程", "Catchment 流量过程", "Reach 流量过程"])
        with tab1:
            _plot_series_group("Node Total Inflows", output.get("node_total_inflows", {}), times)
        with tab2:
            _plot_series_group("Catchment Runoffs", output.get("catchment_runoffs", {}), times)
            _plot_series_group("Catchment Routed Flows", output.get("catchment_routed_flows", {}), times)
        with tab3:
            _plot_series_group("Reach Flows", output.get("reach_flows", {}), times)

        with st.expander("原始输出 JSON"):
            st.json(output)

    except Exception as exc:  # noqa: BLE001
        st.error(f"计算失败: {exc}")


if __name__ == "__main__":
    main()
