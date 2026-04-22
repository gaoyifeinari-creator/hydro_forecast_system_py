"""
新安江产流模型 — 与 ``HFXAJAlg.java``（Run 方法）数值与变量命名对齐。

面雨量 P、蒸发 E 单位：mm/时段；面积 area：km²；输出为经过单位线卷积后的出口流量（与 Java ``m_runRes`` 一致）。

率定上下界与 Java ``m_douParaDBArr`` / ``m_douParaUBArr`` / ``m_douParaIsCaliArr`` 见
:mod:`hydro_engine.models.runoff.calibration_bounds`（``XINANJIANG_PARAM_BOUNDS`` 等）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Sequence, Tuple

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries


@dataclass(frozen=True)
class XinanjiangParams:
    """与 Java ``m_douParaArr[0..12]`` + ``[20]`` 一致；``[13..19]`` 见 :class:`XinanjiangState`。"""

    wum: float = 20.0
    wlm: float = 40.0
    wdm: float = 40.0
    k: float = 0.8
    c: float = 0.1
    b: float = 0.3
    imp: float = 0.02
    sm: float = 30.0
    ex: float = 1.2
    kss: float = 0.4
    kg: float = 0.3
    kkss: float = 0.9
    kkg: float = 0.95
    area: float = 0.0
    unit_graph: Tuple[float, float, float] = (0.2, 0.7, 0.1)


@dataclass
class XinanjiangState:
    """与 Java ``m_douParaArr[13..19]`` 初始值一致；每次 ``run`` 结束写回末时刻状态。"""

    wu: float = 5.0
    wl: float = 10.0
    wd: float = 20.0
    fr: float = 0.01
    s: float = 6.0
    qrss0: float = 18.0
    qrg0: float = 20.0


def _discretization_java(time_step_seconds: float) -> Tuple[float, float]:
    """
    对应 Java ``Run`` 中 ``switch (timeType)``：
    - RSDay: D=24, Dtt=Dt*24（此处 Dtt 以小时计，一步为 1 日时）
    - RSHour / default: D = 24/Dt, Dtt = Dt（小时）
    """
    dt_hours = time_step_seconds / 3600.0
    if dt_hours <= 0:
        raise ValueError("time_step must be positive")
    if dt_hours >= 23.99:
        return 24.0, 24.0
    return 24.0 / dt_hours, dt_hours


def _unit_hydrograph_convolve(qtr: List[float], uh: Sequence[float]) -> List[float]:
    """与 Java 末尾二重循环等价，输出长度 ``len(qtr)``。"""
    num_calc = len(qtr)
    num_uh = len(uh)
    qr_out = [0.0] * (num_calc + num_uh)
    for j in range(num_calc):
        for k in range(num_uh):
            qr_out[j + k] += qtr[j] * uh[k]
    return [qr_out[j] for j in range(num_calc)]


@dataclass(frozen=True)
class XinanjiangRunoffModel(IHydrologicalModel):
    """
    新安江模型（HFXAJAlg），需面雨量与蒸发能力序列。

    ``run`` 返回经单位线卷积后的流域出口流量序列（与 Java ``m_runRes`` 一致）。
    """

    params: XinanjiangParams = field(default_factory=XinanjiangParams)
    state: XinanjiangState = field(default_factory=XinanjiangState)

    @classmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        return frozenset({ForcingKind.PRECIPITATION, ForcingKind.POTENTIAL_EVAPOTRANSPIRATION})

    def run(self, forcing: ForcingData) -> TimeSeries:
        rain_ts = forcing.require(ForcingKind.PRECIPITATION)
        pet_ts = forcing.require(ForcingKind.POTENTIAL_EVAPOTRANSPIRATION)
        if rain_ts.values.ndim != 1 or pet_ts.values.ndim != 1:
            raise ValueError("XinanjiangRunoffModel requires 1-D forcing series")
        if (
            rain_ts.start_time != pet_ts.start_time
            or rain_ts.time_step != pet_ts.time_step
            or rain_ts.time_steps != pet_ts.time_steps
        ):
            raise ValueError("PRECIPITATION and POTENTIAL_EVAPOTRANSPIRATION series must align")

        p_input = [float(v) for v in rain_ts.values.tolist()]
        e_input = [float(v) for v in pet_ts.values.tolist()]
        num_calc = len(p_input)

        wm = [self.params.wum, self.params.wlm, self.params.wdm]
        k = self.params.k
        c = self.params.c
        b = self.params.b
        imp = self.params.imp
        sm = self.params.sm
        ex = self.params.ex
        kss = self.params.kss
        kg = self.params.kg
        kkss = self.params.kkss
        kkg = self.params.kkg
        area = self.params.area

        w = [self.state.wu, self.state.wl, self.state.wd]
        fr = self.state.fr
        s = self.state.s
        qrss0 = self.state.qrss0
        qrg0 = self.state.qrg0

        ts_sec = rain_ts.time_step.total_seconds()
        d, dtt = _discretization_java(ts_sec)
        u = area / (dtt * 3.6)
        ci = kkss ** (1.0 / d)
        cg = kkg ** (1.0 / d)
        kssd = (1.0 - abs(1.0 - (kg + kss)) ** (1.0 / d)) / (1.0 + kg / kss)
        kgd = kssd * kg / kss

        qtr_array: List[float] = []

        for rowi in range(num_calc):
            curp = p_input[rowi]
            cure = e_input[rowi]
            pe = curp - cure * k

            wm0 = wm[0] + wm[1] + wm[2]
            w0 = w[0] + w[1] + w[2]
            r = 0.0
            rimp = 0.0
            ww = 0.0

            if pe > 0:
                ww = 1.0 - w0 / wm0
                if ww < 0:
                    ww = 0.0
                wmm = (1.0 + b) * wm0 / (1.0 - imp)
                a = wmm * (1.0 - ww ** (1.0 / (1.0 + b)))
                if pe + a < wmm:
                    r = pe - wm0 + w0 + wm0 * ((1.0 - (pe + a) / wmm) ** (1.0 + b))
                else:
                    r = pe - (wm0 - w0)

            w[0] = w[0] + pe - r

            if w[0] < 0:
                if w[1] <= c * wm[1]:
                    w[1] = w[1] + c * w[0]
                    w[0] = 0.0
                    if w[1] < 0:
                        w[2] = w[2] + w[1]
                        w[1] = 0.0
                else:
                    w[1] = w[1] + w[0] * w[1] / wm[1]
                    w[0] = 0.0
                    if w[1] < 0:
                        w[2] = w[2] + c * w[1]
                        w[1] = 0.0
            else:
                if w[0] > wm[0]:
                    w[1] = w[1] + w[0] - wm[0]
                    w[0] = wm[0]
                    if w[1] > wm[1]:
                        w[2] = w[2] + w[1] - wm[1]
                        w[1] = wm[1]

            if w[0] < 0:
                w[0] = 0.0
            if w[1] < 0:
                w[1] = 0.0
            if w[2] < 0:
                w[2] = 0.0

            w0 = w[0] + w[1] + w[2]
            if w[1] < 0:
                w[0] = 0.0
                w[1] = 0.0
                w[2] = 0.0

            rs = 0.0
            rss = 0.0
            rg = 0.0

            if pe <= 0:
                r = 0.0
                rs = 0.0
                rg = s * fr * kgd
                rss = rg / kgd * kssd if kgd != 0 else 0.0
                if fr != 0:
                    s = s - (rss + rg) / fr
            else:
                au = 0.0
                smm = 0.0
                tt = 0.0

                rimp = imp * pe
                tt = r - rimp
                x = fr
                fr = tt / pe if pe != 0 else fr
                if fr != 0:
                    s = x * s / fr
                ss = s
                q = tt / fr if fr != 0 else 0.0
                nn = int(q / 5.0) + 1
                q = q / nn
                kssdd = (
                    (1.0 - (1.0 - (kgd + kssd)) ** (1.0 / nn)) / (1.0 + kgd / kssd)
                    if kssd != 0
                    else 0.0
                )
                kgdd = kssdd * kgd / kssd if kssd != 0 else 0.0
                rs = 0.0
                rss = 0.0
                rg = 0.0
                smm = (1.0 + ex) * sm

                for j in range(nn):
                    if q + s <= sm:
                        ww = 1.0 - s / sm
                        if ww < 0:
                            ww = 0.0
                        au = smm * (1.0 - ww ** (1.0 / (1.0 + ex)))
                        if au + q < smm:
                            rr = (
                                (q + s - sm + sm * ((1.0 - (q + au) / smm) ** (1.0 + ex)))
                                * fr
                            )
                        else:
                            rr = (q + s - sm) * fr
                    else:
                        rr = (q + s - sm) * fr

                    rs = rr + rs
                    s = q - rr / fr + s if fr != 0 else s
                    rg = s * kgdd * fr + rg
                    rss = s * kssdd * fr + rss
                    s = (j + 1) * q + ss - (rs + rss + rg) / fr if fr != 0 else s

            rs = rs + rimp

            if qrss0 < 0:
                qrss0 = 0.0
            if qrg0 < 0:
                qrg0 = 0.0

            qrss = qrss0 * ci + rss * (1.0 - ci) * u
            qrg = qrg0 * cg + rg * (1.0 - cg) * u

            if fr < 0:
                fr = 0.01
            if s < 0:
                s = 0.0

            qtr = rs * u + qrss + qrg
            qtr_array.append(qtr)

            qrss0 = qrss
            qrg0 = qrg

        uh = list(self.params.unit_graph)
        m_run_res = _unit_hydrograph_convolve(qtr_array, uh)

        self.state.wu = w[0]
        self.state.wl = w[1]
        self.state.wd = w[2]
        self.state.fr = fr
        self.state.s = s
        self.state.qrss0 = qrss0
        self.state.qrg0 = qrg0

        return TimeSeries(rain_ts.start_time, rain_ts.time_step, m_run_res)
