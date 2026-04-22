"""
新安江滞时河网汇流模型 — 与 ``HFXAJCSAlg.java`` 的 ``Run`` 方法对齐。

坡地汇流至 ``QTR`` 后，用滞时 + 线性水库作河网汇流：
``Q_out(t) = Q_out(t-1)*Cs + Q_in(t-Lag)*(1-Cs)``，其中 ``Q_in`` 为 ``QTR``。

率定上下界见 :mod:`hydro_engine.models.runoff.calibration_bounds`
（``XINANJIANG_CS_PARAM_BOUNDS``、``XINANJIANG_CS_LAG_BOUNDS`` 等）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import List

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries

from hydro_engine.models.runoff.xinanjiang import _discretization_java


@dataclass(frozen=True)
class XinanjiangCSParams:
    """与 Java：滞时 ``lag`` + ``m_douParaArr[0..13]`` + ``area``；``[14..21]`` 见 State。"""

    lag: int = 1
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
    cs: float = 0.8
    area: float = 0.0


@dataclass
class XinanjiangCSState:
    """与 Java ``m_douParaArr[14..21]``；``qs0`` 为初始地表径流参与首时刻出流。"""

    wu: float = 5.0
    wl: float = 10.0
    wd: float = 20.0
    fr: float = 0.01
    s: float = 6.0
    qrss0: float = 18.0
    qrg0: float = 20.0
    qs0: float = 20.0


def _lag_cs_routing(
    qtr_array: List[float],
    cs: float,
    lag: int,
    init_q: float,
) -> List[float]:
    """对应 Java 末尾 ``outData`` 循环。"""
    n = len(qtr_array)
    if n == 0:
        return []
    out_data = [0.0] * n
    out_data[0] = init_q
    lag = max(0, int(lag))
    for i in range(1, n):
        inflow = qtr_array[i - lag] if i - lag >= 0 else qtr_array[0]
        out_data[i] = out_data[i - 1] * cs + inflow * (1.0 - cs)
    return out_data


def _java_div(numer: float, denom: float) -> float:
    """Emulate Java double division semantics (NaN/Inf instead of Python exception)."""
    if denom != 0.0:
        return numer / denom
    if numer == 0.0:
        return float("nan")
    return math.copysign(float("inf"), numer)


def _java_pow(base: float, exp: float) -> float:
    """Emulate Java Math.pow invalid-domain behavior by returning NaN."""
    try:
        return math.pow(base, exp)
    except ValueError:
        return float("nan")


@dataclass(frozen=True)
class XinanjiangCSRunoffModel(IHydrologicalModel):
    """
    新安江（滞时河网汇流），需面雨量与潜在蒸发。

    ``run`` 返回河网汇流后的流域出口流量（与 Java ``m_runRes`` 一致）。
    """

    params: XinanjiangCSParams = field(default_factory=XinanjiangCSParams)
    state: XinanjiangCSState = field(default_factory=XinanjiangCSState)
    # 调试开关：True 时记录逐时中间量，便于与 HFXAJCSAlg 逐步对表。
    debug_trace: bool = False
    debug_rows: List[dict] = field(default_factory=list, compare=False)

    @classmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        return frozenset({ForcingKind.PRECIPITATION, ForcingKind.POTENTIAL_EVAPOTRANSPIRATION})

    def run(self, forcing: ForcingData) -> TimeSeries:
        self.debug_rows.clear()
        rain_ts = forcing.require(ForcingKind.PRECIPITATION)
        pet_ts = forcing.require(ForcingKind.POTENTIAL_EVAPOTRANSPIRATION)
        if rain_ts.values.ndim != 1 or pet_ts.values.ndim != 1:
            raise ValueError("XinanjiangCSRunoffModel requires 1-D forcing series")
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
        cs_coef = self.params.cs
        lag = self.params.lag

        w = [self.state.wu, self.state.wl, self.state.wd]
        fr = self.state.fr
        s = self.state.s
        qrss0 = self.state.qrss0
        qrg0 = self.state.qrg0
        qs0 = self.state.qs0
        qrss0_init = qrss0
        qrg0_init = qrg0
        qs0_init = qs0

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
                rss = _java_div(rg, kgd) * kssd
                s = s - _java_div((rss + rg), fr)
            else:
                au = 0.0
                smm = 0.0
                tt = 0.0

                rimp = imp * pe
                tt = r - rimp
                x = fr
                fr = _java_div(tt, pe)
                s = _java_div((x * s), fr)
                ss = s
                q = _java_div(tt, fr)
                nn = int(q / 5.0) + 1
                q = q / nn
                kssdd = _java_div(
                    (1.0 - _java_pow((1.0 - (kgd + kssd)), (1.0 / nn))),
                    (1.0 + _java_div(kgd, kssd)),
                )
                kgdd = _java_div((kssdd * kgd), kssd)
                rs = 0.0
                rss = 0.0
                rg = 0.0
                smm = (1.0 + ex) * sm

                for j in range(nn):
                    if q + s <= sm:
                        ww = 1.0 - s / sm
                        if ww < 0:
                            ww = 0.0
                        au = smm * (1.0 - _java_pow(ww, (1.0 / (1.0 + ex))))
                        if au + q < smm:
                            rr = (
                                (q + s - sm + sm * _java_pow((1.0 - (q + au) / smm), (1.0 + ex)))
                                * fr
                            )
                        else:
                            rr = (q + s - sm) * fr
                    else:
                        rr = (q + s - sm) * fr

                    rs = rr + rs
                    s = q - _java_div(rr, fr) + s
                    rg = s * kgdd * fr + rg
                    rss = s * kssdd * fr + rss
                    s = (j + 1) * q + ss - _java_div((rs + rss + rg), fr)

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

            if self.debug_trace:
                self.debug_rows.append(
                    {
                        "step": rowi,
                        "p": float(curp),
                        "pet": float(cure),
                        "pe": float(pe),
                        "r": float(r),
                        "rs": float(rs),
                        "rss": float(rss),
                        "rg": float(rg),
                        "ci": float(ci),
                        "cg": float(cg),
                        "qrss0": float(qrss0),
                        "qrg0": float(qrg0),
                        "u": float(u),
                        "qtr": float(qtr),
                        "qrss": float(qrss),
                        "qrg": float(qrg),
                        "wu": float(w[0]),
                        "wl": float(w[1]),
                        "wd": float(w[2]),
                        "fr": float(fr),
                        "s": float(s),
                    }
                )

            qrss0 = qrss
            qrg0 = qrg

        init_q = qrss0_init + qrg0_init + qs0_init
        if len(qtr_array) == 0:
            return TimeSeries(rain_ts.start_time, rain_ts.time_step, [])

        m_run_res = _lag_cs_routing(qtr_array, cs_coef, lag, init_q)

        if self.debug_trace:
            for i in range(num_calc):
                t = rain_ts.start_time + rain_ts.time_step * i
                self.debug_rows[i]["out"] = float(m_run_res[i]) if i < len(m_run_res) else 0.0
                self.debug_rows[i]["time"] = t.isoformat(sep=" ")

        self.state.wu = w[0]
        self.state.wl = w[1]
        self.state.wd = w[2]
        self.state.fr = fr
        self.state.s = s
        self.state.qrss0 = qrss0
        self.state.qrg0 = qrg0
        self.state.qs0 = qs0

        return TimeSeries(rain_ts.start_time, rain_ts.time_step, m_run_res)

    def get_debug_rows(self) -> List[dict]:
        return [dict(r) for r in self.debug_rows]
