from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta

import _sys_path  # noqa: F401

from hydro_engine.core.context import ForecastTimeContext, TimeType
from hydro_engine.core.data_pool import DataPool
from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.interfaces import IHydrologicalModel
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.domain.catchment import SubCatchment
from hydro_engine.engine.scheme import ForecastingScheme
from hydro_engine.processing.pipeline import CatchmentDataSynthesizer


@dataclass(frozen=True)
class DummyModelNeedsPrecipAndTemp(IHydrologicalModel):
    @classmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        return frozenset({ForcingKind.PRECIPITATION, ForcingKind.AIR_TEMPERATURE})

    def run(self, forcing: ForcingData) -> TimeSeries:
        # 本测试只验证 ForcingData 合成，不执行真实模型。
        return forcing.require(ForcingKind.PRECIPITATION)


@dataclass(frozen=True)
class DummyModelNeedsPet(IHydrologicalModel):
    @classmethod
    def required_inputs(cls) -> frozenset[ForcingKind]:
        return frozenset({ForcingKind.POTENTIAL_EVAPOTRANSPIRATION})

    def run(self, forcing: ForcingData) -> TimeSeries:
        # 本测试只验证 ForcingData 合成，不执行真实模型。
        return forcing.require(ForcingKind.POTENTIAL_EVAPOTRANSPIRATION)


class TestCatchmentSynthesizer(unittest.TestCase):
    def test_synthesize_multivar_precip_and_temp(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0)
        step = timedelta(hours=1)

        # warmup=0, correction=0, historical=0, forecast=3 => step_count=3
        ctx = ForecastTimeContext.from_period_counts(
            warmup_start_time=start,
            time_type=TimeType.HOUR,
            step_size=1,
            warmup_period_steps=0,
            correction_period_steps=0,
            historical_display_period_steps=0,
            forecast_period_steps=3,
        )

        pool = DataPool()
        # P1 在中间点缺测（NaN），用于验证插补
        p1 = TimeSeries(start, step, [1.0, float("nan"), 3.0])
        p2 = TimeSeries(start, step, [2.0, 2.0, 2.0])
        t1 = TimeSeries(start, step, [10.0, 11.0, 12.0])
        t2 = TimeSeries(start, step, [20.0, 21.0, 22.0])

        pool.add_observed("P1", ForcingKind.PRECIPITATION, p1)
        pool.add_observed("P2", ForcingKind.PRECIPITATION, p2)
        pool.add_observed("T1", ForcingKind.AIR_TEMPERATURE, t1)
        pool.add_observed("T2", ForcingKind.AIR_TEMPERATURE, t2)

        scheme = ForecastingScheme()
        scheme.add_catchment(
            SubCatchment(id="CA", runoff_model=DummyModelNeedsPrecipAndTemp())
        )

        binding_specs = [
            {
                "catchment_id": "CA",
                "variables": [
                    {
                        "kind": "precipitation",
                        "method": "weighted_average",
                        "stations": [
                            {"id": "P1", "weight": 0.7},
                            {"id": "P2", "weight": 0.3},
                        ],
                    },
                    {
                        "kind": "air_temperature",
                        "method": "arithmetic_mean",
                        "stations": [{"id": "T1"}, {"id": "T2"}],
                    },
                ],
            }
        ]

        synthesizer = CatchmentDataSynthesizer()
        synthesizer.synthesize(
            scheme=scheme,
            data_pool=pool,
            scenario_id="S0",
            binding_specs=binding_specs,
            time_context=ctx,
        )

        forcing = pool.get_catchment_forcing("S0", "CA")
        precip_out = forcing.require(ForcingKind.PRECIPITATION).values
        temp_out = forcing.require(ForcingKind.AIR_TEMPERATURE).values

        # P1 缺测点插补为 2.0 => precip:
        # step0: 0.7*1 + 0.3*2 = 1.3
        # step1: 0.7*2 + 0.3*2 = 2.0
        # step2: 0.7*3 + 0.3*2 = 2.7
        expected_precip = [1.3, 2.0, 2.7]
        for a, b in zip(precip_out, expected_precip):
            self.assertAlmostEqual(a, b, places=7)

        expected_temp = [15.0, 16.0, 17.0]
        for a, b in zip(temp_out, expected_temp):
            self.assertAlmostEqual(a, b, places=7)

    def test_pet_use_monthly_when_station_disabled(self) -> None:
        # warmup=0, correction=0, historical=0, forecast=3 => step_count=3
        ctx = ForecastTimeContext.from_period_counts(
            warmup_start_time=datetime(2026, 1, 31, 0, 0, 0),
            time_type=TimeType.DAY,
            step_size=1,
            warmup_period_steps=0,
            correction_period_steps=0,
            historical_display_period_steps=0,
            forecast_period_steps=3,
        )

        pool = DataPool()
        # 构造一个与 monthly_values 明显不同的 PET 测站序列，用于验证“显式关闭测站”生效
        station_pet = TimeSeries(
            ctx.warmup_start_time,
            ctx.time_delta,
            [999.0, 888.0, 777.0],
        )
        pool.add_observed(
            "PET_STA_X",
            ForcingKind.POTENTIAL_EVAPOTRANSPIRATION,
            station_pet,
        )

        scheme = ForecastingScheme()
        scheme.add_catchment(
            SubCatchment(id="CA", runoff_model=DummyModelNeedsPet())
        )

        monthly_values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0]

        binding_specs = [
            {
                "catchment_id": "CA",
                "variables": [
                    {
                        "kind": "potential_evapotranspiration",
                        "method": "weighted_average",
                        "use_station_pet": False,
                        "stations": [{"id": "PET_STA_X", "weight": 1.0}],
                        "monthly_values": monthly_values,
                    }
                ],
            }
        ]

        synthesizer = CatchmentDataSynthesizer()
        synthesizer.synthesize(
            scheme=scheme,
            data_pool=pool,
            scenario_id="S0",
            binding_specs=binding_specs,
            time_context=ctx,
        )

        forcing = pool.get_catchment_forcing("S0", "CA")
        pet_out = forcing.require(ForcingKind.POTENTIAL_EVAPOTRANSPIRATION).values

        # 2026-01-31 属于 Jan => 10；随后是 Feb => 20
        expected = [10.0, 20.0, 20.0]
        for a, b in zip(pet_out, expected):
            self.assertAlmostEqual(a, b, places=7)


if __name__ == "__main__":
    unittest.main()

