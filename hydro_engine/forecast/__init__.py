"""
预报骨架：面雨三情景、数据管理存根 + Mock、实况末态热启动串联。
"""

from hydro_engine.forecast.catchment_forecast_rainfall import CatchmentForecastRainfall
from hydro_engine.forecast.forecast_data_manager import ForecastDataManager
from hydro_engine.forecast.multisource_areal_rainfall import (
    CompileRequest,
    ForecastRainConfigBundle,
    ForecastDbConfig,
    ForecastRainPoint,
    ForecastRainRecord,
    ForecastRainRepository,
    ForecastRainSourceConfig,
    MultiSourceArealRainfallCompiler,
    RainDistributionParam,
    SqlAlchemyForecastRainRepository,
    SubRainSourceConfig,
    load_forecast_db_config_from_jdbc_json,
    parse_forecast_rain_config_from_scheme,
)
from hydro_engine.forecast.scenario_forcing import (
    load_catchment_forecast_rainfall_map_from_csv,
    patch_catchment_scenario_precipitation,
)
from hydro_engine.forecast.skeleton_pipeline import (
    RunoffWarmstartSnapshot,
    apply_runoff_warmstart,
    capture_runoff_warmstart,
    run_forecast_pipeline,
    run_forecast_pipeline_from_mock_csv,
)

__all__ = [
    "CatchmentForecastRainfall",
    "ForecastDataManager",
    "RunoffWarmstartSnapshot",
    "capture_runoff_warmstart",
    "apply_runoff_warmstart",
    "run_forecast_pipeline",
    "run_forecast_pipeline_from_mock_csv",
    "load_catchment_forecast_rainfall_map_from_csv",
    "patch_catchment_scenario_precipitation",
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
