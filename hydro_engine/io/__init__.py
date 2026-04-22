"""JSON configuration loaders and runners."""

from .json_config import (
    build_catchment_forcing_from_station_packages,
    flatten_stations_catalog,
    legacy_rainfall_dict_to_station_packages,
    load_scheme_from_json,
    run_calculation_from_json,
)
from .scheme_config_utils import (
    catchment_catalog_names_from_scheme,
    read_schemes_list,
    scheme_dbtype,
    select_scheme_dict_exact,
    select_scheme_dict_smallest_step,
    station_catalog_names_from_scheme,
)

__all__ = [
    "load_scheme_from_json",
    "flatten_stations_catalog",
    "build_catchment_forcing_from_station_packages",
    "legacy_rainfall_dict_to_station_packages",
    "run_calculation_from_json",
    "read_schemes_list",
    "select_scheme_dict_exact",
    "select_scheme_dict_smallest_step",
    "scheme_dbtype",
    "station_catalog_names_from_scheme",
    "catchment_catalog_names_from_scheme",
]
