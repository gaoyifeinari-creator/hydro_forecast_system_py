import importlib.util
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "convert_legacy_config.py"


def _load_converter_module():
    spec = importlib.util.spec_from_file_location("convert_legacy_config_local", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ConvertLegacyConfigCleanupTest(unittest.TestCase):
    def test_generated_config_omits_redundant_node_mapping_and_fixed_units(self):
        module = _load_converter_module()
        legacy = {
            "sections": [
                {
                    "secId": "N1",
                    "secName": "Node 1",
                    "HFDataSource": {
                        "flowStationId": "Q1",
                        "stageStationId": "Z1",
                        "name": "Node 1 Station",
                    },
                    "units": [
                        {
                            "unitId": "C1",
                            "unitName": "Catch 1",
                            "prestations": [{"id": "P1", "preStaName": "Rain 1"}],
                            "evstations": [{"id": "E1", "evStaName": "PET 1"}],
                            "unitGModel": {"name": "XinanjiangRunoffModel", "params": {}},
                            "unitCModel": {"name": "MuskingumRoutingModel", "params": {}},
                        }
                    ],
                }
            ]
        }

        converted = module.convert_legacy_to_project_config(legacy, forecast_steps=24)
        scheme = converted["schemes"][0]
        catchment = scheme["catchments"][0]
        flow_station = scheme["stations"]["flow_stations"][0]
        stage_station = scheme["stations"]["stage_stations"][0]

        self.assertNotIn("downstream_node_id", catchment)
        self.assertEqual({"id", "name"}, set(flow_station.keys()))
        self.assertEqual({"id", "name"}, set(stage_station.keys()))


if __name__ == "__main__":
    unittest.main()
