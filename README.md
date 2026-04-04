# Hydro Forecast System (Python)

Python implementation of a flood forecasting engine based on Node-Link topology and DAG routing.

The project focuses on:

- Multi-catchment runoff + routing simulation
- Node-level water processing (cross section / reservoir / diversion)
- Flexible forcing inputs (`ForcingData`) and JSON-driven scheme configuration
- Real-time forecast and historical simulation modes
- Desktop client for local analysis and debug tracing

---

## Core Features

- **DAG-based scheduling**: network built by `ForecastingScheme`, executed in topological order
- **Model strategy abstraction + registry**: runoff/routing/correction models implement `IHydrologicalModel` / `IErrorUpdater`; all models self-register via `MODEL_REGISTRY` (plugin-friendly, no hard-coded if-branches in core loader)
- **Time-context driven simulation**: `ForecastTimeContext` controls warmup/correction/history/forecast windows
- **Observed-data integration**:
  - node observed routing relay
  - optional observed inflow override for reservoir simulation
  - optional error updater (`IErrorUpdater`)
- **XAJ / XAJCS support**:
  - `XinanjiangRunoffModel`
  - `XinanjiangCSRunoffModel` with debug trace output
- **Catchment runoff parallelization**:
  - `catchment_workers=1` for single-thread
  - `None`/`<=0` for auto worker estimation
- **Automatic calibration (SCE-UA)**:
  - Global optimization for Xinanjiang / Xinanjiang CS parameters (`hydro_engine.calibration`)
  - Entry script: `scripts/run_sceua_calibration.py` — writes `output/calibration/` and optional `configs/calibrated_scheme.json`
- **Rolling forecast evaluation**:
  - Long-run historical-simulation tests with RE / KGE / NSE: `tests/test_rolling_forecast_eval.py`
  - Example outputs under `output/rolling_forecast_eval/` (large CSV/JSON; regenerate locally if needed)

---

### Model Registry (Plugin System)

Models are registered via `hydro_engine.models.register_model(name, factory)`.  
All runoff, routing, and correction models self-register on import — no central if-branches needed.

**How to add a new model** (e.g. `SARunoffModel`):

```python
# In hydro_engine/models/runoff/sarunnof.py
class SARunoffModel(IHydrologicalModel):
    ...

# In hydro_engine/models/runoff/__init__.py (bottom of file):
def _make_sarunoff(model_data) -> SARunoffModel:
    params = model_data.get("params", {})
    return SARunoffModel(k=float(params.get("k", 0.5)), ...)

register_model("SARunoffModel", _make_sarunoff)
```

The JSON config loader (`json_config._build_model`) will now automatically find it — no changes to core loading code required.

---

## Project Structure

```text
hydro_project/
├── configs/                    # Scheme JSON (e.g. forecastSchemeConf, calibrated_scheme)
├── docs/                       # Architecture docs
├── hydro_engine/
│   ├── core/                   # Context, TimeSeries, Forcing, interfaces
│   ├── calibration/            # SCE-UA + HydroModelCalibrator
│   ├── processing/             # Station -> catchment forcing synthesis
│   ├── read_data/              # Data readers: file / database / api
│   ├── domain/                 # Catchment, Reach, Node domain objects
│   ├── models/
│   │   ├── runoff/             # Runoff models (XAJ/XAJCS/Tank/...)
│   │   ├── routing/            # Routing models (Muskingum/Dummy)
│   │   └── correction/         # Error updater models
│   ├── engine/                 # Scheme + calculator
│   └── io/                     # JSON loading and calculation API
├── output/                     # Generated results (calibration, rolling eval); may be large
├── scripts/                    # Apps, run_sceua_calibration.py, helpers
└── tests/                      # Unit/integration + rolling forecast eval
```

---

## Quick Start

### 1) Requirements

- Python 3.8+
- `networkx`
- Web UI: `streamlit`, `matplotlib`, `pandas`

Install minimal dependencies:

```bash
pip install networkx pandas matplotlib numpy
```

(`numpy` is required for calibration and evaluation scripts.)

### 2) Run Web App (Streamlit)

```bash
python scripts/web_calculation_app.py
```

or on Windows:

```bat
start_web_calculation_app.bat
```

### 3) Run Tests

```bash
python -m unittest discover -v
```

### 4) SCE-UA calibration (optional)

From the `hydro_project` directory, after placing rain/flow CSV paths as configured in `scripts/run_sceua_calibration.py`:

```bash
python scripts/run_sceua_calibration.py
```

Outputs: `output/calibration/` (parameters, eval CSVs) and `configs/calibrated_scheme.json` when the script completes successfully.

### 5) Rolling forecast evaluation (optional)

```bash
python tests/test_rolling_forecast_eval.py
```

Writes under `output/rolling_forecast_eval/`; the run can take a long time (many daily origins).

---

## Main Calculation APIs

### Load scheme from JSON

`hydro_engine.io.json_config.load_scheme_from_json(...)`

Returns:

- `scheme`
- `binding_specs`
- `ForecastTimeContext`

### Run calculation from JSON

`hydro_engine.io.json_config.run_calculation_from_json(...)`

Important parameters:

- `station_packages: Dict[str, ForcingData]`
- `time_type`, `step_size`, `warmup_start_time`
- `observed_flows` (optional)
- `forecast_mode`:
  - `realtime_forecast` (default)
  - `historical_simulation`
- `catchment_workers` (optional): single-thread or auto parallel workers

---

## Time setup and reading node results (quick test)

### How the simulation window is defined

1. **Pick a scheme in JSON**  
   Match `time_type` and `step_size` to the `schemes[]` entry you want (e.g. `Hour` + `1`).

2. **Set relative lengths in `time_axis`** (inside that scheme):
   - `warmup_period_steps`
   - `correction_period_steps`
   - `historical_display_period_steps`
   - `forecast_period_steps`  

   These are **counts of steps**, not calendar dates. The engine derives absolute times from the anchor below.

3. **Pass the calendar anchor at runtime**  
   `warmup_start_time` is the **first timestep** of the whole run (start of the warmup segment).  
   All phase boundaries (`correction_start_time`, `forecast_start_time`, `display_start_time`, `end_time`) are computed from this anchor and the step counts.

4. **Align forcing data**  
   Every `TimeSeries` in `station_packages` must use:
   - `start_time == warmup_start_time`
   - `time_step ==` the scheme native step (same as `ForecastTimeContext.time_delta`)
   - `len(values) ==` total step count for the window (same as `time_context.step_count` after loading)

After `run_calculation_from_json`, inspect **`output["time_context"]`** for the resolved ISO timestamps:
`warmup_start_time`, `correction_start_time`, `forecast_start_time`, `display_start_time`, `end_time`.

### Node-related fields in the result dict

| Key | Meaning |
|-----|---------|
| `node_total_inflows` | `dict[node_id, list[float]]` — total inflow at each node (same length as simulation steps) |
| `node_outflows` | `dict[node_id, list[float]]` — sum of simulated outflows on all outgoing reaches for that node |
| `node_observed_flows` | `dict[node_id, list[float]]` — observed series when `observed_station_id` is configured (for comparison / relay) |
| `display_results` | Subset keyed as `node:{node_id}` and `reach:{reach_id}` — values only from `display_start_time` to `end_time` (for UI) |

Other useful keys: `reach_flows`, `catchment_runoffs`, `catchment_routed_flows`, `topological_order`, `forecast_mode`.

### Fast smoke test (end-to-end)

From the `hydro_project` directory:

```bash
python -m unittest tests.test_json_config_pipeline.TestJsonConfigPipeline.test_load_and_run -v
```

This loads `configs/example_forecast_config.json`, builds aligned `station_packages`, runs the engine, and asserts `node_total_inflows` / `reach_flows` / `time_context` are present.

### Minimal Python example (copy-paste)

Run from the `hydro_project` directory (or add it to `PYTHONPATH`).  
**Use the same `station_packages` as in** `tests/test_json_config_pipeline.py` (`test_load_and_run`) so every station referenced by `catchment_forcing_bindings` is supplied; series length must equal `time_context.step_count`.

```python
from datetime import datetime, timedelta
from pathlib import Path

from hydro_engine.core.forcing import ForcingData, ForcingKind
from hydro_engine.core.timeseries import TimeSeries
from hydro_engine.io.json_config import load_scheme_from_json, run_calculation_from_json

ROOT = Path(__file__).resolve().parent
config_path = ROOT / "configs" / "example_forecast_config.json"

time_type = "Hour"
step_size = 1
warmup_start_time = datetime(2026, 1, 1, 0, 0, 0)
step = timedelta(hours=step_size)

_, _, time_context = load_scheme_from_json(
    config_path,
    time_type=time_type,
    step_size=step_size,
    warmup_start_time=warmup_start_time,
)
n = time_context.step_count

# Same pattern as tests/test_json_config_pipeline.py — adjust values as needed
station_packages = {
    "STA_A": ForcingData.from_pairs(
        [
            (ForcingKind.PRECIPITATION, TimeSeries(warmup_start_time, step, [100.0] * n)),
            (
                ForcingKind.POTENTIAL_EVAPOTRANSPIRATION,
                TimeSeries(warmup_start_time, step, [3.0] * n),
            ),
        ]
    ),
    "STA_B": ForcingData.single(
        ForcingKind.PRECIPITATION,
        TimeSeries(warmup_start_time, step, [90.0] * n),
    ),
}

output = run_calculation_from_json(
    config_path,
    station_packages,
    time_type=time_type,
    step_size=step_size,
    warmup_start_time=warmup_start_time,
    forecast_mode="realtime_forecast",
    catchment_workers=1,
)

# Node results: list[float] per timestep, keyed by node id
for node_id, series in output["node_total_inflows"].items():
    print(node_id, "inflow len:", len(series))

print("forecast_start:", output["time_context"]["forecast_start_time"])
```

If your own JSON references more stations (rain/PET/flow), add matching keys to `station_packages` or follow `tests/test_json_config_pipeline.py` line by line.

---

## Forecast Modes

- **`realtime_forecast`**
  - After forecast start, observed meteorological forcing is not used for runoff driving
  - Node routing uses observed relay only before forecast start (when enabled)

- **`historical_simulation`**
  - Continue using observed meteorological forcing after forecast start
  - For nodes with `use_observed_for_routing=true`, observed relay can continue after forecast start

---

## Data Reading Layer

Data-source abstraction is under `hydro_engine/read_data/`:

- `file_reader.py` (implemented)
- `database_reader.py` (reserved)
- `api_reader.py` (reserved)
- `factory.py` for unified reader creation

Current apps use file reading by default.

---

## Documentation

- Development manual: `DEVELOPMENT_MANUAL.md`
- Calculation flow notes: `CALCULATION_LOGIC.md`
- Handover summary: `HANDOVER.md`
- Forcing architecture: `docs/FORCING_DATA_ARCHITECTURE.md`

---

## Repository and GitHub

- **Repository**: [gaoyifeinari-creator/hydro_forecast_system_py](https://github.com/gaoyifeinari-creator/hydro_forecast_system_py)
- **Clone** (HTTPS):

  ```bash
  git clone https://github.com/gaoyifeinari-creator/hydro_forecast_system_py.git
  cd hydro_forecast_system_py
  ```

  Work in the repository root where `hydro_engine/` and `configs/` live (this README assumes that layout).

- **Push** (SSH recommended if HTTPS to GitHub is unstable):

  ```bash
  git remote add origin git@github.com:gaoyifeinari-creator/hydro_forecast_system_py.git
  # or: git remote set-url origin git@github.com:gaoyifeinari-creator/hydro_forecast_system_py.git
  git push origin main
  ```

  Use a [Personal Access Token](https://github.com/settings/tokens) as the password when using HTTPS, not your GitHub account password.

- **Security**: do not commit tokens or secrets; revoke any token that was ever pasted into a file or chat.

If your team rebased or replaced `main` and you intend to **overwrite** the remote with this copy, coordinate first, then use `git push --force-with-lease origin main` (or `git push --force origin main` only when necessary).

