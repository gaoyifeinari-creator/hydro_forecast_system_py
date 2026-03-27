@echo off
setlocal
cd /d "%~dp0"

REM Local GUI (tkinter + matplotlib). No browser / Streamlit required.
REM Use the same Python where hydro_engine + matplotlib are installed, e.g.:
REM   conda activate pinn_final
REM   pip install matplotlib

if defined HYDRO_PYTHON (
  if exist "%HYDRO_PYTHON%" (
    "%HYDRO_PYTHON%" -c "import matplotlib" 2>nul
    if errorlevel 1 (
      echo Installing matplotlib for: %HYDRO_PYTHON%
      "%HYDRO_PYTHON%" -m pip install matplotlib
    )
    "%HYDRO_PYTHON%" "scripts\desktop_calculation_app.py"
    goto :end
  )
)

where python >nul 2>&1
if errorlevel 1 (
  echo ERROR: python not on PATH.
  pause
  exit /b 1
)

python -c "import matplotlib" 2>nul
if errorlevel 1 (
  echo Installing matplotlib...
  python -m pip install matplotlib
)

python "scripts\desktop_calculation_app.py"

:end
if errorlevel 1 pause
endlocal
