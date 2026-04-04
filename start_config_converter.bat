@echo off
setlocal EnableExtensions
cd /d "%~dp0"

if defined HYDRO_PYTHON (
  if exist "%HYDRO_PYTHON%" (
    "%HYDRO_PYTHON%" -m streamlit run "scripts\config_converter_app.py" --server.address 127.0.0.1 --server.port 8502 --browser.gatherUsageStats false
    goto :eof
  )
)

where python >nul 2>&1
if errorlevel 1 (
  echo Python not found.
  pause
  exit /b 1
)

python -m streamlit run "scripts\config_converter_app.py" --server.address 127.0.0.1 --server.port 8502 --browser.gatherUsageStats false
