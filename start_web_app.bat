@echo off
setlocal EnableExtensions

cd /d "%~dp0"

echo ============================================
echo Hydro Project - Web Calculation App
echo ============================================
echo Working directory: %CD%
echo.

REM --- Python choice (avoid broken conda envs) ---
REM If you see "Entry Point Not Found" / pythoncom38.dll / PyWinBufferView:
REM   1) Prefer a working interpreter: set HYDRO_PYTHON=D:\anaconda\python.exe  (base)
REM   2) Or fix the env:  conda activate tensorflow  then  pip install --force-reinstall pywin32
REM Do NOT open the browser before Streamlit is listening (race: empty page / connection refused).

REM 1) Explicit override (recommended when one conda env has broken pywin32)
if defined HYDRO_PYTHON (
  if exist "%HYDRO_PYTHON%" (
    call :RunWithPython "%HYDRO_PYTHON%" "HYDRO_PYTHON"
    if not errorlevel 1 goto :done_ok
    echo HYDRO_PYTHON failed or missing streamlit, trying other options...
    echo.
  ) else (
    echo ERROR: HYDRO_PYTHON is set but file not found: %HYDRO_PYTHON%
    goto :fail
  )
)

REM 2) Try common Anaconda *base* installs before PATH (PATH may point to a broken env first)
call :TryCandidate "%LOCALAPPDATA%\anaconda3\python.exe"
if not errorlevel 1 goto :done_ok
call :TryCandidate "%USERPROFILE%\anaconda3\python.exe"
if not errorlevel 1 goto :done_ok
call :TryCandidate "D:\anaconda\python.exe"
if not errorlevel 1 goto :done_ok

REM 3) py launcher
where py >nul 2>&1
if not errorlevel 1 (
  py -3 -m streamlit --version >nul 2>&1
  if not errorlevel 1 (
    call :RunStreamlit py -3
    if not errorlevel 1 goto :done_ok
  )
)

REM 4) whatever "python" is on PATH (may be a broken conda env)
where python >nul 2>&1
if errorlevel 1 (
  echo ERROR: Python not found. Install Python or add it to PATH.
  goto :fail
)

python -m streamlit --version >nul 2>&1
if errorlevel 1 (
  echo ERROR: streamlit is not installed for "python" on PATH.
  echo Try:  python -m pip install streamlit
  echo Or set HYDRO_PYTHON to a python.exe that has streamlit ^(e.g. Anaconda base^).
  goto :fail
)

call :RunStreamlit python
goto :after_run

:done_ok
goto :after_run

:TryCandidate
if "%~1"=="" exit /b 1
if not exist "%~1" exit /b 1
"%~1" -m streamlit --version >nul 2>&1
if errorlevel 1 exit /b 1
call :RunWithPython "%~1" "%~1"
exit /b %errorlevel%

:RunWithPython
set "_EXE=%~1"
set "_LABEL=%~2"
echo Using: %_LABEL%
echo Starting Streamlit...
echo If the browser does not open, visit: http://127.0.0.1:8501
echo Close this window to stop the server.
echo.
"%_EXE%" -m streamlit run "scripts\web_calculation_app.py" --server.address 127.0.0.1 --server.port 8501 --browser.gatherUsageStats false
exit /b %errorlevel%

:RunStreamlit
set "_A=%~1"
set "_B=%~2"
if "%_B%"=="" (
  echo Using: %_A%
  echo Starting Streamlit...
  echo If the browser does not open, visit: http://127.0.0.1:8501
  echo Close this window to stop the server.
  echo.
  %_A% -m streamlit run "scripts\web_calculation_app.py" --server.address 127.0.0.1 --server.port 8501 --browser.gatherUsageStats false
) else (
  echo Using: %_A% %_B%
  echo Starting Streamlit...
  echo If the browser does not open, visit: http://127.0.0.1:8501
  echo Close this window to stop the server.
  echo.
  %_A% %_B% -m streamlit run "scripts\web_calculation_app.py" --server.address 127.0.0.1 --server.port 8501 --browser.gatherUsageStats false
)
exit /b %errorlevel%

:after_run
if errorlevel 1 (
  echo.
  echo Streamlit exited with an error ^(see messages above^).
  goto :fail
)
endlocal
exit /b 0

:fail
echo.
pause
endlocal
exit /b 1
