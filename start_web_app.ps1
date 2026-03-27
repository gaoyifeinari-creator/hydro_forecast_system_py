Set-Location $PSScriptRoot
$ErrorActionPreference = "Continue"

Write-Host "============================================"
Write-Host "Hydro Project - Web Calculation App"
Write-Host "============================================"
Write-Host "Working directory: $PWD"
Write-Host ""

# Entry Point / pythoncom38.dll: fix pywin32 in that conda env, or set HYDRO_PYTHON to base python.exe

function Test-StreamlitOk {
    param([string]$Exe, [string[]]$PrefixArgs)
    $a = @()
    if ($PrefixArgs) { $a += $PrefixArgs }
    $a += "-m", "streamlit", "--version"
    $null = & $Exe @a 2>&1
    return $LASTEXITCODE -eq 0
}

function Run-Streamlit {
    param([string]$Exe, [string[]]$PrefixArgs)
    $a = @()
    if ($PrefixArgs) { $a += $PrefixArgs }
    $a += "-m", "streamlit", "run", "scripts/web_calculation_app.py",
        "--server.address", "127.0.0.1", "--server.port", "8501",
        "--browser.gatherUsageStats", "false"
    & $Exe @a
}

function Try-RunWithPython {
    param([string]$PythonExe, [string]$Label)
    if (-not (Test-Path -LiteralPath $PythonExe)) { return $false }
    if (-not (Test-StreamlitOk $PythonExe @())) { return $false }
    Write-Host "Using: $Label"
    Write-Host "Starting Streamlit..."
    Write-Host "If the browser does not open, visit: http://127.0.0.1:8501"
    Write-Host "Close this window to stop the server."
    Write-Host ""
    Run-Streamlit $PythonExe @()
    exit $LASTEXITCODE
}

if ($env:HYDRO_PYTHON) {
    if (Test-Path -LiteralPath $env:HYDRO_PYTHON) {
        if (Try-RunWithPython $env:HYDRO_PYTHON "HYDRO_PYTHON") { }
        Write-Host "HYDRO_PYTHON failed or missing streamlit, trying other options..."
        Write-Host ""
    } else {
        Write-Host "ERROR: HYDRO_PYTHON is set but file not found: $($env:HYDRO_PYTHON)"
        Read-Host "Press Enter to exit"
        exit 1
    }
}

$candidates = @(
    (Join-Path $env:LOCALAPPDATA "anaconda3\python.exe"),
    (Join-Path $env:USERPROFILE "anaconda3\python.exe"),
    "D:\anaconda\python.exe"
)
foreach ($p in $candidates) {
    if (Test-Path -LiteralPath $p) {
        if (Try-RunWithPython $p $p) { }
    }
}

if (Get-Command py -ErrorAction SilentlyContinue) {
    if (Test-StreamlitOk "py" @("-3")) {
        Write-Host "Using: py -3"
        Write-Host "Starting Streamlit..."
        Write-Host "If the browser does not open, visit: http://127.0.0.1:8501"
        Write-Host "Close this window to stop the server."
        Write-Host ""
        Run-Streamlit "py" @("-3")
        exit $LASTEXITCODE
    }
}

if (Get-Command python -ErrorAction SilentlyContinue) {
    if (Test-StreamlitOk "python" @()) {
        Write-Host "Using: python"
        Write-Host "Starting Streamlit..."
        Write-Host "If the browser does not open, visit: http://127.0.0.1:8501"
        Write-Host "Close this window to stop the server."
        Write-Host ""
        Run-Streamlit "python" @()
        exit $LASTEXITCODE
    }
}

Write-Host "ERROR: No Python on PATH has streamlit installed."
Write-Host "Install:  python -m pip install streamlit"
Write-Host "Or set HYDRO_PYTHON to a python.exe that has streamlit (e.g. Anaconda base)."
Read-Host "Press Enter to exit"
exit 1
