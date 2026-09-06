@echo off
setlocal
cd /d "%~dp0"

if not exist "logs" mkdir "logs"

for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"`) do set "TS=%%i"
for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMddHHmm"`) do set "SCRAPE_RUN_ID=%%i"

set "LOG=logs\scrape_%TS%.log"
set "PY=%~dp0.venv\Scripts\python.exe"
set "PYTHONIOENCODING=utf-8"

if not exist "%PY%" (
    echo [ERROR] venv python not found: %PY%>>"%LOG%"
    exit /b 9009
)

echo [%DATE% %TIME%] scrape START  run_id=%SCRAPE_RUN_ID%>>"%LOG%"

"%PY%" main.py scrape >>"%LOG%" 2>&1
set "RC=%ERRORLEVEL%"

echo.>>"%LOG%"
echo [%DATE% %TIME%] scrape END  exit_code=%RC%>>"%LOG%"

powershell -NoProfile -Command "Get-ChildItem -Path 'logs\scrape_*.log' -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -Skip 30 | Remove-Item -Force -ErrorAction SilentlyContinue"

exit /b %RC%
