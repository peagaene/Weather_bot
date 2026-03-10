@echo off
cd /d %~dp0
call C:\Users\pe_hn\anaconda3\condabin\conda.bat activate weather-hrrr
if errorlevel 1 (
    echo Falha ao ativar o ambiente weather-hrrr.
    exit /b 1
)
set WEATHER_SKIP_DOTENV=1

start "Weather Dashboard" cmd /k "cd /d %~dp0 && call C:\Users\pe_hn\anaconda3\condabin\conda.bat activate weather-hrrr && set WEATHER_SKIP_DOTENV=1 && python -m streamlit run dashboard.py --server.address 127.0.0.1 --server.headless true"

timeout /t 2 /nobreak >nul
start "" http://localhost:8501

start "Weather Micro Live" cmd /k "cd /d %~dp0 && set WEATHER_SKIP_DOTENV=1 && call run_micro_live_safe.bat"
