@echo off
cd /d %~dp0
call C:\Users\pe_hn\anaconda3\condabin\conda.bat activate base
if errorlevel 1 (
    echo Falha ao ativar o ambiente base.
    exit /b 1
)
set WEATHER_HRRR_CONDA_ENV=base
set WEATHER_EXECUTE_TOP=0
set WEATHER_POLICY_TOMORROW_MAX_PRICE_CENTS=62
set PAPERBOT_MIN_STAKE_USD=1
set PAPERBOT_MAX_STAKE_USD=5
python run_weather_models.py --top 10 --show-blocked 10 --execute-top 0
pause
