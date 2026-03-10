@echo off
cd /d %~dp0
call C:\Users\pe_hn\anaconda3\condabin\conda.bat activate weather-hrrr
if errorlevel 1 (
    echo Falha ao ativar o ambiente weather-hrrr.
    exit /b 1
)
rem Discovery mais ampla para gerar mais contexto, em modo observacao (sem envio de ordem).
set WEATHER_MONITOR_TOP=10
set WEATHER_MONITOR_INTERVAL_SECONDS=300
set WEATHER_AUTO_TRADE_INTERVAL_SECONDS=300
set WEATHER_MIN_EDGE=6
set WEATHER_MIN_MODEL_PROB=12
set WEATHER_MIN_CONSENSUS=0.25
set WEATHER_MAX_MODEL_SPREAD=5
set WEATHER_ENABLE_HRRR=1
set WEATHER_HRRR_CONDA_ENV=weather-hrrr
set PAPERBOT_BANKROLL_USD=18
set WEATHER_AUTO_TRADE_ENABLED=1
set WEATHER_ALLOW_UNAPPROVED_REPLAY_FOR_MICRO_LIVE=1
set PAPERBOT_MIN_STAKE_USD=2
set PAPERBOT_MAX_STAKE_USD=2
set WEATHER_DAILY_LIVE_LIMIT=3
set WEATHER_BUCKET_LIVE_LIMIT=2
set WEATHER_EXECUTE_TOP=0
set WEATHER_MAX_ORDERS_PER_EVENT=1
set WEATHER_MAX_SHARE_SIZE=20
python run_auto_trade.py
