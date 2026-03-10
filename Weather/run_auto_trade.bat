@echo off
cd /d %~dp0
call C:\Users\pe_hn\anaconda3\condabin\conda.bat activate base
if errorlevel 1 (
    echo Falha ao ativar o ambiente base.
    exit /b 1
)
set WEATHER_HRRR_CONDA_ENV=base
python run_auto_trade.py --live
