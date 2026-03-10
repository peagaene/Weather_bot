@echo off
cd /d %~dp0
call C:\Users\pe_hn\anaconda3\condabin\conda.bat activate weather-hrrr
if errorlevel 1 (
    echo Falha ao ativar o ambiente weather-hrrr.
    exit /b 1
)
python run_auto_trade.py --live
