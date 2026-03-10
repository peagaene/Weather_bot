@echo off
cd /d C:\Bot_poly\Weather
call C:\Users\pe_hn\anaconda3\condabin\conda.bat activate weather-hrrr
if errorlevel 1 (
    echo Falha ao ativar o ambiente weather-hrrr.
    exit /b 1
)
start "" http://localhost:8501
python -m streamlit run dashboard.py --server.address 127.0.0.1 --server.headless true
