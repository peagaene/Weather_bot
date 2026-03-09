@echo off
cd /d C:\Bot_poly\Weather
start "" http://localhost:8501
python -m streamlit run dashboard.py --server.address 127.0.0.1
