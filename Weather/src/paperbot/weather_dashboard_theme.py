from __future__ import annotations

import streamlit as st


PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(17,18,22,0.0)",
    font=dict(color="#f5f1e8"),
)


def configure_dashboard_theme() -> None:
    st.set_page_config(page_title="Weather Bot", page_icon="WS", layout="wide")
    st.markdown(
        """
        <style>
        :root {
            --bg: #07080c;
            --panel: #12141a;
            --border: #2b2f39;
            --text: #f5f1e8;
            --muted: #9f9b92;
            --green: #71f06f;
            --blue: #7aa8ff;
            --red: #ff6b7a;
        }
        .stApp {
            background:
                radial-gradient(circle at 10% 10%, rgba(113, 240, 111, 0.08), transparent 18%),
                radial-gradient(circle at 90% 0%, rgba(122, 168, 255, 0.08), transparent 20%),
                linear-gradient(180deg, #050608 0%, #090b10 100%);
            color: var(--text);
        }
        .main .block-container {
            padding-top: 1.1rem;
            padding-bottom: 2rem;
            max-width: 1500px;
        }
        h1, h2, h3, h4, h5, h6, p, label, span, div {
            color: var(--text);
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(16, 18, 23, 0.98) 0%, rgba(9, 10, 14, 0.98) 100%);
            border-right: 1px solid var(--border);
        }
        [data-testid="stSidebar"] * {
            color: var(--text);
        }
        [data-testid="stSidebar"] [role="radiogroup"] {
            gap: 0.45rem;
        }
        [data-testid="stSidebar"] [role="radiogroup"] label {
            background: rgba(245, 241, 232, 0.04);
            border: 1px solid rgba(245, 241, 232, 0.08);
            border-radius: 14px;
            padding: 0.55rem 0.7rem;
            margin-bottom: 0.15rem;
        }
        [data-testid="stSidebar"] [role="radiogroup"] label:hover {
            border-color: rgba(113, 240, 111, 0.35);
            background: rgba(113, 240, 111, 0.08);
        }
        .sidebar-shell {
            background: rgba(245, 241, 232, 0.03);
            border: 1px solid rgba(245, 241, 232, 0.07);
            border-radius: 18px;
            padding: 0.9rem 1rem 0.6rem 1rem;
            margin-bottom: 1rem;
        }
        .sidebar-kicker {
            color: var(--green);
            font-size: 0.72rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }
        .sidebar-headline {
            font-size: 1.05rem;
            font-weight: 700;
            margin-bottom: 0.2rem;
        }
        .sidebar-sub {
            color: var(--muted);
            font-size: 0.85rem;
            line-height: 1.4;
        }
        [data-baseweb="select"] > div,
        [data-baseweb="input"] > div {
            background: #1a1d25;
            border: 1px solid var(--border);
            color: var(--text);
        }
        .stButton > button {
            background: linear-gradient(180deg, #1c2330, #121722);
            color: var(--text);
            border: 1px solid var(--border);
            border-radius: 12px;
        }
        div[data-testid="stMetric"] {
            background: rgba(245, 241, 232, 0.04);
            border: 1px solid rgba(245, 241, 232, 0.07);
            border-radius: 16px;
            padding: 0.8rem 0.9rem;
        }
        [data-testid="stDataFrame"] {
            background: rgba(10, 15, 28, 0.85);
            border: 1px solid var(--border);
            border-radius: 16px;
        }
        .top-title {
            display: flex;
            align-items: center;
            gap: 0.7rem;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            font-weight: 700;
            margin-bottom: 0.9rem;
        }
        .diamond {
            width: 12px;
            height: 12px;
            background: var(--green);
            transform: rotate(45deg);
            border-radius: 2px;
            box-shadow: 0 0 14px rgba(113, 240, 111, 0.5);
        }
        .section-card {
            background: rgba(18, 20, 26, 0.92);
            border: 1px solid var(--border);
            border-radius: 20px;
            padding: 1rem;
            margin-bottom: 1rem;
            box-shadow: 0 16px 40px rgba(0, 0, 0, 0.22);
        }
        .section-title {
            font-size: 1rem;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            margin-bottom: 0.8rem;
            color: var(--text);
            font-weight: 700;
        }
        .scan-card {
            background: rgba(245, 241, 232, 0.04);
            border: 1px solid rgba(245, 241, 232, 0.07);
            border-radius: 18px;
            padding: 0.9rem;
            min-height: 210px;
        }
        .scan-badge {
            display: inline-block;
            padding: 0.2rem 0.55rem;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 700;
            text-transform: uppercase;
            margin-bottom: 0.7rem;
        }
        .badge-new {
            background: rgba(113, 240, 111, 0.14);
            border: 1px solid rgba(113, 240, 111, 0.2);
            color: var(--green);
        }
        .badge-open {
            background: rgba(255, 107, 122, 0.14);
            border: 1px solid rgba(255, 107, 122, 0.2);
            color: #ffb6bf;
        }
        .scan-title-text {
            font-size: 1.05rem;
            line-height: 1.35;
            min-height: 58px;
            margin-bottom: 0.8rem;
        }
        .scan-link {
            color: var(--text);
            text-decoration: none;
        }
        .scan-link:hover {
            color: #ffffff;
            text-decoration: underline;
        }
        .scan-meta {
            color: var(--muted);
            font-size: 0.9rem;
            margin-bottom: 0.2rem;
        }
        .positions-shell {
            background: rgba(18, 20, 26, 0.92);
            border: 1px solid var(--border);
            border-radius: 20px;
            padding: 1rem;
            box-shadow: 0 16px 40px rgba(0, 0, 0, 0.22);
        }
        .positions-header {
            display: grid;
            grid-template-columns: 2.7fr 1.2fr 0.9fr 0.9fr 1.2fr 0.8fr;
            gap: 0.8rem;
            padding: 0 0.25rem 0.55rem 0.25rem;
            color: #8ea4c6;
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        .position-row {
            display: grid;
            grid-template-columns: 2.7fr 1.2fr 0.9fr 0.9fr 1.2fr 0.8fr;
            gap: 0.8rem;
            padding: 0.9rem 0.25rem;
            border-top: 1px solid rgba(245, 241, 232, 0.06);
            align-items: center;
        }
        .market-cell {
            display: flex;
            align-items: center;
            gap: 0.8rem;
        }
        .market-thumb {
            width: 44px;
            height: 44px;
            border-radius: 12px;
            object-fit: cover;
            background: rgba(245, 241, 232, 0.05);
            border: 1px solid rgba(245, 241, 232, 0.06);
        }
        .market-title {
            color: var(--text);
            text-decoration: none;
            font-weight: 700;
            line-height: 1.3;
        }
        .market-title:hover {
            color: #ffffff;
            text-decoration: underline;
        }
        .market-sub {
            color: var(--muted);
            font-size: 0.82rem;
            margin-top: 0.2rem;
        }
        .side-pill {
            display: inline-block;
            padding: 0.15rem 0.45rem;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 700;
            margin-right: 0.45rem;
        }
        .side-pill-yes {
            background: rgba(113, 240, 111, 0.12);
            color: var(--green);
            border: 1px solid rgba(113, 240, 111, 0.18);
        }
        .side-pill-no {
            background: rgba(255, 107, 122, 0.12);
            color: #ffb6bf;
            border: 1px solid rgba(255, 107, 122, 0.18);
        }
        .pos-main {
            font-weight: 700;
            font-size: 1rem;
        }
        .pos-sub {
            color: var(--muted);
            font-size: 0.84rem;
            margin-top: 0.15rem;
        }
        .pos-link {
            display: inline-block;
            text-align: center;
            padding: 0.6rem 0.8rem;
            border-radius: 12px;
            background: linear-gradient(180deg, #2396ff, #1479d6);
            color: #ffffff;
            text-decoration: none;
            font-weight: 700;
        }
        .pos-link:hover {
            color: #ffffff;
            text-decoration: none;
            filter: brightness(1.05);
        }
        .muted {
            color: var(--muted);
        }
        .side-no {
            color: var(--red);
            font-weight: 700;
        }
        .side-yes {
            color: var(--green);
            font-weight: 700;
        }
        .dashboard-subhead {
            color: var(--muted);
            font-size: 0.9rem;
            margin-bottom: 1rem;
        }
        .kpi-grid {
            display: grid;
            grid-template-columns: repeat(6, minmax(0, 1fr));
            gap: 0.85rem;
            margin-bottom: 1rem;
        }
        .kpi-card {
            background: linear-gradient(180deg, rgba(17, 27, 60, 0.95), rgba(16, 22, 44, 0.92));
            border: 1px solid rgba(91, 131, 220, 0.28);
            border-radius: 16px;
            padding: 0.9rem 1rem 0.85rem 1rem;
            min-height: 96px;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.02), 0 16px 30px rgba(0,0,0,0.18);
        }
        .kpi-label {
            color: #7f92bf;
            font-size: 0.72rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 0.55rem;
            font-weight: 700;
        }
        .kpi-value {
            color: #f3f7ff;
            font-size: 1.9rem;
            font-weight: 800;
            line-height: 1;
            margin-bottom: 0.35rem;
        }
        .kpi-value.positive {
            color: #57e389;
        }
        .kpi-value.negative {
            color: #ff7b88;
        }
        .kpi-sub {
            color: var(--muted);
            font-size: 0.78rem;
        }
        .ops-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1rem;
            margin-bottom: 1rem;
        }
        .ops-panel {
            background: linear-gradient(180deg, rgba(18, 25, 52, 0.95), rgba(13, 19, 39, 0.94));
            border: 1px solid rgba(91, 131, 220, 0.22);
            border-radius: 18px;
            padding: 0.8rem 0.8rem 0.6rem 0.8rem;
        }
        .ops-head {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.75rem;
            padding: 0 0.2rem;
        }
        .ops-title {
            font-size: 0.95rem;
            font-weight: 700;
            color: #e8eefc;
        }
        .ops-badge {
            background: rgba(57, 114, 215, 0.22);
            border: 1px solid rgba(99, 155, 255, 0.22);
            color: #9fc2ff;
            border-radius: 999px;
            padding: 0.15rem 0.55rem;
            font-size: 0.72rem;
            font-weight: 700;
        }
        .table-shell {
            border-top: 1px solid rgba(255,255,255,0.05);
        }
        .table-header, .table-row {
            display: grid;
            gap: 0.75rem;
            align-items: center;
            padding: 0.7rem 0.2rem;
        }
        .table-header {
            color: #7287b1;
            font-size: 0.68rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            font-weight: 700;
        }
        .table-row {
            border-top: 1px solid rgba(255,255,255,0.05);
            font-size: 0.86rem;
        }
        .market-col {
            min-width: 0;
        }
        .market-name {
            color: #eff4ff;
            font-weight: 700;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .market-meta {
            color: var(--muted);
            font-size: 0.76rem;
            margin-top: 0.15rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .pill {
            display: inline-block;
            padding: 0.12rem 0.38rem;
            border-radius: 999px;
            font-size: 0.67rem;
            font-weight: 700;
            text-transform: uppercase;
            margin-right: 0.35rem;
        }
        .pill-buy, .pill-yes, .pill-filled {
            color: #57e389;
            background: rgba(87, 227, 137, 0.13);
            border: 1px solid rgba(87, 227, 137, 0.18);
        }
        .pill-sell, .pill-no {
            color: #c6a8ff;
            background: rgba(147, 112, 255, 0.16);
            border: 1px solid rgba(147, 112, 255, 0.18);
        }
        .pill-open {
            color: #9fc2ff;
            background: rgba(82, 140, 255, 0.15);
            border: 1px solid rgba(82, 140, 255, 0.18);
        }
        .pill-closed {
            color: #aeb8cd;
            background: rgba(174, 184, 205, 0.12);
            border: 1px solid rgba(174, 184, 205, 0.16);
        }
        .num-positive {
            color: #57e389;
            font-weight: 700;
        }
        .num-negative {
            color: #ff7b88;
            font-weight: 700;
        }
        .curve-panel {
            background: linear-gradient(180deg, rgba(18, 25, 52, 0.95), rgba(13, 19, 39, 0.94));
            border: 1px solid rgba(91, 131, 220, 0.22);
            border-radius: 18px;
            padding: 0.85rem 0.85rem 0.5rem 0.85rem;
        }
        .curve-legend-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.75rem;
            margin-bottom: 0.8rem;
        }
        .curve-legend-card {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.06);
            border-radius: 14px;
            padding: 0.7rem 0.8rem;
        }
        .curve-legend-label {
            color: #7f92bf;
            font-size: 0.68rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
            font-weight: 700;
        }
        .curve-legend-value {
            color: #ecf3ff;
            font-size: 1.1rem;
            font-weight: 800;
            line-height: 1.1;
        }
        .curve-legend-sub {
            color: var(--muted);
            font-size: 0.76rem;
            margin-top: 0.15rem;
        }
        @media (max-width: 1100px) {
            .kpi-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
            .ops-grid {
                grid-template-columns: 1fr;
            }
            .curve-legend-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
