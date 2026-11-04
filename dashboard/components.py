from __future__ import annotations

from html import escape

import plotly.express as px
import streamlit as st


PALETTE = {
    "navy": "#172B4D",
    "blue": "#2563EB",
    "teal": "#0F766E",
    "muted": "#64748B",
    "grid": "#E2E8F0",
    "background": "#F5F7FB",
    "white": "#FFFFFF",
    "green": "#15803D",
    "red": "#B91C1C",
    "amber": "#B45309",
}


def apply_dashboard_styles() -> None:
    st.markdown(
        """
        <style>
        :root { color-scheme: light; }
        .stApp { background: #F5F7FB; color: #172B4D; }
        [data-testid="stAppViewContainer"] > .main { background: #F5F7FB; }
        [data-testid="stMainBlockContainer"] { max-width: 1500px; padding-top: 2rem; padding-bottom: 3rem; }
        [data-testid="stSidebar"] { background: #111827; border-right: 1px solid #263244; }
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
        [data-testid="stSidebar"] [data-testid="stCaptionContainer"],
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span { color: #F8FAFC !important; }
        [data-testid="stSidebar"] [role="radiogroup"] { gap: .35rem; }
        [data-testid="stSidebar"] [role="radiogroup"] label {
            background: transparent; border: 1px solid transparent; border-radius: 10px;
            padding: .6rem .75rem; transition: background-color .15s ease;
        }
        [data-testid="stSidebar"] [role="radiogroup"] label:hover { background: #1F2937; }
        [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
            background: #1E3A5F; border-color: #3B82F6; color: #FFFFFF !important;
        }
        h1, h2, h3, h4 { color: #172B4D !important; letter-spacing: -.025em; }
        [data-testid="stCaptionContainer"], small { color: #64748B !important; }
        a { color: #1D4ED8 !important; }
        [data-testid="stTextInput"] input, [data-testid="stNumberInput"] input,
        [data-testid="stDateInput"] input, [data-testid="stTextArea"] textarea {
            color: #172B4D !important; background: #FFFFFF !important;
            border-color: #CBD5E1 !important; border-radius: 8px !important;
        }
        [data-baseweb="select"] > div { color: #172B4D !important; background: #FFFFFF !important; border-color: #CBD5E1 !important; }
        [data-baseweb="popover"] ul, [data-baseweb="menu"] { background: #FFFFFF !important; }
        [role="option"] { color: #172B4D !important; }
        [role="option"][aria-selected="true"] { color: #FFFFFF !important; background: #2563EB !important; }
        [data-testid="stCheckbox"] label, [data-testid="stRadio"] label { color: #172B4D !important; }
        [data-testid="stButton"] button, [data-testid="stDownloadButton"] button {
            color: #172B4D !important; background: #FFFFFF !important;
            border: 1px solid #CBD5E1 !important; border-radius: 8px !important;
            font-weight: 600 !important; min-height: 2.5rem;
        }
        [data-testid="stButton"] button:hover, [data-testid="stDownloadButton"] button:hover {
            color: #1D4ED8 !important; background: #EFF6FF !important; border-color: #2563EB !important;
        }
        [data-testid="stButton"] button[kind="primary"] {
            color: #FFFFFF !important; background: #2563EB !important; border-color: #2563EB !important;
        }
        [data-testid="stButton"] button[kind="primary"]:hover { color: #FFFFFF !important; background: #1D4ED8 !important; }
        [data-testid="stButton"] button:disabled, [data-testid="stDownloadButton"] button:disabled {
            color: #64748B !important; background: #E2E8F0 !important; border-color: #CBD5E1 !important;
        }
        [data-testid="stAlert"] { border-radius: 10px; border: 1px solid #CBD5E1; }
        [data-testid="stAlert"] p { color: #172B4D !important; }
        [data-testid="stDataFrame"], [data-testid="stTable"] { border: 1px solid #E2E8F0; border-radius: 10px; }
        [data-testid="stExpander"] { border: 1px solid #E2E8F0; border-radius: 10px; background: #FFFFFF; }
        [data-testid="stMetric"] { min-height: 116px; padding: 1.15rem 1.25rem; background: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; box-shadow: 0 2px 8px rgba(23,43,77,.05); }
        [data-testid="stMetricLabel"] p { color: #64748B !important; font-size: .9rem !important; font-weight: 600 !important; }
        [data-testid="stMetricValue"] { color: #172B4D !important; font-size: clamp(1.4rem, 2vw, 2rem) !important; font-weight: 750 !important; }
        [data-testid="stMetricDelta"] { font-weight: 600; }
        .page-intro { padding-bottom: .8rem; }
        .page-intro p { color: #64748B; font-size: 1rem; margin-top: -.25rem; }
        .run-meta { display:flex; flex-wrap:wrap; gap:.55rem 1.25rem; align-items:center; padding:.9rem 1rem; margin:.5rem 0 1rem; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:12px; color:#475569; }
        .run-meta strong { color:#172B4D; }
        .status-pill { display:inline-flex; align-items:center; gap:.4rem; padding:.28rem .7rem; border-radius:999px; font-size:.82rem; font-weight:700; }
        .status-success { color:#166534; background:#DCFCE7; }
        .status-running { color:#1D4ED8; background:#DBEAFE; }
        .status-failed { color:#991B1B; background:#FEE2E2; }
        .status-warning { color:#92400E; background:#FEF3C7; }
        .overview-panel { background:#FFFFFF; border:1px solid #E2E8F0; border-radius:12px; padding:1rem 1.15rem; margin:.25rem 0 1rem; }
        .overview-panel-title { color:#64748B; font-weight:650; font-size:.82rem; text-transform:uppercase; letter-spacing:.045em; margin-bottom:.55rem; }
        .quality-summary { display:flex; flex-wrap:wrap; gap:.6rem 1.5rem; color:#334155; }
        .quality-summary strong { color:#172B4D; }
        @media (max-width: 900px) {
            [data-testid="stMainBlockContainer"] { padding: 1.2rem 1rem 2rem; }
            [data-testid="stMetric"] { min-height: 100px; padding: .9rem; }
        }
        @media (max-width: 640px) {
            [data-testid="stHorizontalBlock"] { flex-wrap: wrap !important; }
            [data-testid="stHorizontalBlock"] > [data-testid="column"] { min-width: min(100%, 300px); flex: 1 1 300px !important; }
            [data-testid="stMetricValue"] { font-size: 1.35rem !important; }
            .run-meta { gap: .45rem; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def page_header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="page-intro"><h1>{escape(title)}</h1><p>{escape(subtitle)}</p></div>',
        unsafe_allow_html=True,
    )


def metric_row(items: list[tuple[str, str, str | None]]) -> None:
    columns = st.columns(len(items))
    for column, (label, value, delta) in zip(columns, items):
        column.metric(label, value, delta=delta)


def style_figure(figure, *, height: int = 330):
    figure.update_layout(
        height=height,
        margin=dict(l=12, r=12, t=36, b=12),
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(family="Inter, Arial, sans-serif", color=PALETTE["navy"], size=12),
        title_font=dict(color=PALETTE["navy"], size=16),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hoverlabel=dict(bgcolor="#FFFFFF", bordercolor=PALETTE["grid"], font=dict(color=PALETTE["navy"], size=12)),
    )
    figure.update_xaxes(showgrid=False, linecolor=PALETTE["grid"], tickfont=dict(color=PALETTE["muted"]))
    figure.update_yaxes(gridcolor=PALETTE["grid"], zeroline=False, tickfont=dict(color=PALETTE["muted"]))
    return figure


def empty_state(message: str) -> None:
    st.info(message, icon="ℹ️")


def status_pill(status: str) -> str:
    normalized = status.strip().upper()
    classes = {
        "SUCCEEDED": "status-success",
        "SUCCESS": "status-success",
        "RUNNING": "status-running",
        "SUBMITTED": "status-running",
        "ACCEPTED": "status-running",
        "FAILED": "status-failed",
        "KILLED": "status-failed",
        "WARNING": "status-warning",
    }
    class_name = classes.get(normalized, "status-warning")
    return f'<span class="status-pill {class_name}">{escape(normalized.title())}</span>'


def chart_note(text: str) -> None:
    st.caption(text)


def revenue_bar(frame, x: str, y: str, title: str, *, color: str | None = None):
    fig = px.bar(frame, x=x, y=y, title=title, color=color, color_discrete_sequence=[PALETTE["blue"]])
    return style_figure(fig)
