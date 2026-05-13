from __future__ import annotations

import io
import json
import hashlib
import hmac
import math
import re
import tomllib
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any

import folium
import numpy as np
import pandas as pd
import shapefile
import streamlit as st
import streamlit.components.v1 as components
from folium.plugins import FastMarkerCluster, Fullscreen, MarkerCluster, MeasureControl, MiniMap, MousePosition

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
except Exception:
    service_account = None
    build = None
    MediaIoBaseDownload = None

# Paths
APP_ROOT = Path(__file__).resolve().parent
STREAMLIT_DIR = APP_ROOT / ".streamlit"
SETTINGS_PATH = STREAMLIT_DIR / "project_settings.json"
CREDENTIALS_PATH = STREAMLIT_DIR / "gdrive_service_account.json"
SECRETS_PATH = STREAMLIT_DIR / "secrets.toml"
MAP_DIR = APP_ROOT / "data" / "maps" / "afg_admin_boundaries"
ADMIN0_SHP = MAP_DIR / "afg_admin0.shp"
ADMIN1_SHP = MAP_DIR / "afg_admin1.shp"
ADMIN2_SHP = MAP_DIR / "afg_admin2.shp"
REGIONS_SHP = MAP_DIR / "afg_regions.shp"

SAMPLE_DIR = APP_ROOT / "data" / "samples"
SAMPLE_FILES: list[Path] = sorted(SAMPLE_DIR.glob("*.xlsx")) if SAMPLE_DIR.exists() else []

# Constants
AFG_BOUNDS = {"lat_min": 29.0, "lat_max": 39.0, "lon_min": 60.0, "lon_max": 75.5}
AFG_LEAFLET_BOUNDS = [[AFG_BOUNDS["lat_min"], AFG_BOUNDS["lon_min"]], [AFG_BOUNDS["lat_max"], AFG_BOUNDS["lon_max"]]]

GPS_ALIASES = {
    "latitude": ["gps-latitude", "gps_latitude", "gps latitude", "latitude", "lat"],
    "longitude": ["gps-longitude", "gps_longitude", "gps longitude", "longitude", "lon", "lng"],
    "altitude": ["gps-altitude", "gps_altitude", "gps altitude", "altitude", "alt"],
    "accuracy": ["gps-accuracy", "gps_accuracy", "gps accuracy", "accuracy"],
}

POINT_COLORS = [
    "#38bdf8", "#6366f1", "#22c55e", "#f59e0b", "#ef4444", "#14b8a6",
    "#8b5cf6", "#06b6d4", "#84cc16", "#f97316", "#3b82f6", "#a855f7",
]

REGION_COLORS = {
    "Capital": "#0891b2",
    "Central Highland": "#7c3aed",
    "Eastern": "#16a34a",
    "North Eastern": "#2563eb",
    "Northern": "#0f766e",
    "South Eastern": "#ea580c",
    "Southern": "#dc2626",
    "Western": "#ca8a04",
}

BASE_MAPS = {
    "Auto theme": {"label": "Auto theme", "kind": "auto", "light": "Light map", "dark": "Dark map"},
    "Light map": {"label": "Light map", "tiles": "CartoDB positron", "attr": "CartoDB"},
    "Dark map": {"label": "Dark map", "tiles": "CartoDB dark_matter", "attr": "CartoDB"},
    "Street map": {"label": "Street map", "tiles": "OpenStreetMap", "attr": "OpenStreetMap"},
    "Voyager": {"label": "Voyager", "tiles": "CartoDB Voyager", "attr": "CartoDB"},
    "Satellite": {
        "label": "Satellite",
        "tiles": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "attr": "Tiles &copy; Esri",
    },
    "Topographic": {
        "label": "Topographic",
        "tiles": "https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        "attr": "Map data: &copy; OpenStreetMap contributors, SRTM | Map style: &copy; OpenTopoMap",
    },
}

ALL_OVERLAYS = ["GPS points", "Afghanistan boundary", "Zones", "Provinces", "Districts"]
DEFAULT_OVERLAYS = ["GPS points", "Afghanistan boundary"]

MAP_LAYOUTS = {
    "Balanced": {"height": 613, "districts": False, "province_weight": 1.8, "district_weight": 0.55, "fill_multiplier": 1.0},
    "Focused": {"height": 380, "districts": False, "province_weight": 1.35, "district_weight": 0.0, "fill_multiplier": 0.65},
    "Detailed": {"height": 500, "districts": True, "province_weight": 2.1, "district_weight": 0.85, "fill_multiplier": 1.25},
}

GPS_QUALITY_ORDER = ["Excellent", "Good", "Fair", "Needs review", "Missing"]
GPS_QUALITY_COLORS = {
    "Excellent": "#22c55e",
    "Good": "#84cc16",
    "Fair": "#eab308",
    "Needs review": "#f97316",
    "Missing": "#ef4444",
}

READABLE_DRIVE_MIMES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "application/vnd.google-apps.spreadsheet",
}


DEFAULT_PROJECT_SETTINGS: dict[str, Any] = {
    "data_source": "google_drive",
    "active_dataset": "",
    "datasets": [],
    "google_drive": {
        "folder_id": "",
        "folder_url": "",
        "credentials_source": "streamlit_secrets",
        "credentials_path": str(CREDENTIALS_PATH),
        "recursive": False,
        "max_files": 50,
        "selected_file_ids": [],
    },
    "columns": {
        "latitude": "",
        "longitude": "",
        "altitude": "",
        "accuracy": "",
        "review_status": "review_status",
        "display": [],
        "popup_display": [],
    },
    "quality": {
        "excellent_max": 10,
        "good_max": 30,
        "fair_max": 100,
    },
}

SECRET_SERVICE_ACCOUNT_KEYS = ("gdrive_service_account", "google_service_account", "service_account")
ADMIN_AUTH_SESSION_KEY = "settings_admin_auth"


def default_dataset_config(name: str = "Default dataset") -> dict[str, Any]:
    return {
        "name": name,
        "data_source": "google_drive",
        "google_drive": dict(DEFAULT_PROJECT_SETTINGS["google_drive"]),
        "columns": dict(DEFAULT_PROJECT_SETTINGS["columns"]),
        "quality": dict(DEFAULT_PROJECT_SETTINGS["quality"]),
    }


@dataclass(frozen=True)
class AdminFeature:
    name: str
    parent: str
    pcode: str
    region: str
    region_code: str
    bbox: tuple[float, float, float, float]
    rings: tuple[tuple[tuple[float, float], ...], ...]


# Style
def page_style() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800;900&display=swap');

        :root {
            --bg: #06111f;
            --bg2: #081527;
            --panel: rgba(12, 22, 40, .86);
            --panel2: rgba(15, 27, 48, .72);
            --text: #f7fbff;
            --muted: #9fb0c9;
            --line: rgba(226, 232, 240, .13);
            --line2: rgba(125, 211, 252, .22);
            --accent: #38bdf8;
            --accent2: #6366f1;
            --shadow: 0 18px 48px rgba(0,0,0,.28);
        }

        html, body, [class*="css"] { font-family: "Manrope", sans-serif; }

        .block-container {
            padding-top: .45rem !important;
            padding-bottom: .55rem !important;
            max-width: 1520px !important;
        }

        section.main > div[data-testid="stVerticalBlock"],
        div[data-testid="stVerticalBlock"] { gap: 1rem !important; }

        [data-testid="stAppViewContainer"] {
            color: var(--text);
            background:
                radial-gradient(circle at 10% 0%, rgba(56,189,248,.12), transparent 28%),
                radial-gradient(circle at 90% 0%, rgba(99,102,241,.13), transparent 30%),
                linear-gradient(180deg, #06111f 0%, #081527 48%, #06101d 100%);
        }

        [data-testid="stHeader"] { background: rgba(6,17,31,.10); backdrop-filter: blur(16px); }
        [data-testid="stSidebar"] { background: linear-gradient(180deg, rgba(8,17,33,.94), rgba(5,12,25,.96)); border-right: 1px solid var(--line); }
        [data-testid="stSidebar"] * { color: #dce7ff !important; }

        h1, h2, h3, p, span, label { color: inherit; }
        h2, h3 { margin-top: .35rem !important; margin-bottom: .18rem !important; }
        hr { border-color: rgba(255,255,255,.08); margin: 8px 0 !important; }

        .page-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            margin-top: 64px;
            margin-bottom: 28px;
            padding: 14px 18px;
            border-radius: 20px;
            border: 1px solid var(--line);
            background: linear-gradient(145deg, rgba(255,255,255,.08), rgba(255,255,255,.025)), rgba(9, 18, 34, .86);
            box-shadow: var(--shadow);
        }
        .page-title { font-size: 1.35rem; font-weight: 900; letter-spacing: -.02em; color: #f8fbff; margin: 0; }
        .page-subtitle { color: var(--muted); font-size: .82rem; font-weight: 700; margin-top: 2px; }
        .page-badge { padding: 8px 12px; border-radius: 999px; border: 1px solid var(--line2); background: linear-gradient(135deg, rgba(56,189,248,.16), rgba(99,102,241,.16)); color: #dff6ff; font-size: .78rem; font-weight: 900; white-space: nowrap; }

        .section-title { font-size: .82rem; font-weight: 900; color: #f8fbff; margin: 2px 0 7px 0; }

        .top-filter-card, .control-card, .side-panel, .map-shell, .metric-card, .chart-card {
            border: 1px solid var(--line);
            background: linear-gradient(145deg, rgba(255,255,255,.075), rgba(255,255,255,.025)), var(--panel);
            box-shadow: var(--shadow), inset 0 1px 0 rgba(255,255,255,.07);
        }

        .top-filter-card { border-radius: 18px; padding: 13px 15px 10px 15px; margin: 16px 0 12px 0; }
        .filter-header { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:10px; }
        .filter-title { font-size: .9rem; font-weight: 900; color:#f8fbff; }
        .filter-note { color: var(--muted); font-size: .74rem; font-weight: 700; }

        .dashboard-grid { display:grid; grid-template-columns: minmax(0, 4.35fr) 310px; gap: 20px; align-items:start; margin-top: 18px; }
        .map-shell { border-radius: 20px; overflow:hidden; background: rgba(7,17,31,.88); }
        .map-head { display:flex; align-items:center; justify-content:space-between; gap:12px; padding:13px 15px; border-bottom:1px solid var(--line); background:linear-gradient(145deg, rgba(15,23,42,.95), rgba(8,13,28,.84)); }
        .brand-title { font-size:1.12rem; font-weight:900; color:#f8fbff; letter-spacing:-.02em; }
        .brand-sub { color:#9fb0cf; font-size:.74rem; font-weight:800; margin-top:1px; }
        .head-actions { display:flex; gap:8px; align-items:center; }
        .status-pill { display:inline-flex; align-items:center; padding:7px 10px; border-radius:999px; border:1px solid var(--line2); background:rgba(56,189,248,.10); color:#dff6ff; font-weight:900; font-size:.73rem; }
        .map-body { overflow:hidden; line-height:0; }

        div[data-testid="stHtml"] { padding:0!important; margin:0!important; line-height:0!important; font-size:0!important; overflow:hidden!important; }
        div[data-testid="stHtml"] iframe, iframe { display:block!important; width:100%!important; border:0!important; margin:0!important; padding:0!important; background:transparent!important; }

        .side-panel { border-radius: 20px; padding: 14px; }
        .side-card { border:1px solid var(--line); border-radius:16px; background:linear-gradient(145deg, rgba(255,255,255,.06), rgba(255,255,255,.02)); padding:14px; margin-bottom:22px; }
        .side-label { color:#b7c5df; font-size:.74rem; font-weight:900; margin-bottom:7px; }
        .side-value { color:#f8fbff; font-size:1.8rem; font-weight:900; letter-spacing:-.04em; line-height:1.05; }
        .side-caption { color:#94a3b8; font-size:.74rem; font-weight:700; margin-top:7px; }
        .compact-side-card { padding:8px 10px; min-height:84px; display:flex; flex-direction:column; justify-content:center; }
        .compact-side-card .side-label { text-align:center; margin-bottom:8px; font-size:.84rem; }
        .compact-side-card .side-caption { text-align:center; font-size:.82rem; }
        .quality-card .quality-row { justify-content:center; gap:12px; text-align:center; font-size:.84rem; padding:5px 0; }
        .quality-card .quality-row .quality-name { justify-content:center; }
        .export-actions { display:flex; flex-direction:column; gap:10px; margin-top:4px; }
        .quality-row { display:flex; align-items:center; justify-content:space-between; gap:10px; padding:7px 0; border-bottom:1px solid rgba(255,255,255,.06); font-size:.76rem; font-weight:900; }
        .quality-row:last-child { border-bottom:0; }
        .quality-name { display:flex; align-items:center; gap:8px; min-width:0; }
        .quality-dot { width:7px; height:7px; border-radius:999px; flex:0 0 7px; }

        .metric-grid { display:grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 18px; margin: 26px 0 22px 0; }
        .metric-card { position:relative; overflow:hidden; border-radius:18px; min-height:132px; padding:12px 16px 16px 16px; }
        .metric-card:before { content:""; position:absolute; left:0; right:0; top:0; height:2px; background:linear-gradient(90deg, var(--accent), var(--accent2), transparent); }
        .metric-title { color:#cbd5e1; font-size:.76rem; font-weight:900; margin-top:34px; margin-bottom:9px; }
        .metric-number { color:#f8fbff; font-size:1.74rem; font-weight:900; letter-spacing:-.04em; line-height:1; }
        .metric-sub { color:#94a3b8; font-size:.74rem; font-weight:700; margin-top:10px; }

        .analytics-grid { display:grid; grid-template-columns:1fr 1fr 1.2fr; gap:18px; margin:0 0 24px 0; }
        .chart-card { border-radius:18px; padding:16px; min-height:205px; }
        .chart-title { color:#f8fbff; font-size:.9rem; font-weight:900; margin-bottom:13px; }
        .bar-row { display:grid; grid-template-columns:82px 1fr 76px; align-items:center; gap:10px; margin:10px 0; color:#dbe7ff; font-size:.75rem; font-weight:900; }
        .bar-track { height:14px; border-radius:999px; background:rgba(148,163,184,.18); overflow:hidden; }
        .bar-fill { height:100%; border-radius:999px; background:linear-gradient(90deg,#38bdf8,#6366f1); }
        .donut-wrap { display:flex; gap:18px; align-items:center; }
        .donut { width:128px; height:128px; border-radius:50%; background:conic-gradient(#7c3aed 0 38%,#2563eb 38% 61%,#34d399 61% 79%,#eab308 79% 90%,#f59e0b 90% 100%); position:relative; flex:0 0 128px; }
        .donut:after { content:""; position:absolute; inset:32px; border-radius:50%; background:#0b1628; border:1px solid rgba(255,255,255,.08); }
        .legend-row { display:flex; align-items:center; justify-content:space-between; gap:12px; margin:9px 0; color:#dbe7ff; font-size:.76rem; font-weight:900; }
        .legend-name { display:flex; align-items:center; gap:8px; min-width:0; }
        .dot { width:9px; height:9px; border-radius:3px; flex:0 0 9px; }

        div[data-baseweb="input"] > div,
        div[data-baseweb="select"] > div,
        textarea,
        input,
        .stTextInput input,
        .stTextArea textarea,
        .stNumberInput input {
            background: #0b1a2d !important;
            color: #f8fbff !important;
            border-radius: 13px !important;
            border: 1px solid rgba(255,255,255,.18) !important;
            box-shadow: none !important;
            outline: none !important;
            background-clip: padding-box !important;
        }
        div[data-baseweb="input"],
        div[data-baseweb="input"] *,
        div[data-baseweb="select"],
        div[data-baseweb="select"] *,
        div[data-baseweb="tag"] {
            box-shadow:none!important;
            background-image:none!important;
            filter:none!important;
        }
        div[data-baseweb="input"]::before,
        div[data-baseweb="input"]::after,
        div[data-baseweb="select"]::before,
        div[data-baseweb="select"]::after,
        .stTextInput *::before,
        .stTextInput *::after,
        .stNumberInput *::before,
        .stNumberInput *::after {
            display:none!important;
            content:none!important;
            border:none!important;
        }
        /* Hard reset Streamlit wrappers so only ONE visible layer remains */
        [data-testid="stTextInputRootElement"] > div,
        [data-testid="stNumberInput"] > div,
        [data-testid="stSelectbox"] > div,
        [data-testid="stMultiSelect"] > div {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
            padding: 0 !important;
        }
        [data-testid="stTextInputRootElement"] [data-baseweb="input"],
        [data-testid="stNumberInput"] [data-baseweb="input"],
        [data-testid="stSelectbox"] [data-baseweb="select"],
        [data-testid="stMultiSelect"] [data-baseweb="select"] {
            background: #0b1a2d !important;
            border: 1px solid rgba(255,255,255,.18) !important;
            border-bottom: 1px solid rgba(255,255,255,.18) !important;
            border-radius: 13px !important;
            box-shadow: none !important;
            outline: none !important;
            overflow: visible !important;
        }
        /* Strict single-layer for Streamlit text inputs */
        [data-testid="stTextInputRootElement"] {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
        }
        [data-testid="stTextInputRootElement"] [data-baseweb="input"] {
            background: #0b1a2d !important;
            border: 1px solid rgba(255,255,255,.18) !important;
            border-radius: 13px !important;
            box-shadow: none !important;
            outline: none !important;
        }
        [data-testid="stTextInputRootElement"] [data-baseweb="input"] * {
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
            background-image: none !important;
        }
        [data-testid="stTextInputRootElement"] [data-baseweb="input"] > div {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
        }
        [data-testid="stTextInputRootElement"] input,
        [data-testid="stTextInputRootElement"] input:focus,
        [data-testid="stTextInputRootElement"] input:focus-visible {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
            border-radius: 0 !important;
        }
        [data-testid="stTextInputRootElement"] [data-baseweb="input"] > div,
        [data-testid="stNumberInput"] [data-baseweb="input"] > div,
        [data-testid="stSelectbox"] [data-baseweb="select"] > div,
        [data-testid="stMultiSelect"] [data-baseweb="select"] > div {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
        }
        [data-testid="stNumberInput"] [data-baseweb="input"] {
            border-bottom: 1px solid rgba(255,255,255,.18) !important;
            min-height: 40px !important;
        }
        /* Kill inner borders on actual input controls (the source of double-line look) */
        [data-testid="stTextInputRootElement"] [data-baseweb="input"] input,
        [data-testid="stNumberInput"] [data-baseweb="input"] input,
        [data-testid="stTextInputRootElement"] [data-baseweb="input"] input:focus,
        [data-testid="stNumberInput"] [data-baseweb="input"] input:focus {
            border: none !important;
            outline: none !important;
            box-shadow: none !important;
            background: transparent !important;
            border-radius: 0 !important;
        }
        /* Keep stepper buttons flat and single-layer next to number input */
        [data-testid="stNumberInput"] button {
            box-shadow: none !important;
            border: 1px solid rgba(255,255,255,.18) !important;
            border-bottom: 1px solid rgba(255,255,255,.18) !important;
            outline: none !important;
            background-image: none !important;
            min-height: 40px !important;
        }
        /* Remove tiny inner "cursor-like" capsule/handles in select & input wrappers */
        div[data-baseweb="select"] [role="combobox"] > div:first-child,
        div[data-baseweb="input"] > div > div:first-child,
        .stTextInput [data-baseweb="input"] > div > div:first-child {
            border:none!important;
            box-shadow:none!important;
            background:transparent!important;
            min-width:0!important;
            width:auto!important;
            padding-left:0!important;
            margin-left:0!important;
        }
        /* Keep single-layer focus state only (no extra ring) */
        input:focus, textarea:focus,
        .stTextInput input:focus, .stTextArea textarea:focus, .stNumberInput input:focus,
        div[data-baseweb="select"] > div:focus-within {
            outline:none!important;
            box-shadow:none!important;
            border:1px solid rgba(255,255,255,.18)!important;
        }
        /* Strict single-layer for all dropdowns (Selectbox + Multiselect) */
        [data-testid="stSelectbox"],
        [data-testid="stMultiSelect"] {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
        }
        [data-testid="stSelectbox"] > div,
        [data-testid="stMultiSelect"] > div,
        [data-testid="stSelectbox"] > div > div,
        [data-testid="stMultiSelect"] > div > div {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
            padding: 0 !important;
        }
        [data-testid="stSelectbox"] [data-baseweb="select"],
        [data-testid="stMultiSelect"] [data-baseweb="select"] {
            background: #0b1a2d !important;
            border: 1px solid rgba(255,255,255,.18) !important;
            border-radius: 13px !important;
            box-shadow: none !important;
            outline: none !important;
            background-image: none !important;
        }
        [data-testid="stSelectbox"] [data-baseweb="select"] *,
        [data-testid="stMultiSelect"] [data-baseweb="select"] * {
            border: none !important;
            box-shadow: none !important;
            outline: none !important;
            background-image: none !important;
            filter: none !important;
        }
        [data-testid="stSelectbox"] [data-baseweb="select"]::before,
        [data-testid="stSelectbox"] [data-baseweb="select"]::after,
        [data-testid="stMultiSelect"] [data-baseweb="select"]::before,
        [data-testid="stMultiSelect"] [data-baseweb="select"]::after {
            content: none !important;
            display: none !important;
            border: none !important;
        }
        [data-testid="stSelectbox"] [data-baseweb="select"]:focus-within,
        [data-testid="stMultiSelect"] [data-baseweb="select"]:focus-within {
            border: 1px solid rgba(255,255,255,.18) !important;
            box-shadow: none !important;
            outline: none !important;
        }
        div[data-baseweb="tag"] {
            background:#1d4ed8!important;
            border:1px solid rgba(255,255,255,.18)!important;
            border-radius:10px!important;
            color:#f8fbff!important;
            font-weight:900!important;
        }
        label, .stMarkdown, .stCaption, .stTextInput label, .stMultiSelect label, .stSelectbox label { color:#dbe7ff!important; font-size:.78rem!important; font-weight:800!important; }

        div[data-testid="stDownloadButton"] button, div[data-testid="stButton"] button {
            border-radius: 13px !important;
            border: 1px solid rgba(255,255,255,.14) !important;
            background: linear-gradient(135deg, #38bdf8, #6366f1) !important;
            color: #f9fbff !important;
            font-weight: 900 !important;
            min-height: 2.35rem;
            box-shadow: 0 10px 24px rgba(0,0,0,.22);
        }
        div[data-testid="stDownloadButton"] + div[data-testid="stDownloadButton"] button { background: rgba(15,23,42,.78) !important; }

        div[data-testid="stDataFrame"], div[data-testid="stTable"] {
            color: var(--text);
            background: transparent !important;
            border: none !important;
            border-radius: 0 !important;
            box-shadow: none !important;
            overflow: visible !important;
            padding: 0 !important;
            margin-top: 0 !important;
        }
        .stTabs [data-baseweb="tab-list"] { gap:.4rem; background:rgba(255,255,255,.045); border:1px solid rgba(255,255,255,.08); border-radius:15px; padding:.28rem; margin-top:.4rem; margin-bottom:.45rem; }
        .stTabs [data-baseweb="tab"] { border-radius:12px!important; color:#dce7ff!important; font-weight:900!important; padding:6px 12px; }
        .stTabs [aria-selected="true"] { background:rgba(255,255,255,.12)!important; border:1px solid rgba(255,255,255,.12)!important; }
        div[data-testid="stAlert"] {
            background:#0b1a2d!important;
            border:1px solid rgba(255,255,255,.18)!important;
            border-radius:14px!important;
            box-shadow:none!important;
            outline:none!important;
            background-image:none!important;
        }
        div[data-testid="stAlert"] * {
            box-shadow:none!important;
            outline:none!important;
            background-image:none!important;
            border-image:none!important;
        }
        div[data-testid="stAlert"]::before,
        div[data-testid="stAlert"]::after,
        div[data-testid="stAlert"] *::before,
        div[data-testid="stAlert"] *::after {
            content:none!important;
            display:none!important;
            border:none!important;
        }
        [data-testid="stFileUploader"] section { background:#0b1a2d; border:1px solid rgba(255,255,255,.18); border-radius:18px; box-shadow:none!important; }

        @media(max-width:1200px){ .dashboard-grid{grid-template-columns:1fr;} .metric-grid{grid-template-columns:repeat(2,minmax(0,1fr));} .analytics-grid{grid-template-columns:1fr;} }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_nav() -> None:
    st.sidebar.markdown("### Navigation")
    st.sidebar.page_link("app.py", label="Dashboard")
    st.sidebar.page_link("pages/1_Reports.py", label="Reports")
    if (APP_ROOT / "pages" / "2_Settings.py").exists():
        st.sidebar.page_link("pages/2_Settings.py", label="Settings")
    st.sidebar.divider()


# Data loading and extraction
def normalize_column(name: Any) -> str:
    return str(name).strip().lower().replace("_", "-")


def deep_merge(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def normalize_dataset_config(dataset: dict[str, Any], fallback_name: str) -> dict[str, Any]:
    base = default_dataset_config(fallback_name)
    normalized = deep_merge(base, dataset if isinstance(dataset, dict) else {})
    normalized["name"] = str(normalized.get("name") or fallback_name).strip() or fallback_name
    return normalized


def migrate_project_settings(settings: dict[str, Any]) -> dict[str, Any]:
    migrated = deep_merge(DEFAULT_PROJECT_SETTINGS, settings)
    raw_datasets = migrated.get("datasets")
    datasets: list[dict[str, Any]] = []
    if isinstance(raw_datasets, list):
        for index, dataset in enumerate(raw_datasets, start=1):
            if isinstance(dataset, dict):
                datasets.append(normalize_dataset_config(dataset, f"Dataset {index}"))

    legacy_has_drive = bool(str(migrated.get("google_drive", {}).get("folder_id") or migrated.get("google_drive", {}).get("folder_url") or "").strip())
    legacy_has_columns = any(str(migrated.get("columns", {}).get(key, "") or "").strip() for key in ["latitude", "longitude", "altitude", "accuracy"])
    if not datasets and (legacy_has_drive or legacy_has_columns):
        datasets.append(normalize_dataset_config({
            "name": "Default dataset",
            "data_source": migrated.get("data_source", "google_drive"),
            "google_drive": migrated.get("google_drive", {}),
            "columns": migrated.get("columns", {}),
            "quality": migrated.get("quality", {}),
        }, "Default dataset"))

    migrated["datasets"] = datasets
    if not str(migrated.get("active_dataset", "") or "").strip() and datasets:
        migrated["active_dataset"] = datasets[0]["name"]
    return migrated


def load_project_settings() -> dict[str, Any]:
    if not SETTINGS_PATH.exists():
        return migrate_project_settings({})
    try:
        saved = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        return migrate_project_settings(saved if isinstance(saved, dict) else {})
    except Exception:
        return migrate_project_settings({})


def save_project_settings(settings: dict[str, Any]) -> None:
    STREAMLIT_DIR.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(json.dumps(migrate_project_settings(settings), indent=2, ensure_ascii=False), encoding="utf-8")


def dataset_profiles(settings: dict[str, Any]) -> list[dict[str, Any]]:
    return list(migrate_project_settings(settings).get("datasets", []))


def dataset_names(settings: dict[str, Any]) -> list[str]:
    return [str(dataset.get("name", "")).strip() for dataset in dataset_profiles(settings) if str(dataset.get("name", "")).strip()]


def settings_for_dataset(settings: dict[str, Any], dataset_name: str | None = None) -> dict[str, Any]:
    migrated = migrate_project_settings(settings)
    profiles = dataset_profiles(migrated)
    if not profiles:
        return migrated
    target = str(dataset_name or migrated.get("active_dataset") or profiles[0]["name"]).strip()
    selected = next((profile for profile in profiles if profile.get("name") == target), profiles[0])
    merged = deep_merge(migrated, selected)
    merged["active_dataset"] = selected["name"]
    return merged


def extract_drive_folder_id(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    patterns = [
        r"/folders/([A-Za-z0-9_-]+)",
        r"[?&]id=([A-Za-z0-9_-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return text


def configured_column(settings: dict[str, Any], kind: str, columns: list[str]) -> str | None:
    configured = str(settings.get("columns", {}).get(kind, "") or "").strip()
    return configured if configured in columns else None


def find_column(columns: list[str], kind: str, settings: dict[str, Any] | None = None) -> str | None:
    if settings:
        configured = configured_column(settings, kind, columns)
        if configured:
            return configured
    normalized = {normalize_column(col): col for col in columns}
    for alias in GPS_ALIASES[kind]:
        key = normalize_column(alias)
        if key in normalized:
            return normalized[key]
    for col in columns:
        ncol = normalize_column(col)
        if "gps" in ncol and kind in ncol:
            return col
    return None


def excel_label(file_obj: Any) -> str:
    if isinstance(file_obj, (str, Path)):
        return Path(file_obj).name
    return getattr(file_obj, "name", "uploaded_file.xlsx")


def selected_display_columns(settings: dict[str, Any], columns: list[str]) -> list[str]:
    chosen = settings.get("columns", {}).get("display", [])
    if not isinstance(chosen, list):
        return []
    return [col for col in chosen if col in columns]


def selected_popup_columns(settings: dict[str, Any], columns: list[str]) -> list[str]:
    chosen = settings.get("columns", {}).get("popup_display", [])
    if not isinstance(chosen, list):
        return []
    return [col for col in chosen if col in columns]


def plain_secret_value(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if isinstance(value, dict):
        return {key: plain_secret_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [plain_secret_value(item) for item in value]
    return value


def normalize_service_account_info(info: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(info)
    private_key = normalized.get("private_key")
    if isinstance(private_key, str):
        normalized["private_key"] = private_key.replace("\\n", "\n")
    return normalized


def get_streamlit_service_account_info() -> dict[str, Any] | None:
    try:
        for key in SECRET_SERVICE_ACCOUNT_KEYS:
            if key in st.secrets:
                value = plain_secret_value(st.secrets[key])
                if isinstance(value, dict):
                    return normalize_service_account_info(value)
        if "GOOGLE_SERVICE_ACCOUNT_JSON" in st.secrets:
            value = st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"]
            if isinstance(value, str):
                return normalize_service_account_info(json.loads(value))
    except Exception:
        return None
    return None


def google_drive_secret_status() -> dict[str, str | bool]:
    info = get_streamlit_service_account_info()
    if info:
        return {
            "available": True,
            "source": "Streamlit secrets",
            "client_email": str(info.get("client_email", "")),
            "project_id": str(info.get("project_id", "")),
        }
    if SECRETS_PATH.exists():
        return {
            "available": False,
            "source": str(SECRETS_PATH),
            "client_email": "",
            "project_id": "",
        }
    return {"available": False, "source": "Not configured", "client_email": "", "project_id": ""}


def get_service_account_credentials_text(settings: dict[str, Any]) -> str:
    info = get_streamlit_service_account_info()
    if info:
        return json.dumps(info)

    drive = settings.get("google_drive", {})
    credentials_path = Path(str(drive.get("credentials_path") or CREDENTIALS_PATH))
    if credentials_path.exists():
        try:
            return credentials_path.read_text(encoding="utf-8")
        except Exception as exc:
            st.error(f"Could not read fallback Google credentials JSON: {exc}")
    return ""


def normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def hash_password_sha256(password: str) -> str:
    return hashlib.sha256(str(password).encode("utf-8")).hexdigest()


def verify_password(password: str, expected: str) -> bool:
    candidate = str(expected or "").strip()
    if not candidate:
        return False
    if candidate.startswith("sha256$"):
        digest = hash_password_sha256(password)
        return hmac.compare_digest(digest, candidate.split("$", 1)[1].strip())
    return hmac.compare_digest(str(password), candidate)


def admin_users_from_secrets() -> list[dict[str, Any]]:
    def append_user_if_valid(target: list[dict[str, Any]], item: dict[str, Any]) -> None:
        email = normalize_email(str(item.get("email", "")))
        role = str(item.get("role", "admin")).strip().lower()
        password = str(item.get("password", "") or item.get("password_hash", "")).strip()
        active = bool(item.get("active", True))
        if email and password and active and role == "admin":
            target.append({"email": email, "role": role, "password": password, "active": True})

    def find_key_recursive(obj: Any, wanted_key: str) -> Any:
        if isinstance(obj, dict):
            if wanted_key in obj:
                return obj[wanted_key]
            for value in obj.values():
                found = find_key_recursive(value, wanted_key)
                if found is not None:
                    return found
        return None

    users: list[dict[str, Any]] = []
    try:
        secrets_all = plain_secret_value(st.secrets.to_dict() if hasattr(st.secrets, "to_dict") else dict(st.secrets))
    except Exception:
        secrets_all = {}
    try:
        raw = plain_secret_value(st.secrets.get("admin_users", []))
    except Exception:
        raw = []
    if (not raw) and isinstance(secrets_all, dict):
        nested = find_key_recursive(secrets_all, "admin_users")
        if nested is not None:
            raw = nested
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                append_user_if_valid(users, item)
    # Also support dict style: [admin_users.user1]
    if not users and isinstance(raw, dict):
        for _, item in raw.items():
            if isinstance(item, dict):
                append_user_if_valid(users, item)
    # Backward-compatible single admin credentials
    if not users:
        try:
            email = normalize_email(
                str(
                    st.secrets.get("admin_email", "")
                    or st.secrets.get("ADMIN_EMAIL", "")
                )
            )
            password = str(
                st.secrets.get("admin_password", "")
                or st.secrets.get("ADMIN_PASSWORD", "")
                or st.secrets.get("admin_password_hash", "")
                or st.secrets.get("ADMIN_PASSWORD_HASH", "")
            ).strip()
            if email and password:
                users.append({"email": email, "role": "admin", "password": password, "active": True})
        except Exception:
            pass
    if not users and isinstance(secrets_all, dict):
        nested_email = str(find_key_recursive(secrets_all, "admin_email") or find_key_recursive(secrets_all, "ADMIN_EMAIL") or "").strip()
        nested_password = str(
            find_key_recursive(secrets_all, "admin_password")
            or find_key_recursive(secrets_all, "ADMIN_PASSWORD")
            or find_key_recursive(secrets_all, "admin_password_hash")
            or find_key_recursive(secrets_all, "ADMIN_PASSWORD_HASH")
            or ""
        ).strip()
        if nested_email and nested_password:
            users.append({"email": normalize_email(nested_email), "role": "admin", "password": nested_password, "active": True})
    # [admin] table support
    if not users:
        try:
            admin_block = plain_secret_value(st.secrets.get("admin", {}))
            if isinstance(admin_block, dict):
                email = normalize_email(str(admin_block.get("email", "") or admin_block.get("admin_email", "")))
                password = str(
                    admin_block.get("password", "")
                    or admin_block.get("admin_password", "")
                    or admin_block.get("password_hash", "")
                    or admin_block.get("admin_password_hash", "")
                ).strip()
                role = str(admin_block.get("role", "admin")).strip().lower()
                if email and password and role == "admin":
                    users.append({"email": email, "role": "admin", "password": password, "active": True})
        except Exception:
            pass
    # [auth] table support
    if not users:
        try:
            auth_block = plain_secret_value(st.secrets.get("auth", {}))
            if isinstance(auth_block, dict):
                email = normalize_email(str(auth_block.get("email", "") or auth_block.get("admin_email", "")))
                password = str(
                    auth_block.get("password", "")
                    or auth_block.get("admin_password", "")
                    or auth_block.get("password_hash", "")
                    or auth_block.get("admin_password_hash", "")
                ).strip()
                role = str(auth_block.get("role", "admin")).strip().lower()
                if email and password and role == "admin":
                    users.append({"email": email, "role": "admin", "password": password, "active": True})
        except Exception:
            pass
    # Hard fallback: parse .streamlit/secrets.toml directly
    if not users and SECRETS_PATH.exists():
        try:
            parsed = tomllib.loads(SECRETS_PATH.read_text(encoding="utf-8"))
            raw_file = parsed.get("admin_users", [])
            if not raw_file:
                nested = find_key_recursive(parsed, "admin_users")
                if nested is not None:
                    raw_file = nested
            if isinstance(raw_file, list):
                for item in raw_file:
                    if isinstance(item, dict):
                        append_user_if_valid(users, item)
            if not users and isinstance(raw_file, dict):
                for _, item in raw_file.items():
                    if isinstance(item, dict):
                        append_user_if_valid(users, item)
            if not users:
                email = normalize_email(
                    str(
                        parsed.get("admin_email", "")
                        or parsed.get("ADMIN_EMAIL", "")
                        or find_key_recursive(parsed, "admin_email")
                        or find_key_recursive(parsed, "ADMIN_EMAIL")
                        or ""
                    )
                )
                password = str(
                    parsed.get("admin_password", "")
                    or parsed.get("ADMIN_PASSWORD", "")
                    or parsed.get("admin_password_hash", "")
                    or parsed.get("ADMIN_PASSWORD_HASH", "")
                    or find_key_recursive(parsed, "admin_password")
                    or find_key_recursive(parsed, "ADMIN_PASSWORD")
                    or find_key_recursive(parsed, "admin_password_hash")
                    or find_key_recursive(parsed, "ADMIN_PASSWORD_HASH")
                ).strip()
                if email and password:
                    users.append({"email": email, "role": "admin", "password": password, "active": True})
        except Exception:
            pass
    # De-duplicate by email
    dedup: dict[str, dict[str, Any]] = {}
    for user in users:
        dedup[normalize_email(str(user.get("email", "")))] = user
    users = list(dedup.values())
    return users


def authenticate_admin_user(email: str, password: str) -> tuple[bool, str]:
    em = normalize_email(email)
    pw = str(password or "")
    if not em or not pw:
        return False, "Email and password are required."
    users = admin_users_from_secrets()
    if not users:
        return False, "Admin users are not configured. Put `admin_users` at root of secrets.toml (or set admin_email/admin_password), then restart app."
    matched_email = False
    for user in users:
        if normalize_email(user.get("email", "")) != em:
            continue
        matched_email = True
        if str(user.get("role", "")).lower() != "admin":
            return False, "This account does not have admin access."
        if verify_password(pw, str(user.get("password", ""))):
            return True, ""
        return False, "Invalid email or password."
    if not matched_email:
        return False, "Email not found in admin users."
    return False, "Invalid email or password."


def settings_admin_authenticated() -> bool:
    session = st.session_state.get(ADMIN_AUTH_SESSION_KEY, {})
    return bool(isinstance(session, dict) and session.get("authenticated") and session.get("role") == "admin")


def require_settings_admin_access() -> None:
    if settings_admin_authenticated():
        with st.sidebar:
            if st.button("Admin logout", use_container_width=True, key="settings_admin_logout_btn"):
                st.session_state.pop(ADMIN_AUTH_SESSION_KEY, None)
                st.rerun()
        return

    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] { display: none !important; }
        section.main > div { max-width: 100% !important; }
        .admin-login-card {
            width: 100%;
            max-width: 420px;
            border-radius: 18px;
            padding: 20px 20px 16px 20px;
            border: 1px solid rgba(255,255,255,.18);
            background: linear-gradient(180deg, rgba(195,149,233,.22), rgba(159,219,224,.18));
            box-shadow: none;
        }
        .admin-login-title {
            text-align: center;
            color: #f8fbff;
            font-size: 2rem;
            font-weight: 400;
            letter-spacing: .08em;
            margin: 8px 0 2px 0;
        }
        .admin-login-sub {
            text-align: center;
            color: #dbe7ff;
            font-size: .82rem;
            margin-bottom: 14px;
        }
        .admin-login-icon {
            text-align: center;
            color: #f8fbff;
            font-size: 2rem;
            line-height: 1;
            margin-top: 4px;
        }
        .admin-login-help {
            text-align: center;
            color: #eaf2ff;
            font-size: .76rem;
            margin-top: 8px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div style="height: 18vh;"></div>', unsafe_allow_html=True)
    left, center, right = st.columns([1.6, 1, 1.6])
    with center:
        st.markdown('<div class="admin-login-card">', unsafe_allow_html=True)
        st.markdown('<div class="admin-login-icon">◯</div>', unsafe_allow_html=True)
        st.markdown('<div class="admin-login-title">Admin Login</div>', unsafe_allow_html=True)
        st.markdown('<div class="admin-login-sub">Authorized admin access only</div>', unsafe_allow_html=True)

        with st.form("settings_admin_login_form", clear_on_submit=False):
            email = st.text_input("Email ID", key="settings_admin_email")
            password = st.text_input("Password", type="password", key="settings_admin_password")
            submitted = st.form_submit_button("LOGIN", use_container_width=True)

        st.markdown('<div class="admin-login-help">Use your admin email and password to open Settings.</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        if submitted:
            ok, message = authenticate_admin_user(email, password)
            if ok:
                st.session_state[ADMIN_AUTH_SESSION_KEY] = {"authenticated": True, "email": normalize_email(email), "role": "admin"}
                st.success("Admin access granted.")
                st.rerun()
            st.error(message)
    st.stop()


def column_match_score(column: str, aliases: list[str]) -> int:
    ncol = normalize_column(column)
    compact = ncol.replace("-", "").replace(" ", "")
    best = 0
    for alias in aliases:
        key = normalize_column(alias)
        key_compact = key.replace("-", "").replace(" ", "")
        if ncol == key:
            best = max(best, 100)
        elif compact == key_compact:
            best = max(best, 95)
        elif key in ncol or key_compact in compact:
            best = max(best, 80)
        elif all(part in ncol for part in key.split("-")):
            best = max(best, 65)
    return best


def smart_column_candidates(columns: list[str], kind: str, settings: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    configured = configured_column(settings or {}, kind, columns) if settings else None
    rows = []
    for col in columns:
        score = column_match_score(col, GPS_ALIASES[kind])
        if configured == col:
            score = max(score, 110)
        if score:
            rows.append({"column": col, "kind": kind, "confidence": min(score, 100), "source": "saved" if configured == col else "name match"})
    return sorted(rows, key=lambda row: (-int(row["confidence"]), str(row["column"]).lower()))


@st.cache_data(show_spinner=False)
def load_excel_bytes(payload: bytes, file_name: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    try:
        excel = pd.ExcelFile(io.BytesIO(payload))
        for sheet in excel.sheet_names:
            try:
                df = pd.read_excel(excel, sheet_name=sheet)
                if df.empty:
                    continue
                df["source_file"] = file_name
                df["source_sheet"] = sheet
                frames.append(df)
            except Exception:
                continue
    except Exception as exc:
        st.warning(f"Could not read {file_name}: {exc}")
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_excel_path(path_text: str) -> pd.DataFrame:
    path = Path(path_text)
    if not path.exists():
        return pd.DataFrame()
    try:
        return load_excel_bytes(path.read_bytes(), path.name)
    except Exception as exc:
        st.warning(f"Could not read sample file {path.name}: {exc}")
        return pd.DataFrame()


def google_drive_available() -> bool:
    return bool(service_account and build and MediaIoBaseDownload)


def build_drive_service(credentials_text: str) -> Any | None:
    if not google_drive_available():
        st.error("Google Drive packages are not installed. Run: pip install -r requirements.txt")
        return None
    if not credentials_text:
        return None
    try:
        info = json.loads(credentials_text)
        credentials = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        return build("drive", "v3", credentials=credentials, cache_discovery=False)
    except Exception as exc:
        st.error(f"Google Drive connection failed: {exc}")
        return None


@st.cache_data(show_spinner=False)
def list_drive_excel_files(credentials_text: str, folder_id: str, recursive: bool, max_files: int) -> list[dict[str, str]]:
    if not credentials_text or not folder_id:
        return []
    service = build_drive_service(credentials_text)
    if service is None:
        return []

    files: list[dict[str, str]] = []
    folders: list[tuple[str, str]] = [(folder_id, "")]
    seen_folders: set[str] = set()
    while folders and len(files) < max_files:
        current_folder, current_path = folders.pop(0)
        if current_folder in seen_folders:
            continue
        seen_folders.add(current_folder)
        page_token = None
        while len(files) < max_files:
            try:
                response = service.files().list(
                    q=f"'{current_folder}' in parents and trashed=false",
                    spaces="drive",
                    fields="nextPageToken, files(id, name, mimeType)",
                    pageToken=page_token,
                    pageSize=100,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                ).execute()
            except Exception as exc:
                st.error(f"Could not list Google Drive folder: {exc}")
                return []
            for item in response.get("files", []):
                mime_type = item.get("mimeType", "")
                if recursive and mime_type == "application/vnd.google-apps.folder":
                    folders.append((item["id"], f"{current_path}{item.get('name', '')}/"))
                elif mime_type in READABLE_DRIVE_MIMES or item.get("name", "").lower().endswith((".xlsx", ".xls")):
                    files.append({
                        "id": str(item.get("id", "")),
                        "name": str(item.get("name", "")),
                        "mimeType": str(mime_type),
                        "path": f"{current_path}{item.get('name', '')}",
                    })
                    if len(files) >= max_files:
                        break
            page_token = response.get("nextPageToken")
            if not page_token:
                break
    return files


@st.cache_data(show_spinner=False)
def load_drive_excel_files(credentials_text: str, folder_id: str, recursive: bool, max_files: int, selected_file_ids: tuple[str, ...] = ()) -> pd.DataFrame:
    if not credentials_text or not folder_id:
        return pd.DataFrame()
    service = build_drive_service(credentials_text)
    if service is None:
        return pd.DataFrame()

    files = list_drive_excel_files(credentials_text, folder_id, recursive, max_files)
    selected = {file_id for file_id in selected_file_ids if file_id}
    if selected:
        files = [item for item in files if item.get("id") in selected]

    frames: list[pd.DataFrame] = []
    for item in files:
        name = item.get("name", "drive_file.xlsx")
        try:
            request = (
                service.files().export_media(fileId=item["id"], mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                if item.get("mimeType") == "application/vnd.google-apps.spreadsheet"
                else service.files().get_media(fileId=item["id"], supportsAllDrives=True)
            )
            payload = io.BytesIO()
            downloader = MediaIoBaseDownload(payload, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            df = load_excel_bytes(payload.getvalue(), name)
            if not df.empty:
                frames.append(df)
        except Exception as exc:
            st.warning(f"Could not read Google Drive file {name}: {exc}")
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def load_google_drive_dataset(settings: dict[str, Any]) -> pd.DataFrame:
    drive = settings.get("google_drive", {})
    folder_id = extract_drive_folder_id(str(drive.get("folder_id") or drive.get("folder_url") or ""))
    if not folder_id:
        return pd.DataFrame()
    credentials_text = get_service_account_credentials_text(settings)
    if not credentials_text:
        st.error("Google Drive credentials were not found. Add [gdrive_service_account] to .streamlit/secrets.toml or Streamlit Cloud Secrets.")
        return pd.DataFrame()
    return load_drive_excel_files(
        credentials_text,
        folder_id,
        bool(drive.get("recursive", False)),
        int(drive.get("max_files", 50) or 50),
        tuple(str(file_id) for file_id in drive.get("selected_file_ids", []) if str(file_id).strip()),
    )


def list_google_drive_dataset_files(settings: dict[str, Any]) -> list[dict[str, str]]:
    drive = settings.get("google_drive", {})
    folder_id = extract_drive_folder_id(str(drive.get("folder_id") or drive.get("folder_url") or ""))
    if not folder_id:
        return []
    credentials_text = get_service_account_credentials_text(settings)
    if not credentials_text:
        st.error("Google Drive credentials were not found in Streamlit Secrets.")
        return []
    return list_drive_excel_files(
        credentials_text,
        folder_id,
        bool(drive.get("recursive", False)),
        int(drive.get("max_files", 50) or 50),
    )


def load_selected_files(uploaded_files: list[Any], use_samples: bool, use_google_drive: bool = False, settings: dict[str, Any] | None = None) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if use_google_drive and settings:
        df = load_google_drive_dataset(settings)
        if not df.empty:
            frames.append(df)
    if use_samples:
        for path in SAMPLE_FILES:
            df = load_excel_path(str(path))
            if not df.empty:
                frames.append(df)
    for file in uploaded_files:
        df = load_excel_bytes(file.getvalue(), excel_label(file))
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def extract_points(raw: pd.DataFrame, settings: dict[str, Any] | None = None) -> tuple[pd.DataFrame, dict[str, str | None]]:
    if raw.empty:
        return raw.copy(), {}
    review_col = str((settings or {}).get("columns", {}).get("review_status", "review_status") or "").strip()
    if review_col and review_col in raw.columns:
        raw = raw[raw[review_col].astype(str).str.strip().str.upper() != "REJECTED"].copy()

    columns = list(raw.columns)
    mapping = {
        "latitude": find_column(columns, "latitude", settings),
        "longitude": find_column(columns, "longitude", settings),
        "altitude": find_column(columns, "altitude", settings),
        "accuracy": find_column(columns, "accuracy", settings),
        "review_status": review_col if review_col in columns else None,
    }
    if not mapping["latitude"] or not mapping["longitude"]:
        return pd.DataFrame(), mapping

    df = raw.copy()
    df["latitude"] = pd.to_numeric(df[mapping["latitude"]], errors="coerce")
    df["longitude"] = pd.to_numeric(df[mapping["longitude"]], errors="coerce")
    df["altitude"] = pd.to_numeric(df[mapping["altitude"]], errors="coerce") if mapping["altitude"] else np.nan
    df["accuracy"] = pd.to_numeric(df[mapping["accuracy"]], errors="coerce") if mapping["accuracy"] else np.nan
    df["has_coordinates"] = df["latitude"].notna() & df["longitude"].notna()
    df["inside_afghanistan_bbox"] = df["latitude"].between(AFG_BOUNDS["lat_min"], AFG_BOUNDS["lat_max"]) & df["longitude"].between(AFG_BOUNDS["lon_min"], AFG_BOUNDS["lon_max"])

    quality = (settings or {}).get("quality", {})
    excellent_max = float(quality.get("excellent_max", 10) or 10)
    good_max = float(quality.get("good_max", 30) or 30)
    fair_max = float(quality.get("fair_max", 100) or 100)
    conditions = [
        ~df["has_coordinates"],
        df["accuracy"].notna() & (df["accuracy"] <= excellent_max),
        df["accuracy"].notna() & (df["accuracy"] <= good_max),
        df["accuracy"].notna() & (df["accuracy"] <= fair_max),
    ]
    choices = ["Missing", "Excellent", "Good", "Fair"]
    df["gps_quality"] = np.select(conditions, choices, default="Needs review")
    df["point_id"] = np.arange(1, len(df) + 1)
    df["point_color"] = [POINT_COLORS[i % len(POINT_COLORS)] for i in range(len(df))]
    return df, mapping


# Geometry helpers
def perpendicular_distance(point: tuple[float, float], start: tuple[float, float], end: tuple[float, float]) -> float:
    if start == end:
        return math.dist(point, start)
    px, py = point
    sx, sy = start
    ex, ey = end
    numerator = abs((ey - sy) * px - (ex - sx) * py + ex * sy - ey * sx)
    denominator = math.hypot(ey - sy, ex - sx) or 1e-12
    return numerator / denominator


def rdp_simplify(points: list[tuple[float, float]], tolerance: float) -> list[tuple[float, float]]:
    if len(points) < 4 or tolerance <= 0:
        return points
    closed = points[0] == points[-1]
    work = points[:-1] if closed else points
    if len(work) < 4:
        return points
    max_distance = 0.0
    index = 0
    for i in range(1, len(work) - 1):
        distance = perpendicular_distance(work[i], work[0], work[-1])
        if distance > max_distance:
            index = i
            max_distance = distance
    if max_distance > tolerance:
        left = rdp_simplify(work[: index + 1], tolerance)
        right = rdp_simplify(work[index:], tolerance)
        result = left[:-1] + right
    else:
        result = [work[0], work[-1]]
    if closed and result[0] != result[-1]:
        result.append(result[0])
    return result


def shape_geojson_coordinates(shape: shapefile.Shape, tolerance: float) -> tuple[str, list[Any]] | None:
    parts = list(shape.parts) + [len(shape.points)]
    polygons = []
    for start, end in zip(parts[:-1], parts[1:]):
        raw_ring = [(float(x), float(y)) for x, y in shape.points[start:end]]
        simplified = rdp_simplify(raw_ring, tolerance)
        ring = [[x, y] for x, y in simplified]
        if len(ring) >= 4:
            polygons.append([ring])
    if not polygons:
        return None
    geometry_type = "MultiPolygon" if len(polygons) > 1 else "Polygon"
    coordinates = polygons if geometry_type == "MultiPolygon" else polygons[0]
    return geometry_type, coordinates


@st.cache_data(show_spinner=False)
def read_admin_geojson(shp_path_text: str, level: int, tolerance: float) -> dict[str, Any]:
    reader = shapefile.Reader(shp_path_text, encoding="utf-8")
    fields = [field[0] for field in reader.fields[1:]]
    features = []
    for shape_record in reader.iterShapeRecords():
        attrs = dict(zip(fields, shape_record.record))
        geometry = shape_geojson_coordinates(shape_record.shape, tolerance)
        if geometry is None:
            continue
        geometry_type, coordinates = geometry
        name_key = f"adm{level}_name"
        pcode_key = f"adm{level}_pcode"
        features.append({
            "type": "Feature",
            "properties": {
                "name": attrs.get(name_key, ""),
                "pcode": attrs.get(pcode_key, ""),
                "province": attrs.get("adm1_name", attrs.get("adm1_ref_name", "")),
                "district": attrs.get("adm2_name", attrs.get("adm2_ref_name", "")),
                "region": attrs.get("regionname", attrs.get("region_nam", "")),
                "area_sqkm": attrs.get("area_sqkm", ""),
            },
            "geometry": {"type": geometry_type, "coordinates": coordinates},
        })
    return {"type": "FeatureCollection", "features": features}


@st.cache_data(show_spinner=False)
def read_region_geojson(shp_path_text: str, tolerance: float) -> dict[str, Any]:
    reader = shapefile.Reader(shp_path_text, encoding="utf-8")
    fields = [field[0] for field in reader.fields[1:]]
    features = []
    for shape_record in reader.iterShapeRecords():
        attrs = dict(zip(fields, shape_record.record))
        geometry = shape_geojson_coordinates(shape_record.shape, tolerance)
        if geometry is None:
            continue
        geometry_type, coordinates = geometry
        features.append({"type": "Feature", "properties": {"name": attrs.get("region_nam", ""), "pcode": attrs.get("region_pco", ""), "area_sqkm": attrs.get("area_sqkm", "")}, "geometry": {"type": geometry_type, "coordinates": coordinates}})
    return {"type": "FeatureCollection", "features": features}


@st.cache_data(show_spinner=False)
def read_admin_features_cached(shp_path_text: str, level: int) -> tuple[tuple[str, str, str, str, str, tuple[float, float, float, float], tuple[tuple[tuple[float, float], ...], ...]], ...]:
    reader = shapefile.Reader(shp_path_text, encoding="utf-8")
    fields = [field[0] for field in reader.fields[1:]]
    features: list[tuple[str, str, str, str, str, tuple[float, float, float, float], tuple[tuple[tuple[float, float], ...], ...]]] = []
    for shape_record in reader.iterShapeRecords():
        attrs = dict(zip(fields, shape_record.record))
        shape = shape_record.shape
        parts = list(shape.parts) + [len(shape.points)]
        rings = []
        for start, end in zip(parts[:-1], parts[1:]):
            ring = tuple((float(x), float(y)) for x, y in shape.points[start:end])
            if len(ring) >= 4:
                rings.append(ring)
        features.append((
            str(attrs.get(f"adm{level}_name", "")),
            str(attrs.get(f"adm{level}_pcode", "")),
            str(attrs.get("adm1_name", "")),
            str(attrs.get("regionname", attrs.get("region_nam", ""))),
            str(attrs.get("regioncode", attrs.get("region_pco", ""))),
            tuple(map(float, shape.bbox)),
            tuple(rings),
        ))
    return tuple(features)


def read_admin_features(shp_path_text: str, level: int) -> list[AdminFeature]:
    rows = read_admin_features_cached(shp_path_text, level)
    return [
        AdminFeature(
            name=name,
            pcode=pcode,
            parent=parent,
            region=region,
            region_code=region_code,
            bbox=bbox,
            rings=rings,
        )
        for name, pcode, parent, region, region_code, bbox, rings in rows
    ]


def point_in_ring(lon: float, lat: float, ring: tuple[tuple[float, float], ...]) -> bool:
    inside = False
    j = len(ring) - 1
    for i, (xi, yi) in enumerate(ring):
        xj, yj = ring[j]
        if (yi > lat) != (yj > lat):
            x_int = (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi
            if lon < x_int:
                inside = not inside
        j = i
    return inside


def locate_admin(lon: float, lat: float, features: list[AdminFeature]) -> AdminFeature | None:
    if math.isnan(lat) or math.isnan(lon):
        return None
    for feature in features:
        minx, miny, maxx, maxy = feature.bbox
        if not (minx <= lon <= maxx and miny <= lat <= maxy):
            continue
        if any(point_in_ring(lon, lat, ring) for ring in feature.rings):
            return feature
    return None


@st.cache_data(show_spinner=True)
def enrich_admin(points_json: str) -> pd.DataFrame:
    try:
        points = pd.read_json(io.StringIO(points_json), orient="records")
    except Exception:
        return pd.DataFrame()
    if points.empty or "latitude" not in points.columns or "longitude" not in points.columns:
        return pd.DataFrame()

    admin1 = read_admin_features(str(ADMIN1_SHP), 1)
    admin2 = read_admin_features(str(ADMIN2_SHP), 2)
    provinces: list[str] = []
    districts: list[str] = []
    district_pcodes: list[str] = []
    regions: list[str] = []
    region_codes: list[str] = []
    inside_polygon: list[bool] = []

    for row in points[["longitude", "latitude"]].itertuples(index=False):
        try:
            province = locate_admin(float(row.longitude), float(row.latitude), admin1)
            district = locate_admin(float(row.longitude), float(row.latitude), admin2)
            region_source = district or province
            provinces.append(province.name if province else "")
            districts.append(district.name if district else "")
            district_pcodes.append(district.pcode if district else "")
            regions.append(region_source.region if region_source else "")
            region_codes.append(region_source.region_code if region_source else "")
            inside_polygon.append(bool(province))
        except Exception:
            provinces.append("")
            districts.append("")
            district_pcodes.append("")
            regions.append("")
            region_codes.append("")
            inside_polygon.append(False)

    points["map_province"] = provinces
    points["map_district"] = districts
    points["map_district_pcode"] = district_pcodes
    points["map_region"] = regions
    points["map_region_code"] = region_codes
    points["inside_afghanistan"] = inside_polygon
    return points


# Display helpers
def clean_display_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value)
    return "" if text.lower() in {"nan", "nat", "none"} else text


def point_tooltip_html(row: pd.Series, popup_columns: list[str] | None = None) -> str:
    priority = ["point_id", "source_file", "source_sheet", "map_region", "map_province", "map_district", "latitude", "longitude", "altitude", "accuracy", "gps_quality", "Province", "District", "Village", "Surveyor_Name", "Date_And_Time"]
    ordered = [c for c in priority if c in row.index]
    ordered += [c for c in row.index if c not in ordered]
    if popup_columns:
        ordered = [c for c in popup_columns if c in row.index]
    skip = {"has_coordinates", "inside_afghanistan_bbox", "inside_afghanistan", "point_color"}
    rows_html = []
    for col in ordered:
        if col in skip:
            continue
        value = clean_display_value(row[col])
        if not value:
            continue
        if len(value) > 180:
            value = value[:177] + "..."
        rows_html.append(f"<tr><th>{escape(str(col))}</th><td>{escape(value)}</td></tr>")
    quality = clean_display_value(row.get("gps_quality", ""))
    q_color = GPS_QUALITY_COLORS.get(quality, "#64748b")
    badge = f'<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:{q_color}22;color:{q_color};border:1px solid {q_color}66;font-size:11px;font-weight:700;">{escape(quality)}</span>' if quality else ""
    return f"""
    <div style="max-height:380px;max-width:520px;overflow:auto;font-family:'Manrope',Arial,sans-serif;padding:4px;">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
        <div style="font-weight:800;font-size:14px;color:#0f172a;">Point #{escape(str(row.get('point_id', '')))}</div>{badge}
      </div>
      <table style="border-collapse:collapse;font-size:12px;line-height:1.4;width:100%;">{''.join(rows_html)}</table>
    </div>
    <style>
      .leaflet-tooltip table th {{ text-align:left;color:#334155;padding:3px 10px 3px 0;white-space:nowrap;vertical-align:top;font-weight:700; }}
      .leaflet-tooltip table td {{ color:#1e293b;padding:3px 0;vertical-align:top; }}
    </style>
    """


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_radius_km = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * earth_radius_km * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def point_label(row: pd.Series) -> str:
    village = clean_display_value(row.get("Village"))
    district = clean_display_value(row.get("map_district"))
    province = clean_display_value(row.get("map_province"))
    location = " / ".join(part for part in [province, district, village] if part)
    return f"Point {int(row['point_id'])}" + (f" - {location}" if location else "")


def sorted_non_empty(series: pd.Series) -> list[str]:
    values = series.dropna().astype(str).str.strip()
    return sorted(v for v in values.unique().tolist() if v)


# Map builder
def resolve_base_layer_name(base_map: str) -> str:
    if base_map == "Auto theme":
        return "Light map" if st.get_option("theme.base") == "light" else "Dark map"
    return base_map


def add_base_maps(fmap: folium.Map, base_map: str) -> None:
    name = resolve_base_layer_name(base_map)
    spec = BASE_MAPS.get(name) or BASE_MAPS["Dark map"]
    folium.TileLayer(tiles=spec["tiles"], name=spec["label"], attr=spec["attr"], control=False, overlay=False, show=True, no_wrap=True, max_native_zoom=19).add_to(fmap)


def add_map_chrome_style(fmap: folium.Map) -> None:
    style = """
    <style>
      html, body { margin:0!important; padding:0!important; width:100%!important; height:100%!important; overflow:hidden!important; background:#07111f!important; }
      body > div, .folium-map, .leaflet-container { width:100%!important; height:100%!important; min-height:100%!important; margin:0!important; padding:0!important; background:#07111f!important; border:0!important; outline:0!important; box-shadow:none!important; }
      .leaflet-tooltip { border:0!important; border-radius:14px!important; background:rgba(248,250,252,.98)!important; color:#1e293b!important; box-shadow:0 18px 44px rgba(0,0,0,.28)!important; padding:10px 14px!important; }
      .leaflet-popup-content-wrapper, .leaflet-popup-tip { border:0!important; background:rgba(248,250,252,.98)!important; box-shadow:0 20px 54px rgba(0,0,0,.34)!important; }
      .leaflet-popup-content-wrapper { border-radius:14px!important; overflow:hidden!important; }
      .leaflet-control-zoom a { background:rgba(7,17,31,.82)!important; color:#e8eefc!important; border-color:rgba(255,255,255,.12)!important; backdrop-filter:blur(14px); }
      .leaflet-control-zoom a:hover { background:rgba(56,189,248,.22)!important; }
    </style>
    """
    fmap.get_root().header.add_child(folium.Element(style))


def build_map(df: pd.DataFrame, selected_pair: tuple[int, int] | None = None, base_map: str = "Auto theme", overlays: list[str] | None = None, map_layout: str = "Balanced", marker_size: int = 7, cluster_points: bool = False, enable_minimap: bool = False, enable_measure: bool = True, enable_mouse_position: bool = True, popup_columns: list[str] | None = None, max_points: int = 3000) -> folium.Map:
    enabled = set(overlays if overlays is not None else DEFAULT_OVERLAYS)
    layout = MAP_LAYOUTS.get(map_layout, MAP_LAYOUTS["Balanced"])
    fmap = folium.Map(location=[34.55, 66.25], zoom_start=6, tiles=None, width="100%", height="100%", control_scale=True, prefer_canvas=True, min_zoom=5, max_bounds=True, min_lat=AFG_BOUNDS["lat_min"], max_lat=AFG_BOUNDS["lat_max"], min_lon=AFG_BOUNDS["lon_min"], max_lon=AFG_BOUNDS["lon_max"], max_bounds_viscosity=1.0)
    add_base_maps(fmap, base_map)
    add_map_chrome_style(fmap)
    fmap.fit_bounds(AFG_LEAFLET_BOUNDS, padding=(8, 8))

    if ADMIN0_SHP.exists() and "Afghanistan boundary" in enabled:
        folium.GeoJson(read_admin_geojson(str(ADMIN0_SHP), 0, 0.012), name="Afghanistan boundary", style_function=lambda _: {"fillColor": "#ffffff", "color": "#0f172a", "weight": 2.6, "fillOpacity": 0.04 * layout["fill_multiplier"]}, tooltip=folium.GeoJsonTooltip(fields=["name"], aliases=["Country"])).add_to(fmap)
    if REGIONS_SHP.exists() and "Zones" in enabled:
        folium.GeoJson(read_region_geojson(str(REGIONS_SHP), 0.03), name="Zones", style_function=lambda f: {"fillColor": REGION_COLORS.get(f["properties"].get("name", ""), "#64748b"), "color": REGION_COLORS.get(f["properties"].get("name", ""), "#64748b"), "weight": 1.1, "fillOpacity": 0.08 * layout["fill_multiplier"]}, tooltip=folium.GeoJsonTooltip(fields=["name", "pcode"], aliases=["Zone", "Code"])).add_to(fmap)
    if ADMIN1_SHP.exists() and "Provinces" in enabled:
        folium.GeoJson(read_admin_geojson(str(ADMIN1_SHP), 1, 0.03), name="Provinces", style_function=lambda _: {"fillColor": "#0f766e", "color": "#0f4f4a", "weight": layout["province_weight"], "fillOpacity": 0.035 * layout["fill_multiplier"]}, tooltip=folium.GeoJsonTooltip(fields=["name", "pcode"], aliases=["Province", "P-code"])).add_to(fmap)
    if ADMIN2_SHP.exists() and layout["districts"] and "Districts" in enabled:
        folium.GeoJson(read_admin_geojson(str(ADMIN2_SHP), 2, 0.03), name="Districts", style_function=lambda _: {"fillColor": "#ffffff", "color": "#607d78", "weight": layout["district_weight"], "fillOpacity": 0.0}, tooltip=folium.GeoJsonTooltip(fields=["district", "province"], aliases=["District", "Province"])).add_to(fmap)

    valid = df[df["inside_afghanistan"] & df["has_coordinates"]].copy() if not df.empty else pd.DataFrame()
    plot_df = valid
    if not valid.empty and len(valid) > max_points:
        plot_df = valid.sample(n=max_points, random_state=42)
    detailed_popup = len(plot_df) <= 1500
    if "GPS points" in enabled and not valid.empty:
        points_group = folium.FeatureGroup(name="GPS points", show=True).add_to(fmap)
        marker_parent = MarkerCluster(name="Point clusters", show=True).add_to(points_group) if cluster_points else points_group
        ultra_fast_mode = len(plot_df) > 700
        if ultra_fast_mode:
            coords = plot_df[["latitude", "longitude"]].astype(float).values.tolist()
            FastMarkerCluster(coords, name="Fast GPS points").add_to(points_group)
            marker_parent = None
        if marker_parent is not None and detailed_popup:
            for _, row in plot_df.iterrows():
                tip_html = point_tooltip_html(row, popup_columns=popup_columns)
                folium.CircleMarker(location=[row["latitude"], row["longitude"]], radius=marker_size, color="#ffffff", weight=1.5, fill=True, fill_color=row["point_color"], fill_opacity=0.9, popup=folium.Popup(tip_html, max_width=560), tooltip=folium.Tooltip(tip_html, sticky=True)).add_to(marker_parent)
        elif marker_parent is not None:
            for row in plot_df.itertuples(index=False):
                point_id = int(getattr(row, "point_id", 0))
                province = clean_display_value(getattr(row, "map_province", ""))
                district = clean_display_value(getattr(row, "map_district", ""))
                label = f"Point {point_id}" + (f" - {province}/{district}" if province or district else "")
                folium.CircleMarker(
                    location=[float(getattr(row, "latitude")), float(getattr(row, "longitude"))],
                    radius=marker_size,
                    color="#ffffff",
                    weight=1.0,
                    fill=True,
                    fill_color=str(getattr(row, "point_color", "#38bdf8")),
                    fill_opacity=0.82,
                    tooltip=label,
                ).add_to(marker_parent)

    if selected_pair and not valid.empty:
        start_id, end_id = selected_pair
        selected = valid[valid["point_id"].isin([start_id, end_id])].set_index("point_id")
        if start_id in selected.index and end_id in selected.index:
            p1 = selected.loc[start_id]
            p2 = selected.loc[end_id]
            coords = [[p1["latitude"], p1["longitude"]], [p2["latitude"], p2["longitude"]]]
            distance = haversine_km(p1["latitude"], p1["longitude"], p2["latitude"], p2["longitude"])
            folium.PolyLine(coords, color="#38bdf8", weight=3, opacity=0.9, dash_array="8 6", tooltip=f"Distance: {distance:.2f} km").add_to(fmap)
            for point_id, color in [(start_id, "#22c55e"), (end_id, "#ef4444")]:
                if point_id in selected.index:
                    p = selected.loc[point_id]
                    folium.CircleMarker(location=[p["latitude"], p["longitude"]], radius=marker_size + 4, color=color, weight=3, fill=False).add_to(fmap)

    if not valid.empty:
        lat_min = max(float(valid["latitude"].min()) - 0.25, AFG_BOUNDS["lat_min"])
        lat_max = min(float(valid["latitude"].max()) + 0.25, AFG_BOUNDS["lat_max"])
        lon_min = max(float(valid["longitude"].min()) - 0.25, AFG_BOUNDS["lon_min"])
        lon_max = min(float(valid["longitude"].max()) + 0.25, AFG_BOUNDS["lon_max"])
        if lat_min < lat_max and lon_min < lon_max:
            fmap.fit_bounds([[lat_min, lon_min], [lat_max, lon_max]], padding=(24, 24))

    if enable_minimap:
        MiniMap(toggle_display=True, minimized=True).add_to(fmap)
    if enable_measure:
        MeasureControl(position="topleft", primary_length_unit="kilometers", secondary_length_unit="meters").add_to(fmap)
    if enable_mouse_position:
        MousePosition(position="bottomright", separator=" | ", prefix="Lat/Lon:", num_digits=5).add_to(fmap)
    Fullscreen(position="topright").add_to(fmap)
    folium.LayerControl(position="topright", collapsed=True).add_to(fmap)
    return fmap


# Data pipeline and summaries
def prepare_points(raw: pd.DataFrame, settings: dict[str, Any] | None = None) -> tuple[pd.DataFrame, dict[str, str | None], int]:
    rejected_count = 0
    review_col = str((settings or {}).get("columns", {}).get("review_status", "review_status") or "").strip()
    if review_col and review_col in raw.columns:
        rejected_count = int((raw[review_col].astype(str).str.strip().str.upper() == "REJECTED").sum())
    points, mapping = extract_points(raw, settings)
    if points.empty:
        return points, mapping, rejected_count
    locatable = points[points["has_coordinates"] & points["inside_afghanistan_bbox"]].copy()
    if locatable.empty:
        for col in ["map_province", "map_district", "map_district_pcode", "map_region", "map_region_code"]:
            points[col] = ""
        points["inside_afghanistan"] = False
    else:
        enriched_cols = ["point_id", "map_province", "map_district", "map_district_pcode", "map_region", "map_region_code", "inside_afghanistan"]
        try:
            enriched = enrich_admin(locatable.to_json(orient="records"))
            if not enriched.empty:
                available = [c for c in enriched_cols if c in enriched.columns]
                points = points.merge(enriched[available], on="point_id", how="left")
        except Exception as exc:
            st.warning(f"Admin enrichment failed: {exc}")
            for col in enriched_cols[1:]:
                points[col] = "" if col != "inside_afghanistan" else False
    for col in ["map_province", "map_district", "map_district_pcode", "map_region", "map_region_code"]:
        if col not in points.columns:
            points[col] = ""
        points[col] = points[col].fillna("")
    if "inside_afghanistan" not in points.columns:
        points["inside_afghanistan"] = False
    points["inside_afghanistan"] = points["inside_afghanistan"].fillna(False).astype(bool)
    return points, mapping, rejected_count


def mapped_points(points: pd.DataFrame) -> pd.DataFrame:
    if points.empty:
        return points.copy()
    return points[points["has_coordinates"] & points["inside_afghanistan"]].copy()


def display_columns_for(df: pd.DataFrame, settings: dict[str, Any] | None = None) -> list[str]:
    wanted = ["point_id", "source_file", "source_sheet", "latitude", "longitude", "altitude", "accuracy", "gps_quality", "map_region", "map_region_code", "map_province", "map_district", "Province", "District", "Village", "Surveyor_Name", "Date_And_Time"]
    if settings:
        wanted += selected_display_columns(settings, list(df.columns))
    return [c for c in wanted if c in df.columns]


def location_summary(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if df.empty or not all(c in df.columns for c in group_cols):
        return pd.DataFrame()
    summary = df.groupby(group_cols, dropna=False).agg(points=("point_id", "count"), avg_accuracy_m=("accuracy", "mean"), min_accuracy_m=("accuracy", "min"), max_accuracy_m=("accuracy", "max"), source_files=("source_file", "nunique")).reset_index().sort_values("points", ascending=False)
    for col in ["avg_accuracy_m", "min_accuracy_m", "max_accuracy_m"]:
        if col in summary.columns:
            summary[col] = summary[col].round(2)
    return summary


def quality_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "gps_quality" not in df.columns:
        return pd.DataFrame()
    summary = df["gps_quality"].value_counts(dropna=False).rename_axis("gps_quality").reset_index(name="points")
    total = max(int(summary["points"].sum()), 1)
    summary["share_%"] = (summary["points"] / total * 100).round(1)
    order_map = {quality: index for index, quality in enumerate(GPS_QUALITY_ORDER)}
    summary["_order"] = summary["gps_quality"].map(lambda q: order_map.get(q, 99))
    return summary.sort_values("_order").drop(columns="_order")


def source_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "source_file" not in df.columns:
        return pd.DataFrame()
    summary = df.groupby("source_file", dropna=False).agg(points=("point_id", "count"), provinces=("map_province", "nunique"), districts=("map_district", "nunique"), avg_accuracy_m=("accuracy", "mean")).reset_index().sort_values("points", ascending=False)
    summary["avg_accuracy_m"] = summary["avg_accuracy_m"].round(2)
    return summary


def ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    cols = [str(c) for c in df.columns]
    seen: dict[str, int] = {}
    unique: list[str] = []
    changed = False
    for col in cols:
        count = seen.get(col, 0) + 1
        seen[col] = count
        if count == 1:
            unique.append(col)
        else:
            changed = True
            unique.append(f"{col} ({count})")
    if not changed:
        return df
    fixed = df.copy()
    fixed.columns = unique
    return fixed


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def render_professional_error(title: str, detail: str = "") -> None:
    st.markdown(
        f"""
        <div style="border:1px solid rgba(248,113,113,.35);background:rgba(127,29,29,.20);border-radius:14px;padding:14px 16px;">
            <div style="font-size:1rem;font-weight:800;color:#fee2e2;margin-bottom:6px;">{escape(title)}</div>
            <div style="font-size:.86rem;color:#fecaca;line-height:1.55;">
                {escape(detail) if detail else "An unexpected issue occurred. Please retry, and if the issue continues, contact the administrator."}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def safe_dataframe(df: pd.DataFrame, **kwargs: Any) -> None:
    try:
        st.dataframe(ensure_unique_columns(df), **kwargs)
    except Exception:
        render_professional_error(
            "Could not render table",
            "The data format is currently not compatible for table rendering. Please adjust the selected dataset columns in Settings and try again.",
        )


def csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def excel_report_bytes(filtered: pd.DataFrame, all_points: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        filtered.to_excel(writer, index=False, sheet_name="Filtered Points")
        mapped_points(all_points).to_excel(writer, index=False, sheet_name="All Mapped Points")
        location_summary(filtered, ["map_region"]).to_excel(writer, index=False, sheet_name="By Zone")
        location_summary(filtered, ["map_region", "map_province"]).to_excel(writer, index=False, sheet_name="By Province")
        location_summary(filtered, ["map_region", "map_province", "map_district"]).to_excel(writer, index=False, sheet_name="By District")
        quality_summary(filtered).to_excel(writer, index=False, sheet_name="GPS Quality")
        source_summary(filtered).to_excel(writer, index=False, sheet_name="Source Files")
    return output.getvalue()


def dataframe_cache_signature(df: pd.DataFrame, columns: list[str] | None = None) -> str:
    if df.empty:
        return "empty"
    cols = [col for col in (columns or list(df.columns)) if col in df.columns]
    base = f"{len(df)}:{','.join(cols)}"
    try:
        sample = df[cols].head(50).tail(50).to_json(date_format="iso", default_handler=str)
    except Exception:
        sample = str(df[cols].head(20).to_dict()) if cols else ""
    return hashlib.sha1(f"{base}:{sample}".encode("utf-8", errors="ignore")).hexdigest()


def cached_excel_report_bytes(cache_key: str, filtered: pd.DataFrame, all_points: pd.DataFrame) -> bytes:
    state_key = "excel_report_bytes_cache"
    cache = st.session_state.get(state_key, {})
    if cache.get("key") == cache_key and isinstance(cache.get("data"), bytes):
        return cache["data"]
    data = excel_report_bytes(filtered, all_points)
    st.session_state[state_key] = {"key": cache_key, "data": data}
    return data


def map_html_from_session_cache(cache_key: str) -> str | None:
    cache = st.session_state.get("dashboard_map_html_cache", {})
    if isinstance(cache, dict) and cache.get("key") == cache_key:
        html = cache.get("html")
        return html if isinstance(html, str) else None
    return None


def save_map_html_to_session_cache(cache_key: str, html: str) -> None:
    st.session_state["dashboard_map_html_cache"] = {"key": cache_key, "html": html}


# Filters and controls
def keep_valid_multiselect_state(key: str, options: list[str]) -> None:
    current = st.session_state.get(key)
    if not isinstance(current, list):
        return
    valid = [value for value in current if value in options]
    if valid != current:
        st.session_state[key] = valid


def filter_points_by_selection(df: pd.DataFrame, regions: list[str] | None = None, provinces: list[str] | None = None, districts: list[str] | None = None, qualities: list[str] | None = None, source_files: list[str] | None = None) -> pd.DataFrame:
    filtered = df.copy()
    if regions and "map_region" in filtered.columns:
        filtered = filtered[filtered["map_region"].isin(regions)]
    if provinces and "map_province" in filtered.columns:
        filtered = filtered[filtered["map_province"].isin(provinces)]
    if districts and "map_district" in filtered.columns:
        filtered = filtered[filtered["map_district"].isin(districts)]
    if qualities and "gps_quality" in filtered.columns:
        filtered = filtered[filtered["gps_quality"].isin(qualities)]
    if source_files and "source_file" in filtered.columns:
        filtered = filtered[filtered["source_file"].isin(source_files)]
    return filtered


def location_filter_panel(mapped: pd.DataFrame, key_prefix: str) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    if mapped.empty:
        return mapped.copy(), {"regions": [], "provinces": [], "districts": [], "qualities": [], "source_files": []}

    st.markdown('<div class="top-filter-card"><div class="filter-header"><div class="filter-title">Location and quality filters</div><div class="filter-note">Use filters to refine displayed GPS points</div></div>', unsafe_allow_html=True)
    region_key = f"{key_prefix}_regions"
    province_key = f"{key_prefix}_provinces"
    district_key = f"{key_prefix}_districts"
    quality_key = f"{key_prefix}_qualities"
    source_key = f"{key_prefix}_sources"

    region_options = sorted_non_empty(mapped["map_region"]) if "map_region" in mapped.columns else []
    keep_valid_multiselect_state(region_key, region_options)
    c1, c2, c3, c4, c5 = st.columns([1, 1.15, 1.25, 1, 1.15], gap="medium")
    with c1:
        selected_regions = st.multiselect("Zone", region_options, key=region_key, placeholder="All zones")
    region_df = filter_points_by_selection(mapped, regions=selected_regions)
    province_options = sorted_non_empty(region_df["map_province"]) if "map_province" in region_df.columns else []
    keep_valid_multiselect_state(province_key, province_options)
    with c2:
        selected_provinces = st.multiselect("Province", province_options, key=province_key, placeholder="All provinces")
    province_df = filter_points_by_selection(region_df, provinces=selected_provinces)
    district_options = sorted_non_empty(province_df["map_district"]) if "map_district" in province_df.columns else []
    keep_valid_multiselect_state(district_key, district_options)
    with c3:
        selected_districts = st.multiselect("District", district_options, key=district_key, placeholder="All districts")
    quality_options = sorted_non_empty(mapped["gps_quality"]) if "gps_quality" in mapped.columns else []
    source_options = sorted_non_empty(mapped["source_file"]) if "source_file" in mapped.columns else []
    keep_valid_multiselect_state(quality_key, quality_options)
    keep_valid_multiselect_state(source_key, source_options)
    with c4:
        selected_qualities = st.multiselect("GPS quality", quality_options, key=quality_key, placeholder="All levels")
    with c5:
        selected_sources = st.multiselect("Source file", source_options, key=source_key, placeholder="All files")

    filters = {"regions": selected_regions, "provinces": selected_provinces, "districts": selected_districts, "qualities": selected_qualities, "source_files": selected_sources}
    filtered = filter_points_by_selection(mapped, **filters)
    active = " | ".join(f"{label}: {len(values)}" for label, values in [("Zones", selected_regions), ("Provinces", selected_provinces), ("Districts", selected_districts), ("Quality", selected_qualities), ("Sources", selected_sources)] if values)
    st.caption(active or "Showing all mapped Afghanistan GPS points.")
    st.markdown('</div>', unsafe_allow_html=True)
    return filtered, filters


def map_studio_controls(key_prefix: str = "dashboard") -> dict[str, Any]:
    base_map = st.selectbox("Map type", list(BASE_MAPS.keys()), index=0, key=f"{key_prefix}_base_map")
    try:
        map_layout = st.segmented_control("Layout", list(MAP_LAYOUTS.keys()), default="Balanced", key=f"{key_prefix}_map_layout")
    except AttributeError:
        map_layout = st.selectbox("Layout", list(MAP_LAYOUTS.keys()), index=0, key=f"{key_prefix}_map_layout")
    overlays = st.multiselect("Layers", ALL_OVERLAYS, default=DEFAULT_OVERLAYS, key=f"{key_prefix}_map_layers")
    cluster_points = st.toggle("Cluster points", value=True, key=f"{key_prefix}_cluster_points")
    enable_measure = st.toggle("Measure tool", value=False, key=f"{key_prefix}_measure_tool")
    enable_mouse_position = st.toggle("Live coordinates", value=False, key=f"{key_prefix}_mouse_position")
    enable_minimap = st.toggle("Mini map", value=False, key=f"{key_prefix}_mini_map")
    marker_size = st.slider("Point size", min_value=3, max_value=12, value=4, key=f"{key_prefix}_marker_size")
    max_render_points = st.slider("Max points on map", min_value=200, max_value=8000, value=800, step=200, key=f"{key_prefix}_max_render_points")
    st.markdown('</div>', unsafe_allow_html=True)
    selected_layout = str(map_layout or "Balanced")
    return {"base_map": base_map, "overlays": overlays, "map_layout": selected_layout, "marker_size": marker_size, "cluster_points": cluster_points, "enable_minimap": enable_minimap, "enable_measure": enable_measure, "enable_mouse_position": enable_mouse_position, "max_render_points": int(max_render_points), "height": MAP_LAYOUTS.get(selected_layout, MAP_LAYOUTS["Balanced"])["height"]}


# Modern dashboard cards
def html_metric_card(title: str, value: str, subtitle: str) -> str:
    return f'<div class="metric-card"><div class="metric-title">{escape(title)}</div><div class="metric-number">{escape(value)}</div><div class="metric-sub">{escape(subtitle)}</div></div>'


def render_metric_cards(points: pd.DataFrame, mapped: pd.DataFrame, filtered: pd.DataFrame, rejected_count: int, distance: float) -> None:
    total_rows = len(points)
    valid_points = len(mapped)
    filtered_points = len(filtered)
    outside_points = int((points["has_coordinates"] & ~points["inside_afghanistan"]).sum()) if not points.empty else 0
    missing_points = int((~points["has_coordinates"]).sum()) if not points.empty else 0
    cards = [
        html_metric_card("Accepted rows", f"{total_rows:,}", "Total processed rows"),
        html_metric_card("Mapped points", f"{valid_points:,}", "Valid Afghanistan locations"),
        html_metric_card("Filtered points", f"{filtered_points:,}", "Visible after filters"),
        html_metric_card("Outside Afghanistan", f"{outside_points:,}", "Coordinates outside boundary"),
        html_metric_card("Selected distance", f"{distance:.2f} km", f"Rejected rows: {rejected_count:,}; missing GPS: {missing_points:,}"),
    ]
    st.markdown('<div class="metric-grid">' + ''.join(cards) + '</div>', unsafe_allow_html=True)


def compact_date_range(df: pd.DataFrame) -> tuple[str, str]:
    candidates = ["Date_And_Time", "date", "Date", "submission_time", "start", "end"]
    for col in candidates:
        if col in df.columns:
            dates = pd.to_datetime(df[col], errors="coerce").dropna()
            if not dates.empty:
                start = dates.min().strftime("%b %d")
                end = dates.max().strftime("%b %d, %Y")
                days = max((dates.max().date() - dates.min().date()).days, 0)
                return f"{start} - {end}", f"{days} days"
    return "Not available", "No date column"


def render_zone_donut(filtered: pd.DataFrame) -> str:
    if filtered.empty or "map_region" not in filtered.columns:
        return ""
    counts = filtered["map_region"].replace("", "Unknown").value_counts().head(5)
    total = max(int(counts.sum()), 1)
    colors = ["#7c3aed", "#2563eb", "#34d399", "#eab308", "#f59e0b"]
    legend = []
    for i, (name, count) in enumerate(counts.items()):
        pct = count / total * 100
        legend.append(f'<div class="legend-row"><span class="legend-name"><span class="dot" style="background:{colors[i % len(colors)]}"></span>{escape(str(name))}</span><span>{int(count)} <span style="color:#94a3b8;">({pct:.1f}%)</span></span></div>')
    return '<div class="chart-card"><div class="chart-title">Points by Zone</div><div class="donut-wrap"><div class="donut"></div><div style="width:100%;">' + ''.join(legend) + '</div></div></div>'


def render_province_bars(filtered: pd.DataFrame) -> str:
    if filtered.empty or "map_province" not in filtered.columns:
        return ""
    counts = filtered["map_province"].replace("", "Other").value_counts().head(6)
    max_value = max(int(counts.max()), 1)
    total = max(int(counts.sum()), 1)
    rows = []
    for name, count in counts.items():
        width = int(count / max_value * 100)
        pct = count / total * 100
        rows.append(f'<div class="bar-row"><div>{escape(str(name))}</div><div class="bar-track"><div class="bar-fill" style="width:{width}%"></div></div><div>{int(count)} <span style="color:#94a3b8;">({pct:.1f}%)</span></div></div>')
    return '<div class="chart-card"><div class="chart-title">Points by Province</div>' + ''.join(rows) + '</div>'


def render_accuracy_panel(filtered: pd.DataFrame) -> str:
    qs = quality_summary(filtered)
    if qs.empty:
        return ""
    max_value = max(int(qs["points"].max()), 1)
    rows = []
    for _, row in qs.iterrows():
        quality = str(row["gps_quality"])
        count = int(row["points"])
        width = int(count / max_value * 100)
        color = GPS_QUALITY_COLORS.get(quality, "#64748b")
        rows.append(f'<div class="bar-row"><div>{escape(quality)}</div><div class="bar-track"><div class="bar-fill" style="width:{width}%;background:linear-gradient(90deg,{color},#38bdf8);"></div></div><div>{count} <span style="color:#94a3b8;">({float(row["share_%"]):.1f}%)</span></div></div>')
    return '<div class="chart-card"><div class="chart-title">Accuracy Summary</div>' + ''.join(rows) + '</div>'


def render_analytics_cards(filtered: pd.DataFrame) -> None:
    cards = [render_zone_donut(filtered), render_province_bars(filtered), render_accuracy_panel(filtered)]
    cards = [card for card in cards if card.strip()]
    if not cards:
        return
    st.markdown('<div class="analytics-grid">' + ''.join(cards) + '</div>', unsafe_allow_html=True)


def render_quality_html(filtered: pd.DataFrame) -> None:
    qs = quality_summary(filtered)
    if qs.empty:
        st.markdown('<div class="side-caption">No quality data available.</div>', unsafe_allow_html=True)
        return
    rows = []
    for _, qrow in qs.iterrows():
        quality = str(qrow["gps_quality"])
        color = GPS_QUALITY_COLORS.get(quality, "#64748b")
        rows.append(f'<div class="quality-row"><span class="quality-name"><span class="quality-dot" style="background:{color};"></span><span style="color:{color};">{escape(quality)}</span></span><span>{int(qrow["points"])} <span style="color:#94a3b8;">({float(qrow["share_%"]):.1f}%)</span></span></div>')
    st.markdown(''.join(rows), unsafe_allow_html=True)


# Legacy full dashboard kept for reference; the active main below renders the simplified map view.
def legacy_dashboard_main() -> None:
    st.set_page_config(page_title="GPS Tracker", page_icon="GPS", layout="wide", initial_sidebar_state="expanded")
    page_style()
    settings = load_project_settings()
    render_sidebar_nav()

    st.markdown('<div class="page-header"><div><div class="page-title">GPS Tracker</div><div class="page-subtitle">Verify Afghanistan GPS points, measure distances, and monitor quality.</div></div><div class="page-badge">Prepared by PPC</div></div>', unsafe_allow_html=True)

    with st.sidebar:
        profiles = dataset_profiles(settings)
        selected_dataset_name = ""
        if profiles:
            names = dataset_names(settings)
            default_name = str(settings.get("active_dataset") or names[0])
            selected_dataset_name = st.selectbox("Project dataset", names, index=names.index(default_name) if default_name in names else 0, key="dashboard_dataset_profile")
            active_settings = settings_for_dataset(settings, selected_dataset_name)
        else:
            active_settings = settings
            st.warning("No saved datasets yet. Open Settings to add one.")
        use_drive = True
        use_samples = False
        drive = active_settings.get("google_drive", {})
        folder_id = extract_drive_folder_id(str(drive.get("folder_id") or drive.get("folder_url") or ""))
        secret_status = google_drive_secret_status()
        if folder_id and secret_status["available"]:
            st.success("Dataset loaded ")
        else:
            st.warning("Open Settings and configure datasets")
        review_col = active_settings.get("columns", {}).get("review_status", "review_status")
        st.divider()
        map_config = map_studio_controls("dashboard")

    dataset_cache_key = json.dumps(
        {
            "dataset_name": selected_dataset_name,
            "folder_id": extract_drive_folder_id(str(active_settings.get("google_drive", {}).get("folder_id") or active_settings.get("google_drive", {}).get("folder_url") or "")),
            "selected_file_ids": list(active_settings.get("google_drive", {}).get("selected_file_ids", [])),
            "columns": active_settings.get("columns", {}),
            "quality": active_settings.get("quality", {}),
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    cached_key = st.session_state.get("dashboard_dataset_cache_key")
    if cached_key == dataset_cache_key and "dashboard_cached_points" in st.session_state:
        points = st.session_state["dashboard_cached_points"].copy()
        mapping = dict(st.session_state.get("dashboard_cached_mapping", {}))
        rejected_count = int(st.session_state.get("dashboard_cached_rejected", 0))
    else:
        raw = load_selected_files([], use_samples, use_drive, active_settings)
        if raw.empty:
            st.info("No data loaded from Settings. Open Settings and select files from your Google Drive dataset.")
            return
        with st.spinner("Processing GPS points..."):
            points, mapping, rejected_count = prepare_points(raw, active_settings)
        st.session_state["dashboard_dataset_cache_key"] = dataset_cache_key
        st.session_state["dashboard_cached_points"] = points.copy()
        st.session_state["dashboard_cached_mapping"] = dict(mapping)
        st.session_state["dashboard_cached_rejected"] = int(rejected_count)

    if points.empty:
        st.error("No usable latitude/longitude columns found in the uploaded data.")
        with st.expander("Column mapping debug"):
            st.write(mapping)
        return

    mapped = mapped_points(points)
    st.session_state["gps_points"] = points
    st.session_state["gps_mapped"] = mapped
    st.session_state["gps_rejected_count"] = rejected_count
    st.session_state["gps_column_mapping"] = mapping
    st.session_state["gps_dataset_name"] = selected_dataset_name

    filtered, active_filters = location_filter_panel(mapped, key_prefix="dashboard")
    popup_cols = selected_popup_columns(active_settings, list(filtered.columns))

    selected_pair: tuple[int, int] | None = None
    distance = 0.0
    draw_distance_line = False

    st.markdown('<div class="dashboard-grid">', unsafe_allow_html=True)
    left_area, right_area = st.columns([4.35, 1], gap="medium")

    with right_area:
        if len(filtered) >= 2:
            distance_limit = 750
            distance_df = filtered.head(distance_limit)
            if len(filtered) > distance_limit:
                st.caption(f"Distance picker is showing the first {distance_limit:,} filtered points for faster reruns.")
            labels = {
                point_label(row): int(row["point_id"])
                for _, row in distance_df.iterrows()
            }
            label_list = list(labels.keys())
            start_label = st.selectbox("Point A", label_list, index=0, key="dist_a")
            end_label = st.selectbox("Point B", label_list, index=min(1, len(label_list) - 1), key="dist_b")
            start_id = labels[start_label]
            end_id = labels[end_label]
            if start_id != end_id:
                selected_pair = (start_id, end_id)
                p1 = filtered[filtered["point_id"] == start_id].iloc[0]
                p2 = filtered[filtered["point_id"] == end_id].iloc[0]
                distance = haversine_km(p1["latitude"], p1["longitude"], p2["latitude"], p2["longitude"])
                st.markdown(f'<div class="side-card"><div class="side-label">Distance</div><div class="side-value">{distance:.2f} km</div><div class="side-caption">{distance * 1000:,.0f} meters · {distance / 1.852:.2f} nmi</div></div>', unsafe_allow_html=True)
                draw_distance_line = st.toggle("Draw distance line on map", value=False, key="dashboard_draw_distance_line")
            else:
                st.warning("Select two different points.")
        else:
            st.info("At least two mapped points are needed.")

        render_quality_html(filtered)

        date_range, date_span = compact_date_range(filtered)
        st.markdown(f'<div class="side-card"><div class="side-label">Date range</div><div class="side-caption" style="font-size:.86rem;color:#f8fbff;">{escape(date_range)}</div><div class="side-caption">{escape(date_span)}</div></div>', unsafe_allow_html=True)
        st.download_button("Download filtered CSV", data=csv_bytes(filtered), file_name="gps_tracker_filtered.csv", mime="text/csv", use_container_width=True)
        excel_key = dataframe_cache_signature(filtered, display_columns_for(filtered, active_settings)) + ":" + dataframe_cache_signature(points, ["point_id", "latitude", "longitude", "accuracy", "gps_quality"])
        st.download_button("Download Excel report", data=cached_excel_report_bytes(excel_key, filtered, points), file_name="gps_tracker_report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

    with left_area:
        st.markdown('<div class="map-shell"><div class="map-head"><div><div class="brand-title">GPS Tracker Dashboard</div><div class="brand-sub">Operational map and field data overview</div></div><div class="head-actions"><span class="status-pill">Live dashboard</span><span class="status-pill">Map studio in sidebar</span></div></div><div class="map-body">', unsafe_allow_html=True)
        if len(filtered) > int(map_config["max_render_points"]):
            st.caption(f"Performance mode ON: rendering {int(map_config['max_render_points']):,} of {len(filtered):,} points with fast map engine.")
        map_cache_key = json.dumps(
            {
                "dataset": dataset_cache_key,
                "filters": active_filters,
                "pair": selected_pair if draw_distance_line else None,
                "config": map_config,
                "popup": popup_cols,
                "rows": len(filtered),
                "sig": dataframe_cache_signature(filtered, ["point_id", "latitude", "longitude", "point_color", "inside_afghanistan", "has_coordinates"]),
            },
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        map_html = map_html_from_session_cache(map_cache_key)
        if map_html is None:
            with st.spinner("Rendering map..."):
                fmap = build_map(filtered, selected_pair=selected_pair if draw_distance_line else None, base_map=map_config["base_map"], overlays=map_config["overlays"], map_layout=map_config["map_layout"], marker_size=map_config["marker_size"], cluster_points=map_config["cluster_points"], enable_minimap=map_config["enable_minimap"], enable_measure=map_config["enable_measure"], enable_mouse_position=map_config["enable_mouse_position"], popup_columns=popup_cols, max_points=int(map_config["max_render_points"]))
                map_html = fmap.get_root().render()
                save_map_html_to_session_cache(map_cache_key, map_html)
        components.html(map_html, height=int(map_config["height"]), scrolling=False)
        st.markdown('</div></div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

    render_metric_cards(points, mapped, filtered, rejected_count, distance)
    render_analytics_cards(filtered)

    extra_cards = [
        html_metric_card("Data sources", f"{source_summary(filtered).shape[0]:,}", "Files loaded"),
        html_metric_card("Total points", f"{len(points):,}", "Including non-mapped rows"),
        html_metric_card("Average accuracy", f"{filtered['accuracy'].mean():.1f} m" if not filtered.empty and filtered["accuracy"].notna().any() else "N/A", "Across filtered records"),
        html_metric_card("Mapped share", f"{(len(mapped) / max(len(points), 1) * 100):.1f}%", "Mapped over accepted rows"),
        html_metric_card("Last updated", "Just now", "Current session"),
    ]
    st.markdown('<div class="metric-grid">' + ''.join(extra_cards) + '</div>', unsafe_allow_html=True)

    st.subheader("GPS Point Data")
    try:
        data_view = st.segmented_control("Data view", ["Points", "Quality breakdown", "Source files"], default="Points", key="dashboard_data_view")
    except AttributeError:
        data_view = st.selectbox("Data view", ["Points", "Quality breakdown", "Source files"], key="dashboard_data_view")
    if data_view == "Points":
        available_columns = display_columns_for(filtered, active_settings)
        if available_columns:
            display_df = filtered[available_columns].head(700)
            if len(filtered) > len(display_df):
                st.caption(f"Showing first {len(display_df):,} rows for fast rendering. CSV export includes all filtered rows.")
            safe_dataframe(display_df, use_container_width=True, height=420)
        else:
            st.info("No displayable columns found.")
    elif data_view == "Quality breakdown":
        qs = quality_summary(filtered)
        if not qs.empty:
            safe_dataframe(qs, use_container_width=True)
        else:
            st.info("No quality data available.")
    else:
        ss = source_summary(filtered)
        if not ss.empty:
            safe_dataframe(ss, use_container_width=True)
        else:
            st.info("No source file data available.")

def main() -> None:
    legacy_dashboard_main()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        render_professional_error(
            "Dashboard temporarily unavailable",
            "A runtime issue occurred while opening the dashboard. Please refresh once. If it continues, contact the administrator.",
        )
