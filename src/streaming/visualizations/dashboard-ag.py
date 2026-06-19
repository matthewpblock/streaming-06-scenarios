"""src/streaming/visualizations/dashboard-ag.py.

Premium Streamlit dashboard for Maritime Domain Awareness (MDA).
Connects to DuckDB, visualizes Chinese military/government vessels,
and overlays deterministic threat ranges on a live Pydeck map.
"""

from pathlib import Path
import time
from typing import Final

import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

# === CONFIGURATION ===
DB_PATH: Final[Path] = Path("data/output/ais.duckdb")

# Page layout
st.set_page_config(
    page_title="Maritime Domain Awareness | Chinese Naval Stream",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Premium Styling (Dark Mode, Glassmorphism, Modern Fonts)
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
    }

    /* Main Background */
    .stApp {
        background-color: #0B0F19;
        color: #F8FAFC;
    }

    /* Header Gradient styling */
    .mda-title-container {
        padding: 10px 0;
        border-bottom: 2px solid #1E293B;
        margin-bottom: 25px;
    }
    .mda-title {
        font-size: 2.5rem;
        font-weight: 800;
        margin: 0;
        background: linear-gradient(135deg, #38BDF8, #818CF8, #C084FC);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .mda-subtitle {
        font-size: 1rem;
        color: #94A3B8;
        margin: 5px 0 0 0;
    }

    /* Card design for metrics */
    div.stMetric {
        background: linear-gradient(145deg, #1E293B, #0F172A);
        border-radius: 12px;
        padding: 15px 20px;
        border: 1px solid #334155;
        box-shadow: 0 4px 20px 0 rgba(0, 0, 0, 0.2);
    }

    /* Custom Badges for Threat Levels */
    .badge {
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.8rem;
        text-align: center;
        display: inline-block;
    }
    .badge-critical { background-color: rgba(239, 68, 68, 0.2); color: #EF4444; border: 1px solid #EF4444; }
    .badge-high { background-color: rgba(249, 115, 22, 0.2); color: #F97316; border: 1px solid #F97316; }
    .badge-medium { background-color: rgba(245, 158, 11, 0.2); color: #F59E0B; border: 1px solid #F59E0B; }
    .badge-low { background-color: rgba(16, 185, 129, 0.2); color: #10B981; border: 1px solid #10B981; }

    /* Tables */
    .dataframe {
        border-collapse: collapse;
        width: 100%;
        color: #E2E8F0;
    }
    .dataframe th {
        background-color: #1E293B;
        color: #94A3B8;
        padding: 10px;
        text-align: left;
    }
    .dataframe td {
        padding: 10px;
        border-bottom: 1px solid #334155;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_data() -> pd.DataFrame:
    """Retrieve all valid telemetry records from the DuckDB database."""
    if not DB_PATH.exists():
        return pd.DataFrame()

    try:
        # Connect to DuckDB database read-only
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        # Query valid vessels
        df = conn.execute("SELECT * FROM consumed_valid_ais").fetchdf()
        conn.close()
        return df
    except Exception as error:
        st.error(f"Error loading DuckDB storage: {error}")
        return pd.DataFrame()


def get_latest_vessels(df: pd.DataFrame) -> pd.DataFrame:
    """Return only the latest coordinate report for each unique vessel MMSI."""
    if df.empty:
        return df

    # Group by MMSI and pick the record with the maximum timestamp
    idx = df.groupby("mmsi")["timestamp"].idxmax()
    return df.loc[idx].reset_index(drop=True)


def assign_colors(df: pd.DataFrame) -> pd.DataFrame:
    """Assign RGB color lists based on threat risk levels for map plotting."""
    if df.empty:
        return df

    # Color codes
    colors = {
        "CRITICAL": [239, 68, 68],  # Crimson
        "HIGH": [249, 115, 22],  # Coral Orange
        "MEDIUM": [245, 158, 11],  # Amber Yellow
        "LOW": [16, 185, 129],  # Emerald Green
    }

    # Opacity levels
    fill_colors = []
    ring_colors = []

    for _, row in df.iterrows():
        lvl = str(row.get("risk_level", "LOW")).upper()
        rgb = colors.get(lvl, [148, 163, 184])  # Fallback Slate

        fill_colors.append(rgb + [220])  # Solid marker color
        ring_colors.append(rgb + [40])  # Transparent threat circle

    df["fill_color"] = fill_colors
    df["ring_color"] = ring_colors
    return df


# === RENDER INTERFACE ===

st.markdown(
    """
    <div class="mda-title-container">
        <h1 class="mda-title">Maritime Domain Awareness</h1>
        <p class="mda-subtitle">Real-Time Threat Detection & Tracking Network (Chinese Forces / Live WebSocket Feed)</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# Load data
raw_df = load_data()
latest_df = get_latest_vessels(raw_df)
latest_df = assign_colors(latest_df)

# Sidebar controls
st.sidebar.image("https://img.icons8.com/nolan/96/radar.png", width=70)
st.sidebar.markdown("### Radar Control Panel")

# Live connection status
if DB_PATH.exists() and not raw_df.empty:
    st.sidebar.success("📡 Telemetry Stream Active")
    st.sidebar.caption(f"Cached messages: {len(raw_df)} total")
else:
    st.sidebar.warning("⏳ Awaiting Stream Connections")
    st.sidebar.caption("Run the Kafka producer and consumer to populate the database.")

# Auto-refresh checkbox
auto_refresh = st.sidebar.checkbox("Auto-refresh data (5s)", value=True)

# Selectors
if not latest_df.empty:
    risk_options = ["ALL"] + sorted(latest_df["risk_level"].unique().tolist())
    selected_risk = st.sidebar.selectbox("Filter by Risk Level", risk_options)

    category_options = ["ALL"] + sorted(latest_df["vessel_category"].unique().tolist())
    selected_category = st.sidebar.selectbox("Filter by Category", category_options)

    # Specific vessel track tool
    vessel_names = latest_df["ship_name"].tolist()
    vessel_mmsis = latest_df["mmsi"].tolist()
    vessel_options = ["ALL VESSELS"] + [
        f"{name or 'Unclassified'} ({mmsi})"
        for name, mmsi in zip(vessel_names, vessel_mmsis, strict=False)
    ]
    selected_vessel_opt = st.sidebar.selectbox(
        "Select Vessel to Track Path", vessel_options
    )
else:
    selected_risk = "ALL"
    selected_category = "ALL"
    selected_vessel_opt = "ALL VESSELS"

# Apply Risk & Category filters
display_df = latest_df.copy()
if not display_df.empty:
    if selected_risk != "ALL":
        display_df = display_df[display_df["risk_level"] == selected_risk]
    if selected_category != "ALL":
        display_df = display_df[display_df["vessel_category"] == selected_category]

# KPI metrics header
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

total_vessels = len(latest_df)
critical_threats = (
    len(latest_df[latest_df["risk_level"] == "CRITICAL"]) if not latest_df.empty else 0
)
high_threats = (
    len(latest_df[latest_df["risk_level"] == "HIGH"]) if not latest_df.empty else 0
)
avg_speed = (
    latest_df["sog"].mean() if not latest_df.empty and "sog" in latest_df else 0.0
)

kpi1.metric(
    "Vessels Tracked",
    total_vessels,
    help="Total active Chinese military/government vessels",
)
kpi2.metric("Critical Threats", critical_threats, delta_color="inverse")
kpi3.metric("High Threats", high_threats)
kpi4.metric("Avg Speed", f"{avg_speed:.1f} knots")

# Main content tabs
tab_map, tab_matrix, tab_charts = st.tabs(
    ["🗺️ Operation Map", "📋 Alert Log & Matrix", "📈 Operations Analytics"]
)

with tab_map:
    if display_df.empty:
        st.info(
            "No active vessels matching criteria. Start the stream to display radar mapping."
        )
    else:
        # Bounding box / view state setup
        if selected_vessel_opt != "ALL VESSELS":
            # Track a single vessel's history path
            target_mmsi = selected_vessel_opt.split("(")[-1].rstrip(")")
            history_df = raw_df[raw_df["mmsi"] == target_mmsi].sort_values("timestamp")

            st.subheader(f"Telemetry Path History for MMSI {target_mmsi}")

            # Map elements for specific vessel history
            vessel_color = assign_colors(history_df)
            path_layer = pdk.Layer(
                "PathLayer",
                data=[
                    {
                        "path": history_df[["longitude", "latitude"]].values.tolist(),
                        "name": selected_vessel_opt,
                    }
                ],
                get_path="path",
                get_color=[129, 140, 248, 255],  # Custom Indigo line
                width_min_pixels=3,
                pickable=True,
            )

            marker_layer = pdk.Layer(
                "ScatterplotLayer",
                data=history_df,
                get_position="[longitude, latitude]",
                get_color=[239, 68, 68, 200]
                if "fill_color" not in history_df
                else "fill_color",
                get_radius=1000,
                pickable=True,
            )

            view_state = pdk.ViewState(
                latitude=history_df["latitude"].iloc[-1],
                longitude=history_df["longitude"].iloc[-1],
                zoom=6,
                pitch=30,
            )

            deck = pdk.Deck(
                layers=[path_layer, marker_layer],
                initial_view_state=view_state,
                map_provider="carto",
                map_style="dark",
                tooltip={
                    "text": "Vessel: {ship_name}\nMMSI: {mmsi}\nSpeed: {sog} kts\nTime: {timestamp}"
                },
            )
            st.pydeck_chart(deck)

            # Detail table below map
            st.dataframe(
                history_df[
                    ["timestamp", "latitude", "longitude", "sog", "cog", "heading"]
                ]
            )
        else:
            # Show all vessels + threat range circles
            st.subheader("Tactical Radar Overlay")

            ring_layer = pdk.Layer(
                "ScatterplotLayer",
                data=display_df,
                get_position="[longitude, latitude]",
                get_color="ring_color",
                get_radius="threat_range_km * 1000",  # km to meters
                pickable=False,
                filled=True,
                stroked=True,
                get_line_color="fill_color",
                line_width_min_pixels=1,
            )

            marker_layer = pdk.Layer(
                "ScatterplotLayer",
                data=display_df,
                get_position="[longitude, latitude]",
                get_color="fill_color",
                get_radius=3000,
                pickable=True,
            )

            view_state = pdk.ViewState(
                latitude=display_df["latitude"].mean()
                if not display_df.empty
                else 25.0,
                longitude=display_df["longitude"].mean()
                if not display_df.empty
                else 120.0,
                zoom=4.5,
                pitch=40,
            )

            deck = pdk.Deck(
                layers=[ring_layer, marker_layer],
                initial_view_state=view_state,
                map_provider="carto",
                map_style="dark",
                tooltip={
                    "html": "<b>Vessel:</b> {ship_name} (MMSI: {mmsi})<br/>"
                    "<b>Category:</b> {vessel_category}<br/>"
                    "<b>Risk Level:</b> {risk_level}<br/>"
                    "<b>Speed:</b> {sog} knots<br/>"
                    "<b>Threat Envelope:</b> {threat_range_km} km<br/>"
                    "<b>Details:</b> {threat_description}",
                    "style": {"backgroundColor": "#1E293B", "color": "#F8FAFC"},
                },
            )
            st.pydeck_chart(deck)

with tab_matrix:
    st.subheader("Current Operational Alerts")
    if display_df.empty:
        st.info("No active alerts.")
    else:
        # Create a nicely styled table
        alert_table = []
        for _, row in display_df.iterrows():
            mmsi = row.get("mmsi")
            name = row.get("ship_name") or "Unclassified"
            cat = row.get("vessel_category", "Unknown")
            risk = row.get("risk_level", "LOW")
            rng = f"{row.get('threat_range_km', 15.0):.1f} km"
            desc = row.get("threat_description", "")
            time_val = row.get("timestamp", "")
            sog_val = f"{row.get('sog', 0.0):.1f} kts"

            # Badge styles
            badge_class = f"badge-{risk.lower()}"
            badge_html = f"<span class='badge {badge_class}'>{risk}</span>"

            alert_table.append(
                {
                    "MMSI": mmsi,
                    "Vessel Name": name,
                    "Vessel Category": cat,
                    "Threat Risk": badge_html,
                    "Threat Envelope": rng,
                    "Speed": sog_val,
                    "Alert Info": desc,
                    "Last Telemetry (UTC)": time_val,
                }
            )

        alerts_html = pd.DataFrame(alert_table).to_html(
            escape=False, index=False, classes="dataframe"
        )
        st.markdown(alerts_html, unsafe_allow_html=True)

with tab_charts:
    st.subheader("Threat Analytics")
    if display_df.empty:
        st.info("Insufficient telemetry to plot analytics.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Vessel Count by Risk Level")
            risk_counts = display_df["risk_level"].value_counts().reset_index()
            risk_counts.columns = ["Risk Level", "Count"]
            st.bar_chart(risk_counts.set_index("Risk Level"))

        with col2:
            st.markdown("#### Vessel Count by Category")
            cat_counts = display_df["vessel_category"].value_counts().reset_index()
            cat_counts.columns = ["Category", "Count"]
            st.bar_chart(cat_counts.set_index("Category"))

# Auto-refresh loop
if auto_refresh:
    time.sleep(5)
    st.rerun()
