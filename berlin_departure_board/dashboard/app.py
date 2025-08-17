from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from berlin_departure_board.storage.client import RedisClient

BERLIN_CENTER = {"lat": 52.5200, "lon": 13.4050}
BERLIN_BOUNDS = {"lat_min": 52.3, "lat_max": 52.7, "lon_min": 13.0, "lon_max": 13.8}


COLORS = {
    "primary": "#1f77b4",
    "secondary": "#ff7f0e",
    "success": "#2ca02c",
    "warning": "#ff7f0e",
    "danger": "#d62728",
    "background": "#f8f9fa",
    "text": "#212529",
    "muted": "#6c757d",
}


def init_page_config():
    st.set_page_config(
        page_title="Berlin Transit Delay Heatmap",
        page_icon="🗺️",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        f"""
    <style>
    .main {{
        background-color: {COLORS['background']};
        color: {COLORS['text']};
    }}

    .block-container {{
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }}

    .header-container {{
        background: linear-gradient(135deg, {COLORS['primary']} 0%, {COLORS['secondary']} 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }}

    .metric-card {{
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border-left: 4px solid {COLORS['primary']};
        margin: 1rem 0;
    }}

    .metric-title {{
        font-size: 2rem;
        font-weight: bold;
        margin: 0;
        color: {COLORS['primary']};
    }}

    .metric-subtitle {{
        font-size: 0.9rem;
        color: {COLORS['muted']};
        margin: 0.5rem 0 0 0;
    }}

    .status-indicator {{
        display: inline-block;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        margin-right: 8px;
    }}

    .status-good {{ background-color: {COLORS['success']}; }}
    .status-warning {{ background-color: {COLORS['warning']}; }}
    .status-danger {{ background-color: {COLORS['danger']}; }}

    .sidebar .sidebar-content {{
        background-color: white;
    }}
    </style>
    """,
        unsafe_allow_html=True,
    )


@st.cache_resource
def init_redis_client():
    try:
        return RedisClient()
    except Exception as e:
        st.error(f"❌ Failed to connect to Redis: {e}")
        return None


def get_station_delay_data(
    redis_client: RedisClient, minutes_back: int = 15
) -> pd.DataFrame:
    try:
        all_metrics = redis_client.get_all_station_metrics(minutes_back)

        if not all_metrics:
            return pd.DataFrame()

        rows = []
        for station_id, metrics_list in all_metrics.items():
            if not metrics_list:
                continue

            valid_metrics = [m for m in metrics_list if m.get("avg_delay") is not None]

            if not valid_metrics:
                continue

            latest_metric = max(metrics_list, key=lambda x: x.get("window_start", ""))
            lat = latest_metric.get("latitude")
            lon = latest_metric.get("longitude")
            station_name = latest_metric.get("station_name")

            if not lat or not lon:
                continue

            total_departures = sum(
                int(m.get("total_departures", 0)) for m in valid_metrics
            )
            delayed_departures = sum(
                int(m.get("delayed_departures", 0)) for m in valid_metrics
            )
            on_time_departures = sum(
                int(m.get("on_time_departures", 0)) for m in valid_metrics
            )
            significant_delays = sum(
                int(m.get("significant_delays", 0)) for m in valid_metrics
            )

            weighted_delay_sum = sum(
                float(m.get("avg_delay", 0)) * int(m.get("total_departures", 0))
                for m in valid_metrics
                if m.get("avg_delay") is not None
                and int(m.get("total_departures", 0)) > 0
            )
            avg_delay = (
                weighted_delay_sum / total_departures if total_departures > 0 else 0.0
            )

            max_delay = max(float(m.get("max_delay", 0)) for m in valid_metrics)

            if total_departures > 0:
                max_on_time = min(on_time_departures, total_departures)
                max_delayed = min(delayed_departures, total_departures)

                on_time_pct = max_on_time / total_departures * 100
                delay_pct = max_delayed / total_departures * 100

                if max_on_time + max_delayed > total_departures:
                    scale_factor = total_departures / (max_on_time + max_delayed)
                    on_time_pct = max_on_time * scale_factor / total_departures * 100
                    delay_pct = max_delayed * scale_factor / total_departures * 100
            else:
                on_time_pct = 100.0
                delay_pct = 0.0

            window_starts = [
                m.get("window_start", "")
                for m in valid_metrics
                if m.get("window_start")
            ]
            earliest_window = min(window_starts) if window_starts else ""
            latest_window = max(window_starts) if window_starts else ""

            rows.append(
                {
                    "station_id": station_id,
                    "station_name": station_name,
                    "lat": float(lat),
                    "lon": float(lon),
                    "avg_delay": round(avg_delay, 1),
                    "max_delay": round(max_delay, 1),
                    "total_departures": total_departures,
                    "delayed_departures": delayed_departures,
                    "on_time_departures": on_time_departures,
                    "significant_delays": significant_delays,
                    "on_time_pct": round(on_time_pct, 1),
                    "delay_pct": round(delay_pct, 1),
                    "window_start": earliest_window,
                    "window_end": latest_window,
                    "windows_count": len(valid_metrics),
                    "updated_at": latest_metric.get("updated_at", ""),
                    "raw_metric": str(latest_metric),
                }
            )

        return pd.DataFrame(rows)

    except Exception as e:
        st.error(f"Error fetching station data: {e}")
        return pd.DataFrame()


def create_delay_heatmap(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            title="No Station Data Available",
            geo=dict(
                center={"lat": BERLIN_CENTER["lat"], "lon": BERLIN_CENTER["lon"]},
                projection_scale=50,
                showland=True,
                landcolor="lightgray",
            ),
            height=600,
        )
        return fig

    df["delay_category"] = pd.cut(
        df["avg_delay"],
        bins=[-float("inf"), 0, 2, 5, 10, float("inf")],
        labels=[
            "On Time",
            "Minor Delay",
            "Moderate Delay",
            "Major Delay",
            "Severe Delay",
        ],
    )

    color_map = {
        "On Time": "#64B5F6",
        "Minor Delay": "#42A5F5",
        "Moderate Delay": "#2196F3",
        "Major Delay": "#1976D2",
        "Severe Delay": "#0D47A1",
    }

    df["color"] = df["delay_category"].map(color_map)

    fig = go.Figure()

    legend_categories = [
        ("On Time (< 2 min)", "#64B5F6"),
        ("Minor Delay (2-5 min)", "#42A5F5"),
        ("Moderate Delay (5-10 min)", "#2196F3"),
        ("Major Delay (10+ min)", "#1976D2"),
        ("Severe Delay", "#0D47A1"),
    ]

    for category, color in legend_categories:
        fig.add_trace(
            go.Scattermapbox(
                lat=[],
                lon=[],
                mode="markers",
                marker=dict(size=15, color=color, opacity=0.8),
                name=category,
                showlegend=True,
                hoverinfo="skip",
            )
        )

    for _, row in df.iterrows():
        fig.add_trace(
            go.Scattermapbox(
                lat=[row["lat"]],
                lon=[row["lon"]],
                mode="markers",
                marker=dict(
                    size=max(20, 20 + (row["avg_delay"] * 3)),
                    color=row["color"],
                    opacity=0.9,
                ),
                text=row["station_name"],
                hovertemplate=(
                    f"<b>{row['station_name']}</b><br>"
                    f"Category: {row['delay_category']}<br>"
                    f"Average Delay: {row['avg_delay']:.1f} min<br>"
                    f"Max Delay: {row['max_delay']:.1f} min<br>"
                    f"On Time: {row['on_time_pct']:.1f}%<br>"
                    f"Total Departures: {row['total_departures']}<br>"
                    f"<extra></extra>"
                ),
                showlegend=False,
            )
        )

    fig.update_layout(
        title={
            "text": "Berlin Station Delay Heatmap",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 20, "color": COLORS["text"]},
        },
        mapbox=dict(
            style="open-street-map",
            center={"lat": BERLIN_CENTER["lat"], "lon": BERLIN_CENTER["lon"]},
            zoom=11,
            bearing=0,
            pitch=0,
        ),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="rgba(0,0,0,0.3)",
            borderwidth=1,
            font=dict(size=12, color=COLORS["text"]),
            itemsizing="constant",
        ),
        height=600,
        margin=dict(l=0, r=0, t=60, b=0),
        paper_bgcolor=COLORS["background"],
        plot_bgcolor=COLORS["background"],
    )

    return fig


def render_summary_metrics(df: pd.DataFrame):
    if df.empty:
        st.warning("No station data available for metrics")
        return

    col1, col2, col3 = st.columns(3)

    with col1:
        active_stations = len(df)
        st.markdown(
            f"""
        <div class="metric-card">
            <div class="metric-title">{active_stations}</div>
            <div class="metric-subtitle">Active Stations<br><small>(Rolled up data)</small></div>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        avg_delay = df["avg_delay"].mean()
        delay_status = (
            "status-good"
            if avg_delay < 2
            else "status-warning" if avg_delay < 5 else "status-danger"
        )
        st.markdown(
            f"""
        <div class="metric-card">
            <div class="metric-title">
                <span class="status-indicator {delay_status}"></span>{avg_delay:.1f} min
            </div>
            <div class="metric-subtitle">Average Delay</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col3:
        total_departures = df["total_departures"].sum()
        st.markdown(
            f"""
        <div class="metric-card">
            <div class="metric-title">{total_departures}</div>
            <div class="metric-subtitle">Total Departures<br><small>(Rolled up across windows)</small></div>
        </div>
        """,
            unsafe_allow_html=True,
        )


def render_station_table(df: pd.DataFrame):
    """Render detailed station information table"""
    if df.empty:
        st.warning("No station data available")
        return

    st.subheader("📊 Station Details")

    display_df = df.copy()
    display_df = display_df[
        [
            "station_name",
            "avg_delay",
            "max_delay",
            "on_time_pct",
            "total_departures",
            "delayed_departures",
        ]
    ]
    display_df.columns = [
        "Station",
        "Avg Delay (min)",
        "Max Delay (min)",
        "On Time %",
        "Total Departures",
        "Delayed Departures",
    ]

    display_df = display_df.sort_values("Avg Delay (min)", ascending=False)

    display_df = display_df.round(
        {"Avg Delay (min)": 1, "Max Delay (min)": 1, "On Time %": 1}
    )

    def get_delay_status(delay):
        if delay < 2:
            return "🟢"
        elif delay < 5:
            return "🟡"
        else:
            return "🔴"

    def get_performance_status(on_time_pct):
        if on_time_pct > 80:
            return "🟢"
        elif on_time_pct > 60:
            return "🟡"
        else:
            return "🔴"

    display_df["Status"] = display_df["Avg Delay (min)"].apply(get_delay_status)
    display_df["Performance"] = display_df["On Time %"].apply(get_performance_status)

    cols = [
        "Status",
        "Station",
        "Avg Delay (min)",
        "Max Delay (min)",
        "Performance",
        "On Time %",
        "Total Departures",
        "Delayed Departures",
    ]
    display_df = display_df[cols]

    st.dataframe(display_df, use_container_width=True, hide_index=True)


def main():
    """Main dashboard application"""
    init_page_config()

    st.markdown(
        f"""
    <div class="header-container">
        <h1>🗺️ Berlin Transit Delay Heatmap</h1>
        <p>Real-time delay visualization across Berlin's public transport network</p>
        <p><small>Updated every 5 minutes with windowed analytics</small></p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    redis_client = init_redis_client()
    if not redis_client:
        st.error("Cannot connect to data source. Please check the system status.")
        st.stop()

    with st.sidebar:
        st.markdown("### ⚙️ Controls")

        minutes_back = st.slider(
            "Data Time Range (minutes)",
            min_value=15,
            max_value=120,
            value=15,
            step=15,
            help="How far back to look for station metrics (15min = 1 window, 120min = 2 hours max)",
        )

        auto_refresh = st.checkbox(
            "Auto Refresh",
            value=True,
            help="Automatically refresh data every 30 seconds",
        )

        if st.button("🔄 Refresh Now"):
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.markdown("### 🗺️ Map Legend")
        st.markdown(
            f'<div style="display: flex; align-items: center; margin: 5px 0;"><div style="width: 15px; height: 15px; background-color: #64B5F6; border-radius: 50%; margin-right: 8px; border: 1px solid #ccc;"></div>On Time (< 2 min)</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="display: flex; align-items: center; margin: 5px 0;"><div style="width: 15px; height: 15px; background-color: #42A5F5; border-radius: 50%; margin-right: 8px; border: 1px solid #ccc;"></div>Minor Delay (2-5 min)</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="display: flex; align-items: center; margin: 5px 0;"><div style="width: 15px; height: 15px; background-color: #2196F3; border-radius: 50%; margin-right: 8px; border: 1px solid #ccc;"></div>Moderate Delay (5-10 min)</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="display: flex; align-items: center; margin: 5px 0;"><div style="width: 15px; height: 15px; background-color: #1976D2; border-radius: 50%; margin-right: 8px; border: 1px solid #ccc;"></div>Major Delay (10+ min)</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="display: flex; align-items: center; margin: 5px 0;"><div style="width: 15px; height: 15px; background-color: #0D47A1; border-radius: 50%; margin-right: 8px; border: 1px solid #ccc;"></div>Severe Delay</div>',
            unsafe_allow_html=True,
        )

        st.markdown("---")
        st.markdown("### ℹ️ About")
        st.markdown(
            "**Real-time Transit Monitoring:** Monitors performance across major Berlin stations, recording delays, on-time performance, and departure volumes in real-time."
        )
        st.markdown(
            "**Data Collection:** Continuously tracks BVG (Berlin public transport) departure data using streaming analytics with 15-minute tumbling windows."
        )
        st.markdown(
            f"**Time Range:** Currently showing {minutes_back} minutes of aggregated data across ~{minutes_back // 15} measurement windows."
        )
        st.markdown(
            "**Interactive Map:** Color-coded stations show delay severity - from blue (on-time) to dark blue (severe delays). Larger markers indicate higher average delays."
        )

    with st.spinner("Loading station delay data..."):
        df = get_station_delay_data(redis_client, minutes_back)

    if not df.empty:
        st.sidebar.write(f"**Stations found:** {len(df)}")
        st.sidebar.write(f"**Time Range:** {minutes_back} minutes")
        st.sidebar.write(
            f"**Expected Windows:** ~{minutes_back // 15} (15min tumbling)"
        )

        if "windows_count" in df.columns:
            avg_windows = df["windows_count"].mean()
            max_windows = df["windows_count"].max()
            min_windows = df["windows_count"].min()
            st.sidebar.write(f"**Avg Windows/Station:** {avg_windows:.1f}")
            st.sidebar.write(f"**Windows Range:** {min_windows}-{max_windows}")

        total_departures = df["total_departures"].sum()
        st.sidebar.write(f"**Total Departures (Rolled):** {total_departures}")

    else:
        st.sidebar.write("No data available for debugging")

    if df.empty:
        st.warning(
            "⚠️ No station data available. Please ensure the data pipeline is running."
        )
        st.info("The Spark streaming processor needs to be active to generate metrics.")
        st.stop()

    render_summary_metrics(df)

    st.markdown("---")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("🗺️ Interactive Delay Map")
        fig = create_delay_heatmap(df)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🏆 Best/Worst Performers")

        if not df.empty:
            best_station = df.loc[df["avg_delay"].idxmin()]
            st.markdown(
                f"""
            **🟢 Best Performance:**
            - **{best_station['station_name']}**
            - Avg Delay: {best_station['avg_delay']:.1f} min
            - On Time: {best_station['on_time_pct']:.1f}%
            """
            )

            worst_station = df.loc[df["avg_delay"].idxmax()]
            st.markdown(
                f"""
            **🔴 Needs Attention:**
            - **{worst_station['station_name']}**
            - Avg Delay: {worst_station['avg_delay']:.1f} min
            - On Time: {worst_station['on_time_pct']:.1f}%
            """
            )

            busiest_station = df.loc[df["total_departures"].idxmax()]
            st.markdown(
                f"""
            **🚉 Busiest Station:**
            - **{busiest_station['station_name']}**
            - {busiest_station['total_departures']} departures
            - Avg Delay: {busiest_station['avg_delay']:.1f} min
            """
            )

    st.markdown("---")

    render_station_table(df)

    current_time = datetime.now()
    st.markdown(
        f"""
    <div style="text-align: center; color: {COLORS['muted']}; margin-top: 2rem; padding: 1rem;">
        <small>
            📱 Last updated: {current_time.strftime('%Y-%m-%d %H:%M:%S')} |
            🔄 {'Auto-refresh enabled' if auto_refresh else 'Auto-refresh disabled'}
        </small>
    </div>
    """,
        unsafe_allow_html=True,
    )

    if auto_refresh:
        import time

        time.sleep(30)
        st.rerun()


if __name__ == "__main__":
    main()
