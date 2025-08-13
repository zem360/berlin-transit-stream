import json
import time
from datetime import datetime
from typing import Dict, List

import redis  # type: ignore[import]
import streamlit as st

from berlin_departure_board.config import settings

BERLIN_COLORS = {
    "primary": "#FFD700",
    "secondary": "#000000",
    "background": "#F5F5F5",
    "red": "#DC143C",
    "white": "#FFFFFF",
    "dark_gray": "#2F2F2F",
    "light_yellow": "#FFF8DC",
}

BERLIN_STATIONS = {
    "900100003": "🚉 S+U Alexanderplatz",
    "900100001": "🚉 S+U Friedrichstraße",
    "900003201": "🚉 S+U Berlin Hauptbahnhof",
    "900120003": "🚉 S Ostkreuz",
    "900023201": "🚉 S+U Zoologischer Garten",
    "900017101": "🚉 S+U Potsdamer Platz",
    "900024101": "🚉 S+U Warschauer Straße",
    "900013102": "🚉 S Hackescher Markt",
    "900110001": "🚉 S+U Gesundbrunnen",
    "900230999": "🚉 S+U Schönhauser Allee",
}


TRANSPORT_MODES = {
    "suburban": {"icon": "🚋", "name": "S-Bahn", "color": "#FFD700"},
    "subway": {"icon": "🚇", "name": "U-Bahn", "color": "#0066CC"},
    "bus": {"icon": "🚌", "name": "Bus", "color": "#8B4513"},
    "tram": {"icon": "🚊", "name": "Tram", "color": "#FF6B35"},
    "regional": {"icon": "🚆", "name": "Regional", "color": "#800080"},
    "express": {"icon": "🚄", "name": "Express", "color": "#FF0000"},
}


def init_streamlit_config():
    st.set_page_config(
        page_title="🐻 Berlin Departure Board",
        page_icon="🚋",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        f"""
    <style>
    .main {{
        background-color: {BERLIN_COLORS['background']};
    }}

    .stApp > header {{
        background-color: transparent;
    }}

    .block-container {{
        padding-top: 2rem;
        padding-bottom: 2rem;
    }}

    /* Berlin coat of arms inspired header */
    .berlin-header {{
        background: linear-gradient(90deg, {BERLIN_COLORS['primary']} 0%, {BERLIN_COLORS['light_yellow']} 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        border: 3px solid {BERLIN_COLORS['secondary']};
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        text-align: center;
    }}

    .berlin-title {{
        color: {BERLIN_COLORS['secondary']};
        font-size: 2.5rem;
        font-weight: bold;
        margin: 0;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }}

    .berlin-subtitle {{
        color: {BERLIN_COLORS['dark_gray']};
        font-size: 1.2rem;
        margin: 0.5rem 0 0 0;
    }}

    /* Sidebar styling */
    .css-1d391kg {{
        background-color: {BERLIN_COLORS['secondary']};
    }}

    .css-1d391kg .css-1v3fvcr {{
        color: {BERLIN_COLORS['primary']};
    }}

    /* Departure table styling */
    .departure-card {{
        background: {BERLIN_COLORS['white']};
        border-left: 5px solid {BERLIN_COLORS['primary']};
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }}

    .delay-warning {{
        color: {BERLIN_COLORS['red']};
        font-weight: bold;
    }}

    .on-time {{
        color: #008000;
        font-weight: bold;
    }}

    /* Station selector */
    .stSelectbox > div > div {{
        background-color: {BERLIN_COLORS['primary']};
        border: 2px solid {BERLIN_COLORS['secondary']};
    }}

    /* Metrics styling */
    .metric-container {{
        background: {BERLIN_COLORS['white']};
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid {BERLIN_COLORS['primary']};
        text-align: center;
        margin: 0.5rem 0;
    }}

    /* Auto-refresh indicator */
    .refresh-indicator {{
        position: fixed;
        top: 10px;
        right: 10px;
        background: {BERLIN_COLORS['primary']};
        color: {BERLIN_COLORS['secondary']};
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: bold;
        z-index: 999;
    }}
    </style>
    """,
        unsafe_allow_html=True,
    )


@st.cache_resource
def init_redis_connection():
    """Initialize Redis connection with caching"""
    try:
        redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        redis_client.ping()
        return redis_client
    except Exception as e:
        st.error(f"❌ Could not connect to Redis: {e}")
        return None


def get_station_departures(redis_client: redis.Redis, station_id: str) -> List[Dict]:
    """Fetch departures for a specific station from Redis"""
    try:
        key = f"{settings.REDIS_DEPARTURES_KEY_PREFIX}station:{station_id}"

        raw_departures = redis_client.zrange(key, -20, -1)

        if not raw_departures:
            return []

        departures = []
        current_time = datetime.now()

        for dep_json in raw_departures:
            try:
                departure = json.loads(dep_json)

                planned_dt = datetime.fromisoformat(
                    departure["planned_departure"].replace("Z", "+00:00")
                ).replace(tzinfo=None)

                if planned_dt > current_time:
                    departure["planned_departure_dt"] = planned_dt
                    departures.append(departure)

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                continue

        departures.sort(key=lambda x: x["planned_departure_dt"])
        return departures[:15]

    except Exception as e:
        st.error(f"Error fetching departures: {e}")
        return []


def format_departure_time(departure_dt: datetime) -> tuple[str, str]:
    now = datetime.now()
    diff = departure_dt - now
    actual_time = departure_dt.strftime("%H:%M")

    if diff.total_seconds() < 60:
        return "🟢 **Now**", actual_time
    elif diff.total_seconds() < 300:
        mins = int(diff.total_seconds() / 60)
        return f"🟡 **{mins} min**", actual_time
    else:
        return f"⏰ **{actual_time}**", actual_time


def get_delay_info(departure: Dict) -> tuple[str, str]:
    delay_minutes = departure.get("delay_minutes", 0)

    if delay_minutes == 0:
        return "✅ On time", "on-time"
    elif delay_minutes < 3:
        return f"🟡 +{delay_minutes:.0f} min", "delay-warning"
    else:
        return f"🔴 +{delay_minutes:.0f} min", "delay-warning"


def render_departure_table(departures: List[Dict], station_name: str):
    if not departures:
        st.warning(f"📭 No upcoming departures found for {station_name}")
        st.info(
            "💡 Try selecting a different station or check if the data pipeline is running."
        )
        return

    st.subheader(f"🚋 Next Departures from {station_name}")

    for i, dep in enumerate(departures):
        transport_info = TRANSPORT_MODES.get(
            dep["transport_mode"], {"icon": "🚌", "name": "Unknown", "color": "#666666"}
        )

        departure_time, actual_time = format_departure_time(dep["planned_departure_dt"])
        delay_text, delay_class = get_delay_info(dep)

        col1, col2, col3, col4 = st.columns([1, 3, 2, 2])

        with col1:
            st.markdown(
                f"""
            <div style='text-align: center; font-size: 2rem;'>
                {transport_info['icon']}
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col2:
            st.markdown(
                f"""
            <div class='departure-card'>
                <strong style='color: {transport_info['color']}; font-size: 1.2rem;'>
                    {dep['line_name']}
                </strong><br>
                <span style='color: #666; font-size: 0.9rem;'>
                    🎯 {dep['direction']}
                </span>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col3:
            st.markdown(
                f"""
            <div class='departure-card'>
                {departure_time}<br>
                <span style='font-size: 0.9rem; color: #333; font-weight: bold;'>
                    🕐 {actual_time}
                </span><br>
                <span style='font-size: 0.8rem; color: #666;'>
                    Platform {dep.get('platform', 'TBA')}
                </span>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col4:
            st.markdown(
                f"""
            <div class='departure-card'>
                <span class='{delay_class}'>{delay_text}</span><br>
                <span style='font-size: 0.8rem; color: #666;'>
                    {transport_info['name']}
                </span>
            </div>
            """,
                unsafe_allow_html=True,
            )

        if i < len(departures) - 1:
            st.markdown("---")


def render_station_metrics(departures: List[Dict]):
    """Render station metrics"""
    if not departures:
        return

    col1, col2, col3, col4 = st.columns(4)

    mode_counts: Dict[str, int] = {}
    total_delays = 0
    delayed_count = 0

    for dep in departures:
        mode = dep.get("transport_mode", "unknown")
        mode_counts[mode] = mode_counts.get(mode, 0) + 1

        delay = dep.get("delay_minutes", 0)
        if delay > 0:
            delayed_count += 1
            total_delays += delay

    with col1:
        st.markdown(
            f"""
        <div class='metric-container'>
            <h3 style='color: {BERLIN_COLORS["primary"]}; margin: 0;'>{len(departures)}</h3>
            <p style='margin: 0; color: {BERLIN_COLORS["dark_gray"]};'>Next Departures</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        on_time_pct = (
            ((len(departures) - delayed_count) / len(departures) * 100)
            if departures
            else 0
        )
        st.markdown(
            f"""
        <div class='metric-container'>
            <h3 style='color: {"#008000" if on_time_pct > 80 else BERLIN_COLORS["red"]}; margin: 0;'>{on_time_pct:.0f}%</h3>
            <p style='margin: 0; color: {BERLIN_COLORS["dark_gray"]};'>On Time</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col3:
        avg_delay = (total_delays / delayed_count) if delayed_count > 0 else 0
        st.markdown(
            f"""
        <div class='metric-container'>
            <h3 style='color: {BERLIN_COLORS["red"] if avg_delay > 5 else BERLIN_COLORS["primary"]}; margin: 0;'>{avg_delay:.1f}</h3>
            <p style='margin: 0; color: {BERLIN_COLORS["dark_gray"]};'>Avg Delay (min)</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col4:
        most_common_mode = (
            max(mode_counts.items(), key=lambda x: x[1])[0] if mode_counts else "none"
        )
        mode_info = TRANSPORT_MODES.get(
            most_common_mode, {"icon": "🚌", "name": "Mixed"}
        )
        st.markdown(
            f"""
        <div class='metric-container'>
            <h3 style='color: {BERLIN_COLORS["primary"]}; margin: 0;'>{mode_info["icon"]}</h3>
            <p style='margin: 0; color: {BERLIN_COLORS["dark_gray"]};'>{mode_info["name"]}</p>
        </div>
        """,
            unsafe_allow_html=True,
        )


def main():
    init_streamlit_config()

    current_time = datetime.now()
    current_time_str = current_time.strftime("%H:%M:%S")
    current_date_str = current_time.strftime("%A, %B %d, %Y")

    st.markdown(
        f"""
    <div class='berlin-header'>
        <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;'>
            <div style='flex: 1;'></div>
            <div style='flex: 2; text-align: center;'>
                <h1 class='berlin-title'>🐻 Berlin Departure Board</h1>
            </div>
            <div style='flex: 1; text-align: right;'>
                <div style='background: {BERLIN_COLORS["secondary"]}; color: {BERLIN_COLORS["primary"]};
                           padding: 0.5rem 1rem; border-radius: 8px; font-weight: bold;'>
                    <div style='font-size: 1.5rem;'>🕐 {current_time_str}</div>
                    <div style='font-size: 0.8rem; opacity: 0.9;'>{current_date_str}</div>
                </div>
            </div>
        </div>
        <p class='berlin-subtitle'>Live departures from Berlin's public transport network</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
    <div class='refresh-indicator'>
        🔄 Auto-refresh: 5s
    </div>
    """,
        unsafe_allow_html=True,
    )

    redis_client = init_redis_connection()
    if not redis_client:
        st.stop()

    with st.sidebar:
        st.markdown(
            f"""
        <div style='background: {BERLIN_COLORS["primary"]}; padding: 1rem; border-radius: 10px; margin-bottom: 1rem;'>
            <h2 style='color: {BERLIN_COLORS["secondary"]}; text-align: center; margin: 0;'>
                🚉 Station Selector
            </h2>
        </div>
        """,
            unsafe_allow_html=True,
        )

        available_stations = {}
        for station_id, station_name in BERLIN_STATIONS.items():
            key = f"{settings.REDIS_DEPARTURES_KEY_PREFIX}station:{station_id}"
            if redis_client.exists(key):
                available_stations[station_id] = station_name

        if not available_stations:
            st.error("❌ No stations with data available")
            st.info("💡 Make sure the data pipeline is running")
            st.stop()

        selected_station = st.selectbox(
            "Choose a station:",
            options=list(available_stations.keys()),
            format_func=lambda x: available_stations[x],
            index=0,
        )

        if not selected_station:
            st.error("❌ No station selected")
            st.stop()

        st.markdown("---")
        st.markdown("### 📊 Station Info")
        station_key = (
            f"{settings.REDIS_DEPARTURES_KEY_PREFIX}station:{selected_station}"
        )
        departure_count = redis_client.zcard(station_key)
        last_update = redis_client.ttl(station_key)

        st.metric("Stored Departures", departure_count)
        if last_update > 0:
            st.metric("Data TTL", f"{last_update//60}m {last_update%60}s")

        st.markdown("---")
        st.markdown("### 🎯 Legend")
        st.markdown("🟢 **Now** - Departing")
        st.markdown("🟡 **X min** - Soon")
        st.markdown("⏰ **HH:MM** - Scheduled")
        st.markdown("🕐 **Time** - Exact departure")
        st.markdown("✅ On time")
        st.markdown("🟡 Small delay (<3 min)")
        st.markdown("🔴 Significant delay")

    station_name = available_stations[selected_station]

    placeholder = st.empty()

    with placeholder.container():
        departures = get_station_departures(redis_client, selected_station)

        render_station_metrics(departures)

        st.markdown("---")

        render_departure_table(departures, station_name)

        st.markdown(
            f"""
        <div style='text-align: center; color: {BERLIN_COLORS["dark_gray"]}; margin-top: 2rem;'>
            <small>📱 Last updated: {datetime.now().strftime("%H:%M:%S")} |
            🔄 Next refresh in 30 seconds</small>
        </div>
        """,
            unsafe_allow_html=True,
        )

    time.sleep(5)
    st.rerun()


if __name__ == "__main__":
    main()
