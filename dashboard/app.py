"""Polished Streamlit dashboard for the historical weather pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import subprocess
import sys

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "portfolio.duckdb"
AUTO_REFRESH_INTERVAL = "1h"
DATA_STALE_AFTER = timedelta(hours=1)
CITY_COLORS = {
    "Amsterdam": "#0f766e",
    "Berlin": "#ea580c",
    "Paris": "#2563eb",
}
WINDOW_OPTIONS = {
    "Last 24 hours": timedelta(hours=24),
    "Last 72 hours": timedelta(hours=72),
    "Last 7 days": timedelta(days=7),
}


def load_weather_data(query: str) -> pd.DataFrame:
    """Load dashboard data if the database exists."""
    if not DB_PATH.exists():
        return pd.DataFrame()

    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as connection:
            return connection.execute(query).fetchdf()
    except duckdb.Error:
        return pd.DataFrame()


def inject_styles() -> None:
    """Apply lightweight styling to improve visual polish."""
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top right, rgba(186, 230, 253, 0.55), transparent 24%),
                linear-gradient(180deg, #f3f7fb 0%, #ffffff 22%);
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #f7fafc 0%, #eef4fb 100%);
            border-right: 1px solid #dbe4ef;
        }

        [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #dbe4ef;
            border-radius: 18px;
            padding: 14px 18px;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
        }

        div[data-testid="stMetricLabel"] {
            color: #5b6578;
            font-weight: 600;
        }

        div[data-testid="stMetricValue"] {
            color: #13233a;
        }

        .top-panel {
            background: #ffffff;
            border: 1px solid #dbe4ef;
            border-radius: 22px;
            padding: 24px 26px 20px 26px;
            box-shadow: 0 14px 32px rgba(15, 23, 42, 0.05);
            margin-bottom: 1.1rem;
        }

        .top-kicker {
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 0.76rem;
            font-weight: 700;
            color: #2563eb;
            margin-bottom: 0.55rem;
        }

        .top-title {
            font-size: 2.1rem;
            line-height: 1.08;
            font-weight: 800;
            color: #13233a;
            margin: 0 0 0.45rem 0;
        }

        .top-copy {
            font-size: 1rem;
            line-height: 1.6;
            color: #526176;
            max-width: 60rem;
            margin: 0 0 0.9rem 0;
        }

        .pill-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
        }

        .pill {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            background: #eef5fb;
            border: 1px solid #d7e5f2;
            border-radius: 999px;
            color: #17314f;
            font-size: 0.86rem;
            font-weight: 600;
            padding: 0.35rem 0.78rem;
        }

        .section-note {
            color: #5b6578;
            font-size: 0.94rem;
            margin-top: -0.1rem;
            margin-bottom: 0.8rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_day(value: pd.Timestamp) -> str:
    """Format dates for display."""
    return value.strftime("%d %b %Y")


def format_timestamp(value: datetime | None) -> str:
    """Format a timestamp for display."""
    if value is None:
        return "Not available yet"
    return value.strftime("%d %b %Y %H:%M")


def get_database_updated_at() -> datetime | None:
    """Return the filesystem timestamp for the database file."""
    if not DB_PATH.exists():
        return None
    return datetime.fromtimestamp(DB_PATH.stat().st_mtime)


def run_pipeline_refresh() -> str:
    """Run the historical ingestion pipeline and return its console output."""
    result = subprocess.run(
        [sys.executable, "-m", "de_portfolio_pipeline.utils.run_pipeline"],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip() or "Pipeline refresh completed."


def refresh_data_if_needed(force: bool = False) -> tuple[bool, str]:
    """Refresh the database when forced or when the file is stale."""
    updated_at = get_database_updated_at()
    is_stale = updated_at is None or (datetime.now() - updated_at) >= DATA_STALE_AFTER

    if not force and not is_stale:
        return False, "Historical data is already fresh."

    return True, run_pipeline_refresh()


def build_latest_snapshot(detail_frame: pd.DataFrame) -> tuple[pd.Timestamp, pd.DataFrame]:
    """Return the latest hourly observation for each visible city."""
    latest_timestamp = detail_frame["timestamp"].max()
    latest_frame = detail_frame.loc[
        detail_frame["timestamp"].eq(latest_timestamp),
        ["city", "temperature_2m", "relative_humidity_2m"],
    ].copy()
    return latest_timestamp, latest_frame


def build_headline_metrics(detail_frame: pd.DataFrame) -> dict[str, str]:
    """Create concise, user-facing overview metrics."""
    latest_timestamp, latest_frame = build_latest_snapshot(detail_frame)
    warmest_row = latest_frame.loc[latest_frame["temperature_2m"].idxmax()]
    most_humid_row = latest_frame.loc[latest_frame["relative_humidity_2m"].idxmax()]

    return {
        "latest_recorded_hour": latest_timestamp.strftime("%d %b %Y %H:%M"),
        "warmest_latest": f"{warmest_row['city']} ({warmest_row['temperature_2m']:.1f} C)",
        "most_humid_latest": f"{most_humid_row['city']} ({most_humid_row['relative_humidity_2m']:.0f}%)",
        "average_temperature": f"{detail_frame['temperature_2m'].mean():.1f} C",
    }


def build_comparison_table(detail_frame: pd.DataFrame) -> tuple[str, pd.DataFrame]:
    """Return one concise comparison row per visible city."""
    latest_timestamp, latest_frame = build_latest_snapshot(detail_frame)
    aggregates = (
        detail_frame.groupby("city", as_index=False)
        .agg(
            avg_temperature=("temperature_2m", "mean"),
            avg_humidity=("relative_humidity_2m", "mean"),
        )
    )

    comparison = (
        latest_frame.merge(aggregates, on="city", how="left")
        .rename(
            columns={
                "city": "City",
                "temperature_2m": "Latest temp (C)",
                "relative_humidity_2m": "Latest humidity (%)",
                "avg_temperature": "Average temp (C)",
                "avg_humidity": "Average humidity (%)",
            }
        )
        .sort_values("Latest temp (C)", ascending=False)
    )

    return latest_timestamp.strftime("%d %b %Y %H:%M"), comparison


def build_line_chart(frame: pd.DataFrame, value_column: str, title: str, y_axis_title: str) -> go.Figure:
    """Create a polished Plotly line chart with legend toggling."""
    figure = go.Figure()

    for city in sorted(frame["city"].unique()):
        city_frame = frame[frame["city"] == city]
        figure.add_trace(
            go.Scatter(
                x=city_frame["timestamp"],
                y=city_frame[value_column],
                mode="lines",
                name=city,
                line={"width": 3, "color": CITY_COLORS.get(city, "#475569")},
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>"
                    "%{x|%d %b %Y %H:%M}<br>"
                    f"{y_axis_title}: "
                    "%{y:.1f}<extra></extra>"
                ),
            )
        )

    figure.update_layout(
        title={"text": title, "x": 0.02, "xanchor": "left"},
        height=380,
        margin={"l": 18, "r": 18, "t": 58, "b": 18},
        paper_bgcolor="white",
        plot_bgcolor="white",
        hovermode="x unified",
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0,
            "title": {"text": ""},
            "itemclick": "toggle",
            "itemdoubleclick": "toggleothers",
        },
        xaxis={
            "title": "",
            "showgrid": False,
            "zeroline": False,
        },
        yaxis={
            "title": y_axis_title,
            "gridcolor": "#e2e8f0",
            "zeroline": False,
        },
    )
    return figure


st.set_page_config(page_title="Historical Weather Dashboard", layout="wide")
inject_styles()

if "refresh_notice" not in st.session_state:
    st.session_state["refresh_notice"] = None
if "refresh_error" not in st.session_state:
    st.session_state["refresh_error"] = None

with st.sidebar:
    st.header("Controls")
    st.caption("Use the controls below to shape what appears in the dashboard.")

    raw_frame = load_weather_data(
        """
        select *
        from weather_hourly_bronze
        order by timestamp, city
        """
    )

    city_options = sorted(raw_frame["city"].unique()) if not raw_frame.empty else []
    selected_cities = st.multiselect(
        "Cities in dashboard",
        city_options,
        default=city_options,
        help="This controls which cities appear in the table, metrics, and charts.",
    )
    selected_window_label = st.radio(
        "Visible time window",
        list(WINDOW_OPTIONS.keys()),
        index=1,
        help="A shorter window makes the charts easier to compare.",
    )
    auto_refresh_enabled = st.toggle(
        "Auto-check for fresh data every hour",
        value=True,
        help="This works while the dashboard tab is open. It refreshes the historical dataset if it becomes stale.",
    )
    refresh_clicked = st.button("Refresh historical data now", type="primary", use_container_width=True)
    st.caption(
        "Hourly checks are useful for keeping the dashboard fresh, but historical data sources may only publish newer recorded hours periodically."
    )


@st.fragment(run_every=AUTO_REFRESH_INTERVAL if auto_refresh_enabled else None)
def auto_refresh_fragment() -> None:
    """Refresh the historical dataset while the session is active."""
    try:
        refreshed, message = refresh_data_if_needed(force=False)
    except subprocess.CalledProcessError as error:
        st.session_state["refresh_error"] = error.stderr.strip() or str(error)
        return

    if refreshed:
        st.session_state["refresh_notice"] = message
        st.session_state["refresh_error"] = None
        st.rerun()


if refresh_clicked:
    with st.spinner("Refreshing historical data..."):
        try:
            _, message = refresh_data_if_needed(force=True)
            st.session_state["refresh_notice"] = message
            st.session_state["refresh_error"] = None
        except subprocess.CalledProcessError as error:
            st.session_state["refresh_error"] = error.stderr.strip() or str(error)
    st.rerun()


auto_refresh_fragment()

if st.session_state["refresh_notice"]:
    st.toast(st.session_state["refresh_notice"])
    st.session_state["refresh_notice"] = None

if st.session_state["refresh_error"]:
    st.error(st.session_state["refresh_error"])
    st.session_state["refresh_error"] = None

detail_frame = raw_frame.copy()

if detail_frame.empty:
    st.info("No data available yet. Run the pipeline first.")
else:
    detail_frame["timestamp"] = pd.to_datetime(detail_frame["timestamp"])

    if not selected_cities:
        st.warning("Select at least one city to display the dashboard.")
        st.stop()

    filtered_detail = detail_frame[detail_frame["city"].isin(selected_cities)].copy()
    latest_available_timestamp = filtered_detail["timestamp"].max()
    window_delta = WINDOW_OPTIONS[selected_window_label]
    window_start = latest_available_timestamp - window_delta + timedelta(hours=1)
    visible_detail = filtered_detail[filtered_detail["timestamp"] >= window_start].copy()

    if visible_detail.empty:
        st.warning("No data is available for the current filter selection.")
        st.stop()

    metrics = build_headline_metrics(visible_detail)
    snapshot_timestamp, comparison_table = build_comparison_table(visible_detail)
    database_updated_at = get_database_updated_at()

    st.markdown(
        f"""
        <div class="top-panel">
        <div class="top-kicker">Data Engineering Portfolio Demo</div>
        <div class="top-title">Historical Weather Dashboard</div>
        <p class="top-copy">
            Compare recent recorded weather observations across Amsterdam, Berlin, and Paris.
            This project is intentionally small and factual: it demonstrates ingestion, storage, transformation,
            inspection, and presentation with real historical data, without pretending to be a forecasting or
            production-scale platform.
        </p>
            <div class="pill-row">
                <div class="pill">{len(selected_cities)} cities selected</div>
                <div class="pill">{selected_window_label}</div>
                <div class="pill">Last pipeline refresh: {format_timestamp(database_updated_at)}</div>
                <div class="pill">Auto-check hourly: {"On" if auto_refresh_enabled else "Off"}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    metric_columns = st.columns(4)
    metric_columns[0].metric("Latest recorded hour", metrics["latest_recorded_hour"])
    metric_columns[1].metric("Warmest latest hour", metrics["warmest_latest"])
    metric_columns[2].metric("Most humid latest hour", metrics["most_humid_latest"])
    metric_columns[3].metric("Average temp in view", metrics["average_temperature"])

    st.subheader("At-a-glance city comparison")
    st.markdown(
        f"""
        <div class="section-note">
            Latest values come from the most recent recorded hour at {snapshot_timestamp}.
            Average values summarize the currently visible time window.
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.dataframe(
        comparison_table,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Latest temp (C)": st.column_config.NumberColumn(format="%.1f"),
            "Latest humidity (%)": st.column_config.NumberColumn(format="%.0f"),
            "Average temp (C)": st.column_config.NumberColumn(format="%.1f"),
            "Average humidity (%)": st.column_config.NumberColumn(format="%.0f"),
        },
    )

    st.subheader("Hourly trends")
    st.markdown(
        """
        <div class="section-note">
            Click city names in the legend to hide or restore lines. Double-click a legend item to isolate one city.
        </div>
        """,
        unsafe_allow_html=True,
    )
    chart_columns = st.columns(2)
    chart_columns[0].plotly_chart(
        build_line_chart(
            visible_detail,
            value_column="temperature_2m",
            title="Temperature comparison",
            y_axis_title="Temperature (C)",
        ),
        use_container_width=True,
    )
    chart_columns[1].plotly_chart(
        build_line_chart(
            visible_detail,
            value_column="relative_humidity_2m",
            title="Humidity comparison",
            y_axis_title="Relative humidity (%)",
        ),
        use_container_width=True,
    )
