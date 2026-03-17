"""Starter ingestion logic for public API data."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from de_portfolio_pipeline.config import (
    HISTORY_WINDOW_DAYS,
    INCLUDE_TODAY_IN_HISTORY,
    PIPELINE_TIMEZONE,
    RAW_DIR,
)


OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def get_recent_history_window(
    today: date | None = None,
    history_window_days: int = HISTORY_WINDOW_DAYS,
    include_today: bool = INCLUDE_TODAY_IN_HISTORY,
) -> tuple[date, date]:
    """Return a recent historical date window, optionally including today."""
    if today is None:
        today = datetime.now(ZoneInfo(PIPELINE_TIMEZONE)).date()

    end_date = today if include_today else today - timedelta(days=1)
    start_date = end_date - timedelta(days=history_window_days - 1)
    return start_date, end_date


def get_latest_completed_local_hour(
    timezone_name: str,
    now: datetime | None = None,
) -> datetime:
    """Return the latest completed hour in a local timezone."""
    if now is None:
        now = datetime.now(ZoneInfo(timezone_name))

    return now.replace(minute=0, second=0, microsecond=0, tzinfo=None)


def build_historical_weather_params(
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
) -> dict[str, str | float]:
    """Return a small, interview-friendly historical API request."""
    return {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,relative_humidity_2m",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "timezone": "auto",
    }


def fetch_historical_weather_data(
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
) -> dict:
    """Fetch historical weather data from the Open-Meteo archive API."""
    response = requests.get(
        OPEN_METEO_ARCHIVE_URL,
        params=build_historical_weather_params(latitude, longitude, start_date, end_date),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def save_raw_payload(
    payload: dict,
    city: str,
    destination_dir: Path = RAW_DIR,
    dataset_label: str = "historical",
) -> Path:
    """Persist the raw API response with a timestamped filename."""
    destination_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    safe_city = city.lower().replace(" ", "_")
    output_path = destination_dir / f"weather_{dataset_label}_{safe_city}_{timestamp}.json"
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path
