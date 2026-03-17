"""Starter transformation utilities."""

from __future__ import annotations

import pandas as pd


def hourly_weather_to_frame(payload: dict, city: str) -> pd.DataFrame:
    """Flatten one Open-Meteo payload into a tabular structure."""
    hourly = payload.get("hourly", {})
    frame = pd.DataFrame(
        {
            "timestamp": hourly.get("time", []),
            "temperature_2m": hourly.get("temperature_2m", []),
            "relative_humidity_2m": hourly.get("relative_humidity_2m", []),
        }
    )
    frame["city"] = city
    frame["latitude"] = payload.get("latitude")
    frame["longitude"] = payload.get("longitude")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    return frame
