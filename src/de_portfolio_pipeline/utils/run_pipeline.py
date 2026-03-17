"""Simple entry point for local development."""

from __future__ import annotations

import pandas as pd

from de_portfolio_pipeline.config import CITIES, DB_PATH, SQL_DIR
from de_portfolio_pipeline.models.warehouse import execute_sql_file, load_frame_to_duckdb
from de_portfolio_pipeline.pipelines.ingest import (
    fetch_historical_weather_data,
    get_latest_completed_local_hour,
    get_recent_history_window,
    save_raw_payload,
)
from de_portfolio_pipeline.pipelines.transform import hourly_weather_to_frame
from de_portfolio_pipeline.quality.checks import assert_not_empty, assert_required_columns


def main() -> None:
    """Run a small multi-city historical weather pipeline."""
    bronze_frames: list[pd.DataFrame] = []
    start_date, end_date = get_recent_history_window()

    for city_config in CITIES:
        payload = fetch_historical_weather_data(
            latitude=city_config["latitude"],
            longitude=city_config["longitude"],
            start_date=start_date,
            end_date=end_date,
        )
        save_raw_payload(payload, city=city_config["city"])
        frame = hourly_weather_to_frame(payload, city=city_config["city"])
        latest_completed_hour = get_latest_completed_local_hour(
            payload.get("timezone", "Europe/Amsterdam")
        )
        frame = frame[frame["timestamp"] <= latest_completed_hour].copy()
        bronze_frames.append(frame)

    combined_frame = pd.concat(bronze_frames, ignore_index=True)
    assert_not_empty(combined_frame)
    assert_required_columns(
        combined_frame,
        ["timestamp", "city", "temperature_2m", "relative_humidity_2m"],
    )
    load_frame_to_duckdb(combined_frame, table_name="weather_hourly_bronze", db_path=DB_PATH)
    execute_sql_file(
        sql_path=SQL_DIR / "gold_weather_summary.sql",
        db_path=DB_PATH,
        target_table="weather_daily_gold",
    )
    print(
        "Loaded "
        f"{len(combined_frame)} rows into {DB_PATH} "
        f"for the historical window {start_date.isoformat()} to {end_date.isoformat()}."
    )


if __name__ == "__main__":
    main()
