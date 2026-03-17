from datetime import date
from datetime import datetime

from de_portfolio_pipeline.pipelines.ingest import (
    build_historical_weather_params,
    get_latest_completed_local_hour,
    get_recent_history_window,
)


def test_get_recent_history_window_ends_yesterday() -> None:
    start_date, end_date = get_recent_history_window(
        today=date(2026, 3, 17),
        history_window_days=7,
        include_today=False,
    )

    assert start_date == date(2026, 3, 10)
    assert end_date == date(2026, 3, 16)


def test_get_recent_history_window_can_include_today() -> None:
    start_date, end_date = get_recent_history_window(
        today=date(2026, 3, 17),
        history_window_days=7,
        include_today=True,
    )

    assert start_date == date(2026, 3, 11)
    assert end_date == date(2026, 3, 17)


def test_build_historical_weather_params_contains_date_window() -> None:
    params = build_historical_weather_params(
        latitude=52.3676,
        longitude=4.9041,
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 16),
    )

    assert params["start_date"] == "2026-03-10"
    assert params["end_date"] == "2026-03-16"
    assert params["timezone"] == "auto"


def test_get_latest_completed_local_hour_rounds_down_to_the_hour() -> None:
    latest_hour = get_latest_completed_local_hour(
        "Europe/Amsterdam",
        now=datetime(2026, 3, 17, 13, 53),
    )

    assert latest_hour == datetime(2026, 3, 17, 13, 0)
