from de_portfolio_pipeline.pipelines.transform import hourly_weather_to_frame


def test_hourly_weather_to_frame_returns_expected_columns() -> None:
    payload = {
        "latitude": 52.0,
        "longitude": 4.0,
        "hourly": {
            "time": ["2026-03-17T00:00"],
            "temperature_2m": [7.2],
            "relative_humidity_2m": [81],
        },
    }

    frame = hourly_weather_to_frame(payload, city="Amsterdam")

    assert list(frame.columns) == [
        "timestamp",
        "temperature_2m",
        "relative_humidity_2m",
        "city",
        "latitude",
        "longitude",
    ]
    assert len(frame) == 1
    assert frame.loc[0, "city"] == "Amsterdam"
