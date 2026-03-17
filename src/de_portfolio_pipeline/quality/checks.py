"""Very small data quality checks to expand later."""

from __future__ import annotations

import pandas as pd


def assert_not_empty(frame: pd.DataFrame) -> None:
    """Ensure a transformed dataset contains rows."""
    if frame.empty:
        raise ValueError("DataFrame is empty.")


def assert_required_columns(frame: pd.DataFrame, required_columns: list[str]) -> None:
    """Ensure required columns exist."""
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
