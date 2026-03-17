"""Helpers for loading data into DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def load_frame_to_duckdb(frame: pd.DataFrame, table_name: str, db_path: Path) -> None:
    """Create or replace a table from a pandas DataFrame."""
    with duckdb.connect(str(db_path)) as connection:
        connection.register("input_frame", frame)
        connection.execute(f"create or replace table {table_name} as select * from input_frame")


def execute_sql_file(sql_path: Path, db_path: Path, target_table: str) -> None:
    """Materialize a SQL query into a target table."""
    query = sql_path.read_text(encoding="utf-8")
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(f"create or replace table {target_table} as {query}")
