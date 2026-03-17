"""Simple database inspection helper for DuckDB."""

from __future__ import annotations

import argparse
import re

import duckdb
import pandas as pd

from de_portfolio_pipeline.config import DB_PATH


VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def format_frame(frame: pd.DataFrame) -> str:
    """Return a console-friendly representation of a DataFrame."""
    if frame.empty:
        return "(no rows)"
    return frame.to_string(index=False)


def validate_identifier(identifier: str) -> str:
    """Allow only simple SQL identifiers for table inspection."""
    if not VALID_IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"Invalid table name: {identifier}")
    return identifier


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(description="Inspect the local DuckDB portfolio database.")
    parser.add_argument(
        "--table",
        help="Show the schema and a preview for one table.",
    )
    parser.add_argument(
        "--query",
        help="Run a custom SQL query and print the result.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of preview rows to show for table inspection.",
    )
    return parser


def main() -> None:
    """Run the inspection CLI."""
    parser = build_parser()
    args = parser.parse_args()

    with duckdb.connect(str(DB_PATH), read_only=True) as connection:
        if args.query:
            result = connection.execute(args.query).fetchdf()
            print(format_frame(result))
            return

        if args.table:
            table_name = validate_identifier(args.table)
            schema = connection.execute(f"describe {table_name}").fetchdf()
            preview = connection.execute(f"select * from {table_name} limit {args.limit}").fetchdf()
            print(f"Table: {table_name}\n")
            print("Schema:")
            print(format_frame(schema))
            print("\nPreview:")
            print(format_frame(preview))
            return

        tables = connection.execute("show tables").fetchdf()
        print("Tables:")
        print(format_frame(tables))


if __name__ == "__main__":
    main()
