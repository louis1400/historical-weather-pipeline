"""Project configuration constants."""

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
DB_PATH = ROOT_DIR / "portfolio.duckdb"
SQL_DIR = ROOT_DIR / "sql"
PIPELINE_TIMEZONE = "Europe/Amsterdam"
HISTORY_WINDOW_DAYS = 7
INCLUDE_TODAY_IN_HISTORY = True

CITIES = [
    {"city": "Amsterdam", "latitude": 52.3676, "longitude": 4.9041},
    {"city": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Berlin", "latitude": 52.52, "longitude": 13.405},
]
