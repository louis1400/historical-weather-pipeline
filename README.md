# Historical Weather Data Pipeline

This repository is a small, honest data engineering portfolio project.

The goal is not to look "enterprise" for the sake of it. The goal is to show that I can take real external data, ingest it, model it, store it, inspect it, and present it clearly.

## Vision

This project is meant to communicate:

- I can work with real API data instead of only static CSV files
- I understand the basic data engineering flow from ingestion to analytics output
- I can keep a project readable, reproducible, and explainable
- I can use tools like Codex and Git productively while still understanding the result

Just as importantly, this project is intentionally **not** trying to be:

- a forecasting system
- a machine learning project
- a cloud-heavy production platform
- an overbuilt student project with features I cannot explain confidently

It is a compact historical weather pipeline built to make a credible first impression for entry-level data roles.

## What The Project Does

The current version:

- pulls recent **historical** hourly weather data from the Open-Meteo archive API
- tracks Amsterdam, Berlin, and Paris
- stores raw JSON extracts in `data/raw/`
- transforms hourly weather data into a DuckDB bronze table
- creates a daily summary gold table in SQL
- exposes a Streamlit dashboard for comparison across cities
- includes a small CLI for inspecting the DuckDB database directly

The pipeline only keeps data up to the latest completed local hour, so the dashboard stays factual rather than implying future or forecasted observations.

## Why This Is A Good Portfolio Project

This project demonstrates several practical skills that are relevant to junior data engineering work:

- Python API ingestion
- lightweight pipeline design
- tabular transformation with pandas
- SQL-based aggregation in DuckDB
- data modeling with bronze and gold layers
- local analytics tooling and database inspection
- dashboarding with Streamlit and Plotly
- Git and GitHub workflow for versioned project development

## Architecture

```text
Open-Meteo Archive API
    |
    v
data/raw/ JSON extracts
    |
    v
pandas transformation logic
    |
    v
DuckDB: weather_hourly_bronze
    |
    v
SQL model: weather_daily_gold
    |
    +--> Streamlit dashboard
    |
    +--> inspect_db CLI
```

## Questions The Project Can Answer

- Which city was warmest in the latest recorded hour?
- Which city was most humid in the latest recorded hour?
- How do hourly temperature and humidity trends compare across cities?
- What were the daily average temperature and humidity values over the historical window?
- How many hourly records were collected per city and time range?

## Resume-Friendly Description

You can adapt this for your CV later:

> Built a historical weather data pipeline in Python and DuckDB that ingested public API data for multiple cities, transformed it into analytics-ready tables, and surfaced comparative metrics in an interactive dashboard.

## Tech Stack

- Python
- DuckDB
- pandas
- SQL
- Streamlit
- Plotly
- pytest

Optional future extensions:

- Windows Task Scheduler or Prefect for scheduled runs
- dbt-duckdb for stronger SQL modeling structure
- Docker for easier environment setup
- GitHub Actions for automated checks

## Quickstart

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -e .[dev]
py -m de_portfolio_pipeline.utils.run_pipeline
.\start_dashboard.ps1
```

What this currently does:

- ingests the last 7 days of historical hourly weather data
- refreshes the DuckDB database locally
- lets the dashboard compare recent hourly trends across the selected cities

Dashboard refresh behavior:

- the dashboard can auto-check for fresh historical data every hour while the browser tab is open
- there is also a manual `Refresh historical data now` action in the sidebar
- if I want true background refresh even when the dashboard is closed, the next step would be a scheduled task

## Inspecting The Database

Do not open `portfolio.duckdb` in the browser. It is a database file, not a webpage.

List available tables:

```powershell
py -m de_portfolio_pipeline.utils.inspect_db
```

Inspect one table:

```powershell
py -m de_portfolio_pipeline.utils.inspect_db --table weather_hourly_bronze
```

Run a custom SQL query:

```powershell
py -m de_portfolio_pipeline.utils.inspect_db --query "select city, min(timestamp) as start_ts, max(timestamp) as end_ts, count(*) as row_count from weather_hourly_bronze group by city order by city"
```

## Repo Layout

```text
src/de_portfolio_pipeline/
  pipelines/      <- API ingestion and transformation logic
  models/         <- DuckDB loading helpers
  quality/        <- simple data checks
  utils/          <- pipeline and database inspection entry points
dashboard/        <- Streamlit app
sql/              <- SQL models for analytics tables
tests/            <- lightweight automated checks
data/             <- local raw and derived data folders
```

## Current Scope

The current project is intentionally modest in scope:

- one external data source
- three cities
- one primary hourly fact table
- one daily summary table
- one dashboard
- one local database inspection helper

That is enough to be credible, understandable, and discussable in an interview.

## How To Talk About Codex

The story is not "AI built my project for me."

The better story is:

- I used Codex to accelerate scaffolding and iteration
- I reviewed the code and refined the product decisions myself
- I used AI as part of a normal engineering workflow alongside Git, testing, and debugging

That is a realistic and defensible story for a first professional project.

## Likely Next Improvements

If I continue this project, the most natural next steps are:

- add another factual data source such as air quality
- improve data quality checks and logging
- schedule background refreshes outside the dashboard session
- add a small change log or project notes section to document iterations
