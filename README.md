# Data Engineering Portfolio Project

This repository is a practical first data engineering project you can use to show employers that you understand the core workflow:

- ingest data from external APIs
- store raw and cleaned data in layers
- transform data into analytics-ready tables
- validate data quality
- present business metrics in a small dashboard

## Project Idea

Build a **Historical Weather Analytics Pipeline** for a few European cities.

Why this is a strong first project:

- it uses real API data, which feels more professional than a toy CSV-only project
- it demonstrates common data engineering patterns without needing cloud infrastructure on day one
- it is realistic to finish as a student
- it gives you a clean story to tell in interviews

## What You Will Show

By completing this project, you will be able to talk about:

- API ingestion with Python
- layered data design: raw, bronze, silver, gold
- SQL transformations in DuckDB
- data quality checks
- reproducible project structure
- dashboarding and stakeholder-style metrics
- using Codex to accelerate development responsibly

## Suggested Stack

- Python
- DuckDB
- Pandas or Polars
- SQL
- Streamlit
- `pytest`

Optional later upgrades:

- Prefect for orchestration
- dbt-duckdb for modeling
- Docker
- GitHub Actions

## Architecture

```text
Open-Meteo APIs
    |
    v
data/raw/               <- unmodified API extracts
    |
    v
data/bronze/            <- standardized schema, basic typing
    |
    v
data/silver/            <- cleaned, deduplicated, enriched data
    |
    v
data/gold/              <- analytics tables for dashboard/reporting
    |
    v
dashboard/              <- Streamlit app with KPIs and charts
```

## Example Business Questions

- Which city had the highest average PM2.5 level this month?
- How often do poor air quality days coincide with high temperature days?
- Which cities show the largest week-over-week change in air quality?
- How many observations were missing or delayed by source?

## Resume-Friendly Project Description

You can adapt this for your CV later:

> Built an end-to-end data pipeline in Python and DuckDB that ingested weather and air quality data from public APIs, transformed it into analytics-ready tables, applied data quality checks, and surfaced KPIs in a dashboard.
> Built an end-to-end historical weather pipeline in Python and DuckDB that ingested public API data for multiple cities, transformed it into analytics-ready tables, and surfaced comparative metrics in an interactive dashboard.

## Recommended Build Order

1. Get one API ingestion script working.
2. Save the raw response locally.
3. Flatten the data into tabular form.
4. Load it into DuckDB.
5. Create one clean analytics table in SQL.
6. Add 2-3 data quality checks.
7. Build a simple Streamlit dashboard.
8. Add scheduling or orchestration if you still have time.

## How To Talk About Codex

The point is not "Codex wrote my code."
The point is:

- you used Codex to scaffold and speed up implementation
- you reviewed and understood the code
- you iterated on bugs, structure, and tests
- you used AI as an engineering tool, not a substitute for judgment

That is a very reasonable and modern story for a first job.

## Starter Repo Layout

```text
src/de_portfolio_pipeline/
  pipelines/
  models/
  quality/
  utils/
dashboard/
data/
  raw/
  bronze/
  silver/
  gold/
sql/
tests/
```

## Next Steps

- read `docs` in this README and the starter files
- implement the API ingestion in `src/de_portfolio_pipeline/pipelines/ingest.py`
- connect the transformation flow to DuckDB
- add one dashboard page

If you want, we can now build this project together step by step.

## Quickstart

After Python 3.11+ is installed:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -e .[dev]
py -m de_portfolio_pipeline.utils.run_pipeline
streamlit run dashboard/app.py
```

Or start the dashboard with the helper script:

```powershell
.\start_dashboard.ps1
```

What this currently does:

- pulls the last 7 completed days of historical weather data
- stores raw JSON extracts in `data/raw/`
- loads hourly records into DuckDB
- creates a daily summary table
- serves a dashboard for comparing Amsterdam, Berlin, and Paris

Dashboard refresh behavior:

- the dashboard can auto-check for fresh data every hour while the browser tab is open
- there is also a manual `Refresh historical data now` action in the sidebar
- if you want true background refresh even when the dashboard is closed, the next step would be a Windows Task Scheduler job

## Inspecting The Database

Do not open `portfolio.duckdb` in the browser. It is a database file, not a webpage.

Use the built-in inspection helper instead:

```powershell
py -m de_portfolio_pipeline.utils.inspect_db
```

That lists the available tables.

Inspect one table:

```powershell
py -m de_portfolio_pipeline.utils.inspect_db --table weather_hourly_bronze
```

Run your own SQL:

```powershell
py -m de_portfolio_pipeline.utils.inspect_db --query "select city, min(timestamp) as start_ts, max(timestamp) as end_ts, count(*) as row_count from weather_hourly_bronze group by city order by city"
```

## Current Scope

The starter version now includes:

- historical ingestion for Amsterdam, Paris, and Berlin
- raw JSON storage per city
- a bronze DuckDB table with hourly weather data
- a gold SQL summary table with daily averages per city
- a Streamlit dashboard with multi-city comparison
- a terminal helper for inspecting the DuckDB database

That is already enough for a strong first portfolio project once it runs cleanly and you can explain the flow.
