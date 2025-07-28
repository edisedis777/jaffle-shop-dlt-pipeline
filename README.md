# Jaffle Shop dlt Pipeline

This repo contains a `dlt` pipeline that extracts data from the public Jaffle Shop API and loads it into DuckDB.

## Features
- Incremental loading (`ordered_at`)
- Filtering (`order_total <= 500`)
- Scheduled via GitHub Actions
- Stores data in `duckdb`

## Setup

1. Fork this repo
2. Customize schedule in `.github/workflows/run_pipeline.yml`
3. Watch it run daily!
