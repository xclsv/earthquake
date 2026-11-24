# USGS Earthquake Data Pipeline

A learning data engineering project built on the public
[USGS Earthquake GeoJSON feed](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).

The goal is to build a pipeline that ingests earthquake events in near real time,
handles late updates to already published events, and serves analytical marts
and a dashboard.

## Why this dataset

- Free, no API key, updated every minute
- Events are revised after publication (magnitude, location, review status),
  so the pipeline has to deal with upserts, deduplication and late-arriving data
- Historical data available via the FDSN Event API for backfill

## Status

🚧 Work in progress — currently at stage 0 (data exploration).

## Roadmap

- [x] Stage 0: Explore the data with DuckDB
- [ ] Stage 1: Batch ingestion with Airflow + Postgres (idempotent upserts, backfill)
- [ ] Stage 2: dbt models, data tests, Metabase dashboard
- [ ] Stage 3: Streaming ingestion with Kafka (producer + consumer)
- [ ] Stage 4: Daily reconciliation of late updates, pipeline monitoring

## Findings

- DuckDB's `read_json_auto` detects the GeoJSON structure differently than expected
  (it unnests `features` automatically), so parsing is done explicitly to keep
  the schema stable.

## How to run

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    python explore.py