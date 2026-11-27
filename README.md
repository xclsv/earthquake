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

Stage 1 complete: ~415k events since January 2024, updated every 15 minutes.
Raw responses are stored in S3-compatible object storage. Next: stage 2 (dbt and dashboard).

## Roadmap

- [x] Stage 0: Explore the data with DuckDB
- [x] Stage 1: Batch ingestion with Airflow + Postgres
  - [x] Prototype idempotent upsert in DuckDB
  - [x] Postgres in Docker
  - [x] Loader for Postgres
  - [x] Airflow in Docker
  - [x] Airflow DAG for incremental loading
  - [x] Backfill via FDSN API
  - [x] Raw storage in S3-compatible object storage (RustFS)
- [ ] Stage 2: dbt models, data tests, Metabase dashboard
- [ ] Stage 3: Streaming ingestion with Kafka (producer + consumer)
- [ ] Stage 4: Daily reconciliation of late updates, pipeline monitoring

## Current architecture

    USGS feed (all_day)          USGS FDSN Event API
            │ every 15 min               │ monthly + backfill
            ▼                            ▼
    ┌──────────────────── Docker Compose ────────────────────┐
    │                                                        │
    │   Airflow (standalone, LocalExecutor)                  │
    │   UI: http://localhost:8080                            │
    │     │ fetch                              │ load        │
    │     ▼                                    ▼             │
    │   RustFS (S3 API)  ────── read ──▶     Postgres        │
    │   bucket earthquake-raw                ├── earthquake  │
    │   console: localhost:9001              └── airflow     │
    │                                                        │
    └────────────────────────────────────────────────────────┘

- Every 15 minutes, the `earthquake_incremental` DAG fetches the `all_day`
  feed, stores the raw response in object storage and loads it into Postgres.
- Raw responses are stored unchanged in RustFS, an S3-compatible object
  storage, under keys like `usgs/feed/all_day/2026-11-27T10-15-03Z.geojson`
  and `usgs/fdsn/2024-01-01_2024-02-01.geojson`. Tasks pass object keys
  to each other, not local file paths.
- Airflow runs all its components in a single container (`airflow standalone`)
  with LocalExecutor — no Celery or Redis needed at this scale.
- One Postgres instance hosts two databases: pipeline data and Airflow
  metadata are kept separate.
- `dags/`, `pipeline/` and `data/` are mounted into the Airflow container,
  so code changes are picked up without rebuilding. Python dependencies
  are installed into a custom image (`Dockerfile`, `requirements-airflow.txt`),
  so changing them requires `docker compose up -d --build`.
- The loader (`pipeline/load_snapshot.py`) writes to a temporary staging
  table and upserts into `earthquake_events` by `event_id`, only if the
  incoming version has a newer `updated_at`.

## Pipelines

Both DAGs share the same loader, so all events end up in one table
regardless of where they came from.

### `earthquake_incremental`

- Runs every 15 minutes.
- Fetches the `all_day` feed, stores the raw response in object storage
  and upserts it into Postgres.
- Keeps the table close to real time: new events appear within minutes.

### `earthquake_monthly`

- Runs on the 1st of every month and loads the previous full month
  from the FDSN Event API. Stores the raw response in object storage.
- With `catchup=True` and `start_date` set to February 2024, it also
  backfills the history: Airflow creates one run per missed month,
  starting from January 2024.
- Reloading a whole month picks up late revisions that the incremental
  DAG may have missed after events left its 24-hour window.
- Before downloading, it checks the number of events via the API `count`
  method and fails with a clear error if a month exceeds the API limit
  of 20,000 events.

## How to run

Requirements: Docker (with at least 4 GB of memory available), Python 3.

1. Create `.env` from the template and fill in the passwords
   for Postgres and object storage:

       cp .env.example .env

2. Start Postgres, RustFS and Airflow:

       docker compose up -d

   The first start takes a couple of minutes while Airflow initializes
   its database.

3. Open the UIs:
   - Airflow: http://localhost:8080 (login is disabled for local development)
   - RustFS console: http://localhost:9001 (log in with `S3_ACCESS_KEY`
     and `S3_SECRET_KEY` from `.env`)

4. (Optional) Load a feed snapshot manually from your machine:

       python -m venv .venv
       source .venv/bin/activate
       pip install -r requirements.txt
       mkdir -p data
       curl -o data/snapshot.geojson \
         https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson
       python pipeline/load_snapshot.py data/snapshot.geojson

Recommended after the first start: load the all_month feed once to fill the current month (see Design decisions).

To recreate everything from scratch (deletes all data):

    docker compose down -v
    docker compose up -d

## Project structure

    dags/                     When and in what order: Airflow DAGs
    pipeline/                 What is done: fetching, storage, loading
    exploration/              Data exploration and DuckDB prototype (not used by the pipeline)
    sql/                      Database schema and init scripts
    docker-compose.yml        Postgres, RustFS and Airflow
    Dockerfile                Airflow image with pipeline dependencies
    requirements-airflow.txt  Python dependencies for the Airflow image
    requirements.txt          Python dependencies for local scripts
    .env.example              Template for passwords and connection settings

## Design decisions

- **Idempotent loading.** `event_id` is the primary key. An existing row
  is overwritten only when the incoming version has a newer `updated_at`,
  so repeated loads are a no-op and an older version never overwrites
  a newer one.
- **Each load is a single transaction** via a temporary staging table.
  The loader reports inserted, updated and unchanged rows.
- **No deletions.** The feed is a sliding window, not a source of truth
  for deletions: events leaving the window are kept.
- **Object storage instead of local disk** decouples storage from compute,
  as in production pipelines. On a single machine, local disk would be enough.
- **RustFS instead of MinIO.** MinIO stopped publishing community Docker
  images in 2025, and its repository was archived. RustFS is an actively
  maintained S3-compatible alternative under Apache 2.0.
- **Gap on a fresh start.** ... (перенеси свой абзац как есть)
- **Each monthly run is tied to its logical date** ... (как есть)
- **The busiest month so far** ... (как есть)

**Assumption:** USGS bumps `updated` whenever an event changes. Rows with
the same `updated_at` but different field values would not be updated.

**Verification**

| Check | Expected | Result |
|---|---|---|
| New rows after loading snapshot 2 | = new events from comparison | ✅ 60 |
| Rows with `last_loaded_at > first_seen_at` | = updated events from comparison | ✅ 10 |
| Dropped events | still present in the table | ✅ |
| Loading snapshot 2 again | 0 new rows, 0 updated rows | ✅ |

Reproduce:

    python exploration/create_db.py
    python exploration/load_snapshot.py data/snapshot_1.geojson
    python exploration/load_snapshot.py data/snapshot_2.geojson

## Findings

- DuckDB's `read_json_auto` detects the GeoJSON structure differently than expected
  (it unnests `features` automatically), so parsing is done explicitly to keep
  the schema stable.
- Events are revised after publication. In a snapshot of the `all_month`
  feed taken on 2026-11-24, 21.3% of events were still `automatic`, and the
  median time between an event and its last update was 44+ hours for
  `reviewed` events. Run `python exploration/explore.py` to see current values.

### Comparing two snapshots of the feed

Two snapshots of the `all_month` feed were compared by event `id`
(feed generation time from `metadata.generated`):

| Snapshot | Generated (UTC)  |
|----------|------------------|
| 1        | 2026-11-24 10:14 |
| 2        | 2026-11-24 14:02 |

| Category        | Events |
|-----------------|--------|
| New             | 60     |
| Dropped         | 36     |
| Updated         | 10     |
| Magnitude changed | 3    |

Takeaways:
- New and updated events require an upsert keyed by `id`, applied only
  when `updated` is newer than the stored version.
- The feed is a sliding window, not a source of truth for deletions:
  events leaving the window must be kept in storage.

### Prototype: loading snapshots with an idempotent upsert

Before building the Airflow pipeline, the loading logic was prototyped
in DuckDB on the two feed snapshots.

**How it works**

1. Python flattens each GeoJSON feature into a plain record
   (properties, coordinates, `ids`/`sources` split into lists).
2. Records go to a staging table.
3. A single SQL statement casts types (epoch ms → `timestamptz`,
   0/1 → `boolean`) and upserts into `earthquake_events`.