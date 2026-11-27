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

🚧 Stage 1 in progress: infrastructure is up, next step is the incremental loading DAG.

## Roadmap

- [x] Stage 0: Explore the data with DuckDB
- [ ] Stage 1: Batch ingestion with Airflow + Postgres
  - [x] Prototype idempotent upsert in DuckDB
  - [x] Postgres in Docker
  - [x] Loader for Postgres
  - [x] Airflow in Docker
  - [x] Airflow DAG for incremental loading
  - [ ] Backfill via FDSN API
- [ ] Stage 2: dbt models, data tests, Metabase dashboard
- [ ] Stage 3: Streaming ingestion with Kafka (producer + consumer)
- [ ] Stage 4: Daily reconciliation of late updates, pipeline monitoring

## Findings

- DuckDB's `read_json_auto` detects the GeoJSON structure differently than expected
  (it unnests `features` automatically), so parsing is done explicitly to keep
  the schema stable.
- Events are revised after publication. In a snapshot of the all_month feed taken on 2026-11-24, 21.3% of events were still automatic, and the median time between an event and its last update was 44+ hours for reviewed events. Run python explore.py to see current values

## How to run

Requirements: Docker (with at least 4 GB of memory available), Python 3.

1. Create `.env` from the template and set a password:

       cp .env.example .env

2. Start Postgres and Airflow:

       docker compose up -d

   The first start takes a couple of minutes while Airflow initializes
   its database.

3. Open the Airflow UI at http://localhost:8080. The admin password
   is generated on first start and printed to the logs:

       docker compose logs airflow | grep -i password

4. (Optional) Load a feed snapshot manually from your machine:

       python -m venv .venv
       source .venv/bin/activate
       pip install -r requirements.txt
       mkdir -p data
       curl -o data/snapshot.geojson \
         https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson
       python pipeline/load_snapshot.py data/snapshot.geojson

To recreate everything from scratch (deletes all data):

    docker compose down -v
    docker compose up -d

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

**Design decisions**

- `event_id` is the primary key: it is the stable USGS identifier.
- An existing row is overwritten only when the incoming version has a newer
  `updated_at`. This makes repeated loads a no-op and prevents an older
  version from overwriting a newer one if they arrive out of order.
- Events that leave the feed window are never deleted: the feed is
  a sliding window, not a source of truth for deletions.
- `first_seen_at` is kept on updates, `last_loaded_at` is refreshed —
  this allows tracking how many events were revised after the first load.
- The load is performed as a single transaction via a temporary staging table. The output shows the number of inserted, updated, and unchanged rows

**Assumption**

USGS bumps `updated` whenever an event changes. Rows with the same
`updated_at` but different field values would not be updated.

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

## Current architecture

    USGS feed (all_day)
            │  every 15 min
            ▼
    ┌──────────────── Docker Compose ────────────────┐
    │                                                │
    │   Airflow (standalone, LocalExecutor)          │
    │   UI: http://localhost:8080                    │
    │     │ fetch                  │ load            │
    │     ▼                        ▼                 │
    │   data/raw/all_day/       Postgres             │
    │   (raw GeoJSON)           ├── earthquake       │
    │                           └── airflow          │
    │                                                │
    └────────────────────────────────────────────────┘

- Every 15 minutes, the `earthquake_incremental` DAG fetches the `all_day`
  feed, saves the raw response to `data/raw/all_day/` (named by the feed
  generation time) and loads it into Postgres.
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

## Project structure

    dags/                 Airflow DAGs
    pipeline/             Loading logic used by DAGs and scripts
    exploration/          Data exploration and DuckDB prototype
    sql/                  Database schema and init scripts
    data/                 Local data files (not tracked by git)
    docker-compose.yml    Postgres + Airflow
    .env.example          Template for environment variables