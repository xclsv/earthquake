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

🚧 Work in progress — currently at stage 1 (Airflow + Postgres).

## Roadmap

- [x] Stage 0: Explore the data with DuckDB
- [ ] Stage 1: Batch ingestion with Airflow + Postgres
  - [x] Prototype idempotent upsert in DuckDB
  - [x] Postgres in Docker
  - [ ] Loader for Postgres
  - [ ] Airflow DAG for incremental loading
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

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    python explore.py

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