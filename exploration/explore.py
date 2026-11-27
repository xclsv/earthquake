"""First look at the all_month feed: event types, fields, data quality.

Exploration only, not used by the pipeline.
"""

"""Первый взгляд на фид all_month: типы событий, поля, качество данных.

Только для исследования, не используется в пайплайне.
"""

import json, duckdb, pandas as pd

with open("data/all_month.geojson") as f:
    data = json.load(f)

events = pd.json_normalize(data["features"])  # колонки вида properties.mag, properties.type...
print(duckdb.sql("""
    SELECT "properties.type" AS type, count(*) AS cnt
    FROM events
    GROUP BY 1 ORDER BY cnt DESC
"""))

print(duckdb.sql("""
    SELECT
    count(*) AS total,
    count("properties.mag")  AS has_mag,
    count("properties.felt") AS has_felt,
    count("properties.alert") AS has_alert
    FROM events
"""))

print(duckdb.sql("""
    SELECT "properties.status" AS status, count(*) AS cnt
    FROM events GROUP BY 1 ORDER BY cnt DESC
"""))

print(duckdb.sql("""
    SELECT
    id,
    to_timestamp("properties.time" / 1000)    AS event_time,
    to_timestamp("properties.updated" / 1000) AS updated_time,
    "geometry.coordinates"[3]                 AS depth_km
    FROM events
    ORDER BY event_time DESC
    LIMIT 10
"""))

print(duckdb.sql("""
    SELECT
    "properties.status" AS status,
    round(median(("properties.updated" - "properties.time") / 3600000.0), 1) AS median_hours
    FROM events GROUP BY 1
"""))

print(duckdb.sql("""
    SELECT id, "properties.ids" AS ids
    FROM events
    WHERE len(string_split(trim("properties.ids", ','), ',')) > 1
    LIMIT 10
"""))