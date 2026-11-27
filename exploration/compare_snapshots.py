"""Compare two feed snapshots by event id.

Counts new, dropped, updated events and events with a changed magnitude.
Exploration only. Usage:
    python exploration/compare_snapshots.py snapshot_1.geojson snapshot_2.geojson
"""

"""Сравнивает два снимка фида по идентификатору события.

Подсчитывает новые, удаленные, обновленные события и события с измененной величиной.
Только для исследования. Использование:
    python exploration/compare_snapshots.py snapshot_1.geojson snapshot_2.geojson
"""

import sys, json
import duckdb, pandas as pd

path_1, path_2 = sys.argv[1], sys.argv[2]

with open(path_1) as f:
    data_1 = json.load(f)
with open(path_2) as f:
    data_2 = json.load(f)

s1 = pd.json_normalize(data_1["features"])  # колонки вида properties.mag, properties.type...
s2 = pd.json_normalize(data_2["features"])  # колонки вида properties.mag, properties.type...


print(duckdb.sql("""
    SELECT
    count(*) FILTER (WHERE s1.id IS NULL)                                   AS new_events,
    count(*) FILTER (WHERE s2.id IS NULL)                                   AS dropped_events,
    count(*) FILTER (WHERE s1."properties.updated" < s2."properties.updated") AS updated_events,
    count(*) FILTER (WHERE s1.id IS NOT NULL AND s2.id IS NOT NULL
                   AND s1."properties.mag" IS DISTINCT FROM s2."properties.mag") AS mag_changed
    FROM s1 FULL OUTER JOIN s2 USING (id)
"""))