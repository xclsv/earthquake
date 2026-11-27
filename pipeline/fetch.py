"""Download USGS summary feeds and store raw responses in object storage.

Each response is saved unchanged, under a key based on the feed generation
time, e.g. usgs/feed/all_day/2026-11-27T10-15-03Z.geojson.
Used by the incremental DAG.
"""

"""Скачивает сводные фиды USGS и сохраняет сырые ответы в объектное хранилище.

Каждый ответ сохраняется без изменений, под ключом, основанным на времени генерации фида,
например usgs/feed/all_day/2026-11-27T10-15-03Z.geojson.
Используется инкрементальным DAG'ом.
"""

from datetime import datetime, timezone

import requests

from pipeline.storage import put_object

FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{feed}.geojson"


def fetch_feed(feed: str) -> str:
    """Download a feed, store the raw response and return its object key."""
    response = requests.get(FEED_URL.format(feed=feed), timeout=60)
    response.raise_for_status()

    generated_ms = response.json()["metadata"]["generated"]
    generated = datetime.fromtimestamp(generated_ms / 1000, tz=timezone.utc)

    key = f"usgs/feed/{feed}/{generated:%Y-%m-%dT%H-%M-%SZ}.geojson"
    return put_object(key, response.content)