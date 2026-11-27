import os
from datetime import datetime, timezone

import requests

FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{feed}.geojson"


def fetch_feed(feed: str, raw_dir: str) -> str:
    """Download a feed, save it as is and return the file path."""
    response = requests.get(FEED_URL.format(feed=feed), timeout=60)
    response.raise_for_status()

    generated_ms = response.json()["metadata"]["generated"]
    generated = datetime.fromtimestamp(generated_ms / 1000, tz=timezone.utc)

    feed_dir = os.path.join(raw_dir, feed)
    os.makedirs(feed_dir, exist_ok=True)
    path = os.path.join(feed_dir, generated.strftime("%Y-%m-%dT%H-%M-%SZ") + ".geojson")

    with open(path, "wb") as f:
        f.write(response.content)
    return path