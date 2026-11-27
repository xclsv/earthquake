import os
from datetime import datetime

import requests

FDSN_URL = "https://earthquake.usgs.gov/fdsnws/event/1/"
MAX_EVENTS = 20000  # API limit per request


def fetch_period(start: datetime, end: datetime, raw_dir: str) -> str:
    """Download all events between start and end, save as is, return the path."""
    params = {
        "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    count_response = requests.get(FDSN_URL + "count", params=params, timeout=60)
    count_response.raise_for_status()
    count = int(count_response.text)
    if count > MAX_EVENTS:
        raise ValueError(
            f"{count} events between {start} and {end}, "
            f"more than the API limit of {MAX_EVENTS}. Split the period."
        )

    response = requests.get(
        FDSN_URL + "query",
        params={**params, "format": "geojson", "orderby": "time-asc"},
        timeout=300,
    )
    response.raise_for_status()

    folder = os.path.join(raw_dir, "fdsn")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{start:%Y-%m-%d}_{end:%Y-%m-%d}.geojson")
    with open(path, "wb") as f:
        f.write(response.content)
    return path