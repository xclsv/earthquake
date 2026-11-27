"""Download events for a time period from the USGS FDSN Event API.

Checks the number of events first, because the API returns at most
20,000 events per request. Raw responses are stored in object storage
under usgs/fdsn/. Used by the monthly DAG for backfill and monthly reloads.
"""

"""Скачивает события за период времени из USGS FDSN Event API.
Сначала проверяет количество событий, потому что API возвращает максимум
20 000 событий за один запрос. Сырые ответы сохраняются в объектное хранилище 
под usgs/fdsn/. Используется ежемесячным DAG'ом для бэкфилла и ежемесячной перезагрузки.
"""

from datetime import datetime

import requests

from pipeline.storage import put_object

FDSN_URL = "https://earthquake.usgs.gov/fdsnws/event/1/"
MAX_EVENTS = 20000  # API limit per request


def fetch_period(start: datetime, end: datetime) -> str:
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

    key = f"usgs/fdsn/{start:%Y-%m-%d}_{end:%Y-%m-%d}.geojson"
    return put_object(key, response.content)