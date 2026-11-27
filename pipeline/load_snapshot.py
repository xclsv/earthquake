import os
import sys
import json

import psycopg
from dotenv import load_dotenv

from typing import LiteralString, cast

load_dotenv()  # читает .env из текущей папки


def get_connection():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def split_list(value):
    if not value:
        return None
    return [x for x in value.split(",") if x]


def flatten(feature):
    p = feature["properties"]
    lon, lat, depth = feature["geometry"]["coordinates"]
    return {
        "event_id": feature["id"],
        "net": p.get("net"),
        "code": p.get("code"),
        "ids": split_list(p.get("ids")),
        "sources": split_list(p.get("sources")),
        "event_type": p.get("type"),
        "time_ms": p.get("time"),
        "updated_ms": p.get("updated"),
        "status": p.get("status"),
        "longitude": lon,
        "latitude": lat,
        "depth_km": depth,
        "place": p.get("place"),
        "mag": p.get("mag"),
        "mag_type": p.get("magType"),
        "sig": p.get("sig"),
        "felt": p.get("felt"),
        "cdi": p.get("cdi"),
        "mmi": p.get("mmi"),
        "alert": p.get("alert"),
        "tsunami": p.get("tsunami"),
        "nst": p.get("nst"),
        "gap": p.get("gap"),
        "dmin": p.get("dmin"),
        "rms": p.get("rms"),
        "url": p.get("url"),
    }


# staging хранит данные почти как в JSON: время в мс, tsunami как 0/1
CREATE_STAGING_SQL = """
CREATE TEMP TABLE staging (
    event_id text, net text, code text, ids text[], sources text[],
    event_type text, time_ms bigint, updated_ms bigint, status text,
    longitude double precision, latitude double precision, depth_km double precision,
    place text, mag double precision, mag_type text, sig integer, felt integer,
    cdi double precision, mmi double precision, alert text, tsunami integer,
    nst integer, gap double precision, dmin double precision, rms double precision,
    url text
) ON COMMIT DROP
"""

STAGING_COLUMNS = list(flatten({"id": None, "properties": {},
                                "geometry": {"coordinates": [0, 0, 0]}}).keys())

UPDATE_COLUMNS = [
    "net", "code", "ids", "sources", "event_type", "event_time", "updated_at",
    "status", "longitude", "latitude", "depth_km", "place", "mag", "mag_type",
    "sig", "felt", "cdi", "mmi", "alert", "tsunami", "nst", "gap", "dmin",
    "rms", "url",
]
update_set = ",\n    ".join(f"{c} = EXCLUDED.{c}" for c in UPDATE_COLUMNS)

UPSERT_SQL = f"""
INSERT INTO earthquake_events (event_id, {", ".join(UPDATE_COLUMNS)})
SELECT
    event_id, net, code, ids, sources, event_type,
    to_timestamp(time_ms / 1000.0),
    to_timestamp(updated_ms / 1000.0),
    status, longitude, latitude, depth_km, place, mag, mag_type,
    sig, felt, cdi, mmi, alert, tsunami::boolean,
    nst, gap, dmin, rms, url
FROM staging
ON CONFLICT (event_id) DO UPDATE SET
    {update_set},
    last_loaded_at = now()
WHERE EXCLUDED.updated_at > earthquake_events.updated_at
RETURNING (xmax = 0) AS inserted
"""


def load(path):
    with open(path) as f:
        features = json.load(f)["features"]
    rows = [flatten(f) for f in features]

    insert_staging = (
        f"INSERT INTO staging ({', '.join(STAGING_COLUMNS)}) "
        f"VALUES ({', '.join(['%s'] * len(STAGING_COLUMNS))})"
    )

    with get_connection() as conn:          # одна транзакция на всю загрузку
        with conn.cursor() as cur:
            cur.execute(CREATE_STAGING_SQL)
            cur.executemany(cast(LiteralString, insert_staging),
                            [tuple(r[c] for c in STAGING_COLUMNS) for r in rows])
            cur.execute(cast(LiteralString, UPSERT_SQL))
            results = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT count(*) FROM earthquake_events")
            row = cur.fetchone()
            assert row is not None
            total = row[0]

    inserted = sum(results)
    updated = len(results) - inserted
    print(f"{path}: {len(rows)} events in file, {inserted} inserted, "
          f"{updated} updated, {len(rows) - len(results)} unchanged, {total} rows in table")
    return {"inserted": inserted, "updated": updated,
            "unchanged": len(rows) - len(results)}


if __name__ == "__main__":
    load(sys.argv[1])