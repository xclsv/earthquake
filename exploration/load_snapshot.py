import sys
import json
import duckdb
import pandas as pd

DB_PATH = "data/earthquake.duckdb"  # тот файл, где ты создал таблицу


def split_list(value):
    """',us7000abcd,ci12345,' -> ['us7000abcd', 'ci12345']"""
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


# все поля, которые перезаписываются при обновлении события
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
    to_timestamp(CAST(time_ms AS BIGINT) / 1000),
    to_timestamp(CAST(updated_ms AS BIGINT) / 1000),
    status,
    CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE), CAST(depth_km AS DOUBLE),
    place,
    CAST(mag AS DOUBLE), mag_type,
    CAST(sig AS INTEGER), CAST(felt AS INTEGER),
    CAST(cdi AS DOUBLE), CAST(mmi AS DOUBLE),
    alert, CAST(tsunami AS BOOLEAN),
    CAST(nst AS INTEGER), CAST(gap AS DOUBLE), CAST(dmin AS DOUBLE), CAST(rms AS DOUBLE),
    url
FROM staging
ON CONFLICT (event_id) DO UPDATE SET
    {update_set},
    last_loaded_at = now()
WHERE EXCLUDED.updated_at > earthquake_events.updated_at
"""


def main(path):
    with open(path) as f:
        features = json.load(f)["features"]

    # dtype=object, чтобы pandas не превращал None в NaN
    staging = pd.DataFrame([flatten(f) for f in features], dtype=object)

    con = duckdb.connect(DB_PATH)
    con.register("staging", staging)

    before = con.sql("SELECT count(*) FROM earthquake_events").fetchall()[0][0]
    con.execute(UPSERT_SQL)
    after = con.sql("SELECT count(*) FROM earthquake_events").fetchall()[0][0]

    print(f"{path}: {len(staging)} events in file, "
          f"{after - before} new rows, {after} rows in table")
    con.close()


if __name__ == "__main__":
    main(sys.argv[1])