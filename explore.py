import duckdb, requests

url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
with open("all_month.geojson", "wb") as f:
    f.write(requests.get(url, timeout=60).content)

print(duckdb.sql("""
    SELECT type, count(*) AS cnt
    FROM read_json_auto('all_month.geojson')
    GROUP BY type ORDER BY cnt DESC
"""))