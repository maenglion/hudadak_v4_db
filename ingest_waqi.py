#!/usr/bin/env python3
import os, json, requests, psycopg2
from datetime import datetime, timezone

# --- DB / Token ---
DBNAME = os.getenv("DBNAME", "hudadak_air")
DBUSER = os.getenv("DBUSER", "hudadak_admin")
DBHOST = os.getenv("DBHOST", "/cloudsql/hudadak-air:asia-northeast3:hudadak-2025")
DBPASS = os.getenv("DBPASS")
TOKEN = os.getenv("WAQI_TOKEN")

# --- Target cities ---
CITIES = [
    "seoul","incheon","suwon","anyang","uijeongbu","chuncheon","gangneung",
    "daejeon","cheongju","jeonju","gwangju","daegu","ulsan","busan",
    "pohang","gyeongju","jeju"
]

def parse_waqi_ts(t):
    """Parse time field: prefer epoch 'v' > ISO > s+tz."""
    v = t.get("v")
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, timezone.utc)
    iso = t.get("iso")
    if iso:
        return datetime.fromisoformat(iso)
    s = t.get("s")
    tz = t.get("tz")
    if s and tz:
        return datetime.fromisoformat(s.replace(" ", "T") + tz)
    return datetime.now(timezone.utc)

def ingest_city(conn, city):
    r = requests.get(f"https://api.waqi.info/feed/{city}/?token={TOKEN}", timeout=25).json()
    if r.get("status") != "ok":
        print("WAQI fail:", city, r)
        return

    d = r["data"]
    iaqi = d.get("iaqi", {})
    pm10 = iaqi.get("pm10", {}).get("v")
    pm25 = iaqi.get("pm25", {}).get("v")
    ts = parse_waqi_ts(d.get("time", {}))

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO air.sources(code,name,base_url,kind)
        VALUES ('waqi','WAQI','https://waqi.info','observed')
        ON CONFLICT (code)
        DO UPDATE SET name=EXCLUDED.name, base_url=EXCLUDED.base_url, kind=EXCLUDED.kind
    """)
    name = f"WAQI {city.upper()}"; ext = f"WAQI_{city.upper()}"
    cur.execute("""
        INSERT INTO air.stations(external_code,name,provider,kind,city,country,source_id)
        VALUES (%s,%s,'WAQI','station',%s,%s,(SELECT id FROM air.sources WHERE code='waqi'))
        ON CONFLICT (provider, external_code) DO NOTHING
    """, (ext, name, city.title(), "KR"))
    cur.execute("SELECT id FROM air.stations WHERE provider='WAQI' AND name=%s", (name,))
    sid = cur.fetchone()[0]
    cur.execute("""
        INSERT INTO air.measurements(
            station_id, ts, pm10, pm25, raw, source_id, source_quality, unit_pm10, unit_pm25, aqi_provider
        )
        VALUES (
            %s, %s, %s, %s, %s::jsonb,
            (SELECT id FROM air.sources WHERE code='waqi'),
            'observed', 'ug/m3', 'ug/m3', 'WAQI'
        )
        ON CONFLICT (station_id,ts)
        DO UPDATE SET pm10=EXCLUDED.pm10, pm25=EXCLUDED.pm25, raw=EXCLUDED.raw
    """, (sid, ts, pm10, pm25, json.dumps(d)))
    cur.close()

def main():
    assert TOKEN, "Set WAQI_TOKEN"
    conn = psycopg2.connect(host=DBHOST, dbname=DBNAME, user=DBUSER, password=DBPASS)
    conn.autocommit = False
    for city in CITIES:
        ingest_city(conn, city)
    conn.commit()
    conn.close()
    print("WAQI OK")

if __name__ == "__main__":
    main()
