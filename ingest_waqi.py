#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone

import psycopg2
import requests


DBNAME = os.getenv("DBNAME", "hudadak_air")
DBUSER = os.getenv("DBUSER", "hudadak_admin")
DBHOST = os.getenv(
    "DBHOST", "/cloudsql/hudadak-air:asia-northeast3:hudadak-2025"
)
DBPASS = os.getenv("DBPASS")
TOKEN = os.getenv("WAQI_TOKEN")

TARGETS = [
    "geo:37.3925;126.6399",
    "seoul",
    "incheon",
    "suwon",
    "anyang",
    "uijeongbu",
    "chuncheon",
    "gangneung",
    "daejeon",
    "cheongju",
    "jeonju",
    "gwangju",
    "daegu",
    "ulsan",
    "busan",
    "pohang",
    "gyeongju",
    "jeju",
]


def parse_waqi_ts(value):
    """Parse the actual observation time with its provider timezone."""
    iso = value.get("iso")
    if iso:
        return datetime.fromisoformat(iso)
    local_time = value.get("s")
    offset = value.get("tz")
    if local_time and offset:
        return datetime.fromisoformat(local_time.replace(" ", "T") + offset)
    epoch = value.get("v")
    if isinstance(epoch, (int, float)):
        return datetime.fromtimestamp(epoch, timezone.utc)
    raise ValueError("WAQI observation time is missing")


def ingest_target(conn, target):
    payload = requests.get(
        f"https://api.waqi.info/feed/{target}/?token={TOKEN}", timeout=25
    ).json()
    if payload.get("status") != "ok":
        print("WAQI warning:", target, payload)
        return 0

    data = payload["data"]
    station = data.get("city") or {}
    geo = station.get("geo") or []
    if len(geo) < 2:
        print("WAQI warning: station coordinates missing:", target)
        return 0
    station_uid = data.get("idx")
    if station_uid is None:
        print("WAQI warning: station id missing:", target)
        return 0

    lat, lon = float(geo[0]), float(geo[1])
    observed_at = parse_waqi_ts(data.get("time") or {})
    iaqi = data.get("iaqi") or {}
    pm10 = (iaqi.get("pm10") or {}).get("v")
    pm25 = (iaqi.get("pm25") or {}).get("v")
    if pm10 is None and pm25 is None:
        print("WAQI warning: PM values missing:", target)
        return 0

    external_code = f"WAQI_{station_uid}"
    station_name = station.get("name") or external_code
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO air.sources(code,name,base_url,kind)
            VALUES ('waqi','WAQI','https://waqi.info','observed')
            ON CONFLICT (code) DO UPDATE SET
                name=EXCLUDED.name,
                base_url=EXCLUDED.base_url,
                kind=EXCLUDED.kind
            """
        )
        cur.execute(
            """
            INSERT INTO air.stations(
                external_code, name, provider, kind, city, country,
                lat, lon, geom, source_id
            )
            VALUES (
                %s,%s,'WAQI','station',%s,'KR',%s,%s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                (SELECT id FROM air.sources WHERE code='waqi')
            )
            ON CONFLICT (provider, external_code) DO UPDATE SET
                name=EXCLUDED.name,
                city=EXCLUDED.city,
                country=EXCLUDED.country,
                lat=EXCLUDED.lat,
                lon=EXCLUDED.lon,
                geom=EXCLUDED.geom,
                kind=EXCLUDED.kind,
                source_id=EXCLUDED.source_id
            """,
            (
                external_code,
                station_name,
                target,
                lat,
                lon,
                lon,
                lat,
            ),
        )
        cur.execute(
            """
            SELECT id FROM air.stations
            WHERE provider='WAQI' AND external_code=%s
            """,
            (external_code,),
        )
        station_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO air.measurements(
                station_id, ts, pm10, pm25, raw, source_id, source_quality,
                unit_pm10, unit_pm25, aqi_provider
            )
            VALUES (
                %s,%s,%s,%s,%s::jsonb,
                (SELECT id FROM air.sources WHERE code='waqi'),
                'observed','ug/m3','ug/m3','WAQI'
            )
            ON CONFLICT (station_id,ts) DO UPDATE SET
                pm10=EXCLUDED.pm10,
                pm25=EXCLUDED.pm25,
                raw=EXCLUDED.raw,
                source_quality=EXCLUDED.source_quality,
                unit_pm10=EXCLUDED.unit_pm10,
                unit_pm25=EXCLUDED.unit_pm25,
                aqi_provider=EXCLUDED.aqi_provider
            """,
            (
                station_id,
                observed_at,
                pm10,
                pm25,
                json.dumps(data),
            ),
        )
    return 1


def main():
    if not TOKEN:
        raise RuntimeError("WAQI_TOKEN is not configured")
    conn = psycopg2.connect(
        host=DBHOST, dbname=DBNAME, user=DBUSER, password=DBPASS
    )
    inserted = 0
    try:
        for target in TARGETS:
            try:
                inserted += ingest_target(conn, target)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                print(f"WAQI warning: {target} failed: {exc}")
    finally:
        conn.close()
    if inserted == 0:
        raise RuntimeError("WAQI inserted no PM observations")
    print(f"WAQI OK: {inserted} observations")
    return inserted


if __name__ == "__main__":
    main()
