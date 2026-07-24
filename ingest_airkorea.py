#!/usr/bin/env python3
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
import requests


DBHOST = os.environ["DBHOST"]
DBNAME = os.environ["DBNAME"]
DBUSER = os.environ["DBUSER"]
DBPASS = os.environ["DBPASS"]
AIRKEY = os.environ.get("AIRKOREA_KEY", "")
STATION_NAME = os.environ.get("AIRKOREA_STATION_NAME", "송도")
STATION_LAT = float(os.environ.get("AIRKOREA_STATION_LAT", "37.3925"))
STATION_LON = float(os.environ.get("AIRKOREA_STATION_LON", "126.6399"))


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_airkorea_latest(station_name):
    response = requests.get(
        "https://apis.data.go.kr/B552584/ArpltnInforInqireSvc/"
        "getMsrstnAcctoRltmMesureDnsty",
        params={
            "serviceKey": AIRKEY,
            "returnType": "json",
            "numOfRows": "1",
            "pageNo": "1",
            "stationName": station_name,
            "dataTerm": "DAILY",
            "ver": "1.3",
        },
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def main():
    if not AIRKEY:
        print("AIRKOREA warning: AIRKOREA_KEY is not configured; skipped")
        return 0

    payload = fetch_airkorea_latest(STATION_NAME)
    items = payload.get("response", {}).get("body", {}).get("items") or []
    if not items:
        raise RuntimeError("AirKorea returned no observations")
    item = items[0]
    pm10 = to_int(item.get("pm10Value"))
    pm25 = to_int(item.get("pm25Value"))
    if pm10 is None and pm25 is None:
        raise RuntimeError("AirKorea returned no PM values")
    observed_at = datetime.strptime(
        item["dataTime"], "%Y-%m-%d %H:%M"
    ).replace(tzinfo=ZoneInfo("Asia/Seoul"))

    conn = psycopg2.connect(
        host=DBHOST, dbname=DBNAME, user=DBUSER, password=DBPASS
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO air.sources(code,name,base_url,kind)
                VALUES (
                    'airkorea','AirKorea','https://apis.data.go.kr','observed'
                )
                ON CONFLICT (code) DO UPDATE SET
                    name=EXCLUDED.name,
                    base_url=EXCLUDED.base_url,
                    kind=EXCLUDED.kind
                """
            )
            external_code = f"AIRKOREA_{STATION_NAME}"
            cur.execute(
                """
                INSERT INTO air.stations(
                    external_code, name, provider, kind, region_si, region_gu,
                    region_dong, lat, lon, geom, source_id
                )
                VALUES (
                    %s, %s, 'AIRKOREA', 'airkorea_station',
                    '인천', '연수구', %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    (SELECT id FROM air.sources WHERE code='airkorea')
                )
                ON CONFLICT (provider, external_code) DO UPDATE SET
                    name=EXCLUDED.name,
                    kind=EXCLUDED.kind,
                    lat=EXCLUDED.lat,
                    lon=EXCLUDED.lon,
                    geom=EXCLUDED.geom,
                    source_id=EXCLUDED.source_id
                """,
                (
                    external_code,
                    STATION_NAME,
                    STATION_NAME,
                    STATION_LAT,
                    STATION_LON,
                    STATION_LON,
                    STATION_LAT,
                ),
            )
            cur.execute(
                """
                SELECT id FROM air.stations
                WHERE provider='AIRKOREA' AND external_code=%s
                """,
                (external_code,),
            )
            station_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO air.measurements(
                    station_id, ts, pm10, pm25, pm10_grade, pm25_grade, raw,
                    source_id, source_quality, unit_pm10, unit_pm25,
                    aqi_provider
                )
                VALUES (
                    %s,%s,%s,%s,%s,%s,%s::jsonb,
                    (SELECT id FROM air.sources WHERE code='airkorea'),
                    'observed','ug/m3','ug/m3','AIRKOREA'
                )
                ON CONFLICT (station_id, ts) DO UPDATE SET
                    pm10=EXCLUDED.pm10,
                    pm25=EXCLUDED.pm25,
                    pm10_grade=EXCLUDED.pm10_grade,
                    pm25_grade=EXCLUDED.pm25_grade,
                    raw=EXCLUDED.raw,
                    source_quality=EXCLUDED.source_quality,
                    aqi_provider=EXCLUDED.aqi_provider
                """,
                (
                    station_id,
                    observed_at,
                    pm10,
                    pm25,
                    to_int(item.get("pm10Grade")),
                    to_int(item.get("pm25Grade")),
                    json.dumps(item),
                ),
            )
        conn.commit()
    finally:
        conn.close()
    print("AIRKOREA OK: 1 observation")
    return 1


if __name__ == "__main__":
    main()
