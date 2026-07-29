#!/usr/bin/env python3
import json

from airkorea_common import (
    AIRKOREA_BASE_URL,
    configured_regions,
    ensure_usage_table,
    get_db_connection,
    parse_observed_at,
    request_json,
    station_external_code,
    to_int,
)


REALTIME_ENDPOINT = f"{AIRKOREA_BASE_URL}/getCtprvnRltmMesureDnsty"


def fetch_region(conn, region, decode_key=False):
    return request_json(
        conn,
        REALTIME_ENDPOINT,
        {
            "returnType": "json",
            "numOfRows": "1000",
            "pageNo": "1",
            "sidoName": region,
            "ver": "1.3",
        },
        decode_key=decode_key,
    )


def upsert_region_measurements(conn, region, items):
    upserted = 0
    skipped_without_coordinates = 0
    with conn.cursor() as cur:
        for item in items:
            station_name = (item.get("stationName") or "").strip()
            observed_at_text = item.get("dataTime")
            if not station_name or not observed_at_text:
                continue
            pm10 = to_int(item.get("pm10Value"))
            pm25 = to_int(item.get("pm25Value"))
            if pm10 is None and pm25 is None:
                continue

            external_code = station_external_code(region, station_name)
            cur.execute(
                """
                SELECT id
                FROM air.stations
                WHERE provider='AIRKOREA'
                  AND external_code=%s
                  AND geom IS NOT NULL
                  AND lat IS NOT NULL
                  AND lon IS NOT NULL
                """,
                (external_code,),
            )
            station = cur.fetchone()
            if not station:
                skipped_without_coordinates += 1
                continue

            cur.execute(
                """
                INSERT INTO air.measurements(
                    station_id, ts, pm10, pm25, pm10_grade, pm25_grade,
                    raw, source_id, source_quality, unit_pm10, unit_pm25,
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
                    unit_pm10=EXCLUDED.unit_pm10,
                    unit_pm25=EXCLUDED.unit_pm25,
                    aqi_provider=EXCLUDED.aqi_provider
                """,
                (
                    station[0],
                    parse_observed_at(observed_at_text),
                    pm10,
                    pm25,
                    to_int(item.get("pm10Grade")),
                    to_int(item.get("pm25Grade")),
                    json.dumps(item, ensure_ascii=False),
                ),
            )
            upserted += 1
    conn.commit()
    return upserted, skipped_without_coordinates


def main():
    conn = get_db_connection()
    ensure_usage_table(conn)
    succeeded_regions = 0
    total_measurements = 0
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
        conn.commit()

        for region in configured_regions():
            last_error = None
            for attempt in range(2):
                try:
                    payload = fetch_region(
                        conn, region, decode_key=(attempt == 1)
                    )
                    items = (
                        payload.get("response", {})
                        .get("body", {})
                        .get("items")
                        or []
                    )
                    upserted, missing_coordinates = (
                        upsert_region_measurements(conn, region, items)
                    )
                    total_measurements += upserted
                    succeeded_regions += 1
                    print(
                        f"AIRKOREA region OK: region={region}, "
                        f"measurements={upserted}, "
                        f"missing_coordinates={missing_coordinates}"
                    )
                    last_error = None
                    break
                except Exception as exc:
                    conn.rollback()
                    last_error = exc
                    if attempt == 0:
                        print(
                            f"AIRKOREA region warning: region={region}, "
                            "attempt=1 failed; retrying"
                        )
            if last_error is not None:
                print(
                    f"AIRKOREA region error: region={region}, "
                    "attempts=2, collection failed"
                )
    finally:
        conn.close()

    if succeeded_regions == 0:
        raise RuntimeError("AirKorea collection failed for all regions")
    print(
        f"AIRKOREA OK: regions={succeeded_regions}, "
        f"measurements={total_measurements}"
    )
    return total_measurements


if __name__ == "__main__":
    main()
