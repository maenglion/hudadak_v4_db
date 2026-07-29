#!/usr/bin/env python3
from airkorea_common import (
    AIRKOREA_STATION_BASE_URL,
    configured_regions,
    ensure_usage_table,
    get_db_connection,
    request_json,
    station_belongs_to_region,
    station_external_code,
)


STATION_ENDPOINT = f"{AIRKOREA_STATION_BASE_URL}/getMsrstnList"


def to_coordinate(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_region_stations(conn, region, decode_key=False):
    return request_json(
        conn,
        STATION_ENDPOINT,
        {
            "returnType": "json",
            "numOfRows": "1000",
            "pageNo": "1",
            "addr": region,
        },
        decode_key=decode_key,
    )


def upsert_stations(conn, region, items):
    upserted = 0
    skipped_without_coordinates = 0
    skipped_outside_region = 0
    with conn.cursor() as cur:
        for item in items:
            if not station_belongs_to_region(region, item):
                skipped_outside_region += 1
                continue
            station_name = (item.get("stationName") or "").strip()
            lat = to_coordinate(item.get("dmX"))
            lon = to_coordinate(item.get("dmY"))
            if not station_name or lat is None or lon is None:
                skipped_without_coordinates += 1
                continue
            if not (30 <= lat <= 40 and 120 <= lon <= 135):
                skipped_without_coordinates += 1
                continue

            external_code = station_external_code(region, station_name)
            cur.execute(
                """
                INSERT INTO air.stations(
                    external_code, name, provider, kind, city, country,
                    address, lat, lon, geom, source_id
                )
                VALUES (
                    %s,%s,'AIRKOREA','airkorea_station',%s,'KR',%s,%s,%s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    (SELECT id FROM air.sources WHERE code='airkorea')
                )
                ON CONFLICT (provider, external_code) DO UPDATE SET
                    name=EXCLUDED.name,
                    kind=EXCLUDED.kind,
                    city=EXCLUDED.city,
                    country=EXCLUDED.country,
                    address=EXCLUDED.address,
                    lat=EXCLUDED.lat,
                    lon=EXCLUDED.lon,
                    geom=EXCLUDED.geom,
                    source_id=EXCLUDED.source_id
                """,
                (
                    external_code,
                    station_name,
                    region,
                    (item.get("addr") or "").strip() or None,
                    lat,
                    lon,
                    lon,
                    lat,
                ),
            )
            upserted += 1
        cur.execute(
            """
            WITH mapped AS (
                SELECT
                    s.id,
                    r.code AS sigungu_code,
                    r.parent_code AS sido_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.id
                        ORDER BY ST_Area(r.geom) ASC, r.code ASC
                    ) AS rank
                FROM air.stations s
                JOIN air.admin_regions r
                  ON r.level='sigungu'
                 AND s.provider='AIRKOREA'
                 AND s.geom IS NOT NULL
                 AND ST_Covers(r.geom, s.geom::geometry)
            )
            UPDATE air.stations s
            SET sido_code=m.sido_code,
                sigungu_code=m.sigungu_code
            FROM mapped m
            WHERE s.id=m.id AND m.rank=1
            """
        )
    conn.commit()
    return upserted, skipped_without_coordinates, skipped_outside_region


def main():
    conn = get_db_connection()
    ensure_usage_table(conn)
    succeeded_regions = 0
    total_upserted = 0
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
                    payload = fetch_region_stations(
                        conn, region, decode_key=(attempt == 1)
                    )
                    items = (
                        payload.get("response", {})
                        .get("body", {})
                        .get("items")
                        or []
                    )
                    (
                        upserted,
                        missing_coordinates,
                        outside_region,
                    ) = upsert_stations(conn, region, items)
                    succeeded_regions += 1
                    total_upserted += upserted
                    print(
                        f"AIRKOREA station sync OK: region={region}, "
                        f"stations={upserted}, "
                        f"missing_coordinates={missing_coordinates}, "
                        f"outside_region={outside_region}"
                    )
                    last_error = None
                    break
                except Exception as exc:
                    conn.rollback()
                    last_error = exc
            if last_error is not None:
                print(
                    f"AIRKOREA station sync error: region={region}, "
                    "attempts=2, sync failed"
                )
    finally:
        conn.close()

    if succeeded_regions == 0:
        raise RuntimeError("AirKorea station sync failed for all regions")
    print(
        f"AIRKOREA station sync completed: regions={succeeded_regions}, "
        f"stations={total_upserted}"
    )
    return total_upserted


if __name__ == "__main__":
    main()
