#!/usr/bin/env python3
"""Import official NGII sigungu boundaries and map stations spatially."""

import csv
import os
from pathlib import Path

from airkorea_common import get_db_connection


SOURCE_NAME = "국토교통부 국토지리정보원_공간정보공동활용_시군구"
SOURCE_DATE = "2023-09-15"
SIDO_NAMES = {
    "11": "서울특별시",
    "26": "부산광역시",
    "27": "대구광역시",
    "28": "인천광역시",
    "29": "광주광역시",
    "30": "대전광역시",
    "31": "울산광역시",
    "36": "세종특별자치시",
    "41": "경기도",
    "42": "강원특별자치도",
    "43": "충청북도",
    "44": "충청남도",
    "45": "전북특별자치도",
    "46": "전라남도",
    "47": "경상북도",
    "48": "경상남도",
    "50": "제주특별자치도",
}

COMPOSITE_CITIES = {
    "41110": ("41", "\uc218\uc6d0\uc2dc", "4111_"),
    "41130": ("41", "\uc131\ub0a8\uc2dc", "4113_"),
    "41170": ("41", "\uc548\uc591\uc2dc", "4117_"),
    "41270": ("41", "\uc548\uc0b0\uc2dc", "4127_"),
    "41280": ("41", "\uace0\uc591\uc2dc", "4128_"),
    "41460": ("41", "\uc6a9\uc778\uc2dc", "4146_"),
    "43110": ("43", "\uccad\uc8fc\uc2dc", "4311_"),
    "44130": ("44", "\ucc9c\uc548\uc2dc", "4413_"),
    "45110": ("45", "\uc804\uc8fc\uc2dc", "4511_"),
    "47110": ("47", "\ud3ec\ud56d\uc2dc", "4711_"),
    "48120": ("48", "\ucc3d\uc6d0\uc2dc", "4812_"),
}


def read_sigungu_rows(path):
    csv.field_size_limit(2_147_483_647)
    with Path(path).open(encoding="cp949", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        if len(header) != 6:
            raise ValueError("Unexpected NGII sigungu CSV column count")
        for row in reader:
            if len(row) != 6:
                continue
            code = row[1].strip()
            name = row[2].strip()
            wkb_hex = row[5].strip()
            sido_code = code[:2]
            sido_name = SIDO_NAMES.get(sido_code)
            if len(code) != 5 or not sido_name or not name or not wkb_hex:
                continue
            yield {
                "code": code,
                "name": name,
                "full_name": f"{sido_name} {name}",
                "parent_code": sido_code,
                "wkb_hex": wkb_hex,
            }


def import_boundaries(conn, rows):
    source_features = 0
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE admin_region_stage (
                code text NOT NULL,
                name text NOT NULL,
                full_name text NOT NULL,
                parent_code text NOT NULL,
                geom geometry(MultiPolygon, 4326) NOT NULL
            ) ON COMMIT DROP
            """
        )
        for row in rows:
            cur.execute(
                """
                INSERT INTO admin_region_stage(
                    code, name, full_name, parent_code, geom
                )
                VALUES (
                    %s, %s, %s, %s,
                    ST_Multi(ST_CollectionExtract(ST_MakeValid(
                        ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)
                    ), 3))
                )
                """,
                (
                    row["code"],
                    row["name"],
                    row["full_name"],
                    row["parent_code"],
                    row["wkb_hex"],
                ),
            )
            source_features += 1

        cur.execute(
            """
            INSERT INTO air.admin_regions(
                code, level, name, full_name, parent_code, geom,
                source_name, source_date, imported_at
            )
            SELECT
                code,
                'sigungu',
                MAX(name),
                MAX(full_name),
                MAX(parent_code),
                ST_Multi(ST_UnaryUnion(ST_Collect(geom))),
                %s,
                %s,
                CURRENT_TIMESTAMP
            FROM admin_region_stage
            GROUP BY code
            ON CONFLICT (code) DO UPDATE SET
                level=EXCLUDED.level,
                name=EXCLUDED.name,
                full_name=EXCLUDED.full_name,
                parent_code=EXCLUDED.parent_code,
                geom=EXCLUDED.geom,
                source_name=EXCLUDED.source_name,
                source_date=EXCLUDED.source_date,
                imported_at=CURRENT_TIMESTAMP
            """,
            (SOURCE_NAME, SOURCE_DATE),
        )
        imported_regions = cur.rowcount
    conn.commit()
    return source_features, imported_regions


def rebuild_sido_boundaries(conn):
    with conn.cursor() as cur:
        for code, name in SIDO_NAMES.items():
            cur.execute(
                """
                INSERT INTO air.admin_regions(
                    code, level, name, full_name, parent_code, geom,
                    source_name, source_date, imported_at
                )
                SELECT
                    %s, 'sido', %s, %s, NULL,
                    ST_Multi(ST_UnaryUnion(ST_Collect(geom))),
                    %s, %s, CURRENT_TIMESTAMP
                FROM air.admin_regions
                WHERE level='sigungu' AND parent_code=%s
                HAVING COUNT(*) > 0
                ON CONFLICT (code) DO UPDATE SET
                    level=EXCLUDED.level,
                    name=EXCLUDED.name,
                    full_name=EXCLUDED.full_name,
                    parent_code=NULL,
                    geom=EXCLUDED.geom,
                    source_name=EXCLUDED.source_name,
                    source_date=EXCLUDED.source_date,
                    imported_at=CURRENT_TIMESTAMP
                """,
                (code, name, name, SOURCE_NAME, SOURCE_DATE, code),
            )
    conn.commit()


def rebuild_composite_city_boundaries(conn):
    """Union official child-gu polygons into their official parent-city code."""
    with conn.cursor() as cur:
        for code, (sido_code, name, child_pattern) in COMPOSITE_CITIES.items():
            cur.execute(
                """
                INSERT INTO air.admin_regions(
                    code, level, name, full_name, parent_code, geom,
                    source_name, source_date, imported_at
                )
                SELECT
                    %s, 'sigungu', %s, %s, %s,
                    ST_Multi(ST_UnaryUnion(ST_Collect(geom))),
                    %s, %s, CURRENT_TIMESTAMP
                FROM air.admin_regions
                WHERE level='sigungu'
                  AND code LIKE %s
                  AND code <> %s
                HAVING COUNT(*) > 0
                ON CONFLICT (code) DO UPDATE SET
                    level=EXCLUDED.level,
                    name=EXCLUDED.name,
                    full_name=EXCLUDED.full_name,
                    parent_code=EXCLUDED.parent_code,
                    geom=EXCLUDED.geom,
                    source_name=EXCLUDED.source_name,
                    source_date=EXCLUDED.source_date,
                    imported_at=CURRENT_TIMESTAMP
                """,
                (
                    code,
                    name,
                    f"{SIDO_NAMES[sido_code]} {name}",
                    sido_code,
                    SOURCE_NAME,
                    SOURCE_DATE,
                    child_pattern,
                    code,
                ),
            )
    conn.commit()


def map_stations(conn):
    with conn.cursor() as cur:
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
        mapped = cur.rowcount
        cur.execute(
            """
            SELECT COUNT(*)
            FROM air.stations
            WHERE UPPER(provider) IN ('WAQI', 'AIRKOREA')
              AND geom IS NOT NULL
              AND (sido_code IS NULL OR sigungu_code IS NULL)
            """
        )
        unmapped = cur.fetchone()[0]
    conn.commit()
    return mapped, unmapped


def main():
    boundary_file = os.environ.get("ADMIN_BOUNDARY_CSV")
    if not boundary_file:
        raise RuntimeError("ADMIN_BOUNDARY_CSV is required")
    conn = get_db_connection()
    try:
        source_features, imported = import_boundaries(
            conn, read_sigungu_rows(boundary_file)
        )
        rebuild_composite_city_boundaries(conn)
        rebuild_sido_boundaries(conn)
        mapped, unmapped = map_stations(conn)
    finally:
        conn.close()
    print(
        f"ADMIN boundary sync OK: source_features={source_features}, "
        f"sigungu={imported}, "
        f"stations_mapped={mapped}, stations_unmapped={unmapped}"
    )


if __name__ == "__main__":
    main()
