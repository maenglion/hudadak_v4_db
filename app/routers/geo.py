from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, timezone
_cache = {}
import os, httpx, psycopg2

geo_router = APIRouter(prefix="/geo", tags=["Geolocation"])
KAKAO_REST_KEY = os.getenv("KAKAO_REST_KEY")
KAKAO_BASE = "https://dapi.kakao.com/v2/local"


def _resolve_db_host():
    host = os.getenv("DBHOST") or os.getenv("INSTANCE_UNIX_SOCKET")
    if host:
        return host
    instance = (
        os.getenv("CLOUD_SQL_CONNECTION_NAME")
        or os.getenv("INSTANCE_CONNECTION_NAME")
    )
    return f"/cloudsql/{instance}" if instance else None


def _choose_sigungu_row(rows, query):
    compact_query = "".join((query or "").split())
    matching_rows = [
        row
        for row in rows
        if "".join((row[4] or "").split()) in compact_query
    ]
    if matching_rows:
        return max(
            matching_rows,
            key=lambda item: len("".join((item[4] or "").split())),
        )
    return rows[0]


def _legal_sigungu_code(document):
    address = (document or {}).get("address") or {}
    code = str(address.get("b_code") or "").strip()
    return code[:5] if len(code) >= 5 and code[:5].isdigit() else None


def _administrative_scope(lat, lon, query, fallback_sigungu_code=None):
    required = (
        _resolve_db_host(),
        os.getenv("DBNAME"),
        os.getenv("DBUSER"),
        os.getenv("DBPASS"),
    )
    if not all(required):
        return {}
    conn = None
    try:
        conn = psycopg2.connect(
            host=required[0],
            dbname=required[1],
            user=required[2],
            password=required[3],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH point AS (
                    SELECT ST_SetSRID(
                        ST_MakePoint(%s, %s), 4326
                    ) AS geom
                )
                SELECT
                    sido.code AS sido_code,
                    sido.full_name AS sido_name,
                    sigungu.code AS sigungu_code,
                    sigungu.full_name AS sigungu_name,
                    sigungu.name AS sigungu_short_name,
                    ST_Area(sigungu.geom) AS sigungu_area
                FROM air.admin_regions sigungu
                JOIN air.admin_regions sido
                  ON sido.level='sido'
                 AND sido.code=sigungu.parent_code
                CROSS JOIN point
                WHERE sigungu.level='sigungu'
                  AND ST_Covers(sigungu.geom, point.geom)
                ORDER BY ST_Area(sigungu.geom) ASC, sigungu.code ASC
                """,
                (lon, lat),
            )
            rows = cur.fetchall()
        if not rows and fallback_sigungu_code:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        sido.code AS sido_code,
                        sido.full_name AS sido_name,
                        sigungu.code AS sigungu_code,
                        sigungu.full_name AS sigungu_name,
                        sigungu.name AS sigungu_short_name,
                        ST_Area(sigungu.geom) AS sigungu_area
                    FROM air.admin_regions sigungu
                    JOIN air.admin_regions sido
                      ON sido.level='sido'
                     AND sido.code=sigungu.parent_code
                    WHERE sigungu.level='sigungu'
                      AND sigungu.code=%s
                    """,
                    (fallback_sigungu_code,),
                )
                rows = cur.fetchall()
        if not rows:
            return {}
        compact_query = "".join((query or "").split())
        row = _choose_sigungu_row(rows, query)
        (
            sido_code,
            sido_name,
            sigungu_code,
            sigungu_name,
            sigungu_short,
            _,
        ) = row
        compact_sido = "".join((sido_name or "").split())
        level = "sido" if compact_query == compact_sido else "sigungu"
        normalized_name = sido_name if level == "sido" else sigungu_name
        return {
            "region_level": level,
            "region_code": sido_code if level == "sido" else sigungu_code,
            "region_name": normalized_name,
            "normalized_region_name": normalized_name,
            "sido_code": sido_code,
            "sido_name": sido_name,
            "sigungu_code": sigungu_code,
            "sigungu_name": sigungu_name,
            "sigungu_short_name": sigungu_short,
        }
    except psycopg2.Error:
        return {}
    finally:
        if conn is not None:
            conn.close()

def _headers():
    if not KAKAO_REST_KEY:
        raise HTTPException(status_code=500, detail="KAKAO_REST_KEY not configured.")
    return {"Authorization": f"KakaoAK {KAKAO_REST_KEY}"}

def _get_cache(key):
    v = _cache.get(key)
    if not v: return None
    data, exp = v
    if datetime.now(timezone.utc) > exp:
        _cache.pop(key, None); return None
    return data

def _set_cache(key, data, ttl=300):  # 5분 캐시
    _cache[key] = (data, datetime.now(timezone.utc) + timedelta(seconds=ttl))

@geo_router.get("/address")
async def address(q: str = Query(..., min_length=2)):
    # ✅ 5분 캐시 (키: 검색어)
    ck = ("addr", q.strip())
    hit = _get_cache(ck)
    if hit:
        return hit

    url = f"{KAKAO_BASE}/search/address.json"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=3.0)) as c:
            r = await c.get(
                url,
                params={"query": q, "page": 1, "size": 10, "analyze_type": "similar"},
                headers=_headers()
            )
            r.raise_for_status()
    except httpx.HTTPStatusError as e:
        if r.status_code == 401: raise HTTPException(502, "kakao auth failed")
        if r.status_code == 429: raise HTTPException(503, "kakao rate limited")
        raise HTTPException(502, f"kakao error {r.status_code}")
    except httpx.RequestError as e:
        raise HTTPException(502, f"kakao network error: {e!s}")

    data = r.json()
    doc = (data.get("documents") or [None])[0]
    if not doc:
        raise HTTPException(404, f"No results for: {q}")

    # Kakao: x=lon, y=lat
    x = float(doc.get("x") or (doc.get("address") or {}).get("x"))
    y = float(doc.get("y") or (doc.get("address") or {}).get("y"))
    addr = doc.get("address_name") or (doc.get("address") or {}).get("address_name")

    scope = _administrative_scope(
        y,
        x,
        q,
        fallback_sigungu_code=_legal_sigungu_code(doc),
    )
    resp = {
        "lat": y,
        "lon": x,
        "address": addr,
        "source": "kakao",
        **scope,
    }
    _set_cache(ck, resp, ttl=300)  # ✅ 캐시 저장
    return resp

@geo_router.get("/reverse")
async def reverse(lat: float, lon: float):
    # ✅ 5분 캐시 (키: 좌표를 1e-5으로 라운딩)
    ck = ("rev", round(lat, 5), round(lon, 5))
    hit = _get_cache(ck)
    if hit:
        return hit

    url = f"{KAKAO_BASE}/geo/coord2address.json"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=3.0)) as c:
            r = await c.get(
                url,
                params={"y": lat, "x": lon},  # Kakao: y=lat, x=lon
                headers=_headers()
            )
            r.raise_for_status()
    except httpx.HTTPStatusError as e:
        if r.status_code == 401: raise HTTPException(502, "kakao auth failed")
        if r.status_code == 429: raise HTTPException(503, "kakao rate limited")
        raise HTTPException(502, f"kakao error {r.status_code}")
    except httpx.RequestError as e:
        raise HTTPException(502, f"kakao network error: {e!s}")

    docs = r.json().get("documents") or []
    if not docs:
        raise HTTPException(404, "No address for coords")
    a = docs[0].get("road_address") or docs[0].get("address") or {}
    addr = a.get("address_name") or f"{lat},{lon}"

    scope = _administrative_scope(
        lat,
        lon,
        addr,
        fallback_sigungu_code=_legal_sigungu_code(docs[0]),
    )
    resp = {
        "lat": lat,
        "lon": lon,
        "address": addr,
        "source": "kakao",
        **scope,
    }
    _set_cache(ck, resp, ttl=300)  # ✅ 캐시 저장
    return resp
