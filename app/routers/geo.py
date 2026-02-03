from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, timezone
_cache = {}
import os, httpx

geo_router = APIRouter(prefix="/geo", tags=["Geolocation"])
KAKAO_REST_KEY = os.getenv("KAKAO_REST_KEY")
KAKAO_BASE = "https://dapi.kakao.com/v2/local"

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

    resp = {"lat": y, "lon": x, "address": addr, "source": "kakao"}
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

    resp = {"lat": lat, "lon": lon, "address": addr, "source": "kakao"}
    _set_cache(ck, resp, ttl=300)  # ✅ 캐시 저장
    return resp
