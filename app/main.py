# main.py
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Query, APIRouter, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor
import psycopg2
from datetime import datetime, timedelta, timezone
import os, asyncio, httpx
from routers import geo_router

# --- FastAPI 앱 ---
app = FastAPI(title="Hudadak Air API", version="1.1")

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://maenglion.github.io",
        "https://app-hudadak.netlify.app",
    ],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)
# ==============
#  메모리 캐시
# ==============
_cache: Dict[Any, Any] = {}

def _cache_get(key):
    item = _cache.get(key)
    if not item:
        return None
    val, exp = item
    if datetime.now(timezone.utc) > exp:
        _cache.pop(key, None)
        return None
    return val

def _cache_set(key, val, ttl_sec=120):
    _cache[key] = (val, datetime.now(timezone.utc) + timedelta(seconds=ttl_sec))

# =====================================
#  공통: DB 연결 (Cloud SQL / TCP 모두)
# =====================================
def _resolve_db_host() -> Optional[str]:
    host = os.getenv("DBHOST") or os.getenv("INSTANCE_UNIX_SOCKET")
    if host:
        return host
    inst = (
        os.getenv("CLOUD_SQL_CONNECTION_NAME")
        or os.getenv("INSTANCE_CONNECTION_NAME")
        or os.getenv("CLOUDSQL_INSTANCE")
        or os.getenv("SQL_INSTANCE")
        or os.getenv("DB_INSTANCE")
        or os.getenv("GOOGLE_CLOUD_SQL_INSTANCE")
        or os.getenv("INSTANCE")
    )
    return f"/cloudsql/{inst}" if inst else None

def get_db_connection():
    host = _resolve_db_host()
    name = os.getenv("DBNAME")
    user = os.getenv("DBUSER")
    pwd  = os.getenv("DBPASS")

    print("[DBCFG]", {"host": host, "dbname": name, "user": user, "pwd": bool(pwd)})

    if not all([host, name, user, pwd]):
        return None
    try:
        return psycopg2.connect(
            host=host, dbname=name, user=user, password=pwd, connect_timeout=5
        )
    except Exception as e:
        print("🔥 DATABASE CONNECTION FAILED:", e)
        return None

# ================
#  시간/등급 유틸
# ================
def _now_kst_floor_hour() -> datetime:
    now_utc = datetime.now(timezone.utc)
    kst = now_utc + timedelta(hours=9)
    return kst.replace(minute=0, second=0, microsecond=0)

def _kr_grade_from_pm(pm10: Optional[float], pm25: Optional[float]) -> Optional[int]:
    if pm10 is None and pm25 is None:
        return None
    g10 = 1 if (pm10 is not None and pm10 <= 30) else 2 if (pm10 is not None and pm10 <= 80) else 3 if (pm10 is not None and pm10 <= 150) else 4
    g25 = 1 if (pm25 is not None and pm25 <= 15) else 2 if (pm25 is not None and pm25 <= 35) else 3 if (pm25 is not None and pm25 <= 75) else 4
    if pm10 is None: return g25
    if pm25 is None: return g10
    return max(g10, g25)

# ======================
#  Badge (간단 규칙 샘플)
# ======================
def generate_badges(air: dict) -> List[str]:
    badges: List[str] = []
    if not air:
        return badges
    kind = (air.get("station") or {}).get("kind") or air.get("source_kind") or "unknown"
    if kind == "airkorea_station":
        badges.append("국내 측정소")
    elif kind == "model":
        badges.append("위성/모델 분석")
    pm10 = air.get("pm10") or 0
    pm25 = air.get("pm25") or 0
    if pm10 > 150:
        badges.append("⚠️ 황사 유입")
    if pm25 > 75:
        badges.append("🚨 초미세먼지 심화")
    return badges

app.include_router(geo_router) 

# =======================================
#  Open-Meteo 호출 유틸
# =======================================
OPEN_METEO_AQ = "https://air-quality-api.open-meteo.com/v1/air-quality"
WEATHER_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# 오염물질 키
POLLUTANT_KEYS = [
    "pm2_5", "pm10",
    "ozone",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "carbon_monoxide",
]

# 바람/강수 키
MET_KEYS = [
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
]

async def fetch_openmeteo(lat: float, lon: float, hourly_keys: List[str]) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(hourly_keys),
        "timezone": "Asia/Seoul",
    }
    timeout = httpx.Timeout(15.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(OPEN_METEO_AQ, params=params)
        if r.status_code >= 400:
            try:
                err = r.json()
            except Exception:
                err = {"status_code": r.status_code, "text": r.text[:300]}
            raise HTTPException(status_code=502, detail={"provider": "open-meteo", "error": err})
        return r.json()

async def fetch_weather(lat: float, lon: float, hourly_keys: List[str]) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(hourly_keys),
        "timezone": "Asia/Seoul",
    }
    timeout = httpx.Timeout(15.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(WEATHER_FORECAST_URL, params=params)
        if r.status_code >= 400:
            try:
                err = r.json()
            except Exception:
                err = {"status_code": r.status_code, "text": r.text[:300]}
            raise HTTPException(status_code=502, detail={"provider": "open-meteo-weather", "error": err})
        return r.json()

# 캐시 래퍼
async def cached_fetch_openmeteo(lat, lon, keys):
    ck = ("aq", round(lat,3), round(lon,3), ",".join(keys))
    hit = _cache_get(ck)
    if hit: return hit
    data = await fetch_openmeteo(lat, lon, keys)
    _cache_set(ck, data, 120)
    return data

async def cached_fetch_weather(lat, lon, keys):
    ck = ("wx", round(lat,3), round(lon,3), ",".join(keys))
    hit = _cache_get(ck)
    if hit: return hit
    data = await fetch_weather(lat, lon, keys)
    _cache_set(ck, data, 120)
    return data

def _select_latest_index(times: List[str]) -> Optional[int]:
    if not times:
        return None
    kst_hour = _now_kst_floor_hour().isoformat(timespec="minutes")
    candidate = [i for i, t in enumerate(times) if t <= kst_hour]
    if candidate:
        return candidate[-1]
    return 0

def _pick_latest(aq_json: Dict[str, Any]) -> Dict[str, Any]:
    h = aq_json.get("hourly", {}) if aq_json else {}
    times: List[str] = h.get("time") or []
    idx = _select_latest_index(times)
    if idx is None:
        return {"display_ts": None, "pm10": None, "pm25": None, "o3": None, "no2": None, "so2": None, "co": None}

    def pick(key: str):
        arr = h.get(key) or []
        return arr[idx] if idx < len(arr) else None

    return {
        "display_ts": times[idx] if times else None,
        "pm10": pick("pm10"),
        "pm25": pick("pm2_5"),
        "o3":  pick("ozone"),
        "no2": pick("nitrogen_dioxide"),
        "so2": pick("sulphur_dioxide"),
        "co":  pick("carbon_monoxide"),
    }

# =======================================
#  /nearest : DB 우선 → Open-Meteo 폴백
# =======================================
# 기존
# @app.get("/nearest")
# async def nearest(lat: float, lon: float):

# 교체
@app.get("/nearest")
async def nearest(
    lat: float,
    lon: float,
    source: str = Query("db", pattern="^(db|model|auto)$")  # 기본: db
):
    conn = get_db_connection()
    if conn:
        try:
            q = """
            WITH target AS (
              SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS g
            )
            SELECT
              s.id as station_id,
              s.name,
              s.provider,
              s.kind,
              s.lat, s.lon,
              ST_Distance(s.geom, (SELECT g FROM target)) AS distance_m,
              dl.pm10, dl.pm25,
              dl.unit_pm10, dl.unit_pm25,
              dl.display_ts
            FROM air.stations s
            JOIN air.dashboard_latest dl ON dl.station_id = s.id
            ORDER BY ST_Distance(s.geom, (SELECT g FROM target)) ASC
            LIMIT 1;
            """
            with conn.cursor() as cur:
                cur.execute(q, (lon, lat))
                row = cur.fetchone()
                if row and not isinstance(row, dict):
                    cols = [d[0] for d in cur.description]
                    row = dict(zip(cols, row))
            if row:
                result = {
                    "provider": row.get("provider") or "AIRKOREA",
                    "name": row.get("name"),
                    "station_id": row.get("station_id"),
                    "display_ts": row.get("display_ts"),
                    "pm10": row.get("pm10"),
                    "pm25": row.get("pm25"),
                    "unit_pm10": row.get("unit_pm10") or "µg/m³",
                    "unit_pm25": row.get("unit_pm25") or "µg/m³",
                    "o3": None, "no2": None, "so2": None, "co": None,
                    "source_kind": row.get("kind") or "airkorea_station",
                    "lat": row.get("lat"), "lon": row.get("lon"),
                    "station": {
                        "name": row.get("name"),
                        "provider": row.get("provider"),
                        "kind": row.get("kind"),
                    },
                    "source": "db"  # ← 명시
                }
                result["cai_grade"] = _kr_grade_from_pm(result["pm10"], result["pm25"])
                result["badges"] = generate_badges(result)
                return result
        except Exception as e:
            print(f"[nearest] DB query failed → fallback: {e}")
        finally:
            try: conn.close()
            except: pass

    # 여기까지 왔다는 건: DB 연결 실패 또는 결과 없음
    if source == "db":
        # DB만 쓰기로 했으면 폴백 안 하고 '데이터 없음'으로 반환
        # (프런트에서 필요 시 모델로 별도 호출)
        raise HTTPException(status_code=204, detail="no db rows")

    # 폴백( model 또는 auto )
    aq = await cached_fetch_openmeteo(lat, lon, keys=POLLUTANT_KEYS)
    latest = _pick_latest(aq)
    return {
        "provider": "OPENMETEO",
        "name": f"OpenMeteo({round(lat,4)},{round(lon,4)})",
        "station_id": 0,
        "display_ts": (latest["display_ts"] + ":00") if (latest.get("display_ts") and len(latest["display_ts"]) == 16) else latest.get("display_ts"),
        "pm10": latest["pm10"],
        "pm25": latest["pm25"],
        "unit_pm10": "µg/m³",
        "unit_pm25": "µg/m³",
        "o3": latest["o3"],
        "no2": latest["no2"],
        "so2": latest["so2"],
        "co": latest["co"],
        "source_kind": "model",
        "lat": lat, "lon": lon,
        "station": {"name": "Open-Meteo", "provider": "OPENMETEO", "kind": "model"},
        "source": "model"  # ← 명시
    }

# ==========
#  루트/예보
# ==========
@app.get("/")
def root():
    return {"status": "ok", "message": "Welcome to Hudadak Air API"}

@app.get("/forecast")
async def forecast(
    lat: float = Query(37.57, description="위도"),
    lon: float = Query(126.98, description="경도"),
    horizon: int = Query(24, ge=6, le=120, description="예보 시간(시간 단위)")
):
    """
    공기질은 /v1/air-quality, 바람/강수는 /v1/forecast에서 받아 병합.
    두 응답 모두 timezone=Asia/Seoul 기준의 time 배열("YYYY-MM-DDTHH:MM") 사용.
    """
    # 병렬 호출 (캐시 사용)
    aq_task = cached_fetch_openmeteo(lat, lon, keys=POLLUTANT_KEYS)
    wx_task = cached_fetch_weather(lat, lon, keys=MET_KEYS)
    aq, wx = await asyncio.gather(aq_task, wx_task)

    ah = aq.get("hourly", {}) if aq else {}
    wh = wx.get("hourly", {}) if wx else {}
    times: List[str] = ah.get("time") or []
    if not times:
        raise HTTPException(status_code=502, detail="Open-Meteo air-quality hourly data empty")

    start_idx = _select_latest_index(times) or 0
    end_idx = min(start_idx + horizon, len(times))

    def a(name: str, i: int):
        arr = ah.get(name) or []
        return arr[i] if i < len(arr) else None

    def w(name: str, i: int):
        arr = wh.get(name) or []
        return arr[i] if i < len(arr) else None

    hourly = []
    for i in range(start_idx, end_idx):
        pm10 = a("pm10", i)
        pm25 = a("pm2_5", i)
        ts = times[i]
        if ts and len(ts) == 16:
            ts = ts + ":00"
        hourly.append({
            "ts": ts,                         # KST
            "pm10": pm10,
            "pm25": pm25,
            "grade": _kr_grade_from_pm(pm10, pm25),
            "conf": 0.8,
            "wind_dir": w("wind_direction_10m", i),
            "wind_spd": w("wind_speed_10m", i),
            "precip":  w("precipitation", i),
        })

    issued_ts = times[start_idx] + ":00" if (times[start_idx] and len(times[start_idx]) == 16) else times[start_idx]
    return {
        "station": {
            "id": f"openmeteo-{round(lat,2)},{round(lon,2)}",
            "name": "모델 예보 (Open-Meteo)",
            "distance_m": None
        },
        "horizon": f"{len(hourly)}h",
        "issued_at": issued_ts,
        "hourly": hourly,
        "model": {"type": "openmeteo_hourly+weather_merge", "version": "1.0.1", "mape": None}
    }

# ==============
#  헬스/캐치올
# ==============
@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"ok": True}

@app.get("/{splat:path}", include_in_schema=False)
def catch_all(splat: str):
    return {"status": "ok", "message": "Welcome to Hudadak Air API", "path": f"/{splat}"}
