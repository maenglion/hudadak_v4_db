# --- DB 연결 설정 (Cloud SQL 소켓 방식) ---
import os, psycopg2
DBNAME = os.getenv("DBNAME", "hudadak_air")
DBUSER = os.getenv("DBUSER", "hudadak_admin")
DBHOST = os.getenv("DBHOST", "/cloudsql/hudadak-air:asia-northeast3:hudadak-2025")
DBPASS = os.getenv("DBPASS")
# --- 여기까지 ---

import os, csv, io, requests, psycopg2, json
from psycopg2.extras import execute_values
from datetime import datetime

# --- 여기부터 복사 ---

DBHOST = os.getenv("DBHOST")
DBNAME = os.getenv("DBNAME")
DBUSER = os.getenv("DBUSER")
DBPASS = os.getenv("DBPASS")
KEY = os.getenv("FIRMS_MAP_KEY")
assert DBHOST and DBNAME and DBUSER and DBPASS, "DB* env not set"
assert KEY, "Set FIRMS_MAP_KEY"

# 영역(bbox) API (키 필요) + 공개 24h CSV(폴백)
BBOX     = (126.3, 36.9, 127.8, 38.2)
SENSOR   = "VIIRS_SNPP"
DAYS     = "1"
AREA_URL   = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{KEY}/{SENSOR}/{DAYS}/{BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]}"
PUBLIC_URL = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv"

def fetch_csv(url: str) -> csv.DictReader:
    t = requests.get(url, timeout=60)
    t.raise_for_status()
    return csv.DictReader(io.StringIO(t.text))

# DB 연결 및 source upsert
conn = psycopg2.connect(host=DBHOST, dbname=DBNAME, user=DBUSER, password=DBPASS, connect_timeout=10)
conn.autocommit = True
c = conn.cursor()
c.execute("""
INSERT INTO air.sources(code,name,base_url,kind)
VALUES ('nasa_firms','NASA FIRMS','https://firms.modaps.eosdis.nasa.gov','satellite')
ON CONFLICT (code) DO UPDATE
SET name=EXCLUDED.name, base_url=EXCLUDED.base_url, kind=EXCLUDED.kind
""")

# 피드 가져오기: bbox 우선, 실패/스키마 불일치 시 공개 CSV로 폴백
try:
    r = fetch_csv(AREA_URL)
    f = r.fieldnames or []
    if ("acq_date" not in f) or ("acq_time" not in f):
        raise KeyError("acq_date/acq_time not in AREA feed; fallback")
except Exception:
    r = fetch_csv(PUBLIC_URL)

# 행 파싱 및 INSERT (Batch 방식)
rows = []
for row in r:
    # 날짜/시간 키 유연 매핑
    date = row.get("acq_date") or row.get("acquisition_date") or row.get("date")
    time = row.get("acq_time") or row.get("acquisition_time") or row.get("acq_time_utc") or row.get("time")
    if not (date and time):
        continue

    when = f"{date} {time}"
    ts = None
    for fmt in ("%Y-%m-%d %H%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            ts = datetime.strptime(when, fmt)
            break
        except ValueError:
            pass
    if ts is None:
        continue

    # 좌표
    try:
        lat = float(row["latitude"])
        lon = float(row["longitude"])
    except Exception:
        continue

    sat  = row.get("satellite") or "VIIRS"
    conf = row.get("confidence") or row.get("confidence_text")
    frp  = row.get("frp")
    frp  = None if frp in (None, "", "NA") else float(frp)

    # 데이터를 리스트에 추가 (DB에 바로 INSERT 하지 않음)
    rows.append((ts, lat, lon, sat, conf, frp, json.dumps(row)))

# 반복문이 끝난 후, 모아둔 데이터를 DB에 한 번에 INSERT
if rows:
    execute_values(c, """
        INSERT INTO air.fires(detected_at, lat, lon, satellite, confidence, frp, raw, source_code)
        VALUES %s
    """, rows, template="(%s,%s,%s,%s,%s,%s,%s,'nasa_firms')")
    print(f"FIRMS OK: {len(rows)} rows inserted.")

c.execute("DELETE FROM air.fires WHERE source_code='nasa_firms' AND detected_at < now() - interval '15 days'")
print("FIRMS retention: deleted", c.rowcount, "old rows")

conn.close()

# --- 여기까지 복사 ---
