gcloud config set project rhythme-1983
gcloud sql instances describe rhythme-postgres --format="value(connectionName)"
# --- DB 연결 설정 (Cloud SQL 소켓 방식) ---
import os, psycopg2
DBNAME = os.getenv("DBNAME", "hudadak_air")
DBUSER = os.getenv("DBUSER", "hudadak_admin")
DBHOST = os.getenv("DBHOST", "/cloudsql/hudadak-air:asia-northeast3:hudadak-2025")
DBPASS = os.getenv("DBPASS")
# --- 여기까지 ---

import os, json
from datetime import datetime, timezone
import requests, psycopg2

DBHOST=os.environ['DBHOST']; DBNAME=os.environ['DBNAME']
DBUSER=os.environ['DBUSER']; DBPASS=os.environ['DBPASS']
AIRKEY=os.environ.get('AIRKOREA_KEY','')

def to_int(x):
    try: return int(x)
    except: return None

def fetch_airkorea_latest(station_name):
    url="https://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty"
    params={
        "serviceKey": AIRKEY,
        "returnType":"json",
        "numOfRows":"1",
        "pageNo":"1",
        "stationName": station_name,
        "dataTerm":"DAILY",
        "ver":"1.3"
    }
    r=requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

conn = psycopg2.connect(host=DBHOST, dbname=DBNAME, user=DBUSER, password=DBPASS)
conn.autocommit=False
cur=conn.cursor()

# 소스 보장
cur.execute("""
INSERT INTO air.sources(code,name,base_url)
VALUES ('airkorea','AirKorea','https://api.airkorea.or.kr')
ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, base_url=EXCLUDED.base_url;
""")

# 스테이션 보장 (샘플: 송도신도시)
station_name="송도신도시"
cur.execute("""
INSERT INTO air.stations(external_code, name, provider, region_si, region_gu, region_dong, source_id)
VALUES ('STN_0001', %s, 'AirKorea', '인천','연수구','송도동',
        (SELECT id FROM air.sources WHERE code='airkorea'))
ON CONFLICT (provider, external_code) DO NOTHING;
""", (station_name,))
cur.execute("SELECT id FROM air.stations WHERE provider='AirKorea' AND name=%s LIMIT 1;", (station_name,))
station_id=cur.fetchone()[0]

# 데이터 취득 or 목업
if AIRKEY:
    j=fetch_airkorea_latest(station_name)
    items=j["response"]["body"]["items"]
    item=items[0]
    pm10=to_int(item.get("pm10Value")); pm25=to_int(item.get("pm25Value"))
    g10=to_int(item.get("pm10Grade"));  g25=to_int(item.get("pm25Grade"))
    ts_str=item.get("dataTime")  # "YYYY-MM-DD HH:MM"
    # 서버가 TIMESTAMPTZ이므로 naive로 넣어도 UTC 저장
    from datetime import datetime
    ts=datetime.strptime(ts_str,"%Y-%m-%d %H:%M")
    raw=json.dumps(item)
else:
    # 키 없으면 목업 1건
    ts=datetime.utcnow()
    pm10,pm25,g10,g25,raw=40,17,2,1,json.dumps({"mock":True})

cur.execute("""
INSERT INTO air.measurements(station_id, ts, pm10, pm25, pm10_grade, pm25_grade, raw, source_id)
VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,(SELECT id FROM air.sources WHERE code='airkorea'))
ON CONFLICT (station_id, ts) DO UPDATE
SET pm10=EXCLUDED.pm10, pm25=EXCLUDED.pm25, pm10_grade=EXCLUDED.pm10_grade,
    pm25_grade=EXCLUDED.pm25_grade, raw=EXCLUDED.raw;
""",(station_id, ts, pm10, pm25, g10, g25, raw))

conn.commit()
cur.close(); conn.close()
print("Ingest OK.")
