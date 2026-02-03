# --- DB 연결 설정 (Cloud SQL 소켓 방식) ---
import os, psycopg2
DBNAME = os.getenv("DBNAME", "hudadak_air")
DBUSER = os.getenv("DBUSER", "hudadak_admin")
DBHOST = os.getenv("DBHOST", "/cloudsql/hudadak-air:asia-northeast3:hudadak-2025")
DBPASS = os.getenv("DBPASS")
# --- 여기까지 ---

import os, json, requests, psycopg2
from datetime import datetime, timezone
DBHOST,DBNAME,DBUSER,DBPASS=[os.getenv(k) for k in("DBHOST","DBNAME","DBUSER","DBPASS")]
OWM=os.getenv("OWM_API_KEY"); assert OWM, "Set OWM_API_KEY"
TARGETS = [
    {"name":"OWM Seoul(37.5665,126.9780)","lat":37.5665,"lon":126.9780,"city":"Seoul","country":"KR"},
    {"name":"OWM Incheon(37.4563,126.7052)","lat":37.4563,"lon":126.7052,"city":"Incheon","country":"KR"},
    {"name":"OWM Suwon(37.2636,127.0286)","lat":37.2636,"lon":127.0286,"city":"Suwon","country":"KR"},
    {"name":"OWM Uijeongbu(37.7381,127.0337)","lat":37.7381,"lon":127.0337,"city":"Uijeongbu","country":"KR"},
    {"name":"OWM Chuncheon(37.8813,127.7298)","lat":37.8813,"lon":127.7298,"city":"Chuncheon","country":"KR"},
    {"name":"OWM Gangneung(37.7519,128.8761)","lat":37.7519,"lon":128.8761,"city":"Gangneung","country":"KR"},
    {"name":"OWM Daejeon(36.3504,127.3845)","lat":36.3504,"lon":127.3845,"city":"Daejeon","country":"KR"},
    {"name":"OWM Cheongju(36.6424,127.4890)","lat":36.6424,"lon":127.4890,"city":"Cheongju","country":"KR"},
    {"name":"OWM Jeonju(35.8242,127.1479)","lat":35.8242,"lon":127.1479,"city":"Jeonju","country":"KR"},
    {"name":"OWM Gwangju(35.1595,126.8526)","lat":35.1595,"lon":126.8526,"city":"Gwangju","country":"KR"},
    {"name":"OWM Daegu(35.8714,128.6014)","lat":35.8714,"lon":128.6014,"city":"Daegu","country":"KR"},
    {"name":"OWM Ulsan(35.5384,129.3114)","lat":35.5384,"lon":129.3114,"city":"Ulsan","country":"KR"},
    {"name":"OWM Busan(35.1796,129.0756)","lat":35.1796,"lon":129.0756,"city":"Busan","country":"KR"},
    {"name":"OWM Pohang(36.0190,129.3435)","lat":36.0190,"lon":129.3435,"city":"Pohang","country":"KR"},
    {"name":"OWM Gyeongju(35.8562,129.2247)","lat":35.8562,"lon":129.2247,"city":"Gyeongju","country":"KR"},
    {"name":"OWM Jeju(33.4996,126.5312)","lat":33.4996,"lon":126.5312,"city":"Jeju","country":"KR"}
]
BASE="https://api.openweathermap.org/data/2.5/air_pollution"
def upsert_source_and_station(c,t):
    c.execute("""INSERT INTO air.sources(code,name,base_url,kind)
                 VALUES ('owm','OpenWeatherMap Air Pollution','https://openweathermap.org/api/air-pollution','model')
                 ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, base_url=EXCLUDED.base_url, kind=EXCLUDED.kind""")
    c.execute("""INSERT INTO air.stations(external_code,name,provider,kind,city,country,lat,lon,source_id,grid_res_km)
                 VALUES (%s,%s,'OWM','grid_point',%s,%s,%s,%s,(SELECT id FROM air.sources WHERE code='owm'),5)
                 ON CONFLICT (provider, external_code) DO NOTHING""",
              (f"OWM_{t['lat']}_{t['lon']}", t["name"], t.get("city"), t.get("country"), t["lat"], t["lon"]))
    c.execute("SELECT id FROM air.stations WHERE provider='OWM' AND name=%s",(t["name"],)); return c.fetchone()[0]
def upsert_measure(c,sid,ts,pm10,pm25,raw):
    c.execute("""INSERT INTO air.measurements(station_id,ts,pm10,pm25,raw,source_id,source_quality,unit_pm10,unit_pm25,aqi_provider)
                 VALUES (%s,%s,%s,%s,%s::jsonb,(SELECT id FROM air.sources WHERE code='owm'),'model','µg/m³','µg/m³','OWM')
                 ON CONFLICT (station_id,ts) DO UPDATE SET pm10=EXCLUDED.pm10, pm25=EXCLUDED.pm25, raw=EXCLUDED.raw, source_id=EXCLUDED.source_id""",
              (sid, ts, pm10, pm25, json.dumps(raw, ensure_ascii=False)))
def entries(j):
    for it in j.get("list", []):
        dt=datetime.fromtimestamp(it["dt"], tz=timezone.utc).replace(tzinfo=None)
        comp=it.get("components",{}); yield dt, comp.get("pm10"), comp.get("pm2_5"), it
def fetch_and_ingest(t):
    cur=requests.get(BASE, params={"lat":t["lat"],"lon":t["lon"],"appid":OWM}, timeout=20).json()
    try: fc=requests.get(BASE+"/forecast", params={"lat":t["lat"],"lon":t["lon"],"appid":OWM}, timeout=20).json()
    except Exception: fc={"list":[]}
    conn=psycopg2.connect(host=DBHOST,dbname=DBNAME,user=DBUSER,password=DBPASS); c=conn.cursor()
    sid=upsert_source_and_station(c,t)
    for ts,pm10,pm25,raw in entries(cur): upsert_measure(c,sid,ts,pm10,pm25,raw)
    for ts,pm10,pm25,raw in entries(fc):  upsert_measure(c,sid,ts,pm10,pm25,raw)
    conn.commit(); c.close(); conn.close()
def main():
    for t in TARGETS: fetch_and_ingest(t)
    print("OWM OK")
if __name__=="__main__": main()
