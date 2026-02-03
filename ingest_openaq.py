# --- DB 연결 설정 (Cloud SQL 소켓 방식) ---
import os, psycopg2
DBNAME = os.getenv("DBNAME", "hudadak_air")
DBUSER = os.getenv("DBUSER", "hudadak_admin")
DBHOST = os.getenv("DBHOST", "/cloudsql/hudadak-air:asia-northeast3:hudadak-2025")
DBPASS = os.getenv("DBPASS")
# --- 여기까지 ---

import os, requests, psycopg2, json, datetime as dt
DBHOST,DBNAME,DBUSER,DBPASS=[os.getenv(k) for k in("DBHOST","DBNAME","DBUSER","DBPASS")]
TOK=os.getenv("OPENAQ_TOKEN"); hdr={"X-API-Key":TOK} if TOK else {}
def main():
    u="https://api.openaq.org/v3/measurements"
    params={"country":"KR","parameter":["pm25","pm10"],"limit":100,"order_by":"datetime"}
    j=requests.get(u, params=params, headers=hdr, timeout=25).json()
    conn=psycopg2.connect(host=DBHOST,dbname=DBNAME,user=DBUSER,password=DBPASS); c=conn.cursor()
    c.execute("""INSERT INTO air.sources(code,name,base_url,kind) VALUES ('openaq','OpenAQ','https://openaq.org','aggregate')
                 ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, base_url=EXCLUDED.base_url, kind=EXCLUDED.kind""")
    for r in j.get("results",[]):
        st=(r.get("location") or "unknown"); city=(r.get("city") or "KR")
        lat=r.get("coordinates",{}).get("latitude"); lon=r.get("coordinates",{}).get("longitude")
        ts=dt.datetime.fromisoformat(r["date"]["utc"].replace("Z","+00:00"))
        val=r["value"]; param=r["parameter"]
        c.execute("""INSERT INTO air.stations(external_code,name,provider,kind,city,country,lat,lon,source_id)
                     VALUES (%s,%s,'OPENAQ','station',%s,'KR',%s,%s,(SELECT id FROM air.sources WHERE code='openaq'))
                     ON CONFLICT (provider, external_code) DO NOTHING""",(f"OPENAQ_{st}",st,city,lat,lon))
        c.execute("SELECT id FROM air.stations WHERE provider='OPENAQ' AND name=%s",(st,)); got=c.fetchone()
        if not got: continue
        sid=got[0]; pm10=val if param=="pm10" else None; pm25=val if param in ("pm25","pm2.5") else None
        c.execute("""INSERT INTO air.measurements(station_id,ts,pm10,pm25,raw,source_id,source_quality,unit_pm10,unit_pm25,aqi_provider)
                     VALUES (%s,%s,%s,%s,%s::jsonb,(SELECT id FROM air.sources WHERE code='openaq'),'aggregate','µg/m³','µg/m³','OpenAQ')
                     ON CONFLICT (station_id,ts) DO UPDATE SET
                       pm10=COALESCE(EXCLUDED.pm10, air.measurements.pm10),
                       pm25=COALESCE(EXCLUDED.pm25, air.measurements.pm25),
                       raw =COALESCE(EXCLUDED.raw,  air.measurements.raw)""",(sid,ts,pm10,pm25,json.dumps(r)))
    conn.commit(); c.close(); conn.close(); print("OpenAQ OK")
if __name__=="__main__": main()
