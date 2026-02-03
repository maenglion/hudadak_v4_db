# --- DB 연결 설정 (Cloud SQL 소켓 방식) ---
import os, psycopg2
DBNAME = os.getenv("DBNAME", "hudadak_air")
DBUSER = os.getenv("DBUSER", "hudadak_admin")
DBHOST = os.getenv("DBHOST", "/cloudsql/hudadak-air:asia-northeast3:hudadak-2025")
DBPASS = os.getenv("DBPASS")
# --- 여기까지 ---

import os, csv, io, requests, psycopg2, json
from datetime import datetime
DBHOST,DBNAME,DBUSER,DBPASS=[os.getenv(k) for k in("DBHOST","DBNAME","DBUSER","DBPASS")]
KEY=os.getenv("FIRMS_MAP_KEY"); assert KEY, "Set FIRMS_MAP_KEY"
bbox=(126.3,36.9,127.8,38.2); sensor="VIIRS_SNPP"; days="1"
url=f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{KEY}/{sensor}/{days}/{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
t=requests.get(url, timeout=30); t.raise_for_status()
conn=psycopg2.connect(host=DBHOST,dbname=DBNAME,user=DBUSER,password=DBPASS); c=conn.cursor()
c.execute("""INSERT INTO air.sources(code,name,base_url,kind) VALUES ('nasa_firms','NASA FIRMS','https://firms.modaps.eosdis.nasa.gov','satellite')
             ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, base_url=EXCLUDED.base_url, kind=EXCLUDED.kind""")
r=csv.DictReader(io.StringIO(t.text))
for row in r:
    when=row["acq_date"]+" "+row["acq_time"]; ts=datetime.strptime(when,"%Y-%m-%d %H%M")
    lat=float(row["latitude"]); lon=float(row["longitude"])
    sat=row.get("satellite") or "VIIRS"; conf=row.get("confidence") or row.get("confidence_text")
    frp=row.get("frp"); frp=None if frp in ("","NA") else float(frp)
    c.execute("""INSERT INTO air.fires(detected_at,lat,lon,satellite,confidence,frp,raw,source_code)
                 VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,'nasa_firms')""",(ts,lat,lon,sat,conf,frp,json.dumps(row)))
conn.commit(); c.close(); conn.close(); print("FIRMS OK")
