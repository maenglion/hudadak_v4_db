# --- DB 연결 설정 (Cloud SQL 소켓 방식) ---
import os, psycopg2
DBNAME = os.getenv("DBNAME", "hudadak_air")
DBUSER = os.getenv("DBUSER", "hudadak_admin")
DBHOST = os.getenv("DBHOST", "/cloudsql/hudadak-air:asia-northeast3:hudadak-2025")
DBPASS = os.getenv("DBPASS")
# --- 여기까지 ---

import os, sys, csv, json
from datetime import datetime
import psycopg2

DBHOST=os.environ['DBHOST']; DBNAME=os.environ['DBNAME']
DBUSER=os.environ['DBUSER']; DBPASS=os.environ['DBPASS']

# 컬럼명 후보(대/소문자, 한글/영문 섞여도 대응)
CAND = {
    "station": {"측정소명","지점명","station","station_name","측정소"},
    "datetime": {"측정일시","일시","date","datetime","dataTime"},
    "pm10": {"pm10","PM10","미세먼지","미세먼지(pm10)"},
    "pm25": {"pm25","PM2.5","초미세먼지","초미세먼지(pm2.5)"},
    "g10": {"pm10grade","PM10_GRADE","pm10_grade","등급","pm10등급"},
    "g25": {"pm25grade","PM25_GRADE","pm25_grade","pm25등급"}
}

def pick(row, keys):
    for k in row.keys():
        kl = k.strip().lower()
        if kl in keys or k in keys:
            return row[k].strip()
    return ""

def to_int(s):
    s = (s or "").strip()
    if s in {"", "-", "NA", "null"}: return None
    # KMA가 "20㎍/m3"처럼 줄 때도 있음
    for ch in ["㎍/m3","㎍/㎥","ug/m3","μg/m³"]:
        s = s.replace(ch,"")
    s = s.replace(",","")
    try: return int(float(s))
    except: return None

def parse_ts(s):
    s=(s or "").strip()
    # 흔한 포맷들 대응
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d %H", "%Y/%m/%d %H"):
        try:
            return datetime.strptime(s, fmt)
        except:
            pass
    # 못 맞추면 None
    return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python ingest_kma_csv.py <CSV_PATH>", file=sys.stderr)
        sys.exit(2)
    path = sys.argv[1]

    
    conn.autocommit=False
    cur = conn.cursor()

    # KMA 소스 id
    cur.execute("SELECT id FROM air.sources WHERE code='kma_temp'")
    src = cur.fetchone()
    if not src:
        print("kma_temp source not found; insert it first.", file=sys.stderr)
        conn.rollback(); sys.exit(3)
    source_id = src[0]

    # CSV 읽기
    with open(path, newline='', encoding='utf-8-sig') as f:
        rdr = csv.DictReader(f)
        inserted = updated = skipped = 0
        for row in rdr:
            st_name = pick(row, {x.lower() for x in CAND["station"]})
            dtime   = pick(row, {x.lower() for x in CAND["datetime"]})
            pm10    = to_int(pick(row, {x.lower() for x in CAND["pm10"]}))
            pm25    = to_int(pick(row, {x.lower() for x in CAND["pm25"]}))
            g10     = to_int(pick(row, {x.lower() for x in CAND["g10"]}))
            g25     = to_int(pick(row, {x.lower() for x in CAND["g25"]}))
            ts      = parse_ts(dtime)
            if not st_name or not ts:
                skipped += 1
                continue

            # 스테이션 upsert (KMA 제공명이 곧 name)
            cur.execute("""
                INSERT INTO air.stations(external_code, name, provider, source_id)
                VALUES (%s, %s, 'KMA', %s)
                ON CONFLICT (provider, external_code) DO NOTHING;
            """, (f'KMA_{st_name}', st_name, source_id))

            cur.execute("SELECT id FROM air.stations WHERE provider='KMA' AND name=%s LIMIT 1;", (st_name,))
            r = cur.fetchone()
            if not r:
                skipped += 1
                continue
            station_id = r[0]

            # measurements upsert
            cur.execute("""
                INSERT INTO air.measurements(station_id, ts, pm10, pm25, pm10_grade, pm25_grade, raw, source_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (station_id, ts) DO UPDATE
                SET pm10=EXCLUDED.pm10, pm25=EXCLUDED.pm25,
                    pm10_grade=EXCLUDED.pm10_grade, pm25_grade=EXCLUDED.pm25_grade,
                    raw=EXCLUDED.raw, source_id=EXCLUDED.source_id;
            """, (station_id, ts, pm10, pm25, g10, g25, json.dumps(row, ensure_ascii=False), source_id))
            if cur.rowcount == 1:
                inserted += 1
            else:
                updated += 1

    conn.commit()
    cur.close(); conn.close()
    print(f"Ingest OK. inserted={inserted}, updated={updated}, skipped={skipped}")

conn = psycopg2.connect(host=DBHOST, dbname=DBNAME, user=DBUSER, password=DBPASS)

if __name__ == "__main__":
    main()
