#!/usr/bin/env bash
set -euo pipefail

echo "== 데이터 인제스트 시작 =="
python /app/ingest_owm.py
python /app/ingest_waqi.py
python /app/ingest_openaq.py
python /app/ingest_owm.py
python /app/ingest_firms.py
echo "== 모든 작업 완료 =="
