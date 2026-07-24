FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements-ingest.txt .
RUN pip install --no-cache-dir -r requirements-ingest.txt

COPY ingest-all.sh cleanup_measurements.py ingest_*.py /app/

ENTRYPOINT ["/bin/bash", "/app/ingest-all.sh"]
