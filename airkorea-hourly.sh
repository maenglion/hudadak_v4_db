#!/usr/bin/env bash
set -uo pipefail

status=0
if ! python /app/ingest_airkorea.py; then
  echo "AIRKOREA error: hourly collection failed" >&2
  status=1
fi

if [[ "${RUN_RETENTION_CLEANUP:-false}" == "true" ]]; then
  if ! python /app/cleanup_measurements.py; then
    echo "retention error: cleanup failed; collected data remains committed" >&2
  fi
fi

exit "${status}"
