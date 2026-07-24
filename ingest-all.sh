#!/usr/bin/env bash
set -uo pipefail

core_success=0
core_failure=0

run_core() {
  local provider="$1"
  local script="$2"
  if python "/app/${script}"; then
    core_success=$((core_success + 1))
  else
    core_failure=$((core_failure + 1))
    echo "${provider} error: ingestion failed" >&2
  fi
}

run_optional() {
  local provider="$1"
  local script="$2"
  if ! python "/app/${script}"; then
    echo "${provider} warning: optional ingestion failed; continuing" >&2
  fi
}

echo "== air data ingestion started =="
run_optional "OWM" "ingest_owm.py"
run_core "WAQI" "ingest_waqi.py"
run_core "AIRKOREA" "ingest_airkorea.py"
run_optional "OPENAQ" "ingest_openaq.py"
run_optional "FIRMS" "ingest_firms.py"

if ! python /app/cleanup_measurements.py; then
  echo "retention error: cleanup failed; collected provider data remains committed" >&2
fi

if (( core_success == 0 )); then
  echo "ingestion failed: neither WAQI nor AIRKOREA completed successfully" >&2
  exit 1
fi

echo "== air data ingestion completed: core_success=${core_success}, core_failure=${core_failure} =="
