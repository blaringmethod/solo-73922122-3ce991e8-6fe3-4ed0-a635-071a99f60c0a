#!/bin/sh
# Role dispatcher:
#   web        -> FastAPI + uvicorn (also creates the live file)
#   collector  -> one collector process (COLLECTOR_ID set by compose)
set -eu

SHARED_DIR="${SHARED_DIR:-/data/shared}"
DB_PATH="${DB_PATH:-/data/warehouse/alarms.db}"

mkdir -p "$SHARED_DIR" "$SHARED_DIR/.reclaim" "$(dirname "$DB_PATH")"

case "${1:-web}" in
  web)
    exec uvicorn app.main:app --host 0.0.0.0 --port 8000
    ;;
  collector)
    exec python -m app.collector
    ;;
  *)
    exec "$@"
    ;;
esac
