#!/usr/bin/env sh
set -eu

APP_DIR="${APP_DIR:-/workspace}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
UVICORN_RELOAD="${UVICORN_RELOAD:-false}"

cd "$APP_DIR"

if [ ! -f "genome_manager/main.py" ]; then
  echo "ERROR: genome_manager/main.py not found under APP_DIR=$APP_DIR" >&2
  echo "Hint: mount your project directory to /workspace (or set APP_DIR)." >&2
  exit 1
fi

if [ "$UVICORN_RELOAD" = "true" ]; then
  exec uvicorn genome_manager.main:app --host "$HOST" --port "$PORT" --reload
fi

exec uvicorn genome_manager.main:app --host "$HOST" --port "$PORT"
