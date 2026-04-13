#!/usr/bin/env bash
set -e

source /opt/pipeline/.venv/bin/activate

exec uvicorn api.app:app \
  --app-dir /opt/pipeline \
  --host 0.0.0.0 \
  --port 18000
