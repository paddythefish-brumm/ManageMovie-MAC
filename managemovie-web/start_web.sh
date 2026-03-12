#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")/.." && pwd)/scripts/lib/common.sh"
mm_cd_project_root

PROJECT_DIR="$(mm_project_root)"
mkdir -p "$PROJECT_DIR/logs" "$PROJECT_DIR/work" "$PROJECT_DIR/temp"

if [ -z "${MANAGEMOVIE_DATA_ROOT:-}" ]; then
  export MANAGEMOVIE_DATA_ROOT="$(mm_normalize_data_root "./MovieManager")"
fi
export MANAGEMOVIE_WEB_UI_ONLY="${MANAGEMOVIE_WEB_UI_ONLY:-1}"
export MANAGEMOVIE_TERMINAL_UI="${MANAGEMOVIE_TERMINAL_UI:-0}"
export MANAGEMOVIE_AUTOSTART="${MANAGEMOVIE_AUTOSTART:-1}"
export MANAGEMOVIE_SKIP_CONFIRM="${MANAGEMOVIE_SKIP_CONFIRM:-1}"
export MANAGEMOVIE_FFMPEG_THREADS="${MANAGEMOVIE_FFMPEG_THREADS:-auto}"
export MANAGEMOVIE_COPY_CHUNK_MIB="${MANAGEMOVIE_COPY_CHUNK_MIB:-}"
export MANAGEMOVIE_RUNTIME_PROBE="${MANAGEMOVIE_RUNTIME_PROBE:-auto}"
export MANAGEMOVIE_ANALYZE_RUNTIME_PROBE="${MANAGEMOVIE_ANALYZE_RUNTIME_PROBE:-0}"

VENV_PY="$(mm_venv_python)"
if [ -x "$VENV_PY" ]; then
  exec "$VENV_PY" "$(mm_join_project_path "managemovie-web/web/app.py")"
fi
exec python3 "$(mm_join_project_path "managemovie-web/web/app.py")"
