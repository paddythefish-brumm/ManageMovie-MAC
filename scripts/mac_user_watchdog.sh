#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_ROOT="${MANAGEMOVIE_DATA_ROOT:-$BASE_DIR/MovieManager}"
LOG_DIR="${DATA_ROOT}/logs"
LOG_FILE="${LOG_DIR}/cron-watchdog.log"
STDOUT_FILE="${LOG_DIR}/cron-watchdog.stdout.log"
STDERR_FILE="${LOG_DIR}/cron-watchdog.stderr.log"
APP_MATCH="${BASE_DIR}/managemovie-web/web/app.py"

LAN_IF="$(route -n get default 2>/dev/null | awk '/interface:/{print $2; exit}')"
if [ -z "$LAN_IF" ]; then
  LAN_IF="en0"
fi

LAN_IP="$(ipconfig getifaddr "$LAN_IF" 2>/dev/null || true)"
if [ -z "$LAN_IP" ] && [ "$LAN_IF" = "en0" ]; then
  LAN_IP="$(ipconfig getifaddr en1 2>/dev/null || true)"
fi
if [ -z "$LAN_IP" ]; then
  LAN_IP="127.0.0.1"
fi

STATE_URL="https://${LAN_IP}:8126/api/state"

mkdir -p "$LOG_DIR"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  printf '[%s] [watchdog] %s\n' "$(timestamp)" "$*" >>"$LOG_FILE"
}

if pgrep -f "$APP_MATCH" >/dev/null 2>&1; then
  exit 0
fi

if curl -kfsS --max-time 3 "$STATE_URL" >/dev/null 2>&1; then
  exit 0
fi

log "app nicht aktiv, starte neu"
MANAGEMOVIE_WEB_BIND="${MANAGEMOVIE_WEB_BIND:-0.0.0.0}" \
MANAGEMOVIE_WEB_PORT="${MANAGEMOVIE_WEB_PORT:-8126}" \
MANAGEMOVIE_WEB_TLS="${MANAGEMOVIE_WEB_TLS:-1}" \
MANAGEMOVIE_PROJECT_ROOT="$BASE_DIR" \
MANAGEMOVIE_APP_STDOUT="$STDOUT_FILE" \
MANAGEMOVIE_APP_STDERR="$STDERR_FILE" \
/usr/bin/python3 - <<'PY'
import os
import subprocess

project_root = os.environ["MANAGEMOVIE_PROJECT_ROOT"]
stdout_path = os.environ["MANAGEMOVIE_APP_STDOUT"]
stderr_path = os.environ["MANAGEMOVIE_APP_STDERR"]

with open("/dev/null", "rb") as devnull, open(stdout_path, "ab", buffering=0) as stdout_handle, open(stderr_path, "ab", buffering=0) as stderr_handle:
    subprocess.Popen(
        ["/bin/bash", "./start.sh"],
        cwd=project_root,
        stdin=devnull,
        stdout=stdout_handle,
        stderr=stderr_handle,
        close_fds=True,
        start_new_session=True,
    )
PY

sleep 2
if curl -kfsS --max-time 3 "$STATE_URL" >/dev/null 2>&1; then
  log "restart erfolgreich"
else
  log "restart fehlgeschlagen"
fi
