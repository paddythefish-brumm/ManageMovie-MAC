#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${BASE_DIR}/MovieManager/logs"
LOG_FILE="${LOG_DIR}/mac-boot-smoke.log"

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

STATE_URL_LOCAL="https://${LAN_IP}:8126/api/state"

mkdir -p "$LOG_DIR"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

{
  echo "[$(timestamp)] [boot-smoke] start"
  echo "[$(timestamp)] [boot-smoke] host=$(hostname) cwd=${BASE_DIR} lan_ip=${LAN_IP}"

  for _ in $(seq 1 30); do
    if curl -kfsS --max-time 3 "$STATE_URL_LOCAL" >/tmp/managemovie-boot-state.json 2>/dev/null; then
      break
    fi
    sleep 2
  done

  if [ -f /tmp/managemovie-boot-state.json ]; then
    python3 - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("/tmp/managemovie-boot-state.json").read_text(encoding="utf-8"))
version = payload.get("versioning", {}).get("current", "-")
running = payload.get("job", {}).get("running")
print(f"[boot-smoke] local api ok current={version} running={running}")
PY
    rm -f /tmp/managemovie-boot-state.json
  else
    echo "[$(timestamp)] [boot-smoke] local api failed"
  fi

  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP -sTCP:LISTEN | egrep '(:8126 )|COMMAND' || true
  fi
  echo "[$(timestamp)] [boot-smoke] end"
} >>"$LOG_FILE" 2>&1
