#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/scripts/lib/common.sh"
source "$(mm_join_project_path "scripts/lib/process.sh")"
mm_cd_project_root

docker_compose_mac() {
  docker compose -f "$(mm_join_project_path "docker-compose.mac.yml")" "$@"
}

set_restart_policy_manual() {
  if command -v docker >/dev/null 2>&1; then
    docker update --restart=no MaMo >/dev/null 2>&1 || true
  fi
}

wait_for_docker_desktop() {
  local sock="$HOME/.docker/run/docker.sock"
  local app="/Applications/Docker.app"
  if docker info >/dev/null 2>&1; then
    return 0
  fi
  if [ -d "$app" ]; then
    open -a "$app" >/dev/null 2>&1 || true
  fi
  for _ in $(seq 1 45); do
    if [ -S "$sock" ] && docker info >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

CLI_WEB_PORT="${MANAGEMOVIE_WEB_PORT-}"
CLI_WEB_BIND="${MANAGEMOVIE_WEB_BIND-}"
CLI_WEB_TLS="${MANAGEMOVIE_WEB_TLS-}"

mm_load_project_env
mm_activate_homebrew_path

RUNTIME_MODE="${MANAGEMOVIE_RUNTIME:-local}"

if [ -n "$CLI_WEB_PORT" ]; then
  export MANAGEMOVIE_WEB_PORT="$CLI_WEB_PORT"
fi
if [ -n "$CLI_WEB_BIND" ]; then
  export MANAGEMOVIE_WEB_BIND="$CLI_WEB_BIND"
fi
if [ -n "$CLI_WEB_TLS" ]; then
  export MANAGEMOVIE_WEB_TLS="$CLI_WEB_TLS"
fi

VENV_PY="$(mm_venv_python)"
if [ ! -x "$VENV_PY" ]; then
  echo "Bitte zuerst ausfuehren: ./setup.sh" >&2
  exit 1
fi

if ! "$VENV_PY" -c "import flask, pymysql, cryptography" >/dev/null 2>&1; then
  echo "[start] Fehlende Python-Abhaengigkeiten erkannt, repariere..." >&2
  "$VENV_PY" -m pip install -r "$(mm_join_project_path "requirements.txt")" >/dev/null
fi

if ! "$VENV_PY" -c "import flask, pymysql, cryptography" >/dev/null 2>&1; then
  echo "Fehler: Abhaengigkeiten fehlen weiterhin (flask/pymysql/cryptography). Bitte ./setup.sh ausfuehren." >&2
  exit 1
fi

DATA_ROOT="$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")"
export MANAGEMOVIE_DATA_ROOT="$DATA_ROOT"

if [ "$(uname -s)" = "Darwin" ] && [ "$RUNTIME_MODE" = "docker" ]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "Fehler: Docker fehlt." >&2
    exit 1
  fi
  if ! wait_for_docker_desktop; then
    echo "Fehler: Docker Desktop ist nicht erreichbar." >&2
    exit 1
  fi
  if ! docker compose version >/dev/null 2>&1; then
    echo "Fehler: docker compose fehlt." >&2
    exit 1
  fi

  export MANAGEMOVIE_WEB_PORT="${MANAGEMOVIE_WEB_PORT:-8126}"
  export MANAGEMOVIE_WEB_TLS="${MANAGEMOVIE_WEB_TLS:-1}"
  mm_ensure_data_layout "$DATA_ROOT"
  "$VENV_PY" "$(mm_join_project_path "scripts/mac/local_worker_daemon.py")" --restart-daemon >/dev/null 2>&1 || true

  MANAGEMOVIE_RUNTIME=local "$(mm_join_project_path "stop.sh")" >/dev/null 2>&1 || true

  docker_compose_mac up -d --build
  set_restart_policy_manual

  state_url="https://127.0.0.1:${MANAGEMOVIE_WEB_PORT}/api/state"
  for _ in $(seq 1 20); do
    if curl -kfsS --max-time 3 "$state_url" >/dev/null 2>&1; then
      echo "MaMo Docker läuft auf Port ${MANAGEMOVIE_WEB_PORT}."
      exit 0
    fi
    sleep 2
  done

  echo "Fehler: MaMo Docker startet nicht sauber." >&2
  docker_compose_mac ps >&2 || true
  docker_compose_mac logs --tail=80 >&2 || true
  exit 1
fi

if [ -z "${MANAGEMOVIE_DEFAULT_FOLDER:-}" ]; then
  export MANAGEMOVIE_DEFAULT_FOLDER="$(mm_detect_default_folder)"
fi

if [ -z "${MANAGEMOVIE_BROWSE_ROOT:-}" ]; then
  export MANAGEMOVIE_BROWSE_ROOT="/"
fi

export MANAGEMOVIE_WEB_PORT="${MANAGEMOVIE_WEB_PORT:-8126}"
export MANAGEMOVIE_WEB_BIND="${MANAGEMOVIE_WEB_BIND:-127.0.0.1}"
# Pure web app defaults: no macOS terminal windows, no analyze confirmation dialog.
export MANAGEMOVIE_WEB_UI_ONLY="${MANAGEMOVIE_WEB_UI_ONLY:-1}"
export MANAGEMOVIE_TERMINAL_UI="${MANAGEMOVIE_TERMINAL_UI:-0}"
export MANAGEMOVIE_AUTOSTART="${MANAGEMOVIE_AUTOSTART:-1}"
export MANAGEMOVIE_SKIP_CONFIRM="${MANAGEMOVIE_SKIP_CONFIRM:-1}"
export MANAGEMOVIE_FFMPEG_THREADS="${MANAGEMOVIE_FFMPEG_THREADS:-auto}"
export MANAGEMOVIE_COPY_CHUNK_MIB="${MANAGEMOVIE_COPY_CHUNK_MIB:-}"
export MANAGEMOVIE_RUNTIME_PROBE="${MANAGEMOVIE_RUNTIME_PROBE:-auto}"
export MANAGEMOVIE_ANALYZE_RUNTIME_PROBE="${MANAGEMOVIE_ANALYZE_RUNTIME_PROBE:-0}"
export MANAGEMOVIE_SSL_CERT="${MANAGEMOVIE_SSL_CERT:-$DATA_ROOT/certs/server/managemovie-local.crt}"
export MANAGEMOVIE_SSL_KEY="${MANAGEMOVIE_SSL_KEY:-$DATA_ROOT/certs/server/managemovie-local.key}"
export MANAGEMOVIE_DB_HOST="${MANAGEMOVIE_DB_HOST:-127.0.0.1}"
export MANAGEMOVIE_DB_PORT="${MANAGEMOVIE_DB_PORT:-3306}"
export MANAGEMOVIE_DB_NAME="${MANAGEMOVIE_DB_NAME:-managemovie}"
export MANAGEMOVIE_DB_USER="${MANAGEMOVIE_DB_USER:-managemovie}"
export MANAGEMOVIE_DB_PASSWORD="${MANAGEMOVIE_DB_PASSWORD:-}"
export MANAGEMOVIE_DB_RETENTION_DAYS="${MANAGEMOVIE_DB_RETENTION_DAYS:-365}"
APP_MARKER="$(mm_join_project_path "managemovie-web/web/app.py")"

if [ -z "${MANAGEMOVIE_DB_PASSWORD}" ]; then
  echo "Fehler: MANAGEMOVIE_DB_PASSWORD fehlt. Bitte in .env.local setzen." >&2
  exit 1
fi

if [ -z "${MANAGEMOVIE_STATE_CRYPT_KEY:-${MANAGEMOVIE_SETTINGS_CRYPT_KEY:-}}" ]; then
  echo "Fehler: MANAGEMOVIE_STATE_CRYPT_KEY fehlt. Bitte in .env.local setzen." >&2
  exit 1
fi

if [ "${MANAGEMOVIE_WEB_BIND}" != "127.0.0.1" ] && [ "${MANAGEMOVIE_WEB_BIND}" != "::1" ]; then
  if [ -z "${MANAGEMOVIE_WEB_USER:-}" ] && [ -z "${MANAGEMOVIE_WEB_PASSWORD:-}" ]; then
    echo "[start][warn] Externer Bind ohne Basic Auth. Setze MANAGEMOVIE_WEB_USER/MANAGEMOVIE_WEB_PASSWORD." >&2
  fi
fi

if [ -z "${MANAGEMOVIE_WEB_TLS:-}" ]; then
  if [ -f "$MANAGEMOVIE_SSL_CERT" ] && [ -f "$MANAGEMOVIE_SSL_KEY" ]; then
    export MANAGEMOVIE_WEB_TLS="1"
  else
    export MANAGEMOVIE_WEB_TLS="0"
  fi
fi

mm_ensure_data_layout "$DATA_ROOT"

existing_pids="$(mm_port_pids "${MANAGEMOVIE_WEB_PORT}")"
if [ -n "$existing_pids" ]; then
  state_scheme="http"
  if [ "${MANAGEMOVIE_WEB_TLS}" = "1" ]; then
    state_scheme="https"
  fi
  state_url="${state_scheme}://127.0.0.1:${MANAGEMOVIE_WEB_PORT}/api/state"
  if curl -kfsS --max-time 3 "$state_url" >/dev/null 2>&1; then
    echo "ManageMovie läuft bereits auf Port ${MANAGEMOVIE_WEB_PORT}."
    exit 0
  fi
  declare -a existing_app_pids=()
  for pid in $existing_pids; do
    if mm_is_managemovie_web_pid "$pid" "$APP_MARKER"; then
      existing_app_pids+=("$pid")
    fi
  done
  if [ "${#existing_app_pids[@]}" -gt 0 ]; then
    echo "ManageMovie läuft bereits auf Port ${MANAGEMOVIE_WEB_PORT}."
    exit 0
  fi
  echo "[start] Port ${MANAGEMOVIE_WEB_PORT} ist belegt, stoppe alten ManageMovie-Prozess..." >&2
  "$(mm_join_project_path "stop.sh")" >/dev/null 2>&1 || true
fi

exec "$VENV_PY" "$(mm_join_project_path "managemovie-web/web/app.py")"
