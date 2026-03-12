#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/scripts/lib/common.sh"
mm_cd_project_root
mm_load_project_env

PROJECT_ROOT="$(mm_project_root)"
MAC_PUBLIC_MARKER="$(mm_join_project_path ".managemovie-mac-public")"
MAC_INSTALL_ROOT="${MANAGEMOVIE_MAC_INSTALL_ROOT:-/Applications/ManageMovie}"

if [ "$(uname -s)" = "Darwin" ] && [ -f "$MAC_PUBLIC_MARKER" ] && [ "$PROJECT_ROOT" != "$MAC_INSTALL_ROOT" ]; then
  echo "[setup] Installiere ManageMovie nach ${MAC_INSTALL_ROOT} ..."
  sudo mkdir -p "$MAC_INSTALL_ROOT"
  sudo rsync -a --delete \
    --exclude '.git/' \
    --exclude '.venv/' \
    --exclude 'MovieManager/' \
    "$PROJECT_ROOT/" "$MAC_INSTALL_ROOT/"
  sudo chown -R "$(id -un):staff" "$MAC_INSTALL_ROOT"
  exec /bin/bash "$MAC_INSTALL_ROOT/setup.sh"
fi

APP_WEB="$(mm_join_project_path "managemovie-web/web/app.py")"
mm_require_file "$APP_WEB" "App"

DATA_ROOT="$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")"
export MANAGEMOVIE_DATA_ROOT="$DATA_ROOT"

mm_ensure_data_layout "$DATA_ROOT"
default_folder="$(mm_detect_default_folder)"
mm_init_state_files "$DATA_ROOT" "$default_folder"
mm_seed_local_env "$(mm_project_root)"
mm_load_project_env
mm_activate_homebrew_path
mm_ensure_homebrew
if [ "$(uname -s)" = "Darwin" ]; then
  mm_activate_homebrew_path
  if ! command -v ffmpeg >/dev/null 2>&1 || ! command -v ffprobe >/dev/null 2>&1; then
    echo "[setup] ffmpeg fehlt, installiere..."
    brew install ffmpeg
    mm_activate_homebrew_path
  fi
fi
mm_seed_secret_file "$DATA_ROOT"
if [ "$(id -u)" -eq 0 ]; then
  mm_fix_runtime_permissions "$DATA_ROOT"
fi

mm_ensure_venv
VENV_PY="$(mm_venv_python)"

"$VENV_PY" -m pip install --upgrade pip
"$VENV_PY" -m pip install -r "$(mm_join_project_path "requirements.txt")"

if ! "$VENV_PY" -c "import flask" >/dev/null 2>&1; then
  echo "[setup] Flask fehlt nach Installation, repariere..."
  "$VENV_PY" -m pip install --upgrade "flask>=3.0,<4"
fi

echo "Setup abgeschlossen: $(mm_project_root)"
echo "Datenpfad: $DATA_ROOT"
echo "MariaDB (einmalig): ./setup_mariadb.sh"
