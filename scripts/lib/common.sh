#!/usr/bin/env bash

MM_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MM_PROJECT_ROOT="$(cd "$MM_LIB_DIR/../.." && pwd)"

mm_project_root() {
  printf '%s\n' "$MM_PROJECT_ROOT"
}

mm_load_project_env() {
  local env_file
  for env_file in "$MM_PROJECT_ROOT/.env.local" "$MM_PROJECT_ROOT/.env"; do
    if [ -f "$env_file" ]; then
      set -a
      # shellcheck disable=SC1090
      source "$env_file"
      set +a
    fi
  done
}

mm_activate_homebrew_path() {
  if [ "$(uname -s)" != "Darwin" ]; then
    return 0
  fi

  if [ -x /opt/homebrew/bin/brew ]; then
    export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:$PATH"
  elif [ -x /usr/local/bin/brew ]; then
    export PATH="/usr/local/bin:/usr/local/sbin:$PATH"
  fi
}

mm_cd_project_root() {
  cd "$MM_PROJECT_ROOT"
}

mm_join_project_path() {
  local rel="$1"
  rel="${rel#./}"
  printf '%s/%s\n' "$MM_PROJECT_ROOT" "$rel"
}

mm_normalize_data_root() {
  local raw="${1:-${MANAGEMOVIE_DATA_ROOT:-./MovieManager}}"
  local legacy_root="$MM_PROJECT_ROOT/MovieMaager"
  local normalized="$raw"

  if [ "${normalized##*/}" = "MovieMaager" ]; then
    normalized="${normalized%MovieMaager}MovieManager"
  fi

  if [ "${normalized#/}" = "$normalized" ]; then
    normalized="$(mm_join_project_path "$normalized")"
  fi

  if [ "${normalized##*/}" = "MovieManager" ] && [ ! -e "$normalized" ] && [ -e "$legacy_root" ]; then
    mv "$legacy_root" "$normalized"
  fi

  printf '%s\n' "$normalized"
}

mm_detect_default_folder() {
  if [ -d "/mnt/NFS/GK-Filer" ]; then
    printf '%s\n' "/mnt/NFS/GK-Filer"
    return
  fi
  if [ -d "/mnt" ]; then
    printf '%s\n' "/mnt"
    return
  fi
  printf '%s\n' "$HOME"
}

mm_ensure_data_layout() {
  local data_root="$1"
  mkdir -p \
    "$data_root/work" \
    "$data_root/temp" \
    "$data_root/logs" \
    "$data_root/certs/server" \
    "$data_root/certs/ca" \
    "$data_root/config"
}

mm_start_on_boot_flag_path() {
  local data_root="${1:-$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")}"
  printf '%s\n' "$data_root/config/start_on_boot.flag"
}

mm_read_start_on_boot_flag() {
  local data_root="${1:-$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")}"
  local flag_path
  flag_path="$(mm_start_on_boot_flag_path "$data_root")"

  if [ -f "$flag_path" ]; then
    tr -d '[:space:]' < "$flag_path" 2>/dev/null || printf '1'
    return 0
  fi
  printf '%s\n' "${MANAGEMOVIE_START_ON_BOOT:-1}"
}

mm_write_start_on_boot_flag() {
  local value="${1:-1}"
  local data_root="${2:-$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")}"
  local flag_path
  flag_path="$(mm_start_on_boot_flag_path "$data_root")"
  mkdir -p "$(dirname "$flag_path")"
  printf '%s\n' "$value" > "$flag_path"
}

mm_fix_runtime_permissions() {
  local data_root="$1"
  local service_user="${2:-${MANAGEMOVIE_SYSTEMD_USER:-managemovie-web}}"
  local service_group="${3:-${MANAGEMOVIE_SYSTEMD_GROUP:-$service_user}}"

  mkdir -p "$data_root/work" "$data_root/temp" "$data_root/logs"

  if ! id -u "$service_user" >/dev/null 2>&1; then
    return 0
  fi
  if ! getent group "$service_group" >/dev/null 2>&1; then
    return 0
  fi

  chown -R "$service_user:$service_group" \
    "$data_root/work" \
    "$data_root/temp" \
    "$data_root/logs"
  chmod -R u+rwX,go-rwx \
    "$data_root/work" \
    "$data_root/temp" \
    "$data_root/logs"
}

mm_init_state_files() {
  local data_root="$1"
  local _default_folder="$2"
  [ -f "$data_root/VERSION.current" ] || printf '1\n' > "$data_root/VERSION.current"
}

mm_seed_secret_file() {
  # Legacy no-op: API-Keys werden in MariaDB verwaltet.
  return 0
}

mm_seed_local_env() {
  local project_root="${1:-$MM_PROJECT_ROOT}"
  local env_local="$project_root/.env.local"

  if [ -f "$env_local" ]; then
    return 0
  fi

  local db_password=""
  local state_key=""
  local web_bind="0.0.0.0"
  local web_port="8126"
  local web_tls="1"
  local profile_label="ManageMovie Mac"

  if command -v openssl >/dev/null 2>&1; then
    db_password="$(openssl rand -hex 18)"
    state_key="$(openssl rand -hex 32)"
  else
    db_password="$(python3 - <<'PY'
import secrets
print(secrets.token_hex(18))
PY
)"
    state_key="$(python3 - <<'PY'
import secrets
print(secrets.token_hex(32))
PY
)"
  fi

  umask 077
  cat > "$env_local" <<EOF
# $profile_label
MANAGEMOVIE_DB_HOST='127.0.0.1'
MANAGEMOVIE_DB_PORT='3306'
MANAGEMOVIE_DB_NAME='managemovie'
MANAGEMOVIE_DB_USER='managemovie'
MANAGEMOVIE_DB_PASSWORD='${db_password}'
MANAGEMOVIE_DB_APP_HOST='localhost'
MANAGEMOVIE_WEB_BIND='${web_bind}'
MANAGEMOVIE_WEB_PORT='${web_port}'
MANAGEMOVIE_WEB_TLS='${web_tls}'
MANAGEMOVIE_START_ON_BOOT='1'
MANAGEMOVIE_STATE_CRYPT_KEY='${state_key}'
MANAGEMOVIE_SETTINGS_CRYPT_KEY='${state_key}'
EOF
}

mm_ensure_homebrew() {
  if [ "$(uname -s)" != "Darwin" ]; then
    return 0
  fi

  if command -v brew >/dev/null 2>&1; then
    return 0
  fi

  echo "[setup] Homebrew fehlt, installiere..."
  if [ -t 0 ] && [ -t 1 ]; then
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  else
    echo "Fehler: Homebrew fehlt und kann ohne interaktive sudo-Abfrage nicht installiert werden." >&2
    echo "Bitte setup.sh in einem interaktiven Terminal starten." >&2
    exit 1
  fi

  if [ -x /opt/homebrew/bin/brew ]; then
    mm_activate_homebrew_path
    eval "$(/opt/homebrew/bin/brew shellenv)"
  elif [ -x /usr/local/bin/brew ]; then
    mm_activate_homebrew_path
    eval "$(/usr/local/bin/brew shellenv)"
  fi

  if ! command -v brew >/dev/null 2>&1; then
    echo "Fehler: Homebrew konnte nicht installiert werden." >&2
    exit 1
  fi
}

mm_require_file() {
  local file_path="$1"
  local label="$2"
  if [ ! -f "$file_path" ]; then
    echo "Fehler: ${label} nicht gefunden unter $file_path" >&2
    exit 1
  fi
}

mm_venv_python() {
  printf '%s\n' "$MM_PROJECT_ROOT/.venv/bin/python"
}

mm_ensure_venv() {
  local venv_dir="$MM_PROJECT_ROOT/.venv"
  local venv_py="$venv_dir/bin/python"

  if [ ! -x "$venv_py" ]; then
    echo "[setup] Erstelle virtuelle Umgebung..."
    python3 -m venv "$venv_dir"
  fi

  if ! "$venv_py" -c "import pip" >/dev/null 2>&1; then
    echo "[setup] venv ist unvollstaendig (pip fehlt), erstelle neu..."
    rm -rf "$venv_dir"
    python3 -m venv "$venv_dir"
  fi

  "$venv_py" -m ensurepip --upgrade >/dev/null 2>&1 || true
}
