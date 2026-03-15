#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/scripts/lib/common.sh"
mm_cd_project_root
mm_load_project_env
mm_activate_homebrew_path

if [ "$(uname -s)" != "Darwin" ]; then
  echo "Fehler: Dieses Skript ist nur fuer macOS gedacht." >&2
  exit 1
fi

PROJECT_ROOT="$(mm_project_root)"
DATA_ROOT="$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")"
APP_NAME="ManageMovie"
APP_PATH="/Applications/${APP_NAME}.app"
LEGACY_APP_PATH="/Applications/MaMo.app"
APP_SERVICE_NAME="${MANAGEMOVIE_LAUNCHDAEMON_NAME:-com.${USER}.managemovie.web}"
PF_SERVICE_NAME="${MANAGEMOVIE_PF_LAUNCHDAEMON_NAME:-${APP_SERVICE_NAME}.pf}"
WATCHDOG_SERVICE_NAME="${APP_SERVICE_NAME}.watchdog"
ADMIN_HELPER_PATH="${MANAGEMOVIE_MAC_ADMIN_HELPER:-/usr/local/sbin/managemovie-mac-admin}"
SUDOERS_PATH="${MANAGEMOVIE_MAC_ADMIN_SUDOERS:-/etc/sudoers.d/managemovie-mac-admin}"
USER_LAUNCHAGENT_WATCHDOG="${HOME}/Library/LaunchAgents/${APP_SERVICE_NAME}.user-watchdog.plist"
USER_LAUNCHAGENT_APP="${HOME}/Library/LaunchAgents/${APP_SERVICE_NAME}.user-app.plist"
LEGACY_USER_WATCHDOG="${HOME}/Library/LaunchAgents/com.${USER}.managemovie.web.user-watchdog.plist"
LEGACY_USER_APP="${HOME}/Library/LaunchAgents/com.${USER}.managemovie.web.user-app.plist"
ROOT_LOG_DIR="/usr/local/var/log/managemovie"
USER_LOG_DIR="${HOME}/Library/Logs/MovieManager"
CRON_TAG="# MANAGEMOVIE_WATCHDOG"
CRON_REBOOT_TAG="# MANAGEMOVIE_WATCHDOG_REBOOT"
DB_HOST="${MANAGEMOVIE_DB_HOST:-127.0.0.1}"
DB_PORT="${MANAGEMOVIE_DB_PORT:-3306}"
DB_NAME="${MANAGEMOVIE_DB_NAME:-managemovie}"
DB_USER="${MANAGEMOVIE_DB_USER:-managemovie}"
DB_APP_HOST="${MANAGEMOVIE_DB_APP_HOST:-localhost}"
ROOT_USER="${MANAGEMOVIE_DB_ROOT_USER:-root}"
ROOT_PASS="${MANAGEMOVIE_DB_ROOT_PASSWORD:-}"
CURRENT_USER="$(id -un)"
MYSQL_SOCKET="${MANAGEMOVIE_DB_SOCKET:-/tmp/mysql.sock}"
MYSQL_ADMIN_CLIENT=()

build_app_hosts() {
  local primary="$1"
  printf '%s\n' "$primary"
  if [ "$primary" = "localhost" ]; then
    printf '%s\n' "127.0.0.1"
  elif [ "$primary" = "127.0.0.1" ]; then
    printf '%s\n' "localhost"
  fi
}

detect_mysql_admin_client() {
  if ! command -v mysql >/dev/null 2>&1; then
    return 1
  fi

  local probe_sql="SELECT 1;"
  local candidate=()
  if [ -S "$MYSQL_SOCKET" ]; then
    for user in "$CURRENT_USER" "$ROOT_USER"; do
      candidate=(mysql --protocol=socket -S "$MYSQL_SOCKET" -u "$user")
      if [ "$user" = "$ROOT_USER" ] && [ -n "$ROOT_PASS" ]; then
        candidate+=(-p"$ROOT_PASS")
      fi
      if "${candidate[@]}" -Nse "$probe_sql" >/dev/null 2>&1; then
        MYSQL_ADMIN_CLIENT=("${candidate[@]}")
        return 0
      fi
    done
  fi

  for user in "$ROOT_USER" "$CURRENT_USER"; do
    candidate=(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$user")
    if [ "$user" = "$ROOT_USER" ] && [ -n "$ROOT_PASS" ]; then
      candidate+=(-p"$ROOT_PASS")
    fi
    if "${candidate[@]}" -Nse "$probe_sql" >/dev/null 2>&1; then
      MYSQL_ADMIN_CLIENT=("${candidate[@]}")
      return 0
    fi
  done

  return 1
}

echo "[uninstall] Stoppe ManageMovie..."
if [ -x "$PROJECT_ROOT/stop.sh" ]; then
  "$PROJECT_ROOT/stop.sh" >/dev/null 2>&1 || true
fi
pkill -f "$PROJECT_ROOT/managemovie-web/web/app.py" >/dev/null 2>&1 || true
pkill -f '/ManageMovie-MAC/managemovie-web/web/app.py' >/dev/null 2>&1 || true
pkill -f '/MovieManager/managemovie-web/web/app.py' >/dev/null 2>&1 || true

echo "[uninstall] Entferne User-Autostart..."
launchctl bootout "gui/$(id -u)/${APP_SERVICE_NAME}.user-watchdog" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/${APP_SERVICE_NAME}.user-app" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/com.${USER}.managemovie.web.user-watchdog" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/com.${USER}.managemovie.web.user-app" >/dev/null 2>&1 || true
rm -f \
  "$USER_LAUNCHAGENT_WATCHDOG" \
  "$USER_LAUNCHAGENT_APP" \
  "$LEGACY_USER_WATCHDOG" \
  "$LEGACY_USER_APP"

echo "[uninstall] Entferne Cron-Watchdog..."
CURRENT_CRONTAB="$(mktemp "${TMPDIR:-/tmp}/managemovie-uninstall-crontab.XXXXXX")"
trap 'rm -f "$CURRENT_CRONTAB" "${CURRENT_CRONTAB}.next" "${CURRENT_CRONTAB}.final"' EXIT
crontab -l >"$CURRENT_CRONTAB" 2>/dev/null || true
grep -vF "$CRON_TAG" "$CURRENT_CRONTAB" > "${CURRENT_CRONTAB}.next" || true
grep -vF "$CRON_REBOOT_TAG" "${CURRENT_CRONTAB}.next" > "${CURRENT_CRONTAB}.final" || true
if [ -s "${CURRENT_CRONTAB}.final" ]; then
  crontab "${CURRENT_CRONTAB}.final"
else
  crontab -r >/dev/null 2>&1 || true
fi

echo "[uninstall] Entferne Root-Komponenten..."
if [ -x "$ADMIN_HELPER_PATH" ]; then
  sudo "$ADMIN_HELPER_PATH" cleanup-pf "$APP_SERVICE_NAME" "$PF_SERVICE_NAME" >/dev/null 2>&1 || true
  sudo "$ADMIN_HELPER_PATH" cleanup-root-cron >/dev/null 2>&1 || true
  sudo "$ADMIN_HELPER_PATH" cleanup-legacy "com.${USER}.managemovie.web" "/Library/LaunchDaemons/com.${USER}.managemovie.web.plist" >/dev/null 2>&1 || true
fi
sudo launchctl bootout system/"${WATCHDOG_SERVICE_NAME}" >/dev/null 2>&1 || true
sudo launchctl disable system/"${WATCHDOG_SERVICE_NAME}" >/dev/null 2>&1 || true
sudo rm -f \
  "/Library/LaunchDaemons/${WATCHDOG_SERVICE_NAME}.plist" \
  "/Library/LaunchDaemons/${PF_SERVICE_NAME}.plist" \
  "/Library/LaunchDaemons/com.${USER}.managemovie.web.plist" \
  "/Library/LaunchDaemons/com.paddy.managemovie.pf.plist" \
  "$ADMIN_HELPER_PATH" \
  "$SUDOERS_PATH"
sudo rm -rf "$ROOT_LOG_DIR"
sudo rm -rf /etc/pf.anchors/managemovie
if [ -f /etc/pf.conf ]; then
  PF_TMP="$(mktemp "/tmp/managemovie-pf.XXXXXX")"
  python3 - /etc/pf.conf "$PF_TMP" <<'PY'
from pathlib import Path
import sys

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
filtered = []
for line in src.read_text(encoding="utf-8").splitlines():
    stripped = line.strip()
    if stripped == 'rdr-anchor "managemovie/*"':
        continue
    if stripped.startswith('load anchor "managemovie/'):
        continue
    filtered.append(line)
dst.write_text("\n".join(filtered) + "\n", encoding="utf-8")
PY
  sudo cp "$PF_TMP" /etc/pf.conf
  sudo rm -f "$PF_TMP"
  sudo /sbin/pfctl -f /etc/pf.conf >/dev/null 2>&1 || true
fi

echo "[uninstall] Entferne DB, Settings und API-Keys..."
if command -v mysql >/dev/null 2>&1; then
  if detect_mysql_admin_client; then
    "${MYSQL_ADMIN_CLIENT[@]}" <<SQL >/dev/null 2>&1 || true
DROP DATABASE IF EXISTS \`${DB_NAME}\`;
SQL
    while IFS= read -r app_host; do
      [ -n "$app_host" ] || continue
      "${MYSQL_ADMIN_CLIENT[@]}" <<SQL >/dev/null 2>&1 || true
DROP USER IF EXISTS '${DB_USER}'@'${app_host}';
SQL
    done < <(build_app_hosts "$DB_APP_HOST")
  fi
fi

echo "[uninstall] Entferne lokale Brew-Komponenten..."
if command -v brew >/dev/null 2>&1; then
  brew services stop mariadb >/dev/null 2>&1 || true
  brew uninstall --force mariadb >/dev/null 2>&1 || true
  brew uninstall --force ffmpeg >/dev/null 2>&1 || true
fi

echo "[uninstall] Entferne App-Bundle..."
rm -rf "$APP_PATH" "$LEGACY_APP_PATH"

echo "[uninstall] Entferne Daten, venv und Projekt..."
rm -rf "$DATA_ROOT" "$PROJECT_ROOT/.venv" "$USER_LOG_DIR"

PROJECT_PARENT="$(dirname "$PROJECT_ROOT")"
PROJECT_BASENAME="$(basename "$PROJECT_ROOT")"
(
  sleep 1
  rm -rf "$PROJECT_ROOT"
) >/dev/null 2>&1 &

echo "[uninstall] Fertig."
echo "[uninstall] Entfernt: App, Autostart, Root-Helper, Daten und Projekt."
echo "[uninstall] Wenn Finder/Dock noch alte Eintraege zeigen, einmal neu oeffnen oder den Dock-Eintrag entfernen."
cd "$PROJECT_PARENT" || true
