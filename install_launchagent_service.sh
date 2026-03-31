#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/scripts/lib/common.sh"
mm_cd_project_root
mm_load_project_env

if [ "$(uname -s)" != "Darwin" ]; then
  echo "Fehler: Dieses Skript ist nur fuer macOS gedacht." >&2
  exit 1
fi

PROJECT_ROOT="$(mm_project_root)"
DATA_ROOT="$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")"
WATCHDOG_PATH="$(mm_join_project_path "scripts/mac_user_watchdog.sh")"
LOG_DIR="${DATA_ROOT}/logs"
USER_LOG_DIR="${HOME}/Library/Logs/MovieManager"
CRON_TAG="# MANAGEMOVIE_WATCHDOG"
CRON_REBOOT_TAG="# MANAGEMOVIE_WATCHDOG_REBOOT"
CRON_LINE="* * * * * cd ${PROJECT_ROOT} && /bin/sh ${WATCHDOG_PATH} >/dev/null 2>&1 ${CRON_TAG}"
CRON_REBOOT_LINE="@reboot cd ${PROJECT_ROOT} && /bin/sh ${WATCHDOG_PATH} >/dev/null 2>&1 ${CRON_REBOOT_TAG}"

mkdir -p "$LOG_DIR"
mkdir -p "$USER_LOG_DIR"

if [ ! -x "$WATCHDOG_PATH" ]; then
  echo "Fehler: Watchdog-Skript fehlt unter $WATCHDOG_PATH" >&2
  exit 1
fi
rm -f "${HOME}/Library/LaunchAgents/com.${USER}.managemovie.web.user-watchdog.plist" \
  "${HOME}/Library/LaunchAgents/com.${USER}.managemovie.web.user-app.plist"
launchctl bootout "gui/$(id -u)/com.${USER}.managemovie.web.user-watchdog" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/com.${USER}.managemovie.web.user-app" >/dev/null 2>&1 || true

CURRENT_CRONTAB="$(mktemp "${TMPDIR:-/tmp}/managemovie-crontab.XXXXXX")"
trap 'rm -f "$CURRENT_CRONTAB"' EXIT

crontab -l >"$CURRENT_CRONTAB" 2>/dev/null || true
if grep -Fqx "$CRON_LINE" "$CURRENT_CRONTAB" && grep -Fqx "$CRON_REBOOT_LINE" "$CURRENT_CRONTAB"; then
  echo "[watchdog] cron bereits aktuell"
  echo "[watchdog] skript: $WATCHDOG_PATH"
  echo "[watchdog] log: ${LOG_DIR}/cron-watchdog.log"
  echo "[watchdog] pruefintervall: 60s"
  exit 0
fi

grep -vF "$CRON_TAG" "$CURRENT_CRONTAB" > "${CURRENT_CRONTAB}.next" || true
grep -vF "$CRON_REBOOT_TAG" "${CURRENT_CRONTAB}.next" > "${CURRENT_CRONTAB}.final" || true
printf '%s\n' "$CRON_REBOOT_LINE" >> "${CURRENT_CRONTAB}.final"
printf '%s\n' "$CRON_LINE" >> "${CURRENT_CRONTAB}.final"
crontab "${CURRENT_CRONTAB}.final"

echo "[watchdog] cron installiert"
echo "[watchdog] skript: $WATCHDOG_PATH"
echo "[watchdog] log: ${LOG_DIR}/cron-watchdog.log"
echo "[watchdog] pruefintervall: @reboot + 60s"
