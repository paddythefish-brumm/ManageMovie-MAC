#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/scripts/lib/common.sh"
mm_cd_project_root
mm_load_project_env

if [ "$(uname -s)" != "Darwin" ]; then
  echo "Fehler: Dieses Skript ist nur fuer macOS gedacht." >&2
  exit 1
fi

APP_SERVICE_NAME="${MANAGEMOVIE_LAUNCHDAEMON_NAME:-com.${USER}.managemovie.web}"
PF_SERVICE_NAME="${MANAGEMOVIE_PF_LAUNCHDAEMON_NAME:-${APP_SERVICE_NAME}.pf}"
WATCHDOG_SERVICE_NAME="${APP_SERVICE_NAME}.watchdog"
ADMIN_HELPER_PATH="${MANAGEMOVIE_MAC_ADMIN_HELPER:-/usr/local/sbin/managemovie-mac-admin}"
SUDOERS_PATH="${MANAGEMOVIE_MAC_ADMIN_SUDOERS:-/etc/sudoers.d/managemovie-mac-admin}"
PROJECT_ROOT="$(mm_project_root)"
DATA_ROOT="$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")"
LOG_DIR="${DATA_ROOT}/logs"
ROOT_LOG_DIR="/usr/local/var/log/managemovie"
LAUNCHAGENT_PLIST="${HOME}/Library/LaunchAgents/${APP_SERVICE_NAME}.plist"
BOOTCHECK_PLIST="${HOME}/Library/LaunchAgents/${APP_SERVICE_NAME}.bootcheck.plist"
PORT="${MANAGEMOVIE_WEB_PORT:-8126}"

LAN_IF="$(route -n get default 2>/dev/null | awk '/interface:/{print $2; exit}')"
if [ -z "$LAN_IF" ]; then
  LAN_IF="en0"
fi

LAN_IP="$(ipconfig getifaddr "$LAN_IF" 2>/dev/null || true)"
if [ -z "$LAN_IP" ] && [ "$LAN_IF" = "en0" ]; then
  LAN_IP="$(ipconfig getifaddr en1 2>/dev/null || true)"
fi
if [ -z "$LAN_IP" ]; then
  LAN_IP="$(python3 - <<'PY'
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    sock.connect(("8.8.8.8", 80))
    print(sock.getsockname()[0])
except Exception:
    print("127.0.0.1")
finally:
    sock.close()
PY
)"
fi

mkdir -p "$LOG_DIR" "$DATA_ROOT"

TMP_DIR="$(mktemp -d "/tmp/managemovie-mac-admin.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

HELPER_TMP="${TMP_DIR}/managemovie-mac-admin"
BOOTSTRAP_TMP="${TMP_DIR}/bootstrap_root.sh"
SUDOERS_TMP="${TMP_DIR}/managemovie-mac-admin.sudoers"
WATCHDOG_PLIST_TMP="${TMP_DIR}/${WATCHDOG_SERVICE_NAME}.plist"
WATCHDOG_LOG_STDOUT="${ROOT_LOG_DIR}/launchdaemon-watchdog.stdout.log"
WATCHDOG_LOG_STDERR="${ROOT_LOG_DIR}/launchdaemon-watchdog.stderr.log"
APP_STDOUT="${ROOT_LOG_DIR}/boot-watchdog.app.stdout.log"
APP_STDERR="${ROOT_LOG_DIR}/boot-watchdog.app.stderr.log"

cat > "$HELPER_TMP" <<'EOF'
#!/bin/sh
# MANAGEMOVIE_ADMIN_HELPER_V7
set -eu
PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PATH

usage() {
  echo "Usage: $0 cleanup-pf <app_service> <pf_service> | install-watchdog <watchdog_service> <watchdog_plist> | cleanup-root-cron | ensure-app <run_user> <project_root> <lan_ip> <bind> <port> <tls> <app_stdout> <app_stderr> | cleanup-legacy <old_service> <old_plist>" >&2
  exit 2
}

[ "$#" -ge 1 ] || usage
cmd="$1"
shift

cleanup_pf_config() {
  pf_conf="/etc/pf.conf"
  tmp_file="$(mktemp "${TMPDIR:-/tmp}/pf.conf.XXXXXX")"
  python3 - "$pf_conf" "$tmp_file" <<'PY'
import sys
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
lines = src.read_text(encoding="utf-8").splitlines()
filtered = []
for line in lines:
    stripped = line.strip()
    if stripped == 'rdr-anchor "managemovie/*"':
        continue
    if stripped.startswith('load anchor "managemovie/'):
        continue
    filtered.append(line)
dst.write_text("\n".join(filtered) + "\n", encoding="utf-8")
PY
  cp "$tmp_file" "$pf_conf"
  rm -f "$tmp_file"
  /sbin/pfctl -f /etc/pf.conf >/dev/null 2>&1 || true
}

case "$cmd" in
  cleanup-pf)
    [ "$#" -ge 2 ] || usage
    app_service="$1"
    pf_service="$2"
    launchctl bootout system/"${pf_service}" >/dev/null 2>&1 || true
    launchctl disable system/"${pf_service}" >/dev/null 2>&1 || true
    rm -f "/Library/LaunchDaemons/${pf_service}.plist"
    rm -f "/Library/LaunchDaemons/com.paddy.managemovie.pf.plist"
    rm -f "/etc/pf.anchors/managemovie/${app_service}.conf"
    cleanup_pf_config
    ;;
  install-watchdog)
    [ "$#" -ge 2 ] || usage
    watchdog_service="$1"
    watchdog_plist="$2"
    mkdir -p /Library/LaunchDaemons /usr/local/var/log/managemovie
    cp "$watchdog_plist" "/Library/LaunchDaemons/${watchdog_service}.plist"
    chown root:wheel "/Library/LaunchDaemons/${watchdog_service}.plist"
    chmod 644 "/Library/LaunchDaemons/${watchdog_service}.plist"
    launchctl bootout system/"${watchdog_service}" >/dev/null 2>&1 || true
    launchctl enable system/"${watchdog_service}" >/dev/null 2>&1 || true
    launchctl bootstrap system "/Library/LaunchDaemons/${watchdog_service}.plist"
    ;;
  cleanup-root-cron)
    cron_tag="# MANAGEMOVIE_ROOT_WATCHDOG"
    cron_tmp="$(mktemp "${TMPDIR:-/tmp}/root-cron.XXXXXX")"
    cron_next="${cron_tmp}.next"

    if crontab -u root -l >"$cron_tmp" 2>/dev/null; then
      grep -vF "$cron_tag" "$cron_tmp" >"$cron_next" || true
      if [ -s "$cron_next" ]; then
        crontab -u root "$cron_next"
      else
        crontab -u root -r >/dev/null 2>&1 || true
      fi
    fi

    if [ -f /etc/crontab ]; then
      grep -vF "$cron_tag" /etc/crontab >"$cron_next" || true
      cp "$cron_next" /etc/crontab
      chmod 644 /etc/crontab
    fi

    rm -f "$cron_tmp" "$cron_next"
    ;;
  ensure-app)
    [ "$#" -ge 8 ] || usage
    run_user="$1"
    project_root="$2"
    lan_ip="$3"
    bind_ip="$4"
    port="$5"
    tls="$6"
    app_stdout="$7"
    app_stderr="$8"
    app_match="${project_root}/managemovie-web/web/app.py"
    boot_check_script="${project_root}/scripts/check_start_on_boot.sh"
    state_url="https://${lan_ip}:${port}/api/state"
    run_group="$(id -gn "$run_user")"

    mkdir -p "$(dirname "$app_stdout")"
    touch "$app_stdout" "$app_stderr"
    /usr/sbin/chown "$run_user:$run_group" "$app_stdout" "$app_stderr"
    chmod 644 "$app_stdout" "$app_stderr"

    if pgrep -f "$app_match" >/dev/null 2>&1; then
      exit 0
    fi

    if curl -kfsS --max-time 3 "$state_url" >/dev/null 2>&1; then
      exit 0
    fi

    if [ -x "$boot_check_script" ] && ! "$boot_check_script"; then
      exit 0
    fi

    cd / && /usr/bin/sudo -H -n -u "$run_user" /usr/bin/env \
      MANAGEMOVIE_WEB_BIND="$bind_ip" \
      MANAGEMOVIE_WEB_PORT="$port" \
      MANAGEMOVIE_WEB_TLS="$tls" \
      MANAGEMOVIE_PROJECT_ROOT="$project_root" \
      MANAGEMOVIE_APP_STDOUT="$app_stdout" \
      MANAGEMOVIE_APP_STDERR="$app_stderr" \
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

    i=0
    while [ "$i" -lt 15 ]; do
      sleep 2
      if curl -kfsS --max-time 3 "$state_url" >/dev/null 2>&1; then
        exit 0
      fi
      i=$((i + 1))
    done
    exit 1
    ;;
  cleanup-legacy)
    [ "$#" -ge 2 ] || usage
    old_service="$1"
    old_plist="$2"
    launchctl bootout system/"${old_service}" >/dev/null 2>&1 || true
    launchctl disable system/"${old_service}" >/dev/null 2>&1 || true
    rm -f "$old_plist"
    ;;
  *)
    usage
    ;;
esac
EOF
chmod 755 "$HELPER_TMP"

cat > "$SUDOERS_TMP" <<EOF
${USER} ALL=(root) NOPASSWD: ${ADMIN_HELPER_PATH} *
EOF
chmod 440 "$SUDOERS_TMP"

WATCHDOG_SERVICE_NAME="$WATCHDOG_SERVICE_NAME" \
ADMIN_HELPER_PATH="$ADMIN_HELPER_PATH" \
PROJECT_ROOT="$PROJECT_ROOT" \
LAN_IP="$LAN_IP" \
PORT="$PORT" \
APP_STDOUT="$APP_STDOUT" \
APP_STDERR="$APP_STDERR" \
WATCHDOG_LOG_STDOUT="$WATCHDOG_LOG_STDOUT" \
WATCHDOG_LOG_STDERR="$WATCHDOG_LOG_STDERR" \
python3 - <<'PY' > "$WATCHDOG_PLIST_TMP"
import os
import plistlib
import sys

payload = {
    "Label": os.environ["WATCHDOG_SERVICE_NAME"],
    "ProgramArguments": [
        os.environ["ADMIN_HELPER_PATH"],
        "ensure-app",
        os.environ["USER"],
        os.environ["PROJECT_ROOT"],
        os.environ["LAN_IP"],
        "0.0.0.0",
        os.environ["PORT"],
        "1",
        os.environ["APP_STDOUT"],
        os.environ["APP_STDERR"],
    ],
    "RunAtLoad": True,
    "StartInterval": 60,
    "KeepAlive": False,
    "StandardOutPath": os.environ["WATCHDOG_LOG_STDOUT"],
    "StandardErrorPath": os.environ["WATCHDOG_LOG_STDERR"],
}
plistlib.dump(payload, sys.stdout.buffer, sort_keys=False)
PY

cat > "$BOOTSTRAP_TMP" <<EOF
#!/bin/sh
set -eu
mkdir -p "$(dirname "$ADMIN_HELPER_PATH")" "$(dirname "$SUDOERS_PATH")" "${ROOT_LOG_DIR}"
cp "$HELPER_TMP" "$ADMIN_HELPER_PATH"
chown root:wheel "$ADMIN_HELPER_PATH"
chmod 755 "$ADMIN_HELPER_PATH"
cp "$SUDOERS_TMP" "$SUDOERS_PATH"
chown root:wheel "$SUDOERS_PATH"
chmod 440 "$SUDOERS_PATH"
rm -f "${LAUNCHAGENT_PLIST}" "${BOOTCHECK_PLIST}"
launchctl bootout gui/$(id -u ${USER})/"${APP_SERVICE_NAME}" >/dev/null 2>&1 || true
launchctl bootout gui/$(id -u ${USER})/"${APP_SERVICE_NAME}.bootcheck" >/dev/null 2>&1 || true
"$ADMIN_HELPER_PATH" cleanup-pf "$APP_SERVICE_NAME" "$PF_SERVICE_NAME"
"$ADMIN_HELPER_PATH" cleanup-legacy "$APP_SERVICE_NAME" "/Library/LaunchDaemons/${APP_SERVICE_NAME}.plist"
"$ADMIN_HELPER_PATH" cleanup-legacy "com.paddy.managemovie.pf" "/Library/LaunchDaemons/com.paddy.managemovie.pf.plist"
"$ADMIN_HELPER_PATH" cleanup-root-cron
"$ADMIN_HELPER_PATH" install-watchdog "$WATCHDOG_SERVICE_NAME" "$WATCHDOG_PLIST_TMP"
EOF
chmod 700 "$BOOTSTRAP_TMP"

run_privileged() {
  if [ -x "$ADMIN_HELPER_PATH" ] && grep -q 'MANAGEMOVIE_ADMIN_HELPER_V7' "$ADMIN_HELPER_PATH" 2>/dev/null; then
    sudo -n "$ADMIN_HELPER_PATH" cleanup-pf "$APP_SERVICE_NAME" "$PF_SERVICE_NAME" >/dev/null 2>&1 || true
    sudo -n "$ADMIN_HELPER_PATH" cleanup-legacy "$APP_SERVICE_NAME" "/Library/LaunchDaemons/${APP_SERVICE_NAME}.plist" >/dev/null 2>&1 || true
    sudo -n "$ADMIN_HELPER_PATH" cleanup-legacy "com.paddy.managemovie.pf" "/Library/LaunchDaemons/com.paddy.managemovie.pf.plist" >/dev/null 2>&1 || true
    sudo -n "$ADMIN_HELPER_PATH" cleanup-root-cron >/dev/null 2>&1 || true
    if sudo -n "$ADMIN_HELPER_PATH" install-watchdog "$WATCHDOG_SERVICE_NAME" "$WATCHDOG_PLIST_TMP" >/dev/null 2>&1; then
      return 0
    fi
  fi

  APPLESCRIPT_CMD="$(BOOTSTRAP_TMP="$BOOTSTRAP_TMP" python3 - <<'PY'
import os
path = os.environ["BOOTSTRAP_TMP"]
escaped = path.replace("\\", "\\\\").replace('"', '\\"')
print(f'do shell script "{escaped}" with administrator privileges')
PY
)"
  osascript -e "$APPLESCRIPT_CMD"
}

run_privileged

./install_launchagent_service.sh
/bin/sh "$(mm_join_project_path "scripts/mac_user_watchdog.sh")" || true

echo "[https-admin] HTTPS direkt aktiv fuer https://${LAN_IP}:${PORT}/"
echo "[https-admin] Port 443 bleibt frei."
echo "[https-admin] Root-Helper: ${ADMIN_HELPER_PATH}"
echo "[https-admin] sudoers: ${SUDOERS_PATH}"
echo "[https-admin] App-Autostart: LaunchDaemon ${WATCHDOG_SERVICE_NAME} + User-Watchdog"
