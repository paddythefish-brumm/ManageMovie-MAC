#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/scripts/lib/common.sh"
mm_cd_project_root
mm_load_project_env

if [ "$(uname -s)" != "Darwin" ]; then
  echo "Fehler: Dieses Skript ist nur fuer macOS gedacht." >&2
  exit 1
fi

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Fehlt: $1" >&2; exit 1; }
}

need_cmd osacompile

PROJECT_ROOT="$(mm_project_root)"
APP_NAME="MaMo"
APP_PATH="/Applications/${APP_NAME}.app"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mamo-app.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT
SCRIPT_PATH="$TMP_DIR/${APP_NAME}.applescript"
APP_TMP="$TMP_DIR/${APP_NAME}.app"

cat > "$SCRIPT_PATH" <<EOF
on run
  set projectRoot to "$(printf '%s' "$PROJECT_ROOT" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  set defaultUrl to "https://127.0.0.1:8126/"
  set buttonChoice to button returned of (display dialog "MaMo" with title "ManageMovie" buttons {"Exit", "Open", "Stop", "Start"} default button "Open")
  if buttonChoice is "Start" then
    do shell script "cd " & quoted form of projectRoot & " && nohup ./start.sh >/tmp/mamo-app-start.log 2>/tmp/mamo-app-start.err </dev/null &"
  else if buttonChoice is "Stop" then
    do shell script "cd " & quoted form of projectRoot & " && ./stop.sh >/tmp/mamo-app-stop.log 2>/tmp/mamo-app-stop.err || true"
  else if buttonChoice is "Open" then
    open location defaultUrl
  end if
end run
EOF

osacompile -o "$APP_TMP" "$SCRIPT_PATH" >/dev/null
rm -rf "$APP_PATH"
cp -R "$APP_TMP" "$APP_PATH"
chmod -R a+rX "$APP_PATH"
open -a "$APP_PATH" >/dev/null 2>&1 || true

echo "MaMo-App installiert: $APP_PATH"
