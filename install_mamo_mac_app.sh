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
APP_NAME="ManageMovie"
APP_PATH="/Applications/${APP_NAME}.app"
VERSION="$(PROJECT_ROOT="$PROJECT_ROOT" python3 - <<'PY'
from pathlib import Path
import os, re
text = (Path(os.environ["PROJECT_ROOT"]) / "managemovie-web/app/managemovie.py").read_text(encoding="utf-8")
match = re.search(r'^VERSION = "([^"]+)"', text, re.M)
if not match:
    raise SystemExit("Version nicht gefunden")
print(match.group(1))
PY
)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mamo-app.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT
SCRIPT_PATH="$TMP_DIR/${APP_NAME}.applescript"
APP_TMP="$TMP_DIR/${APP_NAME}.app"
ICON_SOURCE="$TMP_DIR/managemovie.icns"
ICON_GENERATOR="$(mm_join_project_path "scripts/mac/generate_manage_movie_icon.swift")"

cat > "$SCRIPT_PATH" <<EOF
on run
  set projectRoot to "$(printf '%s' "$PROJECT_ROOT" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  set appTitle to "ManageMovie ${VERSION}"
  set defaultUrl to "https://127.0.0.1:8126/"
  set appStatus to do shell script "/bin/bash -lc " & quoted form of "if curl -kfsS --max-time 2 https://127.0.0.1:8126/api/state >/dev/null 2>&1; then printf '🟢 Web-App OK'; else printf '🔴 Web-App AUS'; fi"
  set dbStatus to do shell script "/bin/bash -lc " & quoted form of "if nc -z 127.0.0.1 3306 >/dev/null 2>&1; then printf '🟢 DB OK'; else printf '🔴 DB AUS'; fi"
  repeat
    set promptText to appStatus & return & dbStatus & return & return & "Aktion wählen"
    set actionItems to {"Open in Browser", "Start App", "Start DB", "Stop App", "Stop DB", "Uninstall DB+App"}
    set actionChoice to choose from list actionItems with title appTitle with prompt promptText default items {"Open in Browser"} OK button name "Ausführen" cancel button name "Exit"
    if actionChoice is false then return
    set selectedAction to item 1 of actionChoice
    if selectedAction is "Start App" then
      do shell script "/bin/bash -lc " & quoted form of "cd " & quoted form of projectRoot & " && nohup ./start.sh >/tmp/mamo-app-start.log 2>/tmp/mamo-app-start.err </dev/null &"
    else if selectedAction is "Start DB" then
      do shell script "/bin/bash -lc " & quoted form of "if command -v brew >/dev/null 2>&1; then nohup brew services start mariadb >/tmp/mamo-app-db-start.log 2>/tmp/mamo-app-db-start.err </dev/null & fi"
    else if selectedAction is "Stop App" then
      do shell script "/bin/bash -lc " & quoted form of "cd " & quoted form of projectRoot & " && nohup ./stop.sh >/tmp/mamo-app-stop.log 2>/tmp/mamo-app-stop.err </dev/null &"
    else if selectedAction is "Stop DB" then
      do shell script "/bin/bash -lc " & quoted form of "if command -v brew >/dev/null 2>&1; then nohup brew services stop mariadb >/tmp/mamo-app-db-stop.log 2>/tmp/mamo-app-db-stop.err </dev/null & fi"
    else if selectedAction is "Uninstall DB+App" then
      set uninstallAnswer to button returned of (display dialog "ManageMovie, Datenbank, Einstellungen, API-Keys und App wirklich vollständig entfernen?" with title appTitle buttons {"Abbruch", "Uninstall"} default button "Uninstall" cancel button "Abbruch")
      if uninstallAnswer is "Uninstall" then
        do shell script "cd " & quoted form of projectRoot & " && nohup ./uninstall_ManageMovie.sh >/tmp/mamo-app-uninstall.log 2>/tmp/mamo-app-uninstall.err </dev/null &"
        return
      end if
    else if selectedAction is "Open in Browser" then
      open location defaultUrl
    end if
    delay 1
    set appStatus to do shell script "/bin/bash -lc " & quoted form of "if curl -kfsS --max-time 2 https://127.0.0.1:8126/api/state >/dev/null 2>&1; then printf '🟢 Web-App OK'; else printf '🔴 Web-App AUS'; fi"
    set dbStatus to do shell script "/bin/bash -lc " & quoted form of "if nc -z 127.0.0.1 3306 >/dev/null 2>&1; then printf '🟢 DB OK'; else printf '🔴 DB AUS'; fi"
  end repeat
end run
EOF

osacompile -o "$APP_TMP" "$SCRIPT_PATH" >/dev/null
if [ -x "$ICON_GENERATOR" ] && command -v swift >/dev/null 2>&1; then
  "$ICON_GENERATOR" "$ICON_SOURCE" >/dev/null 2>&1 || true
fi
if [[ ! -f "$ICON_SOURCE" ]]; then
  ICON_SOURCE="/System/Applications/TV.app/Contents/Resources/AppIcon.icns"
fi
if [[ -f "$ICON_SOURCE" ]]; then
  cp "$ICON_SOURCE" "$APP_TMP/Contents/Resources/applet.icns"
fi
plutil -replace CFBundleName -string "$APP_NAME" "$APP_TMP/Contents/Info.plist"
plutil -replace CFBundleDisplayName -string "$APP_NAME" "$APP_TMP/Contents/Info.plist" 2>/dev/null || plutil -insert CFBundleDisplayName -string "$APP_NAME" "$APP_TMP/Contents/Info.plist"
plutil -replace CFBundleIconFile -string "applet" "$APP_TMP/Contents/Info.plist"
plutil -replace CFBundleIconName -string "applet" "$APP_TMP/Contents/Info.plist"
rm -rf /Applications/MaMo.app
rm -rf "$APP_PATH"
cp -R "$APP_TMP" "$APP_PATH"
chmod -R a+rX "$APP_PATH"
open -a "$APP_PATH" >/dev/null 2>&1 || true

echo "ManageMovie-App installiert: $APP_PATH"
