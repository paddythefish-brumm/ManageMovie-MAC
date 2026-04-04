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
need_cmd swiftc

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
LAUNCHER_SOURCE="$(mm_join_project_path "scripts/mac/ManageMovieLauncher.swift")"
LAUNCHER_BINARY_NAME="ManageMovieLauncher"

cat > "$SCRIPT_PATH" <<EOF
on run
  set projectRoot to "$(printf '%s' "$PROJECT_ROOT" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  set appTitle to "ManageMovie ${VERSION}"
  set launcherBinary to POSIX path of ((path to me as text) & "Contents:MacOS:${LAUNCHER_BINARY_NAME}")
  do shell script "/bin/bash -lc " & quoted form of ("nohup " & quoted form of launcherBinary & space & quoted form of projectRoot & space & quoted form of appTitle & " >/tmp/mamo-launcher.log 2>/tmp/mamo-launcher.err </dev/null &")
end run
EOF

osacompile -o "$APP_TMP" "$SCRIPT_PATH" >/dev/null
chmod +x \
  "$(mm_join_project_path "scripts/mac/manage_app_container.sh")" \
  "$(mm_join_project_path "scripts/mac/launcher_state.py")"
swiftc -O -framework AppKit "$LAUNCHER_SOURCE" -o "$APP_TMP/Contents/MacOS/${LAUNCHER_BINARY_NAME}"
if [ -x "$ICON_GENERATOR" ] && command -v swift >/dev/null 2>&1; then
  "$ICON_GENERATOR" "$ICON_SOURCE" >/dev/null 2>&1 || true
fi
rm -f "$APP_TMP/Contents/Resources/Assets.car"
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
rm -f "$APP_PATH/Contents/Resources/Assets.car"
open -a "$APP_PATH" >/dev/null 2>&1 || true

echo "ManageMovie-App installiert: $APP_PATH"
