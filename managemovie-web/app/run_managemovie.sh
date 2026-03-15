#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERSION="0.2.35"
PY_SCRIPT="$SCRIPT_DIR/managemovie_v${VERSION}.py"
FALLBACK_SCRIPT="$SCRIPT_DIR/managemovie.py"

if [ ! -f "$PY_SCRIPT" ]; then
  if [ -f "$FALLBACK_SCRIPT" ]; then
    PY_SCRIPT="$FALLBACK_SCRIPT"
  else
    echo "Fehler: Script nicht gefunden: $PY_SCRIPT" >&2
    exit 1
  fi
fi

SCRIPT_VERSION="$(grep -m1 '^VERSION = "' "$PY_SCRIPT" | cut -d'"' -f2)"
if [ -z "$SCRIPT_VERSION" ]; then
  SCRIPT_VERSION="$VERSION"
fi
echo "ManageMovie startet (Version ${SCRIPT_VERSION})."

WORK_DIR="${MANAGEMOVIE_WORKDIR:-$PROJECT_DIR/work}"
mkdir -p "$WORK_DIR/tmp" "$WORK_DIR/pycache"
export MANAGEMOVIE_WORKDIR="$WORK_DIR"
export PYTHONPYCACHEPREFIX="$WORK_DIR/pycache"
cd "$SCRIPT_DIR"
export TMPDIR="$WORK_DIR/tmp"
export TMP="$WORK_DIR/tmp"
export TEMP="$WORK_DIR/tmp"
export DISABLE_AUTO_TITLE=true
export HISTFILE=/dev/null
set +o history 2>/dev/null || true
if [ -d "$SCRIPT_DIR/.venv" ]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.venv/bin/activate"
fi

set +e
python3 "$PY_SCRIPT" "$@"
RC=$?
set -e

if [ "$RC" -ne 0 ]; then
  echo "ManageMovie beendet mit Fehler (Exit-Code $RC)." >&2
fi

if [ "${MANAGEMOVIE_NO_PAUSE:-0}" != "1" ]; then
  if [ -t 0 ]; then
    if [ "$RC" -eq 0 ]; then
      printf "ManageMovie fertig. Enter zum Schliessen... "
    else
      printf "Zum Schliessen Enter druecken... "
    fi
    read -r _ || true
  else
    PAUSE_SEC="${MANAGEMOVIE_EXIT_PAUSE_SEC:-20}"
    sleep "$PAUSE_SEC"
  fi
fi

exit "$RC"
