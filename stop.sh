#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/scripts/lib/common.sh"
source "$(mm_join_project_path "scripts/lib/process.sh")"
mm_cd_project_root

PORT="${MANAGEMOVIE_WEB_PORT:-8126}"
APP_MARKER="$(mm_join_project_path "managemovie-web/web/app.py")"
pids="$(mm_port_pids "$PORT")"

if [ -z "$pids" ]; then
  echo "Kein Prozess auf Port ${PORT}."
  exit 0
fi

declare -a app_pids=()
for pid in $pids; do
  if mm_is_managemovie_web_pid "$pid" "$APP_MARKER"; then
    app_pids+=("$pid")
  fi
done

if [ "${#app_pids[@]}" -eq 0 ]; then
  echo "Auf Port ${PORT} laeuft kein ManageMovie-Prozess."
  exit 0
fi

kill "${app_pids[@]}" 2>/dev/null || true
sleep 1

remain="$(mm_port_pids "$PORT")"

if [ -n "$remain" ]; then
  declare -a remain_app_pids=()
  for pid in $remain; do
    if mm_is_managemovie_web_pid "$pid" "$APP_MARKER"; then
      remain_app_pids+=("$pid")
    fi
  done
  if [ "${#remain_app_pids[@]}" -gt 0 ]; then
    kill -9 "${remain_app_pids[@]}" 2>/dev/null || true
  fi
fi

echo "App auf Port ${PORT} gestoppt."
