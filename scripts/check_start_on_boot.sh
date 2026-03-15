#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/lib/common.sh"
mm_cd_project_root
mm_load_project_env

DATA_ROOT="$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")"
FLAG_PATH="${DATA_ROOT}/config/start_on_boot.flag"
value="${MANAGEMOVIE_START_ON_BOOT:-1}"

if [ -f "$FLAG_PATH" ]; then
  value="$(tr -d "[:space:]" < "$FLAG_PATH" 2>/dev/null || printf '%s' "$value")"
fi

case "${value:-1}" in
  0|false|FALSE|False|off|OFF|no|NO)
    exit 1
    ;;
  *)
    exit 0
    ;;
esac
