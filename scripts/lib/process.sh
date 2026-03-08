#!/usr/bin/env bash

mm_port_pids() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -ti "tcp:${port}" 2>/dev/null || true
    return
  fi
  if command -v fuser >/dev/null 2>&1; then
    fuser -n tcp "$port" 2>/dev/null || true
    return
  fi
}

mm_pid_cmdline() {
  local pid="$1"
  ps -p "$pid" -o args= 2>/dev/null || true
}

mm_is_managemovie_web_pid() {
  local pid="$1"
  local app_marker="$2"
  local cmdline
  cmdline="$(mm_pid_cmdline "$pid")"
  [ -n "$cmdline" ] || return 1
  case "$cmdline" in
    *"$app_marker"*|*"managemovie-web/web/app.py"*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

