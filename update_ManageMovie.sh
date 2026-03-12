#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
BRANCH="${MANAGEMOVIE_UPDATE_BRANCH:-main}"
TAG=""
CHECK_ONLY=0
USE_BRANCH=0

detect_default_remote_url() {
  local origin_url=""
  if command -v git >/dev/null 2>&1 && git -C "$BASE_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    origin_url="$(git -C "$BASE_DIR" remote get-url origin 2>/dev/null || true)"
  fi
  if [ -n "$origin_url" ]; then
    printf '%s\n' "$origin_url"
    return 0
  fi
  if [ -f "$BASE_DIR/.managemovie-mac-public" ]; then
    printf '%s\n' "https://github.com/paddythefish-brumm/ManageMovie-MAC.git"
    return 0
  fi
  if grep -qi "ManageMovie LXS" "$BASE_DIR/.env.local" 2>/dev/null; then
    printf '%s\n' "https://github.com/paddythefish-brumm/ManageMovie-LXS.git"
    return 0
  fi
  if [ "$(uname -s 2>/dev/null || true)" = "Darwin" ]; then
    printf '%s\n' "https://github.com/paddythefish-brumm/ManageMovie.git"
    return 0
  fi
  printf '%s\n' "https://github.com/paddythefish-brumm/ManageMovie-LXS.git"
}

REMOTE_URL="${MANAGEMOVIE_UPDATE_REPO_URL:-$(detect_default_remote_url)}"

usage() {
  cat <<'EOF'
Usage: ./update_ManageMovie.sh [--check] [--branch NAME] [--tag TAG]

Optionen:
  --check         nur lokalen und entfernten Stand anzeigen
  --branch NAME   Branch statt Release-Tag aktivieren
  --tag TAG       exakten Tag aktivieren
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --check) CHECK_ONLY=1; shift ;;
    --branch) BRANCH="$2"; USE_BRANCH=1; shift 2 ;;
    --tag) TAG="$2"; shift 2 ;;
    --help) usage; exit 0 ;;
    *) echo "Unbekannte Option: $1" >&2; usage; exit 1 ;;
  esac
done

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Fehlt: $1" >&2; exit 1; }
}

need_cmd git
need_cmd rsync
need_cmd curl

resolve_latest_tag() {
  git ls-remote --tags --refs "$REMOTE_URL" 'v[0-9]*' \
    | awk '{print $2}' \
    | sed 's#refs/tags/##' \
    | sort -V \
    | tail -n1
}

restart_service() {
  local service_name="${MANAGEMOVIE_SYSTEMD_SERVICE_NAME:-managemovie-web.service}"
  local boot_check="${BASE_DIR}/scripts/check_start_on_boot.sh"
  local start_on_boot=1
  if [ -x "$boot_check" ] && ! "$boot_check"; then
    start_on_boot=0
  fi
  if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files "$service_name" >/dev/null 2>&1; then
    if [ "$start_on_boot" -eq 1 ]; then
      systemctl restart "$service_name"
      return 0
    fi
  fi
  if [ -x "$BASE_DIR/stop.sh" ]; then
    "$BASE_DIR/stop.sh" >/tmp/managemovie-update-stop.log 2>&1 || true
  fi
  if [ -x "$BASE_DIR/start.sh" ]; then
    nohup "$BASE_DIR/start.sh" >/tmp/managemovie-update-start.log 2>&1 </dev/null &
    return 0
  fi
  echo "Kein Restart-Pfad gefunden." >&2
  return 1
}

wait_for_health() {
  local bind="127.0.0.1"
  local port="8126"
  local tls="1"
  if [ -f "$BASE_DIR/.env.local" ]; then
    bind="$(grep -E "^MANAGEMOVIE_WEB_BIND=" "$BASE_DIR/.env.local" | head -n1 | cut -d= -f2- | tr -d "'\"" || true)"
    port="$(grep -E "^MANAGEMOVIE_WEB_PORT=" "$BASE_DIR/.env.local" | head -n1 | cut -d= -f2- | tr -d "'\"" || true)"
    tls="$(grep -E "^MANAGEMOVIE_WEB_TLS=" "$BASE_DIR/.env.local" | head -n1 | cut -d= -f2- | tr -d "'\"" || true)"
  fi
  if [ -z "$bind" ] || [ "$bind" = "0.0.0.0" ]; then
    bind="127.0.0.1"
  fi
  if [ -z "$port" ]; then
    port="8126"
  fi
  local scheme="https"
  local curl_flags="-ksS"
  if [ "$tls" = "0" ]; then
    scheme="http"
    curl_flags="-fsS"
  fi
  local attempt
  for attempt in $(seq 1 90); do
    if curl $curl_flags "${scheme}://${bind}:${port}/api/state" >/dev/null 2>&1; then
      curl $curl_flags "${scheme}://${bind}:${port}/api/state"
      return 0
    fi
    sleep 2
  done
  echo "Healthcheck fehlgeschlagen: ${scheme}://${bind}:${port}/api/state" >&2
  return 1
}

local_version="$(grep -E -m1 '^VERSION = "[0-9]+\.[0-9]+\.[0-9]+"' "$BASE_DIR/managemovie-web/app/managemovie.py" | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/' || true)"
remote_head="$(git ls-remote "$REMOTE_URL" "refs/heads/${BRANCH}" | awk '{print $1}' | head -n1)"
latest_tag="$(resolve_latest_tag)"
target_ref="$latest_tag"
target_desc="release"

if [ -n "$TAG" ]; then
  target_ref="$TAG"
  target_desc="tag"
elif [ "$USE_BRANCH" -eq 1 ]; then
  target_ref="$BRANCH"
  target_desc="branch"
fi

echo "Lokal:   ${local_version:-unbekannt}"
echo "Remote:  ${REMOTE_URL}"
echo "Branch:  ${BRANCH}"
echo "HEAD:    ${remote_head:-unbekannt}"
echo "Latest:  ${latest_tag:-unbekannt}"
echo "Target:  ${target_ref:-unbekannt} (${target_desc})"

if [ "$CHECK_ONLY" -eq 1 ]; then
  exit 0
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

git clone --depth 1 "$REMOTE_URL" "$TMP_DIR/repo" >/dev/null
git -C "$TMP_DIR/repo" fetch --tags --force >/dev/null
if [ -n "$target_ref" ]; then
  git -C "$TMP_DIR/repo" checkout --force "$target_ref" >/dev/null
elif [ "$USE_BRANCH" -eq 1 ]; then
  git -C "$TMP_DIR/repo" checkout --force "$BRANCH" >/dev/null
fi

rsync -a --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '.env' \
  --exclude '.env.local' \
  --exclude 'MovieManager/' \
  --exclude 'logs/' \
  --exclude 'work/' \
  --exclude 'temp/' \
  --exclude 'certs/' \
  "$TMP_DIR/repo/" "$BASE_DIR/"

for candidate in \
  "$BASE_DIR/setup.sh" \
  "$BASE_DIR/start.sh" \
  "$BASE_DIR/stop.sh" \
  "$BASE_DIR/install_mamo_mac_app.sh" \
  "$BASE_DIR/install_systemd_service.sh" \
  "$BASE_DIR/install_launchdaemon_service.sh" \
  "$BASE_DIR/setup_https.sh" \
  "$BASE_DIR/setup_mariadb.sh" \
  "$BASE_DIR/update_ManageMovie.sh" \
  "$BASE_DIR/scripts/check_start_on_boot.sh"
do
  [ -f "$candidate" ] || continue
  chmod +x "$candidate"
done

cd "$BASE_DIR"
./setup.sh >/tmp/managemovie-update-setup.log 2>&1
restart_service
wait_for_health
