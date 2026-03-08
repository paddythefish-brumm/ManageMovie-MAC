#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/scripts/lib/common.sh"
mm_cd_project_root
mm_load_project_env
mm_activate_homebrew_path

SQL_FILE="$(mm_join_project_path "mariadb/init_managemovie.sql")"

DB_HOST="${MANAGEMOVIE_DB_HOST:-127.0.0.1}"
DB_PORT="${MANAGEMOVIE_DB_PORT:-3306}"
DB_NAME="${MANAGEMOVIE_DB_NAME:-managemovie}"
DB_USER="${MANAGEMOVIE_DB_USER:-managemovie}"
DB_PASS="${MANAGEMOVIE_DB_PASSWORD:-}"
DB_APP_HOST="${MANAGEMOVIE_DB_APP_HOST:-localhost}"

ROOT_USER="${MANAGEMOVIE_DB_ROOT_USER:-root}"
ROOT_PASS="${MANAGEMOVIE_DB_ROOT_PASSWORD:-}"
CURRENT_USER="$(id -un)"
MYSQL_SOCKET="${MANAGEMOVIE_DB_SOCKET:-/tmp/mysql.sock}"

mysql_admin_client=()
mysql_admin_ready=0

build_app_hosts() {
  local primary="$1"
  printf '%s\n' "$primary"
  if [ "$primary" = "localhost" ]; then
    printf '%s\n' "127.0.0.1"
  elif [ "$primary" = "127.0.0.1" ]; then
    printf '%s\n' "localhost"
  fi
}

wait_for_mariadb() {
  local host="$1"
  local port="$2"
  local attempt
  for attempt in $(seq 1 45); do
    if python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(1.5)
try:
    sock.connect((host, port))
except Exception:
    raise SystemExit(1)
finally:
    sock.close()
PY
    then
      return 0
    fi
    sleep 2
  done
  return 1
}

ensure_local_mariadb() {
  if [ "$(uname -s)" != "Darwin" ]; then
    return 0
  fi
  if ! command -v brew >/dev/null 2>&1; then
    return 0
  fi
  if wait_for_mariadb "$DB_HOST" "$DB_PORT"; then
    return 0
  fi

  if ! brew list mariadb >/dev/null 2>&1; then
    echo "[mariadb] installiere Homebrew MariaDB..."
    brew install mariadb
  fi

  echo "[mariadb] starte lokalen MariaDB-Service..."
  brew services start mariadb >/dev/null 2>&1 || true

  if ! wait_for_mariadb "$DB_HOST" "$DB_PORT"; then
    echo "Fehler: Lokaler MariaDB-Service ist nach brew services start nicht erreichbar (${DB_HOST}:${DB_PORT})." >&2
    exit 1
  fi
}

detect_mysql_admin_client() {
  if ! command -v mysql >/dev/null 2>&1; then
    return 1
  fi

  local -a candidate
  local probe_sql="SELECT 1;"

  if [ -S "$MYSQL_SOCKET" ]; then
    for user in "$CURRENT_USER" "$ROOT_USER"; do
      candidate=(mysql --protocol=socket -S "$MYSQL_SOCKET" -u "$user")
      if [ "$user" = "$ROOT_USER" ] && [ -n "$ROOT_PASS" ]; then
        candidate+=(-p"$ROOT_PASS")
      fi
      if "${candidate[@]}" -Nse "$probe_sql" >/dev/null 2>&1; then
        mysql_admin_client=("${candidate[@]}")
        mysql_admin_ready=1
        return 0
      fi
    done
  fi

  for user in "$ROOT_USER" "$CURRENT_USER"; do
    candidate=(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$user")
    if [ "$user" = "$ROOT_USER" ] && [ -n "$ROOT_PASS" ]; then
      candidate+=(-p"$ROOT_PASS")
    fi
    if "${candidate[@]}" -Nse "$probe_sql" >/dev/null 2>&1; then
      mysql_admin_client=("${candidate[@]}")
      mysql_admin_ready=1
      return 0
    fi
  done

  return 1
}

if [[ ! "$DB_APP_HOST" =~ ^[A-Za-z0-9._%-]+$ ]]; then
  echo "Fehler: MANAGEMOVIE_DB_APP_HOST enthaelt ungueltige Zeichen: $DB_APP_HOST" >&2
  exit 1
fi

if [ -z "$DB_PASS" ]; then
  echo "Fehler: MANAGEMOVIE_DB_PASSWORD fehlt. Bitte in .env.local setzen." >&2
  exit 1
fi

if [ ! -f "$SQL_FILE" ]; then
  echo "Fehler: SQL-Datei fehlt: $SQL_FILE" >&2
  exit 1
fi

ensure_local_mariadb
detect_mysql_admin_client || true

use_local_socket_auth=false
if [ "$DB_HOST" = "localhost" ] || [ "$DB_HOST" = "127.0.0.1" ]; then
  use_local_socket_auth=true
fi

if command -v mysql >/dev/null 2>&1; then
  if [ "$mysql_admin_ready" -eq 1 ]; then
    MYSQL_ROOT=("${mysql_admin_client[@]}")
  else
    MYSQL_ROOT=(mysql -u "$ROOT_USER")
    if [ "$use_local_socket_auth" = false ]; then
      MYSQL_ROOT+=(-h "$DB_HOST" -P "$DB_PORT")
    fi
    if [ -n "$ROOT_PASS" ]; then
      MYSQL_ROOT+=(-p"$ROOT_PASS")
    fi
  fi

  APP_HOSTS=()
  while IFS= read -r app_host_line; do
    [ -n "$app_host_line" ] || continue
    APP_HOSTS+=("$app_host_line")
  done < <(build_app_hosts "$DB_APP_HOST")

  "${MYSQL_ROOT[@]}" <<SQL
CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
FLUSH PRIVILEGES;
SQL

  for app_host in "${APP_HOSTS[@]}"; do
    "${MYSQL_ROOT[@]}" <<SQL
CREATE USER IF NOT EXISTS '${DB_USER}'@'${app_host}'
  IDENTIFIED BY '${DB_PASS}';
ALTER USER '${DB_USER}'@'${app_host}'
  IDENTIFIED BY '${DB_PASS}';
GRANT ALL PRIVILEGES ON \`${DB_NAME}\`.* TO '${DB_USER}'@'${app_host}';
FLUSH PRIVILEGES;
SQL
  done

  # App-User immer via TCP pruefen/verwenden, damit Hostregel exakt gilt.
  MYSQL_APP=(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER")
  if [ -n "$DB_PASS" ]; then
    MYSQL_APP+=(-p"$DB_PASS")
  fi
  MYSQL_APP+=("$DB_NAME")

  "${MYSQL_APP[@]}" < "$SQL_FILE"
else
  PYTHON_BIN="$(mm_venv_python)"
  if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="$(command -v python3 || true)"
  fi
  if [ -z "$PYTHON_BIN" ]; then
    echo "Fehler: weder mysql-Client noch python3 gefunden." >&2
    exit 1
  fi

  DB_HOST="$DB_HOST" \
  DB_PORT="$DB_PORT" \
  DB_NAME="$DB_NAME" \
  DB_USER="$DB_USER" \
  DB_PASS="$DB_PASS" \
  DB_APP_HOST="$DB_APP_HOST" \
  ROOT_USER="$ROOT_USER" \
  ROOT_PASS="$ROOT_PASS" \
  "$PYTHON_BIN" - "$SQL_FILE" <<'PY'
import os
import re
import sys

sql_file = sys.argv[1]

try:
    import pymysql
except Exception as exc:
    raise SystemExit(f"Fehler: PyMySQL fehlt ({exc}). Bitte zuerst ./setup.sh ausfuehren.")

db_host = os.environ["DB_HOST"]
db_port = int(os.environ["DB_PORT"])
db_name = os.environ["DB_NAME"]
db_user = os.environ["DB_USER"]
db_pass = os.environ["DB_PASS"]
db_app_host = os.environ["DB_APP_HOST"]
root_user = os.environ["ROOT_USER"]
root_pass = os.environ["ROOT_PASS"]

safe_db = db_name.replace("`", "")
if not re.fullmatch(r"[A-Za-z0-9._%-]+", db_app_host):
    raise SystemExit(f"Fehler: DB_APP_HOST enthaelt ungueltige Zeichen: {db_app_host}")
hosts = [db_app_host]
if db_app_host == "localhost":
    hosts.append("127.0.0.1")
elif db_app_host == "127.0.0.1":
    hosts.append("localhost")
safe_hosts = [host.replace("'", "") for host in hosts]

with pymysql.connect(
    host=db_host,
    port=db_port,
    user=root_user,
    password=root_pass,
    charset="utf8mb4",
    autocommit=True,
) as conn:
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{safe_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        for safe_host in safe_hosts:
            cur.execute(
                f"CREATE USER IF NOT EXISTS %s@'{safe_host}' IDENTIFIED BY %s",
                (db_user, db_pass),
            )
            cur.execute(
                f"ALTER USER %s@'{safe_host}' IDENTIFIED BY %s",
                (db_user, db_pass),
            )
            cur.execute(
                f"GRANT ALL PRIVILEGES ON `{safe_db}`.* TO %s@'{safe_host}'",
                (db_user,),
            )
        cur.execute("FLUSH PRIVILEGES")

with open(sql_file, "r", encoding="utf-8") as handle:
    sql_raw = handle.read()
statements = [part.strip() for part in sql_raw.split(";") if part.strip()]

with pymysql.connect(
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_pass,
    database=db_name,
    charset="utf8mb4",
    autocommit=True,
) as conn:
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
PY
fi

echo "MariaDB Setup abgeschlossen."
echo "DSN: ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "DB-Hostregel fuer App-User: ${DB_APP_HOST}"
echo "Retention (App): MANAGEMOVIE_DB_RETENTION_DAYS=365"
