#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/scripts/lib/common.sh"
mm_cd_project_root

DATA_ROOT="$(mm_normalize_data_root "${MANAGEMOVIE_DATA_ROOT:-}")"
export MANAGEMOVIE_DATA_ROOT="$DATA_ROOT"

CERT_BASE="$DATA_ROOT/certs"
CA_DIR="$CERT_BASE/ca"
SERVER_DIR="$CERT_BASE/server"

mkdir -p "$CA_DIR" "$SERVER_DIR"

CA_KEY="$CA_DIR/managemovie-local-ca.key"
CA_CRT="$CA_DIR/managemovie-local-ca.crt"
SERVER_KEY="$SERVER_DIR/managemovie-local.key"
SERVER_CSR="$SERVER_DIR/managemovie-local.csr"
SERVER_CRT="$SERVER_DIR/managemovie-local.crt"
OPENSSL_CNF="$SERVER_DIR/openssl-local.cnf"

if [ ! -f "$CA_KEY" ]; then
  openssl genrsa -out "$CA_KEY" 4096
fi

if [ ! -f "$CA_CRT" ]; then
  openssl req -x509 -new -nodes \
    -key "$CA_KEY" \
    -sha256 -days 3650 \
    -out "$CA_CRT" \
    -subj "/C=DE/ST=Local/L=Local/O=ManageMovie/CN=ManageMovie Local Root CA"
fi

LAN_IP=""
OS_NAME="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [ "$OS_NAME" = "darwin" ]; then
  LAN_IP="$(ipconfig getifaddr en0 2>/dev/null || true)"
  if [ -z "$LAN_IP" ]; then
    LAN_IP="$(ipconfig getifaddr en1 2>/dev/null || true)"
  fi
else
  LAN_IP="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"
fi

HOSTNAME_FQDN="$(hostname)"

cat > "$OPENSSL_CNF" <<CNF
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = req_ext

[ dn ]
C = DE
ST = Local
L = Local
O = ManageMovie
CN = localhost

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = ${HOSTNAME_FQDN}
IP.1 = 127.0.0.1
CNF

if [ -n "$LAN_IP" ]; then
  echo "IP.2 = ${LAN_IP}" >> "$OPENSSL_CNF"
fi

openssl genrsa -out "$SERVER_KEY" 2048
openssl req -new -key "$SERVER_KEY" -out "$SERVER_CSR" -config "$OPENSSL_CNF"
openssl x509 -req \
  -in "$SERVER_CSR" \
  -CA "$CA_CRT" \
  -CAkey "$CA_KEY" \
  -CAcreateserial \
  -out "$SERVER_CRT" \
  -days 825 -sha256 \
  -extensions req_ext \
  -extfile "$OPENSSL_CNF"

chmod 600 "$CA_KEY" "$SERVER_KEY"
chmod 644 "$CA_CRT" "$SERVER_CRT"

if [ "$OS_NAME" = "darwin" ] && command -v security >/dev/null 2>&1; then
  security add-trusted-cert -d -r trustRoot -k "$HOME/Library/Keychains/login.keychain-db" "$CA_CRT" || true
  echo "CA in macOS Keychain importiert."
else
  echo "CA erzeugt. Import in lokalen Trust-Store bei Bedarf manuell durchfuehren."
fi

echo "HTTPS Setup abgeschlossen"
echo "Datenpfad: $DATA_ROOT"
echo "CA:     $CA_CRT"
echo "Server: $SERVER_CRT"
echo "Key:    $SERVER_KEY"
