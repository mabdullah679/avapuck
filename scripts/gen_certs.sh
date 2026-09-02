#!/usr/bin/env bash
# Generate the local dev CA and the IdP/crypto server certificates.
#
# These are SELF-SIGNED and for local development only. The CA private key
# sits next to the certs it signs, so this trust root is worth exactly as
# much as this laptop. For a real deployment, replace certs/*.crt and
# certs/*.key with company-issued material and leave everything else alone --
# the services and clients only ever read the files, never generate them.
#
# Safe to re-run: refuses to clobber existing material unless --force.
set -euo pipefail
cd "$(dirname "$0")/.."
CERTS=certs

if [[ -f $CERTS/ca.crt && "${1:-}" != "--force" ]]; then
  echo "certs/ already populated; re-run with --force to regenerate." >&2
  echo "Regenerating invalidates every client that trusts the current CA." >&2
  exit 1
fi

mkdir -p $CERTS
openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
  -keyout $CERTS/ca.key -out $CERTS/ca.crt \
  -subj "/C=US/O=avapuck-dev/CN=avapuck-dev-ca" 2>/dev/null

gen_svc () {
  local NAME=$1
  cat > $CERTS/${NAME}.cnf <<EOF
[req]
distinguished_name = dn
req_extensions = v3_req
prompt = no
[dn]
C = US
O = avapuck-dev
CN = ${NAME}
[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt
[alt]
DNS.1 = ${NAME}
DNS.2 = localhost
DNS.3 = pl-${NAME}
IP.1 = 127.0.0.1
EOF
  openssl req -newkey rsa:2048 -nodes \
    -keyout $CERTS/${NAME}.key -out $CERTS/${NAME}.csr \
    -config $CERTS/${NAME}.cnf 2>/dev/null
  openssl x509 -req -in $CERTS/${NAME}.csr \
    -CA $CERTS/ca.crt -CAkey $CERTS/ca.key -CAcreateserial \
    -out $CERTS/${NAME}.crt -days 825 -sha256 \
    -extfile $CERTS/${NAME}.cnf -extensions v3_req 2>/dev/null
  rm -f $CERTS/${NAME}.csr
}

gen_svc idp
gen_svc crypto

# Certs are world-readable (every client needs the CA); keys are not.
# Docker Desktop maps the bind-mount owner onto the container user, so the
# service (uid 10001) reads its own key through 640 without it being
# world-readable. On a Linux host with a literal uid mismatch this is where
# you would grant access via group ownership -- never by loosening to 644.
chmod 644 $CERTS/*.crt
chmod 640 $CERTS/*.key

openssl verify -CAfile $CERTS/ca.crt $CERTS/idp.crt $CERTS/crypto.crt
echo "Wrote CA + idp/crypto certs to $CERTS/ (dev only, do not deploy)."
