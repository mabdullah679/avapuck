#!/usr/bin/env bash
# Create the Secret Manager entries this pipeline expects, with generated
# values. Run ONCE per project. Safe to re-run: existing secrets are skipped,
# never overwritten -- overwriting a live key would break running jobs.
set -euo pipefail

PROJECT="${GCP_PROJECT_ID:?set GCP_PROJECT_ID first}"

create() {
  local name="$1" value="$2"
  if gcloud secrets describe "$name" --project "$PROJECT" >/dev/null 2>&1; then
    echo "  exists, skipping: $name"
    return
  fi
  gcloud secrets create "$name" --project "$PROJECT" --replication-policy=automatic >/dev/null
  printf '%s' "$value" | gcloud secrets versions add "$name" --project "$PROJECT" --data-file=- >/dev/null
  echo "  created: $name"
}

echo "Creating pipeline secrets in project $PROJECT"
create pipeline-postgres-password      "$(openssl rand -base64 24)"
create pipeline-crypto-master-key      "$(openssl rand -base64 32)"   # AES-256
create pipeline-blind-index-key        "$(openssl rand -base64 32)"   # HMAC-SHA256
create pipeline-ranger-admin-password  "$(openssl rand -base64 24)"
create pipeline-airflow-fernet-key     "$(python3 -c 'from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())')"

if ! gcloud secrets describe pipeline-idp-signing-key --project "$PROJECT" >/dev/null 2>&1; then
  tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT
  openssl genrsa -out "$tmp" 2048 2>/dev/null
  gcloud secrets create pipeline-idp-signing-key --project "$PROJECT" --replication-policy=automatic >/dev/null
  gcloud secrets versions add pipeline-idp-signing-key --project "$PROJECT" --data-file="$tmp" >/dev/null
  echo "  created: pipeline-idp-signing-key (RSA 2048)"
else
  echo "  exists, skipping: pipeline-idp-signing-key"
fi

echo
echo "Done. Now run: make secrets-sync"
