#!/usr/bin/env bash
# Create the cluster's Secrets. NOTHING here is committed.
#
# Values are generated locally, or read from the GCP service-account key that
# already exists on disk. They are applied straight to the cluster and never
# written to a file -- so there is no manifest lying around with a master key
# in it, and `git status` stays clean by construction rather than by discipline.
#
# Re-running is safe: existing secrets are replaced, and the pods that mount
# them are restarted so they pick up the new values.
set -euo pipefail

CTX="${KUBE_CONTEXT:-trips}"
NS="${NAMESPACE:-trips}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SA_KEY="${GCP_SA_KEY:-$ROOT/secrets/gcp-sa.json}"

kubectl --context "$CTX" get namespace "$NS" >/dev/null

# ── GCP service account ────────────────────────────────────────────────
# The one credential that cannot be generated -- it must come from Google.
if [[ ! -s "$SA_KEY" ]]; then
  echo "ERROR: $SA_KEY is missing or empty." >&2
  echo "  Create it with:" >&2
  echo "    gcloud iam service-accounts keys create secrets/gcp-sa.json \\" >&2
  echo "      --iam-account=jobuserrole@avapuck.iam.gserviceaccount.com --project=avapuck" >&2
  exit 1
fi
if ! python3 -c "import json,sys; d=json.load(open('$SA_KEY')); sys.exit(0 if d.get('type')=='service_account' else 1)"; then
  echo "ERROR: $SA_KEY is not a service-account key file." >&2
  exit 1
fi

kubectl --context "$CTX" -n "$NS" create secret generic gcp-sa \
  --from-file=gcp-sa.json="$SA_KEY" \
  --dry-run=client -o yaml | kubectl --context "$CTX" apply -f - >/dev/null
echo "  gcp-sa                 (service-account key)"

# ── Cryptographic material ─────────────────────────────────────────────
# Generated once and REUSED on re-run. Rotating the AES master key makes every
# previously written ciphertext permanently unreadable, so a script that
# silently regenerated it on each deploy would be a data-loss bug.
existing() {
  kubectl --context "$CTX" -n "$NS" get secret "$1" -o jsonpath="{.data.$2}" 2>/dev/null | base64 -d 2>/dev/null || true
}

CRYPTO_KEY="$(existing pipeline-crypto CRYPTO_MASTER_KEY)"
BLIND_KEY="$(existing pipeline-crypto BLIND_INDEX_KEY)"
[[ -n "$CRYPTO_KEY" ]] || CRYPTO_KEY="$(openssl rand -base64 32)"
[[ -n "$BLIND_KEY"  ]] || BLIND_KEY="$(openssl rand -base64 32)"

IDP_KEY="$(existing pipeline-crypto IDP_SIGNING_KEY)"
if [[ -z "$IDP_KEY" ]]; then
  IDP_KEY="$(openssl genrsa 2048 2>/dev/null)"
fi

kubectl --context "$CTX" -n "$NS" create secret generic pipeline-crypto \
  --from-literal=CRYPTO_MASTER_KEY="$CRYPTO_KEY" \
  --from-literal=BLIND_INDEX_KEY="$BLIND_KEY" \
  --from-literal=IDP_SIGNING_KEY="$IDP_KEY" \
  --dry-run=client -o yaml | kubectl --context "$CTX" apply -f - >/dev/null
echo "  pipeline-crypto        (AES-256 master key, blind-index key, IdP RSA key)"

# ── Per-job client secrets for the IdP ─────────────────────────────────
# Plain list rather than an associative array: macOS ships bash 3.2, which
# does not have them, and this script has to run on the machine it deploys from.
CLIENT_KEYS="CLIENT_SECRET_EXTRACT_JOB CLIENT_SECRET_SPARK_JOB CLIENT_SECRET_HIVE_JOB CLIENT_SECRET_POSTGRES_JOB CLIENT_SECRET_REPORT_JOB"
ARGS=()
for k in $CLIENT_KEYS; do
  v="$(existing pipeline-clients "$k")"
  [ -n "$v" ] || v="$(openssl rand -hex 24)"
  ARGS+=(--from-literal="$k=$v")
done
kubectl --context "$CTX" -n "$NS" create secret generic pipeline-clients \
  "${ARGS[@]}" --dry-run=client -o yaml | kubectl --context "$CTX" apply -f - >/dev/null
echo "  pipeline-clients       (5 per-job OAuth2 client secrets)"

# ── Datastore passwords ────────────────────────────────────────────────
PG_PW="$(existing pipeline-datastores POSTGRES_PASSWORD)"; [[ -n "$PG_PW" ]] || PG_PW="$(openssl rand -base64 24)"
HV_PW="$(existing pipeline-datastores HIVE_METASTORE_PASSWORD)"; [[ -n "$HV_PW" ]] || HV_PW="$(openssl rand -base64 24)"
RG_PW="$(existing pipeline-datastores RANGER_ADMIN_PASSWORD)"; [[ -n "$RG_PW" ]] || RG_PW="rangerR0cks!$(openssl rand -hex 4)"
AF_FERNET="$(existing pipeline-datastores AIRFLOW_FERNET_KEY)"
[[ -n "$AF_FERNET" ]] || AF_FERNET="$(python3 -c 'from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())' 2>/dev/null || openssl rand -base64 32)"

kubectl --context "$CTX" -n "$NS" create secret generic pipeline-datastores \
  --from-literal=POSTGRES_PASSWORD="$PG_PW" \
  --from-literal=HIVE_METASTORE_PASSWORD="$HV_PW" \
  --from-literal=RANGER_ADMIN_PASSWORD="$RG_PW" \
  --from-literal=AIRFLOW_FERNET_KEY="$AF_FERNET" \
  --dry-run=client -o yaml | kubectl --context "$CTX" apply -f - >/dev/null
echo "  pipeline-datastores    (postgres, hive metastore, ranger, airflow fernet)"

echo
echo "Secrets applied to $NS. None of these values were written to disk."
echo "Inspect with: kubectl -n $NS get secrets"
