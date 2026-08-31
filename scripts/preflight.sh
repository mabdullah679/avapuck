#!/usr/bin/env bash
# Check everything needed BEFORE deploying. Stops on the first real problem.
#
# Exists because the alternative is discovering a 8GB Docker limit twenty
# minutes into an image build.
set -uo pipefail

OK="  [ok]  "; BAD="  [FAIL]"; WARN="  [warn]"
fail=0

echo "Preflight — trips pipeline"
echo "──────────────────────────────────────────────────────"

need() {
  if command -v "$1" >/dev/null 2>&1; then
    echo "$OK $1 $( "$1" version --short 2>/dev/null | head -1 || true)"
  else
    echo "$BAD $1 not found — $2"; fail=1
  fi
}
need docker   "install Docker Desktop"
need minikube "brew install minikube"
need kubectl  "brew install kubectl"

# Architecture: an amd64 minikube on Apple Silicon runs under Rosetta and
# fights the arm64 images.
if command -v minikube >/dev/null 2>&1; then
  host_arch="$(uname -m)"
  mk_arch="$(file "$(command -v minikube)" 2>/dev/null | grep -oE 'arm64|x86_64' | head -1)"
  if [ "$host_arch" = "arm64" ] && [ "$mk_arch" = "x86_64" ]; then
    echo "$WARN minikube is x86_64 on arm64 hardware — it will run under Rosetta."
    echo "         brew install minikube   (gets the native arm64 build)"
  fi
fi

# Docker daemon + memory
if docker info >/dev/null 2>&1; then
  mem_bytes="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
  mem_gb=$(( mem_bytes / 1073741824 ))
  if [ "$mem_gb" -ge 20 ]; then
    echo "$OK docker memory ${mem_gb}GB"
  else
    echo "$BAD docker memory ${mem_gb}GB — this stack needs 20GB+"
    echo "         raise it in Docker Desktop → Settings → Resources → Memory"
    fail=1
  fi
else
  echo "$BAD docker daemon not reachable — is Docker Desktop running?"; fail=1
fi

# Disk
avail_gb="$(df -g . 2>/dev/null | awk 'NR==2{print $4}')"
if [ -n "${avail_gb:-}" ] && [ "$avail_gb" -lt 30 ]; then
  echo "$WARN only ${avail_gb}GB free — images and volumes need ~25GB"
else
  echo "$OK disk space ${avail_gb:-?}GB free"
fi

# The one credential the operator must supply
SA="${GCP_SA_KEY:-secrets/gcp-sa.json}"
if [ -s "$SA" ] && python3 -c "import json,sys;sys.exit(0 if json.load(open('$SA')).get('type')=='service_account' else 1)" 2>/dev/null; then
  echo "$OK GCP service-account key present"
  perms="$(stat -f '%A' "$SA" 2>/dev/null || stat -c '%a' "$SA" 2>/dev/null)"
  case "$perms" in 600|400) ;; *) echo "$WARN key is mode $perms — run: chmod 600 $SA";; esac
else
  echo "$BAD $SA missing or not a service-account key"
  echo "         gcloud iam service-accounts keys create $SA \\"
  echo "           --iam-account=<sa-email> --project=<project-id>"
  fail=1
fi

# Project id must be the ID string, not the number
pid="$(grep -E '^\s*GCP_PROJECT_ID:' k8s/base/02-config.yaml 2>/dev/null | sed -E 's/.*"(.*)".*/\1/')"
if [ -z "$pid" ]; then
  echo "$BAD GCP_PROJECT_ID not set in k8s/base/02-config.yaml"; fail=1
elif echo "$pid" | grep -qE '^[0-9]+$'; then
  echo "$BAD GCP_PROJECT_ID is '$pid' — that is the project NUMBER, not the ID"
  echo "         gcloud projects describe $pid --format='value(projectId)'"
  fail=1
else
  echo "$OK GCP_PROJECT_ID = $pid"
fi

echo "──────────────────────────────────────────────────────"
if [ "$fail" -eq 0 ]; then
  echo "Ready.  Next:  make -f Makefile.k8s up"
else
  echo "Fix the [FAIL] items above, then re-run."
fi
exit "$fail"
