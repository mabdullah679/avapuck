#!/usr/bin/env bash
# One-command setup for the BigQuery → Spark → Hive → Postgres → PDF pipeline.
#
#   ./setup.sh              bring everything up and run the pipeline once
#   ./setup.sh --no-run     set up only, do not run the pipeline
#   ./setup.sh --rebuild    force a rebuild of the locally-built images
#   ./setup.sh --down       stop the stack (keeps data volumes)
#   ./setup.sh --clean      stop the stack AND delete its data volumes
#
# Works on Linux and macOS, x86_64 and arm64. Needs Docker with the Compose
# plugin, and nothing else -- no Python, no Java, no Kafka client on the host.
#
# Every path here is relative to the repository root, which is derived from
# this script's own location. The repo can therefore live anywhere.
set -euo pipefail

# Anchor to the repo root regardless of where this was invoked from. Every
# subsequent path in this script is relative to that.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RUN_PIPELINE=1
REBUILD=0
RECREATE=0
COMPOSE_ARGS="--profile core"

for arg in "$@"; do
  case "$arg" in
    --no-run)  RUN_PIPELINE=0 ;;
    --rebuild) REBUILD=1 ;;
    --down)    docker compose $COMPOSE_ARGS down; exit 0 ;;
    --clean)   docker compose $COMPOSE_ARGS down -v; exit 0 ;;
    -h|--help) sed -n '2,12p' "$0" | cut -c3-; exit 0 ;;
    *) echo "unknown option: $arg (try --help)" >&2; exit 2 ;;
  esac
done

say()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
ok()   { printf '    \033[32m✔\033[0m %s\n' "$*"; }
warn() { printf '    \033[33m!\033[0m %s\n' "$*"; }
die()  { printf '\n\033[31m✖ %s\033[0m\n' "$*" >&2; exit 1; }

# ─────────────────────────────────────────────────────────────────────────
say "Checking prerequisites"

command -v docker >/dev/null 2>&1 || die \
  "docker not found. Install Docker Engine (Linux) or Docker Desktop (macOS/Windows).
   Ubuntu: https://docs.docker.com/engine/install/ubuntu/"

docker compose version >/dev/null 2>&1 || die \
  "the Docker Compose plugin is missing (\`docker compose\`, not \`docker-compose\`).
   Ubuntu: sudo apt-get install docker-compose-plugin"

docker info >/dev/null 2>&1 || die \
  "cannot talk to the Docker daemon.
   Is it running?  sudo systemctl start docker
   Permission denied? Add yourself to the docker group:
       sudo usermod -aG docker \$USER   # then log out and back in"

ok "docker $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo present)"
ok "compose $(docker compose version --short 2>/dev/null || echo present)"

# Memory. The core profile's limits total ~16.5 GB, but those are CAPS, not
# reservations -- the stack idles far below that. Below 8 GB, though, Spark
# and Hive get OOM-killed mid-run, which surfaces as a confusing stage
# failure rather than an obvious out-of-memory error.
MEM_MB=$(docker info --format '{{.MemTotal}}' 2>/dev/null | awk '{printf "%d", $1/1048576}')
if [ -n "${MEM_MB:-}" ] && [ "$MEM_MB" -gt 0 ]; then
  if   [ "$MEM_MB" -lt 7000 ]; then
    warn "Docker sees only ${MEM_MB} MiB. Spark and Hive will likely be OOM-killed."
    warn "Give the VM at least 12 GB (16 GB comfortable) and re-run."
  elif [ "$MEM_MB" -lt 11000 ]; then
    warn "Docker sees ${MEM_MB} MiB. Workable, but 12 GB+ is recommended."
  else
    ok "memory: ${MEM_MB} MiB"
  fi
fi

DISK_AVAIL=$(df -Pm . | awk 'NR==2 {print $4}')
[ "${DISK_AVAIL:-0}" -lt 15000 ] \
  && warn "only ${DISK_AVAIL} MiB free here; images need roughly 15 GB" \
  || ok "disk: ${DISK_AVAIL} MiB free"

ok "architecture: $(uname -m) ($(uname -s))"

# ─────────────────────────────────────────────────────────────────────────
say "Generating TLS certificates"
# No private keys are committed, so every machine generates its own local CA.
if [ -f certs/ca.crt ]; then
  ok "certs/ already present (delete it or run scripts/gen_certs.sh --force to regenerate)"
else
  ./scripts/gen_certs.sh >/dev/null
  ok "wrote a local CA and the idp/crypto server certificates into certs/"
  # Containers bind-mount certs/ and read it at startup. If any were already
  # running when this directory was recreated, they hold a handle to the OLD
  # (now deleted) directory inode and see an empty /certs -- every TLS call
  # then fails closed, which is correct behaviour with a baffling symptom.
  # Flag it and let the normal `up -d` below recreate them in dependency
  # order; recreating a subset here races with the services still starting.
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^pl-'; then
    warn "the stack was running while certificates were regenerated"
    warn "recreating every service so the new CA is actually mounted"
    RECREATE=1
  fi
fi

# ─────────────────────────────────────────────────────────────────────────
say "Preparing directories"
# Bind-mount targets must exist before compose starts, or Docker creates them
# root-owned and the containers cannot write to them.
mkdir -p data/csv data/parquet data/hive data/reports data/cards pipeline-reports secrets
ok "data/, pipeline-reports/ and secrets/ ready"

if [ -f secrets/gcp-sa.json ]; then
  ok "found secrets/gcp-sa.json — live BigQuery is available"
else
  warn "no secrets/gcp-sa.json — the pipeline will use the offline fixture."
  warn "That is the expected default and needs no cloud account. To use live"
  warn "BigQuery, see the 'If you want live BigQuery' section of README.md."
fi

# ─────────────────────────────────────────────────────────────────────────
say "Building and starting the stack"
warn "First run pulls and builds several images. Expect 10-20 minutes."

[ "$REBUILD" = "1" ] && docker compose $COMPOSE_ARGS build --no-cache
if [ "$RECREATE" = "1" ]; then
  docker compose $COMPOSE_ARGS up -d --build --force-recreate
else
  docker compose $COMPOSE_ARGS up -d --build
fi

# ─────────────────────────────────────────────────────────────────────────
say "Waiting for services"

wait_healthy() {
  local name=$1 tries=${2:-60} i=0 st
  printf '    %-22s ' "$name"
  while [ $i -lt "$tries" ]; do
    st=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$name" 2>/dev/null || echo missing)
    case "$st" in
      healthy|running) printf '\033[32m%s\033[0m\n' "$st"; return 0 ;;
      exited|dead)     printf '\033[31m%s\033[0m\n' "$st"; return 1 ;;
    esac
    printf '.'; sleep 5; i=$((i+1))
  done
  printf '\033[31mtimed out (%s)\033[0m\n' "$st"; return 1
}

FAILED=0
for c in pl-idp pl-crypto pl-postgres pl-kafka pl-spark-master pl-airflow; do
  wait_healthy "$c" 72 || FAILED=1
done

# HiveServer2 has no healthcheck; it is ready when a Hive client can list
# databases. Probing with /dev/tcp does NOT work here -- that is a bash
# feature and this image's /bin/sh is dash, so the probe always "failed" and
# reported a healthy Hive as down.
printf '    %-22s ' "pl-hiveserver2"
i=0
HS2_READY=0
while [ $i -lt 60 ]; do
  if docker exec pl-hiveserver2 /opt/hive/bin/beeline \
       -u jdbc:hive2://localhost:10000 --silent=true \
       -e 'show databases;' >/dev/null 2>&1; then
    printf '\033[32mready\033[0m\n'; HS2_READY=1; break
  fi
  printf '.'; sleep 5; i=$((i+1))
done
[ "$HS2_READY" = "0" ] && printf '\033[33mnot ready (Hive registration will skip; the rest still runs)\033[0m\n'

[ "$FAILED" = "1" ] && die \
  "one or more services did not come up.
   Look at: docker compose $COMPOSE_ARGS logs --tail 50 <service>"

say "Creating Kafka topics and ACLs"
# Idempotent: --if-not-exists on the topics, and re-adding an existing ACL is
# a no-op. Reports the resulting topic list rather than only new creations, so
# a second run does not look like it did nothing.
docker compose $COMPOSE_ARGS up kafka-init >/dev/null 2>&1 || \
  warn "topic setup reported an error; check: docker compose $COMPOSE_ARGS logs kafka-init"
docker exec pl-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --command-config /etc/kafka/admin.properties \
  --list 2>/dev/null | sed 's/^/    /' | grep . || warn "no topics found"

# ─────────────────────────────────────────────────────────────────────────
if [ "$RUN_PIPELINE" = "1" ]; then
  say "Running the pipeline end to end"
  RUN_DATE=$(date -u +%Y-%m-%d)
  # Airflow emits each line twice (its own handler plus the task handler);
  # keep one copy, and strip the timestamp prefix so the stage flow reads
  # cleanly. The full logs are in the UI and in `docker compose logs`.
  docker exec pl-airflow airflow dags test trips_pipeline "$RUN_DATE" 2>&1 \
    | grep -oE "(▶ START|✔ SUCCESS|✖ [A-Z]+) +[A-Z_]+( in [0-9.]+s)?" \
    | awk '!seen[$0]++' | sed 's/^/    /' || true

  PDF=$(ls -t pipeline-reports/*.pdf 2>/dev/null | head -1 || true)
  if [ -n "$PDF" ]; then
    ok "report written: $PDF"
  else
    warn "no PDF in pipeline-reports/ — check the stage output above"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────
say "Ready"
cat <<'EOF'
    Airflow UI     http://localhost:8085   (user: admin)
    password       docker exec pl-airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
    Spark UI       http://localhost:8080
    reports        pipeline-reports/

    Run again      docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13
    Stop           ./setup.sh --down
    Stop + wipe    ./setup.sh --clean
EOF
