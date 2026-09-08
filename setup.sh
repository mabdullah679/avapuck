#!/usr/bin/env bash
# One-command setup for the BigQuery → Spark → Kafka → Hive → Postgres → PDF pipeline.
#
#   ./setup.sh              bring everything up and run the pipeline once
#   ./setup.sh --no-run     set up only, do not run the pipeline
#   ./setup.sh --ci         also start local Jenkins (the `ci` profile)
#   ./setup.sh --merge-env  append variables .env.local lacks, with dev defaults
#   ./setup.sh --rebuild    force a rebuild of the locally-built images
#   ./setup.sh --down       stop the stack (keeps data volumes)
#   ./setup.sh --clean      stop the stack AND delete its data volumes
#
# Works on Linux and macOS, x86_64 and arm64. Needs Docker with the Compose
# plugin, and nothing else -- no Python, no Java, no Kafka client on the host.
#
# Every value the stack needs comes from .env.local, which this script creates
# from .env.example on first run. That file is the single place a value lives
# (docs/DECISIONS.md D12); compose has no built-in defaults.
set -euo pipefail

# Anchor to the repo root regardless of where this was invoked from.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RUN_PIPELINE=1
REBUILD=0
RECREATE=0
MERGE_ENV=0
PROFILES="--profile core"

say()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
ok()   { printf '    \033[32m✔\033[0m %s\n' "$*"; }
warn() { printf '    \033[33m!\033[0m %s\n' "$*"; }
die()  { printf '\n\033[31m✖ %s\033[0m\n' "$*" >&2; exit 1; }

for arg in "$@"; do
  case "$arg" in
    --no-run)  RUN_PIPELINE=0 ;;
    --ci)      PROFILES="$PROFILES --profile ci" ;;
    --merge-env) MERGE_ENV=1 ;;
    --rebuild) REBUILD=1 ;;
    --down|--clean) ;;   # handled after the env file exists
    -h|--help) sed -n '2,14p' "$0" | cut -c3-; exit 0 ;;
    *) echo "unknown option: $arg (try --help)" >&2; exit 2 ;;
  esac
done

# ─────────────────────────────────────────────────────────────────────────
say "Environment file"
if [ ! -f .env.local ]; then
  cp .env.example .env.local
  chmod 600 .env.local
  ok "created .env.local from .env.example with DEV defaults"
  warn "every password in it is a dev default; change them before anywhere shared"
else
  ok ".env.local present"
fi
# A variable the template declares but .env.local lacks would fail `up` with
# a compose error naming one variable at a time. Name them all now instead.
MISSING=$(comm -23 \
  <(grep -E '^[A-Z0-9_]+=' .env.example | cut -d= -f1 | sort) \
  <(grep -E '^[A-Z0-9_]+=' .env.local   | cut -d= -f1 | sort) || true)
if [ -n "$MISSING" ]; then
  if [ "$MERGE_ENV" = "1" ]; then
    # Append each missing variable with its DEV default from the template.
    # Existing values are never touched; the appended block is marked so it
    # is easy to find and change.
    {
      printf '\n# ── appended by setup.sh --merge-env on %s (dev defaults; review) ──\n' "$(date -u +%Y-%m-%dT%H:%MZ)"
      for k in $MISSING; do grep -E "^${k}=" .env.example | head -1; done
    } >> .env.local
    ok "appended $(echo "$MISSING" | wc -l | tr -d ' ') missing variable(s) to .env.local with dev defaults:"
    echo "$MISSING" | sed 's/^/        /'
  else
    warn ".env.local is missing variables that .env.example declares:"
    echo "$MISSING" | sed 's/^/        /'
    die "add them to .env.local (copy the lines from .env.example), or re-run with --merge-env to append dev defaults"
  fi
fi
COMPOSE="docker compose --env-file .env.local $PROFILES --profile ci --profile ranger"
COMPOSE_UP="docker compose --env-file .env.local $PROFILES"

for arg in "$@"; do
  case "$arg" in
    --down)  $COMPOSE down; exit 0 ;;
    --clean) $COMPOSE down -v; exit 0 ;;
  esac
done

# ─────────────────────────────────────────────────────────────────────────
say "Checking prerequisites"

command -v docker >/dev/null 2>&1 || die \
  "docker not found. Install Docker Engine (Linux) or Docker Desktop (macOS/Windows).
   Ubuntu: https://docs.docker.com/engine/install/ubuntu/"

docker compose version >/dev/null 2>&1 || die \
  "the Docker Compose plugin is missing (\`docker compose\`, not \`docker-compose\`).
   Ubuntu: sudo apt-get install docker-compose-plugin"

# Docker Desktop that is stopped or PAUSED answers with an error; try to start
# it before giving up (a paused Desktop needs a restart to unpause).
if ! docker info >/dev/null 2>&1; then
  if docker desktop status >/dev/null 2>&1; then
    warn "Docker Desktop is not answering; asking it to (re)start"
    docker desktop restart >/dev/null 2>&1 || docker desktop start >/dev/null 2>&1 || true
    for i in $(seq 1 24); do docker info >/dev/null 2>&1 && break; sleep 5; done
  fi
fi
docker info >/dev/null 2>&1 || die \
  "cannot talk to the Docker daemon.
   Is it running?  sudo systemctl start docker   (Docker Desktop: open it, or unpause it)
   Permission denied? Add yourself to the docker group:
       sudo usermod -aG docker \$USER   # then log out and back in"

ok "docker $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo present)"
ok "compose $(docker compose version --short 2>/dev/null || echo present)"

# Memory. The core profile's caps total ~15 GB, but a cap is a ceiling, not
# a reservation: measured working set across a full run is about 4.7 GB.
MEM_MB=$(docker info --format '{{.MemTotal}}' 2>/dev/null | awk '{printf "%d", $1/1048576}')
if [ -n "${MEM_MB:-}" ] && [ "$MEM_MB" -gt 0 ]; then
  if   [ "$MEM_MB" -lt 5000 ]; then
    warn "Docker sees only ${MEM_MB} MiB. Spark and Hive will likely be OOM-killed."
    warn "Give Docker at least 6 GB (8 GB comfortable) and re-run."
  elif [ "$MEM_MB" -lt 7500 ]; then
    warn "Docker sees ${MEM_MB} MiB. Workable, but 8 GB is more comfortable."
  else
    ok "memory: ${MEM_MB} MiB"
  fi
fi

DISK_AVAIL=$(df -Pm . | awk 'NR==2 {print $4}')
[ "${DISK_AVAIL:-0}" -lt 15000 ] \
  && warn "only ${DISK_AVAIL} MiB free here; images need roughly 15 GB" \
  || ok "disk: ${DISK_AVAIL} MiB free"

# ── Platform detection ───────────────────────────────────────────────────
# WSL2 has two failure modes that do not exist on native Linux or macOS, and
# both are silent -- the stack starts and then behaves badly.
PLATFORM="$(uname -s)"
IS_WSL=0
if grep -qi microsoft /proc/version 2>/dev/null; then
  IS_WSL=1
  PLATFORM="WSL2"
fi
ok "platform: $PLATFORM on $(uname -m)"

if [ "$IS_WSL" = "1" ]; then
  case "$(pwd -P)" in
    /mnt/*)
      warn "this repo lives on the Windows filesystem ($(pwd -P))."
      warn "Bind-mount I/O there is ~10x slower and Spark/Hive will crawl."
      warn "Move it into the Linux filesystem and re-run:"
      warn "    cp -r \"$(pwd -P)\" ~/avapuck && cd ~/avapuck && ./setup.sh"
      if { : </dev/tty; } 2>/dev/null; then
        printf '    continue anyway? [y/N] '
        read -r reply </dev/tty || reply=n
      else
        reply=n
        warn "no terminal to prompt on; refusing to continue from a /mnt path"
      fi
      case "$reply" in [Yy]*) ;; *) die "moved nothing; re-run from a Linux path." ;; esac
      ;;
    *) ok "repo is on the Linux filesystem (fast bind mounts)" ;;
  esac
  WSL_MEM=$(awk '/MemTotal/ {printf "%d", $2/1024}' /proc/meminfo 2>/dev/null || echo 0)
  if [ "${WSL_MEM:-0}" -gt 0 ] && [ "$WSL_MEM" -lt 11000 ]; then
    warn "WSL2 has ${WSL_MEM} MiB of RAM; this stack wants 12 GB+."
    warn "In Windows, create %UserProfile%\\.wslconfig containing:"
    warn "    [wsl2]"
    warn "    memory=12GB"
    warn "    processors=4"
    warn "then run  wsl --shutdown  in PowerShell and start WSL again."
  fi
fi

# ─────────────────────────────────────────────────────────────────────────
say "Generating TLS certificates"
if [ -f certs/ca.crt ]; then
  ok "certs/ already present (delete it or run scripts/gen_certs.sh --force to regenerate)"
else
  ./scripts/gen_certs.sh >/dev/null
  ok "wrote a local CA and the idp/crypto server certificates into certs/"
  # Containers that were running when certs/ was recreated hold the OLD
  # directory inode and see an empty /certs; recreate them in dependency order.
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^pl-'; then
    warn "the stack was running while certificates were regenerated"
    warn "recreating every service so the new CA is actually mounted"
    RECREATE=1
  fi
fi

# ─────────────────────────────────────────────────────────────────────────
say "Preparing directories and git hooks"
mkdir -p data/csv data/parquet data/hive data/reports data/cards data/incoming pipeline-reports secrets
ok "data/, pipeline-reports/ and secrets/ ready"

if [ -d .git ]; then
  install -m 0755 scripts/git-hooks/pre-push .git/hooks/pre-push
  install -m 0755 scripts/git-hooks/post-commit .git/hooks/post-commit
  ok "git hooks installed: pre-push runs make ci; post-commit pokes Jenkins"
fi

if [ -f secrets/gcp-sa.json ]; then
  ok "found secrets/gcp-sa.json — live BigQuery is available"
  # The path in .env.local is used INSIDE the container, where the key is
  # bind-mounted at /secrets. A relative host path there fails every attempt.
  CRED=$(grep -E '^GOOGLE_APPLICATION_CREDENTIALS=' .env.local | tail -1 | cut -d= -f2-)
  case "$CRED" in
    /*) ;;
    *)  warn "GOOGLE_APPLICATION_CREDENTIALS in .env.local is '$CRED', a relative path."
        warn "Inside the container the key is at /secrets/gcp-sa.json; set it to that." ;;
  esac
else
  warn "no secrets/gcp-sa.json — the pipeline will use the offline fixture."
  warn "That is the expected default and needs no cloud account. To use live"
  warn "BigQuery, see the 'If you want live BigQuery' section of README.md."
fi

# ─────────────────────────────────────────────────────────────────────────
say "Building and starting the stack"
warn "First run pulls and builds several images. Expect 10-20 minutes."

[ "$REBUILD" = "1" ] && $COMPOSE_UP build --no-cache
if [ "$RECREATE" = "1" ]; then
  $COMPOSE_UP up -d --build --force-recreate
else
  $COMPOSE_UP up -d --build
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
for c in pl-idp pl-crypto pl-postgres pl-kafka pl-spark-master pl-airflow pl-kafka-ui pl-watchdog pl-tap pl-scaler pl-console; do
  wait_healthy "$c" 72 || FAILED=1
done
case "$PROFILES" in *ci*) wait_healthy pl-jenkins 72 || warn "Jenkins did not come up; the pipeline runs without it" ;; esac

# HiveServer2 has no healthcheck; it is ready when a Hive client can list
# databases. Probing with /dev/tcp does NOT work here -- this image's /bin/sh
# is dash.
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
[ "$HS2_READY" = "0" ] && printf '\033[33mnot ready (Hive registration will be reported as degraded; the rest still runs)\033[0m\n'

[ "$FAILED" = "1" ] && die \
  "one or more services did not come up.
   Look at: $COMPOSE_UP logs --tail 50 <service>"

say "Creating Kafka topics and ACLs"
$COMPOSE_UP up kafka-init >/dev/null 2>&1 || \
  warn "topic setup reported an error; check: $COMPOSE_UP logs kafka-init"
docker exec pl-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --command-config /tmp/admin.properties \
  --list 2>/dev/null | sed 's/^/    /' | grep . || warn "no topics found"

# ─────────────────────────────────────────────────────────────────────────
say "Applying the warehouse schema (idempotent)"
# Postgres only runs init SQL on a FRESH volume. Every statement in the file
# is IF NOT EXISTS, so applying it on every run keeps an older volume current
# without touching data. Persistence is never rebuilt, only brought up to date.
PG_USER=$(grep -E '^POSTGRES_USER=' .env.local | tail -1 | cut -d= -f2-)
PG_DB=$(grep -E '^POSTGRES_DB=' .env.local | tail -1 | cut -d= -f2-)
if docker exec -i pl-postgres psql -q -v ON_ERROR_STOP=1 -U "${PG_USER:-pipeline}" -d "${PG_DB:-analytics}" \
     < docker/init-warehouse.sql >/dev/null 2>&1; then
  ok "warehouse, consumer and monitor schemas present"
else
  warn "could not apply docker/init-warehouse.sql; the services create their tables lazily"
fi

# ─────────────────────────────────────────────────────────────────────────
say "Unpausing the DAGs"
# The consumer DAG's asset watcher only listens while its DAG is unpaused.
for d in trips_pipeline on_run_verdict; do
  for i in $(seq 1 24); do
    docker exec pl-airflow airflow dags list -o plain 2>/dev/null | grep -q "^$d " && break
    sleep 5
  done
  docker exec pl-airflow airflow dags unpause "$d" >/dev/null 2>&1 && ok "$d unpaused" || warn "$d not found yet"
done

# ─────────────────────────────────────────────────────────────────────────
if [ "$RUN_PIPELINE" = "1" ]; then
  # The run slot is the current hour, UTC. Airflow schedules in UTC and every
  # date on the PDF and in the verdict is UTC with a Z suffix -- after 20:00
  # Eastern the slot date is already tomorrow, and that is correct.
  RUN_SLOT=$(date -u +%Y-%m-%dT%H:00:00)
  SLOT_LABEL=$(date -u +%Y%m%dT%H00Z)
  say "Running the pipeline end to end for slot ${RUN_SLOT}Z"
  # Airflow needs a moment after its healthcheck passes before the DAG is
  # parsed and runnable; a run started too early fails with "DAG not found".
  for i in $(seq 1 24); do
    docker exec pl-airflow airflow dags list 2>/dev/null | grep -q trips_pipeline && break
    printf '.'; sleep 5
  done
  RUN_LOG=$(mktemp)
  docker exec pl-airflow airflow dags test trips_pipeline "$RUN_SLOT" > "$RUN_LOG" 2>&1 || true
  grep -oE "(▶ START|✔ SUCCESS|✖ [A-Z]+) +[A-Z_]+( in [0-9.]+s)?" "$RUN_LOG" \
    | awk '!seen[$0]++' | sed 's/^/    /'
  if ! grep -q "✔ SUCCESS  VERDICT" "$RUN_LOG"; then
    warn "the run did not reach the verdict; last lines of the Airflow output:"
    grep -E "Traceback|Error|ERROR|raise |REFUSING|ContractError" "$RUN_LOG" | tail -8 | sed 's/^/        /'
    warn "full output: $RUN_LOG"
  fi

  # The PDF for THIS slot, not whatever is newest in the folder.
  PDF=$(ls pipeline-reports/*_report_${SLOT_LABEL}.pdf 2>/dev/null | head -1 || true)
  if [ -n "$PDF" ]; then
    ok "report written: $PDF"
  else
    warn "no PDF for slot ${SLOT_LABEL} in pipeline-reports/ — check the stage output above"
  fi
  printf '    verdict: '
  curl -sf http://localhost:8446/status 2>/dev/null \
    | sed -E 's/.*"healthy": ?(true|false).*/watchdog healthy=\1/' || echo "(watchdog not reachable)"
fi

# ─────────────────────────────────────────────────────────────────────────
say "Ready"
cat <<'EOF'
    Console        http://localhost:8447   upload a CSV, run it, watch it land
                   login: CONSOLE_ADMIN_USER / CLIENT_SECRET_CONSOLE_ADMIN in .env.local
    Airflow UI     http://localhost:8085   login: AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD
    Kafka UI       http://localhost:8090   login: KAFKA_UI_LOGIN_USER / _PASSWORD
    Watchdog       http://localhost:8446/status
    Scaler         http://localhost:8448/status
    Spark UI       http://localhost:8080
    Jenkins        http://localhost:8095   (./setup.sh --ci; login: JENKINS_ADMIN_*)
    reports        pipeline-reports/       one PDF per run slot, UTC

    Run again      docker exec pl-airflow airflow dags test trips_pipeline 2026-09-13T04:00:00
    Scale test     python3 scripts/make_test_csv.py --rows 100000 > /tmp/small.csv   then upload it
    Tests + CI     make ci
    Stop           ./setup.sh --down
    Stop + wipe    ./setup.sh --clean
EOF
