#!/usr/bin/env bash
# Copy a run's PDF report out of the Airflow container into pipeline-reports/.
#
# Called automatically by the report stage after a successful run (see the
# report task in dags/trips_pipeline_dag.py), and safe to run by hand:
#
#   scripts/collect_report.sh /data/reports/trips_report_2026-09-02.pdf
#   scripts/collect_report.sh            # copies every report in the container
#
# The reports already live on the host via the ./data bind mount; this exists
# so the deliverable PDFs sit in one predictable directory that is not the
# pipeline's scratch space, and so a k8s deployment (no bind mount) can use
# the same entry point by swapping the copy command.
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER=${AIRFLOW_CONTAINER:-pl-airflow}
DEST=${REPORT_DEST:-pipeline-reports}
mkdir -p "$DEST"

if ! docker inspect -f '{{.State.Running}}' "$CONTAINER" >/dev/null 2>&1; then
  echo "collect_report: container $CONTAINER is not running; nothing copied." >&2
  exit 0   # non-fatal: the report itself succeeded, this is delivery
fi

copy_one () {
  local src=$1 base
  base=$(basename "$src")
  if docker cp "$CONTAINER:$src" "$DEST/$base" 2>/dev/null; then
    echo "collect_report: $DEST/$base"
  else
    echo "collect_report: could not copy $src" >&2
  fi
}

if [[ $# -ge 1 && -n "${1:-}" ]]; then
  copy_one "$1"
else
  # No argument: sweep every report the container currently holds.
  docker exec "$CONTAINER" sh -c 'ls /data/reports/*.pdf 2>/dev/null' \
    | while read -r f; do [ -n "$f" ] && copy_one "$f"; done
fi
