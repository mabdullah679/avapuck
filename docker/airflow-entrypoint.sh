#!/bin/sh
# Give Airflow's simple auth manager a KNOWN admin password from .env.local,
# then hand over to the airflow CLI.
#
# By default the simple auth manager generates a random password into
# AIRFLOW_HOME on first start. The console triggers runs through Airflow's
# REST API and needs a credential it can be configured with; writing the
# passwords file from the same variable both sides read keeps one place for
# the value (docs/DECISIONS.md D12, D15).
set -eu
: "${AIRFLOW_ADMIN_PASSWORD:?AIRFLOW_ADMIN_PASSWORD is not set; add it to .env.local}"
FILE="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-/opt/airflow/passwords.json}"
USER_NAME="${AIRFLOW_ADMIN_USER:-admin}"
mkdir -p "$(dirname "$FILE")"
printf '{"%s": "%s"}\n' "$USER_NAME" "$AIRFLOW_ADMIN_PASSWORD" > "$FILE"
chmod 600 "$FILE"
exec airflow "$@"
