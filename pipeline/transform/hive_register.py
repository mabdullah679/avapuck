"""Register the masked Parquet as a Hive external table and repair partitions.

Separate from the mask stage because it is a metadata operation against
HiveServer2, not a data transformation -- and because it must be safe to run
when Hive is down. The pipeline's data path does not depend on Hive being up;
the table is a query surface over files that already exist.
"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
from datetime import date

log = logging.getLogger(__name__)

DATABASE = "trips_warehouse"
TABLE = "trips"
CONTAINER = os.environ.get("HIVE_CONTAINER", "pl-hiveserver2")
WAREHOUSE = os.environ.get("HIVE_WAREHOUSE_PATH", "/opt/hive/data/warehouse/trips")

DDL = f"""
CREATE DATABASE IF NOT EXISTS {DATABASE};
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{TABLE} (
    trip_id STRING, logical_date STRING, start_time STRING, duration_minutes BIGINT,
    bike_id_masked STRING, subscriber_type_masked STRING,
    start_station_masked STRING, end_station_masked STRING,
    start_station_id_masked STRING, end_station_id_masked STRING,
    bike_id_blind_index STRING, start_station_blind_index STRING, masked_by STRING)
PARTITIONED BY (dt STRING) STORED AS PARQUET
LOCATION '{WAREHOUSE}';
MSCK REPAIR TABLE {DATABASE}.{TABLE};
"""


def run(logical_date: date) -> dict:
    """Create the table if absent and pick up the new partition.

    Returns a skipped result rather than raising when Hive is unreachable: the
    masked Parquet is already written and correct, and failing the DAG because
    a metadata service is down would be a false alarm about the data.
    """
    # In Kubernetes there is no docker binary inside the pod, and beeline lives
    # in a different pod entirely. Registration is a METADATA convenience -- the
    # masked Parquet is already written and correct -- so a missing beeline is
    # reported, never fatal. See the non-fatal contract in the docstring.
    if not shutil.which("docker"):
        log.info("docker not available (in-cluster); skipping Hive registration. "
                 "Register with: kubectl -n trips exec deploy/hiveserver2 -- "
                 "/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /data/hive/trips_table.hql")
        return {"registered": False, "reason": "no beeline in this container"}

    proc = subprocess.run(
        ["docker", "exec", CONTAINER, "/opt/hive/bin/beeline",
         "-u", "jdbc:hive2://localhost:10000", "-e", DDL],
        capture_output=True, text=True, timeout=300)

    if proc.returncode != 0:
        log.warning("Hive registration failed (rc=%s); the masked Parquet is "
                    "still written and valid. stderr: %s",
                    proc.returncode, proc.stderr[-300:])
        return {"registered": False, "reason": f"beeline rc={proc.returncode}"}

    log.info("registered %s.%s partition dt=%s", DATABASE, TABLE, logical_date)
    return {"registered": True, "database": DATABASE, "table": TABLE,
            "partition": f"dt={logical_date.isoformat()}"}


def row_count() -> int | None:
    """Query the live table. Returns None when Hive is unreachable."""
    proc = subprocess.run(
        ["docker", "exec", CONTAINER, "/opt/hive/bin/beeline",
         "-u", "jdbc:hive2://localhost:10000", "--silent=true", "--outputformat=csv2",
         "-e", f"SELECT count(*) FROM {DATABASE}.{TABLE};"],
        capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        return None
    for line in proc.stdout.splitlines():
        s = line.strip()
        if s.isdigit():
            return int(s)
    return None
