"""Register the masked Parquet as a Hive external table and repair partitions.

Separate from the mask stage because it is a metadata operation against
HiveServer2, not a data transformation -- and because it must be safe to run
when Hive is down. The pipeline's data path does not depend on Hive being up;
the table is a query surface over files that already exist.

The DDL is built from the dataset's manifest, not written out per dataset:
the masked Parquet's columns are whatever the mask stage produced for THAT
dataset, so a hardcoded trips schema would create a table that does not match
the files under any other dataset.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)

DATABASE = os.environ.get("HIVE_DATABASE", "trips_warehouse")
JDBC_URL = os.environ.get("HIVE_JDBC_URL", "jdbc:hive2://hiveserver2:10000")

# Parquet types are not Hive types. Anything unmapped becomes STRING, which is
# always readable -- a wrong-but-narrow type would fail the query instead.
_HIVE_TYPE = {
    "TEXT": "STRING",
    "BIGINT": "BIGINT",
    "DOUBLE PRECISION": "DOUBLE",
    "TIMESTAMP": "STRING",
}


def _connect():
    """Open a JDBC connection to HiveServer2, or return None if impossible.

    Uses the hive-jdbc standalone jar through jaydebeapi. The jar carries the
    driver but not the BeeLine CLI, and pyspark's `beeline` on PATH is Spark's
    own and cannot talk to HiveServer2 -- so JDBC is the honest route here.

    Returns None rather than raising: Hive being down must not fail the DAG,
    since the masked Parquet is already written and correct.
    """
    jar = os.environ.get("HIVE_JDBC_JAR")
    if not jar or not Path(jar).is_file():
        log.info("no Hive JDBC jar at %s; skipping registration", jar)
        return None
    try:
        import jaydebeapi
    except ImportError:
        log.info("jaydebeapi not installed; skipping Hive registration")
        return None
    # The driver needs Hadoop classes the standalone jar omits, so pass every
    # jar in the classpath directory rather than the driver jar alone.
    # Colon-separated list of DIRECTORIES, each contributing its jars: the
    # driver needs Hive's lib plus Hadoop's common jars, which live apart.
    cp = os.environ.get("HIVE_JDBC_CLASSPATH", "")
    jars = sorted(str(j) for d in cp.split(":") if d
                  for j in Path(d).glob("*.jar")) or [jar]
    try:
        return jaydebeapi.connect(
            "org.apache.hive.jdbc.HiveDriver", JDBC_URL, ["", ""], jars or [jar])
    except Exception as e:  # noqa: BLE001
        log.warning("HiveServer2 unreachable at %s (%s); the masked Parquet is "
                    "still written and valid", JDBC_URL, e)
        return None


def _execute(statements: list[str]) -> tuple[bool, str]:
    """Run statements in order. Returns (ok, reason)."""
    conn = _connect()
    if conn is None:
        return False, "hive unreachable"
    try:
        cur = conn.cursor()
        try:
            for sql in statements:
                cur.execute(sql)
        finally:
            cur.close()
        return True, ""
    except Exception as e:  # noqa: BLE001
        log.warning("Hive registration failed (%s); the masked Parquet is "
                    "still written and valid", e)
        return False, str(e)[:200]
    finally:
        conn.close()


def build_ddl(parquet_path: Path, database: str = DATABASE) -> tuple[str, str]:
    """Return (table_name, DDL) for the masked Parquet at `parquet_path`.

    Columns come from the file itself rather than the manifest: the mask stage
    renames and drops columns (`x` -> `x_masked`, plaintext dropped), so the
    file is the only accurate description of what the table must expose.
    """
    import pyarrow.parquet as pq

    schema = pq.read_schema(parquet_path)
    table = parquet_path.stem.replace("_masked", "")

    cols = []
    for name, dtype in zip(schema.names, schema.types):
        if name == "dt":            # the partition column, declared separately
            continue
        d = str(dtype)
        if d.startswith("int"):
            hive_type = "BIGINT"
        elif d.startswith(("double", "float")):
            hive_type = "DOUBLE"
        elif d.startswith("timestamp"):
            hive_type = "STRING"
        else:
            hive_type = "STRING"
        cols.append(f"    `{name}` {hive_type}")

    # The table points at the dataset's own directory, one level above the
    # dt=<date> partition dirs the mask stage writes.
    location = str(parquet_path.parent.parent)
    # Separate statements: a JDBC cursor executes one at a time, unlike a
    # beeline -e blob.
    ddl = [
        f"CREATE DATABASE IF NOT EXISTS {database}",
        (f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (\n"
         + ",\n".join(cols)
         + "\n) PARTITIONED BY (dt STRING) STORED AS PARQUET"
         f" LOCATION '{location}'"
),
        f"MSCK REPAIR TABLE {database}.{table}",
    ]
    return table, ddl


def _add_missing_columns(table: str, parquet_path: Path) -> list[str]:
    """Add columns present in the file but absent from the live Hive table.

    Only ever ADDS. Dropping or retyping a column would discard data that
    older partitions still hold, so a column that disappears from the source
    stays in the table and simply reads NULL for newer partitions.
    """
    conn = _connect()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"DESCRIBE {DATABASE}.{table}")
            live = {r[0].strip() for r in cur.fetchall() if r and r[0]}

            _, ddl = build_ddl(parquet_path)
            # The column list lives in the CREATE statement; parse it back out
            # rather than re-deriving it, so the two can never disagree.
            create = ddl[1]
            body = create[create.index("(") + 1:create.rindex(") PARTITIONED BY")]
            wanted = []
            for line in body.split(",\n"):
                line = line.strip()
                if not line:
                    continue
                name = line.split()[0].strip("`")
                hive_type = line.split(maxsplit=1)[1]
                wanted.append((name, hive_type))

            missing = [(n, t) for n, t in wanted if n not in live]
            if not missing:
                return []
            cols = ", ".join(f"`{n}` {t}" for n, t in missing)
            cur.execute(f"ALTER TABLE {DATABASE}.{table} ADD COLUMNS ({cols})")
            log.info("added %d column(s) to %s.%s: %s",
                     len(missing), DATABASE, table, [n for n, _ in missing])
            return [n for n, _ in missing]
        finally:
            cur.close()
    except Exception as e:  # noqa: BLE001
        log.warning("could not reconcile columns for %s.%s (%s); the table is "
                    "registered but may be missing newer columns",
                    DATABASE, table, e)
        return []
    finally:
        conn.close()


def run(logical_date: date, parquet_path: Path | str | None = None) -> dict:
    """Create the table if absent and pick up the new partition.

    Returns a skipped result rather than raising when Hive is unreachable: the
    masked Parquet is already written and correct, and failing the DAG because
    a metadata service is down would be a false alarm about the data.
    """
    if parquet_path is None:
        return {"registered": False, "reason": "no parquet path supplied"}

    parquet_path = Path(parquet_path)
    if not parquet_path.is_file():
        return {"registered": False, "reason": f"no such parquet: {parquet_path}"}

    table, ddl = build_ddl(parquet_path)
    ok, reason = _execute(ddl)
    if not ok:
        return {"registered": False, "reason": reason}

    # CREATE TABLE IF NOT EXISTS leaves an EXISTING table's schema alone, so a
    # dataset that gains a column (a new card_info in a later export, say)
    # stays invisible in Hive while every log line reports success. Reconcile
    # the live schema against the file and add what is missing.
    added = _add_missing_columns(table, parquet_path)

    log.info("registered %s.%s partition dt=%s", DATABASE, table, logical_date)
    return {"registered": True, "database": DATABASE, "table": table,
            "partition": f"dt={logical_date.isoformat()}",
            "columns_added": added}


def row_count(table: str = "trips") -> int | None:
    """Query the live table. Returns None when Hive is unreachable."""
    conn = _connect()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT count(*) FROM {DATABASE}.{table}")
            row = cur.fetchone()
            return int(row[0]) if row else None
        finally:
            cur.close()
    except Exception:  # noqa: BLE001
        return None
    finally:
        conn.close()
