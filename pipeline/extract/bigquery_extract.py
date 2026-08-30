"""BigQuery extract — 100 rows/day to local CSV, with hard cost controls.

THE BILLING MODEL, because getting this wrong is the expensive mistake:
BigQuery bills BYTES SCANNED, not rows returned. `LIMIT 100` does not reduce
the bill at all -- a `SELECT * ... LIMIT 100` on a large table scans every byte
of every referenced column and bills for all of it. The LIMIT only truncates
the result set.

Three controls actually reduce cost, and this module uses all three:

  1. Named columns, never SELECT *. BigQuery is columnar, so unreferenced
     columns are never scanned. Biggest lever by far.
  2. A predicate on the partitioning column, so partition pruning applies.
  3. maximum_bytes_billed on the job. This is the HARD STOP: BigQuery refuses
     to run a query that would exceed it, rather than running it and billing
     you. LIMIT is not a safety mechanism; this is.

A dry run happens first, so the estimate is known and asserted BEFORE any
billable query executes.
"""
from __future__ import annotations

import csv
import logging
import os
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

# Only these columns are read. Adding one here increases bytes scanned.
COLUMNS = [
    "trip_id",
    "bikeid",
    "subscriber_type",
    "start_station_id",
    "start_station_name",
    "end_station_id",
    "end_station_name",
    "start_time",
    "duration_minutes",
]


@dataclass(frozen=True)
class ExtractResult:
    logical_date: str
    row_count: int
    bytes_processed: int
    bytes_billed: int
    estimated_bytes: int
    csv_path: str
    query_id: str | None

    @property
    def megabytes_billed(self) -> float:
        return self.bytes_billed / 1_048_576

    def as_dict(self) -> dict:
        d = asdict(self)
        d["megabytes_billed"] = round(self.megabytes_billed, 3)
        return d


class BigQueryExtractor:
    """Reads a bounded slice of a public dataset."""

    def __init__(self, project_id: str, dataset: str, table: str,
                 row_limit: int = 100, max_bytes_billed: int = 50 * 1024 * 1024,
                 location: str = "US"):
        if row_limit > 1000:
            raise ValueError(
                f"row_limit {row_limit} exceeds the pipeline's 1000-row sanity "
                f"ceiling; this pipeline is specified for 100/day")
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.row_limit = row_limit
        self.max_bytes_billed = max_bytes_billed
        self.location = location

    @property
    def fqtn(self) -> str:
        return f"`{self.dataset}.{self.table}`"

    def build_query(self, logical_date: date) -> tuple[str, list]:
        """Parameterised query. Named columns, partition predicate, LIMIT.

        Parameters rather than f-string interpolation: even against a public
        dataset with a date, string-building a query is a habit that becomes an
        injection the first time a value comes from somewhere less trusted.
        """
        from google.cloud import bigquery

        cols = ",\n               ".join(COLUMNS)
        sql = f"""
            SELECT {cols}
            FROM {self.fqtn}
            WHERE start_time >= @window_start
              AND start_time <  @window_end
              AND trip_id IS NOT NULL
            ORDER BY start_time
            LIMIT @row_limit
        """
        window_start = datetime.combine(logical_date, datetime.min.time())
        params = [
            bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
            bigquery.ScalarQueryParameter("window_end", "TIMESTAMP",
                                          window_start + timedelta(days=1)),
            bigquery.ScalarQueryParameter("row_limit", "INT64", self.row_limit),
        ]
        return sql, params

    def dry_run_bytes(self, logical_date: date) -> int:
        """Ask BigQuery what this would scan, without running it. Free."""
        from google.cloud import bigquery
        client = bigquery.Client(project=self.project_id, location=self.location)
        sql, params = self.build_query(logical_date)
        cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False,
                                      query_parameters=params)
        job = client.query(sql, job_config=cfg)
        return int(job.total_bytes_processed or 0)

    def extract(self, logical_date: date, out_dir: Path) -> ExtractResult:
        from google.cloud import bigquery

        estimated = self.dry_run_bytes(logical_date)
        log.info("dry run: %.3f MiB would be scanned", estimated / 1_048_576)
        if estimated > self.max_bytes_billed:
            raise RuntimeError(
                f"query would scan {estimated / 1_048_576:.1f} MiB, over the "
                f"{self.max_bytes_billed / 1_048_576:.1f} MiB cap. Refusing to run. "
                f"Narrow the partition predicate or drop columns.")

        client = bigquery.Client(project=self.project_id, location=self.location)
        sql, params = self.build_query(logical_date)
        cfg = bigquery.QueryJobConfig(
            query_parameters=params,
            maximum_bytes_billed=self.max_bytes_billed,   # the hard stop
            use_query_cache=True,
        )
        job = client.query(sql, job_config=cfg)
        rows = list(job.result())

        # The cap is enforced in SQL; assert it held. A silent over-fetch would
        # inflate every downstream stage and the bill with it.
        if len(rows) > self.row_limit:
            raise RuntimeError(
                f"row cap breached: got {len(rows)}, limit {self.row_limit}")

        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"trips_{logical_date.isoformat()}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(COLUMNS)
            for r in rows:
                w.writerow([_cell(r.get(c)) for c in COLUMNS])

        result = ExtractResult(
            logical_date=logical_date.isoformat(),
            row_count=len(rows),
            bytes_processed=int(job.total_bytes_processed or 0),
            bytes_billed=int(job.total_bytes_billed or 0),
            estimated_bytes=estimated,
            csv_path=str(csv_path),
            query_id=job.job_id,
        )
        log.info("extracted %d rows, billed %.3f MiB -> %s",
                 result.row_count, result.megabytes_billed, csv_path)
        return result


def _cell(v):
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def extractor_from_env() -> BigQueryExtractor:
    project = os.environ.get("GCP_PROJECT_ID")
    if not project:
        raise RuntimeError(
            "GCP_PROJECT_ID is not set. Put the project ID (not the number) in "
            ".env.local -- see docs/SECRETS.md.")
    return BigQueryExtractor(
        project_id=project,
        dataset=os.environ.get("BQ_DATASET", "bigquery-public-data.austin_bikeshare"),
        table=os.environ.get("BQ_TABLE", "bikeshare_trips"),
        row_limit=int(os.environ.get("BQ_ROW_LIMIT", "100")),
        max_bytes_billed=int(os.environ.get("BQ_MAX_BYTES_BILLED", str(50 * 1024 * 1024))),
        location=os.environ.get("BQ_LOCATION", "US"),
    )
