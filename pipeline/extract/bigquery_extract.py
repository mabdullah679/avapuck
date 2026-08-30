"""BigQuery extract — 100 rows/day to local CSV, with hard cost controls.

THE BILLING MODEL, because getting this wrong is the expensive mistake:
BigQuery bills BYTES SCANNED, not rows returned. `LIMIT 100` does not reduce
the bill at all -- a `SELECT * ... LIMIT 100` on a large table scans every byte
of every referenced column and bills for all of it. The LIMIT only truncates
the result set.

IMPORTANT, VERIFIED AGAINST THE LIVE TABLE: austin_bikeshare.bikeshare_trips
is **not partitioned and not clustered**. An earlier version of this module
assumed a date predicate would prune partitions; it does not, because there
are none. A WHERE clause on `start_time` filters rows AFTER scanning them, so
it reduces the result set and not the bill.

What actually controls cost here:

  1. Named columns, never SELECT *. BigQuery is columnar, so unreferenced
     columns are never scanned. On this table that is the ONLY structural
     lever, and it is why COLUMNS is explicit and minimal.
  2. maximum_bytes_billed on the job -- the HARD STOP. BigQuery refuses to run
     a query that would exceed it rather than running it and billing you.
     LIMIT is not a safety mechanism; this is.
  3. Query cache. A repeated identical query inside 24h is free. Idempotent
     re-runs of the same logical date therefore cost nothing after the first.

MEASURED on 2026-08-30 (dry runs, which are free):

    SELECT *                          252.06 MiB
    9 named columns                   231.68 MiB
    9 named columns + date predicate  231.68 MiB   <- predicate does NOTHING
    7 columns (no station ids)        201.58 MiB

One run scans ~232 MiB: a FULL SCAN of the selected columns, every time. At
one run per day that is ~6.8 GiB/month against a 1 TiB/month free tier, about
0.7%. Comfortably free, but not the near-zero incremental read that partition
pruning would give -- and that difference is stated rather than implied.

The default cap is therefore 300 MiB, not the 50 MiB this module originally
carried. A cap below the only achievable cost is not a safety control, it is a
permanent outage; 300 MiB leaves headroom for dataset growth while still
catching a SELECT * regression on a larger table.

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
#
# Verified against the live schema on 2026-08-30. Note `bike_id`, not
# `bikeid` -- the column names were guessed when this was first written and
# BigQuery rejected the query. Do not edit this list from memory; confirm
# against `client.get_table(...).schema`.
COLUMNS = [
    "trip_id",
    "bike_id",
    "subscriber_type",
    "start_station_id",
    "start_station_name",
    "end_station_id",
    "end_station_name",
    "start_time",
    "duration_minutes",
]


# The public dataset is a HISTORICAL ARCHIVE, not a live feed: it ends
# 2024-06-30 (verified 2026-08-30). A daily pipeline pointed at "yesterday"
# would therefore return zero rows every single run, forever, while still
# being billed for the scan -- a silent failure that looks like success.
#
# So the logical date is mapped onto the archive's own timeline: the pipeline
# advances one archive-day per run, deterministically derived from the logical
# date. That keeps runs idempotent (same logical date -> same window -> same
# rows) and keeps a daily schedule meaningful against frozen data.
DATA_START = date(2013, 12, 12)
DATA_END = date(2024, 6, 30)


@dataclass(frozen=True)
class ExtractResult:
    logical_date: str
    window_date: str
    window_end: str
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
                 row_limit: int = 100, max_bytes_billed: int = 300 * 1024 * 1024,
                 location: str = "US", window_hours: int = 4):
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
        self.window_hours = window_hours

    @property
    def fqtn(self) -> str:
        return f"`{self.dataset}.{self.table}`"

    def window_for(self, logical_ts: datetime | date) -> tuple[datetime, datetime]:
        """Map a logical timestamp onto a concrete window inside the archive.

        The pipeline runs every 4 hours, so each run needs its OWN slice --
        six runs a day reading the same rows would be five wasted runs and
        five no-op upserts.

        The mapping is a pure function of the logical timestamp, which is what
        keeps runs idempotent: re-running the 10:00 slice for a given day
        always reads exactly the same archive window, so the upsert replaces
        rather than duplicates.

        Timestamps already inside the archive are used as-is (so a genuine
        historical backfill does the obvious thing). Timestamps beyond it wrap
        deterministically onto the archive span.
        """
        if isinstance(logical_ts, datetime):
            ts = logical_ts.replace(tzinfo=None)
        else:
            ts = datetime.combine(logical_ts, datetime.min.time())

        archive_start = datetime.combine(DATA_START, datetime.min.time())
        archive_end = datetime.combine(DATA_END, datetime.max.time())

        if archive_start <= ts <= archive_end:
            start = ts
        else:
            # Total 4-hour slots in the archive, indexed by how many slots have
            # elapsed since the archive ended. Deterministic and reversible.
            slots = int((archive_end - archive_start).total_seconds() // 3600 // self.window_hours)
            elapsed = int((ts - archive_end).total_seconds() // 3600 // self.window_hours)
            start = archive_start + timedelta(hours=self.window_hours * (elapsed % slots))

        # Snap to the window grid so slices tile the archive without gaps.
        floor_h = (start.hour // self.window_hours) * self.window_hours
        start = start.replace(hour=floor_h, minute=0, second=0, microsecond=0)
        return start, start + timedelta(hours=self.window_hours)

    def build_query(self, logical_date: date) -> tuple[str, list]:
        """Parameterised query: named columns, date predicate, LIMIT.

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
        window_start, window_end = self.window_for(logical_date)
        params = [
            bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
            bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
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

        window_start, window_end = self.window_for(logical_date)
        log.info("logical %s -> archive window %s .. %s (%dh)",
                 logical_date, window_start, window_end, self.window_hours)

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

        # An empty result is a failure, not a quiet success. Being billed for a
        # scan that yields nothing is the worst of both outcomes, and a daily
        # job that silently writes empty CSVs is the kind of thing discovered
        # weeks later.
        if not rows:
            raise RuntimeError(
                f"query returned 0 rows for window {window_start} .. {window_end} "
                f"(logical date {logical_date.isoformat()}) while scanning "
                f"{int(job.total_bytes_billed or 0) / 1048576:.1f} MiB. The "
                f"dataset spans {DATA_START} to {DATA_END}; check the window "
                f"mapping rather than accepting an empty extract.")

        out_dir.mkdir(parents=True, exist_ok=True)
        # Name by DATE and slot hour, never by the full timestamp: downstream
        # stages resolve inputs by date, and an ISO timestamp in a filename
        # also carries colons, which are hostile in paths.
        _d = logical_date.date() if hasattr(logical_date, "date") else logical_date
        csv_path = out_dir / f"trips_{_d.isoformat()}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(COLUMNS)
            for r in rows:
                w.writerow([_cell(r.get(c)) for c in COLUMNS])

        result = ExtractResult(
            logical_date=logical_date.isoformat(),
            window_date=window_start.isoformat(),
            window_end=window_end.isoformat(),
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
        max_bytes_billed=int(os.environ.get("BQ_MAX_BYTES_BILLED", str(300 * 1024 * 1024))),
        location=os.environ.get("BQ_LOCATION", "US"),
        window_hours=int(os.environ.get("BQ_WINDOW_HOURS", "4")),
    )
