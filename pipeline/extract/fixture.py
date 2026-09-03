"""Offline extract path — real BigQuery schema, synthetic rows.

Exists so the pipeline is runnable and testable before GCP credentials are in
place, and so CI never needs a cloud credential. It implements the SAME
interface as BigQueryExtractor, so switching is a config flag and not a code
change.

The rows are synthetic but the SCHEMA and value shapes are the real
austin_bikeshare ones: real Austin station names, plausible trip durations,
the same column order and types. That matters -- a fixture with the wrong
shape lets a mapping bug hide until the live path runs.

Every run through this path is marked in TRUST-BOUNDARY.md as unverified
against live BigQuery.
"""
from __future__ import annotations

import csv
import hashlib
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from pipeline.extract.bigquery_extract import COLUMNS, ExtractResult

# Real Austin station names, so the masking output looks like the live thing.
STATIONS = [
    (2537, "Zilker Park West"), (2536, "Barton Springs & Riverside"),
    (2499, "Congress & 6th"), (2575, "Republic Square"),
    (2494, "2nd & Congress"), (2568, "Rainey St & Cummings"),
    (2498, "Convention Center / 3rd & Trinity"), (2712, "Nueces & 3rd"),
    (2545, "East 6th & Pedernales"), (3792, "21st & Speedway @PCL"),
    (2574, "Lavaca & 6th"), (2707, "Rainey & River St"),
]
SUBSCRIBER_TYPES = [
    "Local365", "Walk Up", "Local30", "24 Hour Walk Up Pass",
    "Weekender", "Annual Membership", "Student Membership",
]


class FixtureExtractor:
    """Drop-in replacement for BigQueryExtractor. Same interface, no cloud."""

    def __init__(self, row_limit: int = 100, **_ignored):
        self.row_limit = row_limit

    def extract(self, logical_date: date, out_dir: Path) -> ExtractResult:
        # Seeded by date, so a re-run of the same logical date produces
        # byte-identical output. Idempotency is testable offline.
        seed = int(hashlib.sha256(str(logical_date).encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        out_dir.mkdir(parents=True, exist_ok=True)
        _d = logical_date.date() if hasattr(logical_date, "date") else logical_date
        csv_path = out_dir / f"trips_{_d.isoformat()}.csv"

        _base_d = logical_date.date() if hasattr(logical_date, "date") else logical_date
        base = datetime.combine(_base_d, datetime.min.time()) + timedelta(hours=5)
        rows = []
        for i in range(self.row_limit):
            start_id, start_name = rng.choice(STATIONS)
            end_id, end_name = rng.choice(STATIONS)
            start = base + timedelta(minutes=rng.randint(0, 1020))
            rows.append({
                "trip_id": f"{seed:08x}{i:04d}",
                "bike_id": str(rng.randint(1, 900)),
                "subscriber_type": rng.choice(SUBSCRIBER_TYPES),
                "start_station_id": start_id,
                "start_station_name": start_name,
                "end_station_id": end_id,
                "end_station_name": end_name,
                "start_time": start.isoformat(),
                # Long tail: most trips short, a few very long. Makes the PDF's
                # duration chart look like real data rather than a uniform blob.
                "duration_minutes": max(1, int(rng.lognormvariate(2.5, 0.8))),
            })
        rows.sort(key=lambda r: r["start_time"])

        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(COLUMNS)
            for r in rows:
                w.writerow([r[c] for c in COLUMNS])

        # The manifest is what the downstream stages read instead of a
        # hardcoded column list. Written here too -- not just on the CSV-source
        # path -- so trips flows through exactly the same generic stages as any
        # other dataset, rather than keeping a private route through them.
        from pipeline.metadata import schema as schema_mod
        schema_mod.write_manifest(
            schema_mod.infer(csv_path, dataset="trips"), csv_path)

        return ExtractResult(
            logical_date=logical_date.isoformat(),
            # The fixture has no archive to map onto -- it synthesises rows for
            # whatever date it is given, so the window IS the logical date.
            window_date=logical_date.isoformat(),
            window_end=logical_date.isoformat(),
            row_count=len(rows),
            bytes_processed=0,
            bytes_billed=0,
            estimated_bytes=0,
            csv_path=str(csv_path),
            query_id=None,
        )


def get_extractor():
    """Choose the CSV source, live BigQuery, or the fixture, from config.

    Defaults to the fixture ONLY when nothing else is configured, so that a
    misconfigured live run fails loudly rather than silently producing
    synthetic data that looks real.

    CSV_SOURCE_PATH wins over everything when set, including EXTRACT_MODE=live:
    naming a specific file is an unambiguous instruction, and silently querying
    BigQuery instead would be both surprising and billable.
    """
    import os
    mode = os.environ.get("EXTRACT_MODE", "auto").lower()

    if mode == "csv" or os.environ.get("CSV_SOURCE_PATH"):
        from pipeline.extract.csv_source import extractor_from_env as csv_from_env
        return csv_from_env()
    if mode == "fixture":
        return FixtureExtractor(row_limit=int(os.environ.get("BQ_ROW_LIMIT", "100")))
    if mode == "live":
        from pipeline.extract.bigquery_extract import extractor_from_env
        return extractor_from_env()
    if os.environ.get("GCP_PROJECT_ID"):
        from pipeline.extract.bigquery_extract import extractor_from_env
        return extractor_from_env()
    return FixtureExtractor(row_limit=int(os.environ.get("BQ_ROW_LIMIT", "100")))

def extract_with_fallback(logical_ts, out_dir, log=None):
    """Try BigQuery up to BQ_ATTEMPTS times, then fall back to the CSV source.

    Returns (result, extractor_used, bq_attempts, fell_back).

    The client requirement is "if BigQuery fails 3 times, default to the CSV
    workflow". Retrying inside the task rather than relying on Airflow's task
    retries is deliberate: an Airflow retry re-runs the WHOLE task, so it would
    retry the fallback too and there would be no way to say "three BigQuery
    attempts, then switch". Here the attempt count applies to BigQuery alone.

    A fallback is never silent. Every failed attempt is logged with its
    exception, and the caller records `mode` and `fell_back_to_csv` in the
    stage result so the Airflow UI shows which source actually ran.
    """
    import logging
    import os
    log = log or logging.getLogger(__name__)

    ex = get_extractor()
    attempts_allowed = int(os.environ.get("BQ_ATTEMPTS", "3"))

    # Only the BigQuery path gets retried -- a CSV or fixture source that
    # fails has nothing to fall back TO, and retrying it would just delay a
    # legitimate failure.
    if type(ex).__name__ != "BigQueryExtractor":
        log.info("extract: source is %s; no BigQuery fallback needed",
                 type(ex).__name__)
        return ex.extract(logical_ts, out_dir), ex, 0, False

    last = None
    for attempt in range(1, attempts_allowed + 1):
        try:
            log.info("extract: BigQuery attempt %d/%d", attempt, attempts_allowed)
            r = ex.extract(logical_ts, out_dir)
            if attempt > 1:
                log.info("extract: BigQuery succeeded on attempt %d", attempt)
            return r, ex, attempt, False
        except Exception as e:  # noqa: BLE001
            last = e
            log.warning("extract: BigQuery attempt %d/%d failed: %s",
                        attempt, attempts_allowed, e)

    log.warning("extract: BigQuery failed %d times (last: %s); FALLING BACK to "
                "the CSV source", attempts_allowed, last)

    # The fallback needs its OWN path variable. Setting CSV_SOURCE_PATH would
    # short-circuit get_extractor() above and BigQuery would never be tried at
    # all; leaving it unset would make the CSV extractor raise KeyError here.
    fallback = os.environ.get("CSV_FALLBACK_PATH") or os.environ.get("CSV_SOURCE_PATH")
    if not fallback:
        log.error("extract: BigQuery failed %d times and CSV_FALLBACK_PATH is "
                  "not set -- nothing to fall back to", attempts_allowed)
        raise last

    from pipeline.extract.csv_source import extractor_from_env as csv_from_env
    prev = os.environ.get("CSV_SOURCE_PATH")
    os.environ["CSV_SOURCE_PATH"] = fallback
    try:
        csv_ex = csv_from_env()
        r = csv_ex.extract(logical_ts, out_dir)
    finally:
        if prev is None:
            os.environ.pop("CSV_SOURCE_PATH", None)
        else:
            os.environ["CSV_SOURCE_PATH"] = prev
    log.warning("extract: fell back to %s -- this run did NOT read BigQuery",
                type(csv_ex).__name__)
    return r, csv_ex, attempts_allowed, True
