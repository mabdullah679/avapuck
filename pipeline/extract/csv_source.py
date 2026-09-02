"""Extract path for a CSV that already exists on disk.

Third extractor behind the same interface as BigQueryExtractor and
FixtureExtractor, for the case where the source is a file someone dropped in
rather than a query: an export, a hand-supplied dataset, a one-off load.

WHAT IT ADDS OVER `cp`
======================
It infers the schema and writes the manifest beside the landed CSV, which is
what lets the downstream stages run without knowing the dataset. It also
enforces the same row cap the BigQuery path does -- non-negotiable #4 exists
because an unbounded run is a billing incident there, and keeping the cap
uniform means the stages after it see the same shape of data regardless of
which extractor ran.
"""
from __future__ import annotations

import csv as csvmod
import logging
import os
import shutil
from datetime import date
from pathlib import Path

from pipeline.extract.bigquery_extract import ExtractResult
from pipeline.metadata import schema as schema_mod

log = logging.getLogger(__name__)


class CsvSourceExtractor:
    """Lands an existing CSV and infers its schema. Same interface as the rest."""

    def __init__(self, source_path: Path, row_limit: int = 100,
                 dataset: str | None = None):
        self.source_path = Path(source_path)
        self.row_limit = row_limit
        self.dataset = dataset

    def extract(self, logical_date, out_dir: Path) -> ExtractResult:
        if not self.source_path.exists():
            raise FileNotFoundError(
                f"CSV_SOURCE_PATH points at {self.source_path}, which does not "
                f"exist. Set it to a readable CSV or unset it to use the "
                f"fixture/BigQuery path.")

        d = logical_date.date() if hasattr(logical_date, "date") else logical_date
        dataset = self.dataset or schema_mod.dataset_name_for(self.source_path)

        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"{dataset}_{d.isoformat()}.csv"

        rows_written = self._land(csv_path)

        manifest = schema_mod.infer(csv_path, dataset=dataset)
        schema_mod.write_manifest(manifest, csv_path)
        log.info("landed %d rows to %s; %d/%d columns sensitive",
                 rows_written, csv_path,
                 len(manifest.sensitive_columns), len(manifest.columns))

        return ExtractResult(
            logical_date=str(logical_date),
            # A file source has no archive window to map onto; the window is
            # the logical date, same as the fixture path.
            window_date=d.isoformat(),
            window_end=d.isoformat(),
            row_count=rows_written,
            bytes_processed=self.source_path.stat().st_size,
            # Nothing is billed for reading a local file, and saying 0 keeps
            # the cost column in the audit table honest rather than blank.
            bytes_billed=0,
            estimated_bytes=self.source_path.stat().st_size,
            csv_path=str(csv_path),
            query_id=None,
        )

    def _land(self, csv_path: Path) -> int:
        """Copy at most row_limit rows, preserving the header.

        Reads and rewrites rather than copying the file whole, because the cap
        has to be enforced on the way in -- a 5M-row export landed intact would
        break the same assumption the BigQuery LIMIT protects.
        """
        with self.source_path.open(newline="", encoding="utf-8-sig") as src:
            reader = csvmod.reader(src)
            try:
                header = next(reader)
            except StopIteration:
                raise RuntimeError(f"{self.source_path} is empty") from None

            with csv_path.open("w", newline="", encoding="utf-8") as dst:
                writer = csvmod.writer(dst)
                writer.writerow(header)
                written = 0
                for row in reader:
                    if written >= self.row_limit:
                        log.info("row cap %d reached; ignoring the rest of %s",
                                 self.row_limit, self.source_path.name)
                        break
                    writer.writerow(row)
                    written += 1

        if written == 0:
            raise RuntimeError(f"{self.source_path} has a header but no data rows")
        return written


def extractor_from_env() -> CsvSourceExtractor:
    return CsvSourceExtractor(
        source_path=Path(os.environ["CSV_SOURCE_PATH"]),
        row_limit=int(os.environ.get("BQ_ROW_LIMIT", "100")),
        dataset=os.environ.get("DATASET_NAME") or None,
    )
