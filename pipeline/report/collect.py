"""Deliver a finished PDF report to the host-visible reports directory.

The report stage writes into PIPELINE_DATA_ROOT (/data in the container).
That path is a bind mount, so the file is already on the host -- but it sits
in the pipeline's scratch tree next to CSVs, Parquet and Hive output. This
copies the finished artifact into one predictable delivery directory.

Deliberately filesystem-level, not `docker cp`: the task runs INSIDE the
container, which has no docker binary. scripts/collect_report.sh is the
outside-in equivalent for manual use and for deployments with no bind mount.

Never fatal. The report has already succeeded by the time this runs; failing
to file a copy must not turn a good run red.
"""
from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_DEST = "/opt/pipeline/pipeline-reports"


def collect(pdf_path: str | Path, dest_dir: str | Path | None = None) -> dict:
    """Copy `pdf_path` into the delivery directory. Returns a result dict."""
    src = Path(pdf_path)
    dest_root = Path(dest_dir or os.environ.get("REPORT_DEST", DEFAULT_DEST))

    if not src.is_file():
        log.warning("collect_report: %s does not exist; nothing copied", src)
        return {"collected": False, "reason": "source missing", "src": str(src)}

    try:
        dest_root.mkdir(parents=True, exist_ok=True)
        out = dest_root / src.name
        # copy2 preserves mtime, so the delivered file keeps the run's time
        # rather than the time it happened to be filed.
        shutil.copy2(src, out)
    except OSError as e:
        # A read-only or missing mount is a delivery problem, not a run
        # failure. Report it loudly and let the DAG stay green.
        log.warning("collect_report: could not copy %s -> %s: %s", src, dest_root, e)
        return {"collected": False, "reason": str(e), "src": str(src)}

    log.info("collect_report: %s", out)
    return {"collected": True, "dest": str(out), "bytes": out.stat().st_size}
