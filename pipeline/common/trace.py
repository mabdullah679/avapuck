"""Correlation IDs — one traceable path per run, across five DAGs.

THE PROBLEM THIS SOLVES: five independently-scheduled DAGs mean five separate
sets of logs. When a PDF looks wrong, "which BigQuery job produced these rows,
which key version encrypted them, and which Ranger policy masked them?" has no
answer unless the run carries an identifier that survives every hop.

THE IDENTIFIER: derived deterministically from the logical timestamp rather
than generated randomly. Two consequences that matter:

  * A re-run of the same logical slice produces the SAME trace id, so a repair
    is visibly the same logical unit of work rather than a new one.
  * Any stage can compute it from its own context without being told, so it
    survives asset-driven scheduling where stages do not talk to each other.

Every stage stamps it on its output and logs it. The chain is then greppable
end to end with a single string.
"""
from __future__ import annotations

import hashlib
import logging
import os
from datetime import date, datetime

log = logging.getLogger(__name__)


def trace_id(logical_ts: datetime | date, pipeline: str = "trips") -> str:
    """Stable identifier for one logical slice of work.

    Format: trips-20260830T1600-a3f9c1  (pipeline, slot, digest)

    The digest disambiguates pipelines sharing a slot without making the id
    opaque -- the human-readable slot stays visible so an operator can eyeball
    which window a trace belongs to.
    """
    ts = (logical_ts if isinstance(logical_ts, datetime)
          else datetime.combine(logical_ts, datetime.min.time()))
    slot = ts.strftime("%Y%m%dT%H%M")
    digest = hashlib.sha256(f"{pipeline}|{slot}".encode()).hexdigest()[:6]
    return f"{pipeline}-{slot}-{digest}"


def stage_event(stage: str, trace: str, **fields) -> dict:
    """Emit one structured, greppable line per stage boundary.

    Deliberately one line and machine-parseable: `grep <trace-id> logs/` should
    reconstruct the whole path without a log aggregator.
    """
    parts = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
    log.info("TRACE %s stage=%s %s", trace, stage, parts)
    return {"trace_id": trace, "stage": stage, **fields}


def trace_from_context(ctx: dict, pipeline: str = "trips") -> str:
    """Derive the trace id from an Airflow task context.

    Falls back through logical_date -> data_interval_start -> now, because
    Airflow's context keys have shifted across versions and a missing key
    should not break tracing.
    """
    ts = ctx.get("logical_date") or ctx.get("data_interval_start")
    if ts is None:
        ts = datetime.now()
    return trace_id(ts, pipeline)
