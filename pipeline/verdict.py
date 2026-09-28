"""The run verdict: one pure function that decides whether downstream proceeds.

This is the single point of correctness for the whole platform (docs/DECISIONS.md
D4). Consumers do not evaluate conditions; they read `proceed`. Every rule about
what counts as a finished run lives here and nowhere else, which is why this
module carries the heaviest tests in the repository and no I/O at all.

INPUT   the stage events -- the dict each task returned, keyed by stage name,
        None where a stage produced nothing (failed, skipped, never ran);
        and, optionally, the FAILURES -- for a stage that raised, the error
        text the runner recorded before the exception left it.
OUTPUT  a Verdict: proceed (the only field a machine reads), status and
        reasons (for people and the PDF), per-stage detail, and the run's
        outputs so a consumer can find what was produced without guessing.

WHY FAILURES ARE AN INPUT. Seen live (2026-09-08): a console upload was
stopped by the dataset contract gate in under a second, and the verdict said
"extract did not complete" seven times over -- the one sentence that
explained the run lived only in the Airflow task log. A stage that raised is
a different fact from a stage that never ran, and the verdict says which.

THE RULE
  proceed  = extract, card_split, encrypt, publish, mask, load and report all
             reported and succeeded.
  degraded = proceed, but something fell back or a non-load-bearing step did
             not complete: BigQuery -> CSV, Spark -> pyarrow when Spark was
             configured, the Hive table not registered, the delivery copy
             failed. The data in Postgres and the PDF are correct in every one
             of those cases, so downstream is not held. Each is named in
             `reasons` so the degradation is visible, not silent.
  failed   = not proceed.

Hive registration is the judgment call the owner confirmed: a metastore
hiccup must not stop every team for data that is correct. Change that here,
once, with a test.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Mapping

STAGES = ["extract", "card_split", "encrypt", "publish", "mask",
          "hive_register", "load", "report"]

# Stages whose success is a precondition for proceed. Everything else
# contributes to status only.
REQUIRED_FOR_PROCEED = frozenset(s for s in STAGES if s != "hive_register")

STATUS_COMPLETE = "complete"
STATUS_DEGRADED = "degraded"
STATUS_FAILED = "failed"


@dataclass
class Verdict:
    proceed: bool
    status: str
    reasons: list[str]
    stages: dict[str, dict]
    outputs: dict
    data_window: dict
    computed_at: str

    def to_payload(self) -> dict:
        return asdict(self)


def _iso_now(now: datetime | None) -> str:
    ts = now or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _detail(ev: dict, keys: tuple[str, ...]) -> dict:
    return {k: ev.get(k) for k in keys if k in ev}


_DETAIL_KEYS = {
    "extract": ("rows", "mode", "bq_attempts", "fell_back_to_csv", "window_date",
                "window_end", "window_source", "bytes_billed"),
    "card_split": ("rows", "cards", "dataset"),
    "encrypt": ("rows", "engine", "spark_configured", "encrypted_columns"),
    "publish": ("rows", "topic_encrypted", "topic_public"),
    "mask": ("rows", "masked_by", "policies", "policy_file"),
    "hive_register": ("registered", "table", "partition", "reason"),
    "load": ("inserted", "updated", "run_id", "dataset"),
    "report": ("rows", "pdf", "collected"),
}


def compute(events: Mapping[str, dict | None], now: datetime | None = None,
            failures: Mapping[str, str] | None = None) -> Verdict:
    """Decide. Pure: same events in, same verdict out (bar computed_at).

    `failures` maps a stage name to the error it raised. It is consulted only
    for stages with no event: a stage that reported succeeded, whatever an
    earlier try of it may have recorded.
    """
    reasons: list[str] = []
    stages: dict[str, dict] = {}
    failures = failures or {}

    for stage in STAGES:
        ev = events.get(stage)
        reported = ev is not None
        ok = reported
        entry: dict = {"ok": ok, "reported": reported}
        if reported:
            entry["detail"] = _detail(ev, _DETAIL_KEYS.get(stage, ()))
        elif failures.get(stage):
            entry["error"] = _one_line(failures[stage])
        stages[stage] = entry

    # ── Required stages: any one missing means the run did not finish ──
    # A stage that raised says so, with the error; a stage that never ran
    # (upstream failed, timed out before it) "did not complete".
    for stage in STAGES:
        if stage in REQUIRED_FOR_PROCEED and not stages[stage]["reported"]:
            if "error" in stages[stage]:
                reasons.append(f"{stage} failed: {stages[stage]['error']}")
            else:
                reasons.append(f"{stage} did not complete")

    proceed = all(stages[s]["reported"] for s in REQUIRED_FOR_PROCEED)

    # ── Degradations: the data is right, but something worth knowing happened ──
    ex = events.get("extract")
    if ex and ex.get("fell_back_to_csv"):
        reasons.append(f"extract fell back to CSV after {ex.get('bq_attempts')} "
                       f"BigQuery attempt(s); this run did not read BigQuery")

    enc = events.get("encrypt")
    if enc and enc.get("spark_configured") and enc.get("engine") != "spark":
        reasons.append(f"encrypt ran on {enc.get('engine')} although Spark was configured")

    hive = events.get("hive_register")
    if hive is None:
        if "error" in stages["hive_register"]:
            reasons.append(f"hive_register failed: {stages['hive_register']['error']}")
        elif proceed:
            reasons.append("hive_register did not report; table registration unknown")
    elif not hive.get("registered"):
        stages["hive_register"]["ok"] = False
        reasons.append(f"hive table not registered: {hive.get('reason') or 'no reason given'}")

    rep = events.get("report")
    if rep and _collect_failed(rep.get("collected")):
        reasons.append(f"delivery copy failed: {rep.get('collected')}")

    if not proceed:
        status = STATUS_FAILED
    elif reasons:
        status = STATUS_DEGRADED
    else:
        status = STATUS_COMPLETE

    return Verdict(
        proceed=proceed,
        status=status,
        reasons=reasons,
        stages=stages,
        outputs=_outputs(events),
        data_window=_data_window(ex),
        computed_at=_iso_now(now),
    )


MAX_ERROR_CHARS = 1000


def _one_line(text: str) -> str:
    """An error as one bounded line: a reason is read in a table cell, a PDF
    line and a log line, none of which want a traceback."""
    flat = " ".join(part.strip() for part in str(text).splitlines() if part.strip())
    return flat if len(flat) <= MAX_ERROR_CHARS else flat[:MAX_ERROR_CHARS - 1] + "…"


def _collect_failed(collected) -> bool:
    """`collected` is a filesystem path on success, otherwise the failure reason."""
    if collected is None:
        return True
    s = str(collected)
    return not (s.startswith("/") or s.endswith(".pdf"))


def _outputs(events: Mapping[str, dict | None]) -> dict:
    load = events.get("load") or {}
    hive = events.get("hive_register") or {}
    rep = events.get("report") or {}
    pub = events.get("publish") or {}
    topics = {}
    if pub.get("topic_encrypted"):
        topics["encrypted"] = pub["topic_encrypted"]
    if pub.get("topic_public"):
        topics["flat"] = pub["topic_public"]
    rows_loaded = None
    if "inserted" in load and "updated" in load:
        rows_loaded = int(load["inserted"]) + int(load["updated"])
    return {
        "warehouse_table": f"warehouse.{load['dataset']}" if load.get("dataset") else None,
        "hive_table": hive.get("table") if hive.get("registered") else None,
        "pdf": rep.get("pdf"),
        "rows_published": pub.get("rows"),
        "rows_loaded": rows_loaded,
        "topics": topics,
    }


def _data_window(extract: dict | None) -> dict:
    if not extract:
        return {"start": None, "end": None, "source": "unknown"}
    return {
        "start": extract.get("window_date"),
        "end": extract.get("window_end"),
        "source": extract.get("window_source") or "unknown",
    }
