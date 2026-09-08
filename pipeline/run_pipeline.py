"""Run the whole chain for one run slot, without Airflow.

Airflow orchestrates this in production; this entry point exists so the data
path can be exercised and tested without a scheduler running. It walks the
SAME stage list the DAG does (pipeline/stages.py) -- there is no second
description of the pipeline here to drift from the first.

    python -m pipeline.run_pipeline                        # now, all stages
    python -m pipeline.run_pipeline --slot 2026-09-06T04:00Z
    python -m pipeline.run_pipeline --stage encrypt        # one stage, reusing
                                                           # the slot's inputs
    python -m pipeline.run_pipeline --csv path/to/export.csv
    python -m pipeline.run_pipeline --no-verdict           # skip the Kafka verdict
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import stages  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("pipeline")

DATA = Path(os.environ.get("PIPELINE_DATA_ROOT") or (ROOT / "data"))


def parse_slot(text: str | None) -> datetime:
    """An ISO timestamp (Z or offset) or a bare date; UTC either way."""
    if not text:
        return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--slot", help="run slot, e.g. 2026-09-06T04:00Z or 2026-09-06 "
                                   "(default: the current hour, UTC)")
    ap.add_argument("--date", dest="slot_alias", help=argparse.SUPPRESS)  # old spelling
    ap.add_argument("--stage", default="all", choices=["all", *stages.STAGE_NAMES],
                    help="run one stage instead of the whole chain")
    ap.add_argument("--csv", help="ingest this CSV instead of the configured extractor, "
                                  "uncapped, exactly as a console upload would")
    ap.add_argument("--dataset", help="dataset name for --csv (default: derived from the file name)")
    ap.add_argument("--row-limit", type=int, default=0,
                    help="cap for --csv; 0 = uncapped (default)")
    ap.add_argument("--no-verdict", action="store_true",
                    help="do not publish the run verdict to Kafka")
    ap.add_argument("--run-id", help="identifier for this emission (default: manual__<slot>)")
    args = ap.parse_args(argv)

    conf = {}
    if args.csv:
        conf = {"csv_path": str(Path(args.csv).resolve()), "row_limit": args.row_limit,
                "source": "upload"}
        if args.dataset:
            conf["dataset"] = args.dataset

    env = stages.RunEnv.build(DATA, parse_slot(args.slot or args.slot_alias),
                              run_id=args.run_id, conf=conf)
    log.info("run slot %s  trace %s  run_id %s", env.run_slot.isoformat(), env.trace_id, env.run_id)

    events: dict[str, dict | None] = {}
    wanted = stages.STAGE_NAMES if args.stage == "all" else [args.stage]
    failed: Exception | None = None
    for name in stages.STAGE_NAMES:
        if name not in wanted:
            events[name] = None
            continue
        try:
            ev = stages.run_stage(name, env, events)
            events[name] = ev
            log.info("%-14s %s", name.upper(),
                     " ".join(f"{k}={v}" for k, v in ev.items()
                              if k not in ("trace_id", "stage") and v is not None)[:200])
        except Exception as e:  # noqa: BLE001 -- recorded, then re-raised after the verdict
            log.error("%-14s FAILED: %s", name.upper(), e)
            events[name] = None
            failed = e
            break

    if args.stage == "all" and not args.no_verdict:
        try:
            v = stages.emit_verdict(env, events)
            log.info("VERDICT        proceed=%s status=%s event=%s", v["proceed"], v["status"],
                     v["event_id"])
            events["verdict"] = v
        except Exception as e:  # noqa: BLE001
            log.error("VERDICT        could not be published: %s", e)
            if failed is None:
                failed = e

    print("\n" + json.dumps(events, indent=2, default=str))
    if failed is not None:
        raise failed
    return 0


if __name__ == "__main__":
    sys.exit(main())
