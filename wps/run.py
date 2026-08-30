"""End-to-end batch: Bronze -> Silver -> Gold -> projections -> Delta.

Each stage is also callable on its own, which is what the Airflow DAGs do.
Running this module directly is the happy path without an orchestrator, so the
walkthrough never depends on Airflow being up.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import date

from wps.config import load_bundle
from wps.medallion import (AS_OF, LAKE, build_gold, land_bronze, land_silver,
                           _write)
from wps.operators import validate_registry
from wps.projections import project


def main(argv=None):
    ap = argparse.ArgumentParser(description="WPS batch")
    ap.add_argument("--stage", choices=["all", "bronze", "silver", "gold"], default="all")
    args = ap.parse_args(argv)

    t0 = time.time()
    bundle = load_bundle()
    validate_registry(bundle)
    print(f"contract {bundle.contract_version}  dictionary {bundle.dictionary_version}  "
          f"bundle {bundle.bundle_hash}")

    bronze = land_bronze(bundle)
    print(f"BRONZE  {sum(bronze.values()):6d} records  {bronze}")
    if args.stage == "bronze":
        return

    counts, rows, failures = land_silver(bundle)
    print(f"SILVER  {sum(counts.values()):6d} rows     {counts}")
    if failures:
        _write("_audit/silver_failures", failures)
        print(f"        {len(failures)} mapping failures written to lake/_audit/silver_failures")
    if args.stage == "silver":
        return

    gold, flags = build_gold(bundle, rows)
    proj = project(gold, AS_OF)

    cols = sorted({k for r in gold + proj for k in r})
    def norm(r):
        return {c: r.get(c) for c in cols}
    all_gold = [norm(r) for r in gold] + [norm(r) for r in proj]
    _write("gold/quarterly_performance", all_gold)
    if flags:
        _write("_audit/reconciliation_flags", flags)

    actual = len(gold)
    print(f"GOLD    {actual:6d} actual rows + {len(proj)} projected rows "
          f"-> lake/gold/quarterly_performance")
    print(f"        {len(flags)} reconciliation flags -> lake/_audit/reconciliation_flags")
    print(f"elapsed {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
