"""Compare the Spark medallion output against the local delta-rs output.

Two engines running the SAME bindings, the SAME 20 operators and the SAME
frozen contract should produce the same Gold. Where they do not, the
difference is a defect in one of the two engine adapters -- never in the
mappings, which neither engine is allowed to contain.

This check is the reason the Spark path is worth anything. "It ran without
error" says nothing; "it produced identical figures to an independently
written path" is evidence.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from deltalake import DeltaTable

ROOT = Path(__file__).resolve().parents[1]
LAKE = ROOT / "lake"

METRICS = ["active_accounts", "gross_volume_minor", "net_revenue_minor",
           "settled_txn_count", "refund_count", "chargeback_count"]


def key(r):
    return (r["merchant_id"], r["contract_id"], r["period_id"])


def main() -> int:
    local = {key(r): r for r in
             DeltaTable(str(LAKE / "gold/quarterly_performance"))
             .to_pyarrow_table().to_pylist() if not r["is_projection"]}
    spark = {key(r): r for r in
             DeltaTable(str(LAKE / "gold_spark")).to_pyarrow_table().to_pylist()}

    print("=" * 70)
    print("ENGINE EQUIVALENCE — local delta-rs vs Spark cluster".center(70))
    print("=" * 70)
    print(f"local gold rows : {len(local)}")
    print(f"spark gold rows : {len(spark)}")

    only_local = set(local) - set(spark)
    only_spark = set(spark) - set(local)
    if only_local or only_spark:
        print(f"GRAIN MISMATCH  : {len(only_local)} local-only, {len(only_spark)} spark-only")
    else:
        print("grain keys      : identical")

    shared = set(local) & set(spark)
    diffs = {m: [] for m in METRICS}
    src_diffs = []
    for k in shared:
        l, s = local[k], spark[k]
        for m in METRICS:
            if l.get(m) != s.get(m):
                diffs[m].append((k, l.get(m), s.get(m)))
        for m in ("active_accounts", "gross_volume_minor", "net_revenue_minor"):
            lv = json.loads(l.get(f"{m}_by_source") or "{}")
            sv = json.loads(s.get(f"{m}_by_source") or "{}")
            if {a: b.get("value") for a, b in lv.items()} != \
               {a: b.get("value") for a, b in sv.items()}:
                src_diffs.append((k, m))

    print("-" * 70)
    total = 0
    for m in METRICS:
        n = len(diffs[m])
        total += n
        flag = "OK" if n == 0 else f"{n} DIFFER"
        print(f"  {m:26s} {flag}")
    print(f"  {'by_source preservation':26s} "
          f"{'OK' if not src_diffs else f'{len(src_diffs)} DIFFER'}")
    print("-" * 70)

    if total == 0 and not src_diffs and not only_local and not only_spark:
        print("ENGINES AGREE — identical Gold from two independent execution paths.")
        print("The mappings live in config; only the engine adapter differs.")
        print("=" * 70)
        return 0

    print(f"ENGINES DISAGREE on {total} metric values.")
    for m in METRICS:
        for k, lv, sv in diffs[m][:4]:
            print(f"  {k} {m}: local={lv} spark={sv}")
    print("=" * 70)
    return 1


if __name__ == "__main__":
    sys.exit(main())
