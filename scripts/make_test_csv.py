"""Generate a CSV of any size for the console upload and the scale test.

    python3 scripts/make_test_csv.py --rows 1000 > small.csv          # ~120 KB
    python3 scripts/make_test_csv.py --rows 100000 > medium.csv       # ~12 MB
    python3 scripts/make_test_csv.py --gigabytes 2 > big.csv          # the 2 GB test

Rows carry the shapes the pipeline classifies as sensitive -- a name, an
email, a street address, a Luhn-valid test card number -- so the encrypt,
card-split and mask stages all do real work at volume, plus a timestamp and
two numbers for the report's charts. Nothing here is real data. Stdlib only,
streams to stdout, constant memory. docs/DECISIONS.md D16.
"""
from __future__ import annotations

import argparse
import csv
import random
import sys
from datetime import datetime, timedelta

FIRST = ["Ada", "Grace", "Linus", "Margaret", "Alan", "Radia", "Ken", "Barbara", "Dennis", "Frances"]
LAST = ["Lovelace", "Hopper", "Torvalds", "Hamilton", "Turing", "Perlman", "Thompson", "Liskov", "Ritchie", "Allen"]
STREETS = ["Congress Ave", "Barton Springs Rd", "Lavaca St", "Rainey St", "Speedway", "Guadalupe St"]
CITIES = ["Austin", "Denver", "Portland", "Raleigh", "Madison", "Boise"]
CATEGORIES = ["grocery", "fuel", "pharmacy", "hardware", "apparel", "dining", "electronics"]
STATUS = ["settled", "settled", "settled", "pending", "refunded"]
BINS = ["411111", "455555", "520000", "555555", "601100"]

HEADER = ["order_id", "customer_name", "customer_email", "street_address", "city",
          "card_info", "category", "status", "sale_price", "quantity", "sold_at"]

# ~120 bytes per row with this shape.
BYTES_PER_ROW = 120


def luhn_card(rng: random.Random) -> str:
    body = rng.choice(BINS) + "".join(rng.choice("0123456789") for _ in range(9))
    total = 0
    for i, ch in enumerate(reversed(body)):
        d = int(ch)
        if i % 2 == 0:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return body + str((10 - total % 10) % 10)


def rows(n: int, seed: int):
    rng = random.Random(seed)
    base = datetime(2026, 9, 1)
    for i in range(n):
        f, l = rng.choice(FIRST), rng.choice(LAST)
        yield [
            f"ord-{i:010d}", f"{f} {l}", f"{f.lower()}.{l.lower()}{i}@example.com",
            f"{rng.randint(1, 9999)} {rng.choice(STREETS)}", rng.choice(CITIES),
            luhn_card(rng), rng.choice(CATEGORIES), rng.choice(STATUS),
            f"{rng.uniform(1, 500):.2f}", rng.randint(1, 6),
            (base + timedelta(minutes=rng.randint(0, 60 * 24 * 30))).isoformat(timespec="seconds"),
        ]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--rows", type=int)
    g.add_argument("--gigabytes", type=float)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    n = a.rows if a.rows else int(a.gigabytes * (1 << 30) / BYTES_PER_ROW)
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(HEADER)
    for r in rows(n, a.seed):
        w.writerow(r)
    print(f"wrote {n} rows", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
