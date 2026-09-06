"""The two CI tiers for contracts, as one script with two subcommands.

    python scripts/ci/contracts_check.py examples
        Service tier: every file in contracts/examples validates against the
        schema its name selects.

    python scripts/ci/contracts_check.py platform [--base main]
        Platform tier: every payload schema embeds the envelope by reference
        without overriding it, and every schema is backward-compatible with
        the version at --base (a git ref). A change to the envelope fails
        every payload schema whose example no longer validates, and the red
        list printed here is the blast radius.

Exit status is the number of problems, capped at 1, so make and Jenkins both
read it as pass/fail. docs/DECISIONS.md D10, D11.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pipeline.messaging import contracts as C  # noqa: E402


def tier_examples() -> list[str]:
    problems = []
    examples = C.iter_examples()
    if not examples:
        problems.append("contracts/examples is empty")
    for path, doc in examples:
        name = C.schema_name_for_example(path)
        try:
            errors = list(C.validator_for(name).iter_errors(doc))
        except C.ContractError as e:
            problems.append(f"{path.name}: {e}")
            continue
        for err in errors[:3]:
            where = "/".join(str(p) for p in err.absolute_path) or "<root>"
            problems.append(f"{path.name} vs {name} at {where}: {err.message}")
        if not errors:
            print(f"  ok  {path.name} validates against {name}")
    return problems


def _at_ref(ref: str, rel: str) -> dict | None:
    r = subprocess.run(["git", "show", f"{ref}:{rel}"], cwd=ROOT,
                       capture_output=True, text=True)
    if r.returncode != 0:
        return None
    return json.loads(r.stdout)


def tier_platform(base: str) -> list[str]:
    problems = []

    embed = C.payload_schemas_embed_envelope()
    for p in embed:
        problems.append(f"embedding: {p}")
    if not embed:
        print("  ok  every payload schema embeds the envelope without overriding it")

    for path in C.schema_files():
        rel = path.relative_to(ROOT).as_posix()
        old = _at_ref(base, rel)
        if old is None:
            print(f"  new {rel} (not present at {base}); nothing to compare")
            continue
        new = json.loads(path.read_text())
        breaks = C.check_backward_compatible(old, new)
        if breaks:
            for b in breaks:
                problems.append(f"{rel} vs {base}: {b}")
        else:
            print(f"  ok  {rel} is backward-compatible with {base}")
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="tier", required=True)
    sub.add_parser("examples")
    p = sub.add_parser("platform")
    p.add_argument("--base", default="main", help="git ref to compare against")
    args = ap.parse_args(argv)

    print(f"contracts: {args.tier} tier")
    problems = tier_examples() if args.tier == "examples" else tier_platform(args.base)
    if problems:
        print(f"\n{len(problems)} problem(s):")
        for pr in problems:
            print(f"  FAIL {pr}")
        return 1
    print("  all good")
    return 0


if __name__ == "__main__":
    sys.exit(main())
