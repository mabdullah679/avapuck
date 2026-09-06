"""Dataset data contracts: inference proposes, the committed file decides.

The pipeline infers a dataset's columns, types, sensitivity and key from the
CSV itself (schema.py). That is a guess, and TRUST-BOUNDARY §7 lists the ways
it can be wrong. Until now nothing held the guess still: a later export with
an extra column, a changed type, or a column the classifier now saw
differently was absorbed silently -- Postgres grew a column, Hive gained one,
and nobody was asked.

This module makes the guess a CONTRACT (docs/DECISIONS.md D10), using the
same pattern non-negotiable 3 already applies to Ranger policy:

  * The first time a dataset is seen, its inferred schema is written into
    `config/datasets/<dataset>.json` under a `schema` key, marked
    `_generated`. That file already existed as the per-dataset override file,
    so there is one place per dataset, not two.
  * It is never regenerated. A later run re-infers, compares, and FAILS on
    any difference -- naming the column, the field, the committed value and
    the observed one. Changing the contract is an edit and a commit, which the
    CI platform tier then checks.
  * `python -m pipeline.metadata.contract accept <dataset> <csv>` rewrites
    the block from the current inference and prints what changed. That is a
    human running an authoring tool, not the pipeline deciding.

A dataset may also declare `event_time_column`. When it does, the extract's
data window is the earliest and latest value of that column in the landed
rows, and the PDF prints that rather than the run date (D13).
"""
from __future__ import annotations

import csv as csvmod
import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

from pipeline.metadata import schema as schema_mod
from pipeline.metadata.schema import SchemaManifest

log = logging.getLogger(__name__)

CONTRACT_KEY = "schema"
EVENT_TIME_KEY = "event_time_column"

# Facts the contract holds per column. Order and nullability are deliberately
# NOT here: a reordered export or an empty cell is not a schema change.
_COLUMN_FACTS = ("source_name", "sql_type", "sensitivity", "treatment")


class ContractMismatch(RuntimeError):
    """The inferred schema disagrees with the committed contract."""

    def __init__(self, dataset: str, path: Path, differences: list[str]):
        self.dataset = dataset
        self.path = path
        self.differences = differences
        body = "\n  ".join(differences)
        super().__init__(
            f"dataset {dataset!r} no longer matches its contract {path}:\n  {body}\n"
            f"If the change is intended, update the contract and commit it:\n"
            f"  python -m pipeline.metadata.contract accept {dataset} <landed csv>")


@dataclass
class ContractCheck:
    path: Path
    created: bool
    differences: list[str] = field(default_factory=list)
    event_time_column: str | None = None
    window: tuple[str, str] | None = None


def contract_dir() -> Path:
    """Resolved at call time so tests can point it at a temp directory."""
    return schema_mod.DATASET_CONFIG_DIR


def contract_path(dataset: str) -> Path:
    return contract_dir() / f"{dataset}.json"


def block_from_manifest(manifest: SchemaManifest) -> dict:
    """The contract block: exactly the facts a later run is held to."""
    return {
        "_generated": True,
        "primary_key": manifest.primary_key,
        "key_is_synthetic": manifest.key_is_synthetic,
        "columns": {
            c.name: {k: getattr(c, k) for k in _COLUMN_FACTS}
            for c in manifest.columns
        },
    }


def diff(committed: dict, manifest: SchemaManifest) -> list[str]:
    """Every way the inferred manifest departs from the committed block."""
    out: list[str] = []
    if committed.get("primary_key") != manifest.primary_key:
        out.append(f"primary_key: contract {committed.get('primary_key')!r}, "
                   f"observed {manifest.primary_key!r}")
    if bool(committed.get("key_is_synthetic")) != bool(manifest.key_is_synthetic):
        out.append(f"key_is_synthetic: contract {committed.get('key_is_synthetic')!r}, "
                   f"observed {manifest.key_is_synthetic!r}")

    want = committed.get("columns", {})
    have = {c.name: c for c in manifest.columns}
    for name in sorted(set(want) - set(have)):
        out.append(f"column {name!r}: in the contract, missing from the data")
    for name in sorted(set(have) - set(want)):
        out.append(f"column {name!r}: in the data, not in the contract "
                   f"(observed {have[name].sensitivity}, {have[name].sql_type})")
    for name in sorted(set(want) & set(have)):
        for fact in _COLUMN_FACTS:
            c, o = want[name].get(fact), getattr(have[name], fact)
            if c != o:
                out.append(f"column {name!r} {fact}: contract {c!r}, observed {o!r}")
    return out


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise RuntimeError(f"dataset contract {path} is not valid JSON: {e}") from e


def _write(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2) + "\n")


def ensure_contract(manifest: SchemaManifest, csv_path: Path | None = None) -> ContractCheck:
    """Hold the manifest to its contract. Write one if none exists. Raise on drift.

    Returns a ContractCheck the caller records in the stage event: whether a
    contract was created this run (a reviewer should look), the event-time
    column if declared, and the data window derived from it.
    """
    path = contract_path(manifest.dataset)
    doc = _load(path)
    event_time = doc.get(EVENT_TIME_KEY)

    if CONTRACT_KEY not in doc:
        doc[CONTRACT_KEY] = block_from_manifest(manifest)
        doc.setdefault("_comment", [
            f"Data contract for dataset {manifest.dataset!r}.",
            "",
            "The `schema` block was GENERATED from inference the first time this",
            "dataset was seen and is never regenerated. A later run whose inferred",
            "schema differs FAILS naming the difference. To accept a change, edit",
            "this block (or run `python -m pipeline.metadata.contract accept`) and",
            "commit. `columns` above/below are sensitivity overrides fed INTO",
            "inference; `schema` is what inference produced. Optional:",
            "`event_time_column` names the column the PDF's data window is read from.",
        ])
        _write(path, doc)
        log.warning("no data contract for %s; wrote %s from inference -- REVIEW it",
                    manifest.dataset, path)
        check = ContractCheck(path=path, created=True, event_time_column=event_time)
    else:
        differences = diff(doc[CONTRACT_KEY], manifest)
        if differences:
            raise ContractMismatch(manifest.dataset, path, differences)
        check = ContractCheck(path=path, created=False, event_time_column=event_time)

    if event_time and csv_path is not None:
        if manifest.column(event_time) is None:
            raise RuntimeError(
                f"{path} declares event_time_column {event_time!r}, which is not a "
                f"column of {manifest.dataset}")
        check.window = event_time_window(Path(csv_path), manifest, event_time)
    return check


def event_time_window(csv_path: Path, manifest: SchemaManifest,
                      column: str) -> tuple[str, str] | None:
    """(earliest, latest) non-empty value of `column`, as ISO-ish strings.

    Compared lexically, which is correct for ISO 8601 and for the
    YYYY-MM-DD[ T]HH:MM shapes the type inferencer accepts as TIMESTAMP.
    """
    spec = manifest.column(column)
    source = spec.source_name if spec else column
    lo = hi = None
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        for row in csvmod.DictReader(fh):
            v = (row.get(source) or "").strip()
            if not v:
                continue
            lo = v if lo is None or v < lo else lo
            hi = v if hi is None or v > hi else hi
    if lo is None:
        return None
    return lo, hi


def accept(dataset: str, csv_path: Path) -> list[str]:
    """Authoring tool: rewrite the schema block from current inference."""
    manifest = schema_mod.infer(csv_path, dataset=dataset)
    path = contract_path(dataset)
    doc = _load(path)
    before = doc.get(CONTRACT_KEY, {})
    changes = diff(before, manifest) if before else ["(no contract existed)"]
    block = block_from_manifest(manifest)
    block["_generated"] = False          # a human accepted this one
    doc[CONTRACT_KEY] = block
    _write(path, doc)
    return changes


def main(argv=None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 3 or args[0] != "accept":
        print("usage: python -m pipeline.metadata.contract accept <dataset> <landed csv>",
              file=sys.stderr)
        return 2
    changes = accept(args[1], Path(args[2]))
    print(f"contract for {args[1]!r} updated at {contract_path(args[1])}")
    for c in changes:
        print(f"  {c}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
