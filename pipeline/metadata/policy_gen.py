"""Generate Apache Ranger masking policy JSON for an inferred schema.

WHY GENERATE RATHER THAN DECIDE AT RUNTIME
==========================================
Non-negotiable #3: masking rules live in Ranger policy JSON, authored once and
enforced at the Hive boundary -- never in transformation code. A pipeline that
accepts unknown datasets still has to honour that, so this module writes a
POLICY FILE, and the mask stage then reads it through the same Ranger provider
as the hand-authored trips policy. Nothing here runs at mask time.

The generated file is a starting point a human can edit, not a final authority
that regenerates behind their back:

  * It is written once, when a dataset is first seen, and never silently
    overwritten -- see `ensure_policy`. An edited mask type therefore survives
    the next run, which is the entire point of authoring policy in a file.
  * Every policy carries a `_generated` marker and the reason the classifier
    gave, so a reviewer can tell a guessed policy from an authored one.

The output is real Ranger policy JSON in the shape the REST API accepts, so
`RangerAdminProvider.push_policies` can upload it unchanged.
"""
from __future__ import annotations

import json
from dataclasses import replace
import logging
from pathlib import Path

from pipeline.metadata.schema import (
    DEFAULT_MASK_FOR_CLASS,
    DIRECT_IDENTIFIER,
    SchemaManifest,
)

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
POLICY_DIR = ROOT / "config" / "ranger"

SERVICE = "hive_pipeline"

# The audience split mirrors the hand-authored trips policy: analysts see
# masked values, data stewards see through the mask via a separate access path.
ANALYST_GROUP = "analyst"
STEWARD_GROUP = "data_steward"


def policy_path(dataset: str) -> Path:
    return POLICY_DIR / f"{dataset}_masking_policies.json"


def _policy_for_column(dataset: str, database: str, table: str,
                       column, mask_type: str) -> dict:
    return {
        "service": SERVICE,
        "name": f"mask-{dataset}-{column.name}".replace("_", "-"),
        "policyType": 1,
        "description": (
            f"{column.name} classified {column.sensitivity} by schema inference: "
            f"{column.reason}."),
        "isEnabled": True,
        # Not part of Ranger's schema; marks provenance for a human reviewer.
        "_generated": True,
        "_sensitivity": column.sensitivity,
        "resources": {
            "database": {"values": [database], "isExcludes": False, "isRecursive": False},
            "table": {"values": [table], "isExcludes": False, "isRecursive": False},
            "column": {"values": [column.name], "isExcludes": False, "isRecursive": False},
        },
        "dataMaskPolicyItems": [
            {
                "accesses": [{"type": "select", "isAllowed": True}],
                "users": [], "groups": [ANALYST_GROUP], "roles": [],
                "dataMaskInfo": {"dataMaskType": mask_type},
            },
            {
                "accesses": [{"type": "select", "isAllowed": True}],
                "users": [], "groups": [STEWARD_GROUP], "roles": [],
                "dataMaskInfo": {"dataMaskType": "MASK_NONE"},
            },
        ],
    }


def build(manifest: SchemaManifest, database: str, table: str,
          overrides: dict | None = None) -> dict:
    """Build the policy document for every sensitive column in the manifest."""
    overrides = overrides or {}
    col_overrides = overrides.get("columns", {})

    policies = []
    for column in manifest.columns:
        if not column.sensitive:
            continue
        declared = (col_overrides.get(column.name) or {}).get("mask")
        mask_type = declared or DEFAULT_MASK_FOR_CLASS.get(
            column.sensitivity, DEFAULT_MASK_FOR_CLASS[DIRECT_IDENTIFIER])
        policies.append(
            _policy_for_column(manifest.dataset, database, table, column, mask_type))

    return {
        "_comment": [
            f"GENERATED Ranger masking policies for dataset {manifest.dataset!r}.",
            "",
            "Written once by pipeline/metadata/policy_gen.py when this dataset was",
            "first ingested, from the sensitivity inferred for each column. It is",
            "NOT regenerated on later runs -- edit the mask types here and your",
            "edits survive, which is what authoring policy in a file is for.",
            "",
            "Each policy records why the classifier thought the column was",
            "sensitive. Review those reasons: a guess that reads wrong is a",
            "signal to fix the mask type here, or the sensitivity itself in",
            f"config/datasets/{manifest.dataset}.json.",
        ],
        "policies": policies,
    }


def _existing_coverage(database: str, table: str) -> dict[Path, set[str]]:
    """Which columns of this table are already covered, and by which file.

    Scans every policy file in config/ranger, not just this dataset's own,
    because the trips policy was authored by hand under a different filename
    (hive_masking_policies.json) long before generation existed. Generating a
    second policy for a column that already has one would shadow a deliberate
    masking choice with a guessed default -- the exact failure this prevents.
    """
    coverage: dict[Path, set[str]] = {}
    if not POLICY_DIR.exists():
        return coverage
    for path in sorted(POLICY_DIR.glob("*.json")):
        try:
            policies = json.loads(path.read_text()).get("policies", [])
        except json.JSONDecodeError:
            log.warning("skipping unreadable policy file %s", path)
            continue
        columns: set[str] = set()
        for p in policies:
            if not p.get("isEnabled", True) or p.get("policyType") != 1:
                continue
            res = p.get("resources", {})
            if database not in res.get("database", {}).get("values", []):
                continue
            if table not in res.get("table", {}).get("values", []):
                continue
            columns.update(res.get("column", {}).get("values", []))
        if columns:
            coverage[path] = columns
    return coverage


def ensure_policy(manifest: SchemaManifest, database: str, table: str,
                  overrides: dict | None = None) -> tuple[Path, bool]:
    """Ensure this table's sensitive columns have policy. Returns (path, created).

    Generates nothing when the columns are already covered -- by this
    dataset's file or any other, such as the hand-authored trips policy. An
    authored policy is the authority; regenerating over it, or alongside it,
    would quietly replace a deliberate masking decision with a default.
    """
    path = policy_path(manifest.dataset)
    needed = set(manifest.sensitive_columns)

    coverage = _existing_coverage(database, table)
    covered: set[str] = set()
    for covered_cols in coverage.values():
        covered |= covered_cols

    if needed and needed <= covered:
        # Point the caller at whichever file actually covers the columns, so
        # the mask stage loads that one rather than a file it never wrote.
        best = max(coverage, key=lambda p: len(coverage[p] & needed))
        log.info("%s already has masking policy for %s in %s; not generating",
                 table, sorted(needed), best.name)
        return best, False

    if path.exists():
        # The file exists but does not cover every sensitive column -- which
        # happens when a dataset gains one (a new `card_info` column in a
        # later export, say). Returning here unchanged would leave a policy
        # with a hole in it and fail the mask stage, so append policy for the
        # UNCOVERED columns only.
        #
        # Existing entries are never touched: generation is authoring, and an
        # edited mask type must survive. Only additions are made, and each new
        # entry carries `_generated` for review like any other.
        missing = sorted(needed - covered)
        if missing:
            doc = json.loads(path.read_text())
            partial = replace(manifest, columns=[c for c in manifest.columns
                                                 if c.name in missing])
            added = build(partial, database, table, overrides)["policies"]
            doc.setdefault("policies", []).extend(added)
            path.write_text(json.dumps(doc, indent=2))
            log.warning("%s gained sensitive column(s) %s; appended %d generated "
                        "policy entr%s to %s -- REVIEW the mask types",
                        table, missing, len(added),
                        "y" if len(added) == 1 else "ies", path.name)
            return path, True
        return path, False

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(build(manifest, database, table, overrides), indent=2))
    log.info("generated Ranger policy for %s at %s (%d sensitive columns)",
             manifest.dataset, path, len(manifest.sensitive_columns))
    return path, True
