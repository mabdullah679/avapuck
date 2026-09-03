"""Infer a dataset's schema, sensitivity and key from the CSV itself.

WHY THIS EXISTS
===============
Every stage used to name trip columns literally: SENSITIVE_COLUMNS in the
encrypt job, COLUMN_MAP in the mask job, an UPSERT keyed on trip_id, a Ranger
policy keyed to `trips`. That made the pipeline correct for exactly one
dataset. This module derives the same facts from the data, so the stages can
be written once and run against anything.

WHAT IT PRODUCES
================
A SchemaManifest: the column list with inferred types, which columns are
sensitive (and therefore encrypted, then masked), which are blind-indexed, and
what to key the warehouse upsert on. The manifest is written next to the data
and read by every later stage -- so all five stages agree on the schema by
construction rather than by each re-deriving it.

CLASSIFICATION IS A GUESS, AND SAYS SO
======================================
Sensitivity is inferred from column names and value shapes. That is a
heuristic and it will sometimes be wrong, which is why every verdict carries a
`reason` and can be overridden per-dataset in config/datasets/<name>.json.
A wrong guess is then a config edit, not a code change. The classifier is
deliberately biased toward over-classifying: a column wrongly treated as
sensitive costs an encrypt round trip, while one wrongly treated as public
leaks.
"""
from __future__ import annotations

import csv as csvmod
import hashlib
import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
DATASET_CONFIG_DIR = ROOT / "config" / "datasets"

MANIFEST_SUFFIX = ".schema.json"

# How many rows the inferencer reads to decide types and uniqueness. The whole
# file would be more accurate and is not worth it: the cap is 100 rows/run by
# design, and a sample this size settles types on far larger inputs too.
SAMPLE_ROWS = 500


# ── Sensitivity classes ───────────────────────────────────────────────────
#
# Ordered most to least sensitive. The class drives BOTH the encryption
# decision and the default Ranger mask type, so the two can never disagree
# about what a column is.
DIRECT_IDENTIFIER = "direct_identifier"   # names, emails, phones, SSNs
QUASI_IDENTIFIER = "quasi_identifier"     # ids/locations that re-identify in combination
PUBLIC = "public"                         # measures, dates, codes, geometry

# Special downstream handling for a column, beyond encrypt-then-mask. Set on
# the manifest at inference time so no stage has to recognise a column by name.
TREATMENT_CARD_SPLIT = "card_split"   # PAN -> (prefix, encrypted middle, suffix)

# Default Ranger mask per class. Authored here as a DEFAULT for policy
# GENERATION only -- the generated policy JSON is what actually enforces, and
# it is reviewable and editable. No masking decision is read from this map at
# transform time.
DEFAULT_MASK_FOR_CLASS = {
    DIRECT_IDENTIFIER: "MASK_HASH",
    QUASI_IDENTIFIER: "MASK_SHOW_LAST_4",
}

# Column-name patterns that signal a direct identifier. Matched against the
# normalised (lowercased, non-alphanumerics collapsed) column name.
_DIRECT_PATTERNS = [
    (r"(^|_)(email|e_?mail)($|_)", "column name denotes an email address"),
    (r"(^|_)(phone|mobile|telephone|fax)($|_)", "column name denotes a phone number"),
    (r"(^|_)(ssn|social_security|nin|tax_?id)($|_)", "column name denotes a government id"),
    (r"(^|_)(first|last|full|given|sur|middle)_?name($|_)", "column name denotes a person name"),
    (r"(^|_)(customer|person|user|member|employee|rider|owner|operator|lessee|applicant)_?name($|_)",
     "column name denotes a named party"),
    (r"(^|_)(address|street|addr|postcode|postal_?code|zip|zip_?code)($|_)",
     "column name denotes a postal address"),
    (r"(^|_)(dob|birth_?date|date_?of_?birth)($|_)", "column name denotes a date of birth"),
    (r"(^|_)(passport|licen[cs]e|driver)($|_)", "column name denotes a licence or passport"),
    (r"(^|_)(credit_?card|iban|account_?number)($|_)", "column name denotes a financial account"),
    # `card_info`, `card_number`, `cardno`, `pan` -- a column about a payment
    # card is a direct identifier whatever the export happened to call it.
    (r"(^|_)(card|card_?no|card_?num|card_?number|card_?info|pan)($|_)",
     "column name denotes a payment card"),
]

# Patterns that signal a quasi-identifier: not identifying alone, but narrowing
# in combination, so it is encrypted at rest and masked on the way out.
_QUASI_PATTERNS = [
    (r"(^|_)name($|_)", "a bare `name` column usually labels a real entity"),
    (r"(^|_)(station|site|facility|location|place|venue)($|_)", "column names a specific location"),
    (r"(^|_)(lat|latitude|lon|lng|longitude|geo|coord)($|_)", "column carries a precise coordinate"),
    (r"(^|_)(county|city|town|district|region|parcel)($|_)", "column carries a narrowing geography"),
    (r"(^|_)(bike|vehicle|device|asset|serial)_?id($|_)", "column identifies a trackable asset"),
    (r"(^|_)(subscriber|membership|tier|plan|segment)($|_)", "column carries a customer attribute"),
    (r"(^|_)(guid|uuid|globalid)($|_)", "column is a globally unique handle for one record"),
]

# Value-shape probes, applied when the NAME says nothing. Names are unreliable
# in real exports (`f_3`, `col_7`), so the values get a vote too.
_VALUE_PROBES = [
    (re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$", re.I), DIRECT_IDENTIFIER,
     "values look like email addresses"),
    (re.compile(r"^\+?\d[\d\s().-]{7,}$"), DIRECT_IDENTIFIER,
     "values look like phone numbers"),
    (re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I),
     QUASI_IDENTIFIER, "values are UUIDs, which uniquely handle one record"),
    # 13-19 digits, optionally grouped. Narrowed by a Luhn check in _classify
    # so that long numeric ids (parcel numbers, object ids) are not swept up.
    (re.compile(r"^\d[\d \-]{11,21}\d$"), DIRECT_IDENTIFIER,
     "values look like payment card numbers (Luhn-valid)"),
]

# Names that are structural rather than descriptive -- never sensitive, and
# never chosen as a natural key on name alone.
_NEVER_SENSITIVE = {
    "logical_date", "dt", "masked_by", "loaded_at", "row_hash", "_ingested_at",
}


def _normalise(name: str) -> str:
    """Lowercase, strip a BOM, collapse punctuation to underscores.

    Real CSV headers arrive as `Sale Year`, `Oil & Gas Sale Status`, or with a
    UTF-8 BOM glued to the first one. Matching patterns against the raw header
    would miss all of those.
    """
    n = name.replace("﻿", "").strip().lower()
    n = re.sub(r"[^a-z0-9]+", "_", n)
    return n.strip("_")


def safe_column(name: str) -> str:
    """A SQL-safe identifier for a column, stable across runs.

    Postgres and Hive both reject `Shape__Area` unquoted and choke on `Oil &
    Gas Sale Status` entirely. Normalising once here means the warehouse DDL,
    the upsert and the Ranger policy all name the column the same way.
    """
    n = _normalise(name)
    if not n:
        n = "col"
    if n[0].isdigit():
        n = f"c_{n}"
    return n[:63]  # Postgres identifier limit.


@dataclass
class ColumnSpec:
    """One column, as inferred."""
    source_name: str          # exactly as it appears in the CSV header
    name: str                 # SQL-safe name used everywhere downstream
    sql_type: str             # TEXT / BIGINT / DOUBLE PRECISION / TIMESTAMP
    sensitivity: str          # one of the three classes above
    reason: str               # why the classifier said so; shown in the manifest
    nullable: bool = True
    unique: bool = False      # every sampled value distinct and non-null
    treatment: str = ""       # optional special handling, e.g. TREATMENT_CARD_SPLIT

    @property
    def sensitive(self) -> bool:
        return self.sensitivity in (DIRECT_IDENTIFIER, QUASI_IDENTIFIER)

    @property
    def split_as_card(self) -> bool:
        """Whether the split stage should break this column into 3 parts.

        A marker on the manifest, not a name check: the split stage asks the
        metadata what to do and never learns that any column is called
        `card_info`. Rename the column, or add a second card column, and the
        behaviour follows the data.
        """
        return self.treatment == TREATMENT_CARD_SPLIT


@dataclass
class SchemaManifest:
    """Everything the later stages need to know about a dataset."""
    dataset: str
    source_file: str
    row_count: int
    columns: list[ColumnSpec]
    primary_key: str          # column name, or ROW_HASH_KEY
    key_is_synthetic: bool
    overrides_applied: list[str] = field(default_factory=list)

    # ── Views the stages ask for ──────────────────────────────────────────
    @property
    def card_columns(self) -> list[str]:
        """Columns the split stage must break into prefix/middle/suffix."""
        return [c.name for c in self.columns if c.split_as_card]

    @property
    def sensitive_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.sensitive]

    @property
    def public_columns(self) -> list[str]:
        return [c.name for c in self.columns if not c.sensitive]

    @property
    def blind_indexed(self) -> list[str]:
        """Sensitive columns that also get a deterministic blind index.

        Only columns that are plausibly JOINED on are worth the extra index:
        a unique or id-like sensitive column. Blind-indexing a free-text
        address buys nothing and widens what a stolen index reveals.

        A sensitive PRIMARY KEY is always included, and that is load-bearing
        rather than an optimisation: the encrypt stage keys such a row on its
        blind index, because writing the key in plaintext would undo the
        encryption the column just went through.
        """
        out = []
        for c in self.columns:
            if not c.sensitive:
                continue
            if c.unique or c.name == self.primary_key or re.search(r"(^|_)id($|_)", c.name):
                out.append(c.name)
        return out

    @property
    def key_is_sensitive(self) -> bool:
        col = self.column(self.primary_key)
        return bool(col and col.sensitive)

    def column(self, name: str) -> ColumnSpec | None:
        return next((c for c in self.columns if c.name == name), None)

    def to_json(self) -> str:
        d = asdict(self)
        d["sensitive_columns"] = self.sensitive_columns
        d["blind_indexed"] = self.blind_indexed
        return json.dumps(d, indent=2)

    @classmethod
    def from_json(cls, text: str) -> SchemaManifest:
        d = json.loads(text)
        return cls(
            dataset=d["dataset"],
            source_file=d["source_file"],
            row_count=d["row_count"],
            columns=[ColumnSpec(**c) for c in d["columns"]],
            primary_key=d["primary_key"],
            key_is_synthetic=d["key_is_synthetic"],
            overrides_applied=d.get("overrides_applied", []),
        )


ROW_HASH_KEY = "row_hash"


def row_hash(row: dict, columns: list[str], logical_date: str) -> str:
    """A deterministic key for a row that has no natural one.

    Keyed on the row's own values plus the logical date, so re-running the same
    date produces the same hashes and the upsert replaces rather than
    duplicates -- the same idempotency guarantee trip_id gave, without needing
    the source to supply a key.
    """
    payload = "\x1f".join(f"{c}={row.get(c) if row.get(c) is not None else ''}"
                          for c in sorted(columns))
    return hashlib.sha256(f"{logical_date}\x1e{payload}".encode()).hexdigest()[:32]


# ── Type inference ────────────────────────────────────────────────────────
_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?(\d+\.\d*|\.\d+|\d+)([eE][-+]?\d+)?$")
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?)?")


def _infer_type(values: list[str]) -> str:
    """Narrowest type every non-empty value fits, else TEXT."""
    seen = [v for v in values if v not in (None, "")]
    if not seen:
        return "TEXT"
    if all(_INT_RE.match(v) for v in seen):
        # Values beyond int8 must not silently wrap; keep them as text.
        return "BIGINT" if all(abs(int(v)) < 2**63 for v in seen) else "TEXT"
    if all(_FLOAT_RE.match(v) for v in seen):
        return "DOUBLE PRECISION"
    if all(_TS_RE.match(v) for v in seen):
        return "TIMESTAMP"
    return "TEXT"


def _luhn_ok(value: str) -> bool:
    """Luhn checksum, the standard validity test for a payment card number.

    Used to separate real card numbers from other long digit strings: parcel
    numbers and object ids are the same shape, and misclassifying one as a
    card would encrypt a column nobody needs encrypted. A false NEGATIVE here
    is the dangerous direction, so the name patterns catch card-ish columns
    independently -- this probe only has to catch the unnamed case.
    """
    digits = [int(c) for c in value if c.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False
    total, parity = 0, len(digits) % 2
    for i, d in enumerate(digits):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _treatment_for(reason: str, values: list[str]) -> str:
    """Special handling implied by the classifier's verdict, if any.

    Driven by the reason the classifier already produced rather than a second
    pass over column names -- one place decides what a column is, and the
    treatment follows from that decision.

    A card column is only marked for splitting if its values actually look
    like PANs. A column NAMED like a card but holding something else (a
    truncated `card_info` that is already `****1111`, say) is left alone: the
    split stage would produce nonsense from it, and it is still encrypted as
    a whole by virtue of being sensitive.
    """
    if "payment card" not in reason:
        return ""
    sample = [v for v in values if v not in (None, "")][:50]
    if sample and sum(bool(_luhn_ok(v)) for v in sample) >= max(3, len(sample) * 0.8):
        return TREATMENT_CARD_SPLIT
    return ""


def _classify(source_name: str, values: list[str],
              sql_type: str = "TEXT") -> tuple[str, str]:
    """Decide a column's sensitivity class, and say why.

    Name patterns first (they carry intent), then value shapes (they catch
    uninformative headers). Anything unmatched is PUBLIC -- but note the
    caller still lets a per-dataset override raise it.
    """
    norm = _normalise(source_name)
    if norm in _NEVER_SENSITIVE:
        return PUBLIC, "structural column added by the pipeline"

    for pattern, why in _DIRECT_PATTERNS:
        if re.search(pattern, norm):
            return DIRECT_IDENTIFIER, why
    for pattern, why in _QUASI_PATTERNS:
        if re.search(pattern, norm):
            return QUASI_IDENTIFIER, why

    # Value probes only run on genuinely textual columns. A column that parses
    # cleanly as a number is a measurement, not an identifier -- without this
    # guard the phone-number probe matches any decimal (1527.959... reads as
    # digits-and-punctuation) and encrypts an area column as if it were PII.
    sample = [v for v in values if v not in (None, "")][:50]

    # A bare card number parses as BIGINT, so the card probe has to run on
    # numeric columns too -- but it is gated on Luhn, which a parcel number or
    # an OBJECTID will not pass. Every other probe stays TEXT-only, because a
    # column that parses cleanly as a number is a measurement, not an
    # identifier: without that guard the phone probe matches any decimal
    # (1527.959... reads as digits-and-punctuation) and encrypts an area
    # column as if it were PII.
    if sample and sum(bool(_luhn_ok(v)) for v in sample) >= max(3, len(sample) * 0.8):
        return DIRECT_IDENTIFIER, "values are Luhn-valid payment card numbers"

    if sql_type == "TEXT" and sample:
        for probe, klass, why in _VALUE_PROBES:
            if sum(bool(probe.match(v)) for v in sample) >= max(3, len(sample) * 0.8):
                return klass, why

    return PUBLIC, "no identifier pattern matched name or values"


def _load_overrides(dataset: str) -> dict:
    """Per-dataset corrections to the classifier's guesses.

    Shape: {"columns": {"<col>": {"sensitivity": "...", "mask": "..."}},
            "primary_key": "<col>"}
    Absent file means "the guesses stand", which is the common case.
    """
    path = DATASET_CONFIG_DIR / f"{dataset}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"dataset override {path} is not valid JSON: {e}. Refusing to "
            f"guess at sensitivity when someone has tried to declare it.") from e


def dataset_name_for(csv_path: Path) -> str:
    """A stable, SQL-safe dataset name derived from the file name.

    Trailing export junk (`-2400573660170243848`) and per-date suffixes are
    stripped so that `trips_2026-09-01.csv` and `trips_2026-09-02.csv` are
    recognised as the SAME dataset -- otherwise every day would create its own
    warehouse table.
    """
    stem = csv_path.stem
    stem = re.sub(r"[-_]?\d{4}-\d{2}-\d{2}$", "", stem)   # trailing ISO date
    stem = re.sub(r"[-_]?-?\d{12,}$", "", stem)           # long export id
    return safe_column(stem) or "dataset"


def infer(csv_path: Path, dataset: str | None = None) -> SchemaManifest:
    """Read a CSV and derive its schema, sensitivity and key."""
    dataset = dataset or dataset_name_for(csv_path)

    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csvmod.DictReader(fh)
        headers = reader.fieldnames or []
        sample, total = [], 0
        for row in reader:
            total += 1
            if len(sample) < SAMPLE_ROWS:
                sample.append(row)

    if not headers:
        raise RuntimeError(f"{csv_path} has no header row; cannot infer a schema.")
    if total == 0:
        raise RuntimeError(f"{csv_path} has a header but no rows.")

    overrides = _load_overrides(dataset)
    col_overrides = overrides.get("columns", {})
    applied: list[str] = []

    seen_names: set[str] = set()
    columns: list[ColumnSpec] = []
    for header in headers:
        values = [r.get(header) for r in sample]
        name = safe_column(header)
        # Two source headers can normalise to the same SQL name
        # (`Sale Year` / `Sale_Year`); keep both, disambiguated.
        if name in seen_names:
            suffix = 2
            while f"{name}_{suffix}" in seen_names:
                suffix += 1
            name = f"{name}_{suffix}"
        seen_names.add(name)

        sql_type = _infer_type(values)
        sensitivity, reason = _classify(header, values, sql_type)
        override = col_overrides.get(name) or col_overrides.get(header)
        if override and "sensitivity" in override:
            sensitivity = override["sensitivity"]
            reason = f"declared in config/datasets/{dataset}.json"
            applied.append(name)

        non_null = [v for v in values if v not in (None, "")]
        columns.append(ColumnSpec(
            source_name=header,
            name=name,
            sql_type=sql_type,
            sensitivity=sensitivity,
            reason=reason,
            nullable=len(non_null) < len(values),
            unique=bool(non_null) and len(set(non_null)) == len(non_null) == len(values),
            treatment=_treatment_for(reason, non_null),
        ))

    primary_key, synthetic = _choose_key(columns, overrides, dataset)

    manifest = SchemaManifest(
        dataset=dataset,
        source_file=str(csv_path),
        row_count=total,
        columns=columns,
        primary_key=primary_key,
        key_is_synthetic=synthetic,
        overrides_applied=applied,
    )
    log.info("inferred %s: %d columns, %d sensitive, key=%s%s",
             dataset, len(columns), len(manifest.sensitive_columns),
             primary_key, " (synthetic)" if synthetic else "")
    return manifest


def _choose_key(columns: list[ColumnSpec], overrides: dict,
                dataset: str) -> tuple[str, bool]:
    """Pick the upsert key: declared, else natural, else synthetic.

    A natural key must be unique across the WHOLE sample and non-null -- the
    `unique` flag already encodes both. Preferring an id-ish name among the
    candidates keeps the choice stable when several columns happen to be
    unique in a small sample.
    """
    declared = overrides.get("primary_key")
    if declared:
        if not any(c.name == declared for c in columns):
            raise RuntimeError(
                f"config/datasets/{dataset}.json declares primary_key "
                f"{declared!r}, which is not a column in the data.")
        return declared, False

    candidates = [c for c in columns if c.unique]
    if candidates:
        id_like = [c for c in candidates if re.search(r"(^|_)(id|key|code|guid|uuid)($|_)", c.name)]
        return (id_like or candidates)[0].name, False

    # Nothing unique: fall back to a hash of the row. Idempotency still holds
    # because the hash is derived deterministically from the row and date.
    return ROW_HASH_KEY, True


def manifest_path(data_path: Path) -> Path:
    """Where the manifest for a data file lives: a HIDDEN sidecar beside it.

    The leading dot is load-bearing, not cosmetic. Hive registers a partition
    directory as an external table and reads EVERY file in it, so a plain
    `*.schema.json` sitting next to the Parquet is handed to the Parquet
    reader and the query dies with "not a Parquet file. Expected magic number
    at tail". Hadoop, Hive and Spark all skip files starting with `.` or `_`,
    so hiding the manifest keeps it beside its data -- which is what makes the
    pipeline self-describing -- without it being mistaken for data.
    """
    return data_path.with_name("." + data_path.name + MANIFEST_SUFFIX)


def write_manifest(manifest: SchemaManifest, csv_path: Path) -> Path:
    path = manifest_path(csv_path)
    path.write_text(manifest.to_json())
    return path


def load_manifest(csv_path: Path) -> SchemaManifest:
    """Read the manifest written beside a CSV, or infer and write one.

    Inferring on demand keeps a stage runnable when it is invoked directly
    (a repair run, a test) without the extract stage having gone first.
    """
    path = manifest_path(csv_path)
    if path.exists():
        return SchemaManifest.from_json(path.read_text())
    log.info("no manifest at %s; inferring from the CSV", path)
    manifest = infer(csv_path)
    write_manifest(manifest, csv_path)
    return manifest
