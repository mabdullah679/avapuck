"""Split payment card numbers into (prefix, encrypted middle, suffix).

Runs between extract and encrypt. For every column the manifest marks with
TREATMENT_CARD_SPLIT, the PAN is broken into three parts and written to a
`card_info` table:

    card_prefix   first 6 digits, CLEAR   -- the issuer BIN
    card_middle   the digits between,     -- AES-256-GCM via the crypto service
                  ENCRYPTED
    card_suffix   last 4 digits, CLEAR    -- the usual display tail

Which columns get this treatment comes from the manifest, never from a column
name: `manifest.card_columns` is the whole input. A dataset with no card
column produces no card_info output and the stage is a no-op.

SECURITY NOTE -- read before trusting this with real cards.
First-6/last-4 is the maximum PCI-DSS permits to be stored in the clear, and
this stage never widens it. But because the middle is ENCRYPTED rather than
discarded, the full PAN remains recoverable, and for a 16-digit card only six
digits are protected -- of which the last is a Luhn check digit derivable from
the rest. The real guessing space is therefore ~10^5, which is trivial to
brute-force offline for anyone holding the table. That is acceptable for this
demo and NOT acceptable for live cardholder data; see TRUST-BOUNDARY.md §2.8.
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path

from pipeline.metadata import schema as schema_mod

log = logging.getLogger(__name__)

PREFIX_LEN = 6   # PCI-DSS permits at most the first 6 in the clear.
SUFFIX_LEN = 4   # ...and the last 4.


def split_pan(pan: str) -> tuple[str, str, str]:
    """Break one PAN into (prefix, middle, suffix).

    Digits only -- separators in the source are dropped so the parts line up
    for cards written `4111 1111 1111 1111`. A value too short to split into
    three non-empty parts yields an empty middle: nothing to encrypt, and the
    prefix/suffix still describe it. Callers must handle an empty middle
    rather than assume every row has one.
    """
    digits = "".join(ch for ch in str(pan) if ch.isdigit())
    if len(digits) <= PREFIX_LEN + SUFFIX_LEN:
        # Too short to have a middle: keep what is there as the prefix so no
        # digits are silently lost, and leave the middle empty.
        return digits, "", ""
    return (digits[:PREFIX_LEN],
            digits[PREFIX_LEN:-SUFFIX_LEN],
            digits[-SUFFIX_LEN:])


def run(logical_date, csv_path: Path, out_dir: Path, crypto) -> dict:
    """Write one card_info table per card column found in the manifest.

    Returns a summary dict. `cards` is 0 when the dataset has no card column,
    which is the normal case for most datasets and not an error.
    """
    csv_path = Path(csv_path)
    manifest = schema_mod.load_manifest(csv_path)
    card_cols = [c for c in manifest.columns if c.split_as_card]

    if not card_cols:
        log.info("no card columns in %s; card split is a no-op", manifest.dataset)
        return {"dataset": manifest.dataset, "cards": 0, "columns": [],
                "rows": 0, "output": None}

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    d = logical_date.date() if hasattr(logical_date, "date") else logical_date
    out_path = out_dir / f"{manifest.dataset}_card_info_{d.isoformat()}.csv"

    # The manifest key is the SQL-safe name; the CSV header is the raw source
    # name. Resolve one to the other through the manifest rather than guessing
    # at either -- a synthetic key has no source column at all, in which case
    # there is nothing to carry and row_key stays empty.
    key_col = next((c for c in manifest.columns
                    if c.name == manifest.primary_key), None)
    key = key_col.source_name if key_col else None
    written = 0
    fieldnames = ["source_column", "row_key", "card_prefix",
                  "card_middle_encrypted", "card_suffix"]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for col in card_cols:
            raw = [r.get(col.source_name, "") for r in rows]
            parts = [split_pan(v) for v in raw]
            middles = [m for _, m, _ in parts]

            # One batched call per column, matching how the encrypt stage
            # talks to the crypto service. Empty middles are sent as None so
            # the service does not encrypt an empty string into something
            # that looks like real ciphertext.
            to_encrypt = [m if m else None for m in middles]
            ciphertexts, _ = crypto.encrypt_column(f"{col.name}_middle", to_encrypt)

            for row, (prefix, middle, suffix), ct in zip(rows, parts, ciphertexts):
                w.writerow({
                    "source_column": col.name,
                    "row_key": row.get(key, "") if key else "",
                    "card_prefix": prefix,
                    "card_middle_encrypted": "" if not middle else ct,
                    "card_suffix": suffix,
                })
                written += 1

    # A plaintext PAN reaching this file would defeat the entire stage, so
    # verify rather than trust: no row may contain a full card number.
    _assert_no_plaintext_pan(out_path)

    log.info("card split: %d rows from %d column(s) -> %s",
             written, len(card_cols), out_path)
    return {"dataset": manifest.dataset,
            "cards": len(card_cols),
            "columns": [c.name for c in card_cols],
            "rows": written,
            "output": str(out_path)}


def _assert_no_plaintext_pan(path: Path) -> None:
    """Fail loudly if any written row still holds a Luhn-valid full PAN.

    The prefix and suffix are meant to be in the clear; a value long enough to
    BE a card, sitting in a clear column, means the split did not happen.
    """
    with path.open(newline="", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f), start=2):
            for field in ("card_prefix", "card_suffix"):
                v = "".join(ch for ch in row.get(field, "") if ch.isdigit())
                if len(v) >= 13 and schema_mod._luhn_ok(v):
                    raise AssertionError(
                        f"{path}:{i}: {field} holds what looks like a full card "
                        f"number; the split stage did not protect this row")
