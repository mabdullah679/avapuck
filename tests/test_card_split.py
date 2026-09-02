"""Tests for the card split stage (pipeline stage 02).

The security-relevant claims are: the split is correct, no full PAN survives
in a clear column, and WHICH columns get split comes from the manifest rather
than from a column name.
"""
import csv

import pytest

from pipeline.metadata import schema as schema_mod
from pipeline.transform.card_split import PREFIX_LEN, SUFFIX_LEN, run, split_pan

VISA = "4111111111111111"
MC = "5555555555554444"
SHORT_VISA = "4222222222222"          # 13-digit, still Luhn-valid


class TestSplitPan:
    def test_splits_16_digit_pan_into_6_6_4(self):
        assert split_pan(VISA) == ("411111", "111111", "1111")

    def test_separators_are_ignored(self):
        assert split_pan("4111 1111 1111 1111") == split_pan(VISA)
        assert split_pan("4111-1111-1111-1111") == split_pan(VISA)

    def test_parts_reassemble_to_the_original(self):
        for pan in (VISA, MC, SHORT_VISA):
            assert "".join(split_pan(pan)) == pan

    def test_clear_parts_never_exceed_pci_limits(self):
        for pan in (VISA, MC, SHORT_VISA):
            prefix, _, suffix = split_pan(pan)
            assert len(prefix) <= PREFIX_LEN
            assert len(suffix) <= SUFFIX_LEN

    def test_short_value_yields_empty_middle_and_loses_no_digits(self):
        prefix, middle, suffix = split_pan("123")
        assert middle == ""
        assert prefix + middle + suffix == "123"


class TestClassification:
    """The split is driven by the manifest, not by a column name."""

    def test_card_columns_are_marked_whatever_they_are_called(self):
        for name in ("card_info", "card_number", "cardno", "pan"):
            sensitivity, reason = schema_mod._classify(name, [VISA, MC, VISA])
            assert sensitivity == schema_mod.DIRECT_IDENTIFIER
            assert schema_mod._treatment_for(reason, [VISA, MC, VISA]) == \
                schema_mod.TREATMENT_CARD_SPLIT

    def test_unnamed_column_of_card_values_is_still_caught(self):
        sensitivity, reason = schema_mod._classify("f_7", [VISA, MC, VISA], "BIGINT")
        assert sensitivity == schema_mod.DIRECT_IDENTIFIER

    def test_long_ids_that_are_not_cards_are_not_split(self):
        # Luhn-invalid digit strings of card-ish length: parcel ids, object ids.
        vals = ["12345678901234", "23456789012345", "34567890123456"]
        sensitivity, reason = schema_mod._classify("objectid", vals, "BIGINT")
        assert schema_mod._treatment_for(reason, vals) == ""

    def test_a_single_card_shaped_value_does_not_trigger_a_split(self):
        """One stray PAN in an otherwise ordinary column must not mark it.

        The threshold is deliberate. Note the safe failure mode: such a column
        is still classified sensitive by NAME and encrypted whole -- declining
        to split never means declining to protect.
        """
        vals = [VISA, "ordinary", "text"]
        _, reason = schema_mod._classify("notes", vals)
        assert schema_mod._treatment_for(reason, vals) == ""

    def test_card_named_column_holding_non_cards_is_not_split(self):
        # Already-truncated display values must not be split into nonsense.
        vals = ["****1111", "****4444", "****2222"]
        _, reason = schema_mod._classify("card_info", vals)
        assert schema_mod._treatment_for(reason, vals) == ""


class _FakeCrypto:
    """Records what it was asked to encrypt; returns marked ciphertext."""

    def __init__(self):
        self.calls = []

    def encrypt_column(self, field, values):
        self.calls.append((field, list(values)))
        return ["enc:test:" + (v or "") if v is not None else None
                for v in values], [None] * len(values)


def _write_csv(path, header, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


class TestRun:
    def test_no_card_column_is_a_clean_noop(self, tmp_path):
        csv_path = tmp_path / "plain_2026-01-01.csv"
        _write_csv(csv_path, ["id", "acres"], [["1", "5.0"], ["2", "6.0"]])
        schema_mod.write_manifest(schema_mod.infer(csv_path), csv_path)

        # Passing None as the crypto client proves the stage never calls out
        # when there is nothing to encrypt.
        r = run("2026-01-01", csv_path, tmp_path / "out", None)
        assert r["cards"] == 0
        assert r["rows"] == 0
        assert r["output"] is None

    def test_card_column_is_split_and_only_middle_is_encrypted(self, tmp_path):
        csv_path = tmp_path / "cards_2026-01-01.csv"
        _write_csv(csv_path, ["id", "card_info"],
                   [["1", VISA], ["2", MC], ["3", VISA]])
        schema_mod.write_manifest(schema_mod.infer(csv_path), csv_path)

        crypto = _FakeCrypto()
        import datetime
        r = run(datetime.date(2026, 1, 1), csv_path, tmp_path / "out", crypto)

        assert r["cards"] == 1
        assert r["rows"] == 3

        # Only the middles were ever sent to the crypto service.
        assert crypto.calls == [("card_info_middle",
                             ["111111", "555555", "111111"])]

        out = list(csv.DictReader(open(r["output"])))
        assert [row["card_prefix"] for row in out] == ["411111", "555555", "411111"]
        assert [row["card_suffix"] for row in out] == ["1111", "4444", "1111"]
        assert all(row["card_middle_encrypted"].startswith("enc:") for row in out)

    def test_no_full_pan_survives_in_the_output(self, tmp_path):
        csv_path = tmp_path / "cards_2026-01-01.csv"
        _write_csv(csv_path, ["id", "card_info"],
                   [["1", VISA], ["2", MC], ["3", VISA]])
        schema_mod.write_manifest(schema_mod.infer(csv_path), csv_path)

        import datetime
        r = run(datetime.date(2026, 1, 1), csv_path, tmp_path / "out", _FakeCrypto())
        blob = open(r["output"]).read()
        assert VISA not in blob
        assert MC not in blob

    def test_row_key_carries_the_manifest_primary_key(self, tmp_path):
        csv_path = tmp_path / "cards_2026-01-01.csv"
        _write_csv(csv_path, ["OBJECTID", "card_info"],
                   [["7", VISA], ["9", MC], ["11", VISA]])
        schema_mod.write_manifest(schema_mod.infer(csv_path), csv_path)

        import datetime
        r = run(datetime.date(2026, 1, 1), csv_path, tmp_path / "out", _FakeCrypto())
        out = list(csv.DictReader(open(r["output"])))
        # The source header is OBJECTID; the manifest key is the safe name.
        # Resolving through the manifest is what makes this non-empty.
        assert [row["row_key"] for row in out] == ["7", "9", "11"]
