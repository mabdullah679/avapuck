"""Tests for the BigQuery-retry-then-CSV-fallback behaviour.

Client requirement: "if BigQuery fails 3 times, default to the CSV workflow".
"""
import os
from datetime import datetime

import pytest

from pipeline.extract import fixture


class _Result:
    row_count = 42


class FlakyBQ:
    """A stand-in BigQueryExtractor that fails a fixed number of times."""

    def __init__(self, fail_times):
        self.fail_times = fail_times
        self.calls = 0

    def extract(self, ts, out_dir):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError(f"simulated BigQuery failure {self.calls}")
        return _Result()


# The retry path keys off the class NAME, so the fake must present it.
FlakyBQ.__name__ = "BigQueryExtractor"


class TestRetry:
    def test_succeeds_first_try_without_falling_back(self, monkeypatch):
        bq = FlakyBQ(fail_times=0)
        monkeypatch.setattr(fixture, "get_extractor", lambda: bq)
        r, ex, attempts, fell = fixture.extract_with_fallback(
            datetime(2026, 1, 1), "/tmp")
        assert (r.row_count, attempts, fell) == (42, 1, False)
        assert bq.calls == 1

    def test_retries_then_succeeds_without_falling_back(self, monkeypatch):
        bq = FlakyBQ(fail_times=2)
        monkeypatch.setattr(fixture, "get_extractor", lambda: bq)
        monkeypatch.setenv("BQ_ATTEMPTS", "3")
        r, ex, attempts, fell = fixture.extract_with_fallback(
            datetime(2026, 1, 1), "/tmp")
        assert (attempts, fell) == (3, False)
        assert bq.calls == 3

    def test_three_failures_fall_back_to_csv(self, monkeypatch, tmp_path):
        bq = FlakyBQ(fail_times=99)
        monkeypatch.setattr(fixture, "get_extractor", lambda: bq)
        monkeypatch.setenv("BQ_ATTEMPTS", "3")

        csv_path = tmp_path / "fallback.csv"
        csv_path.write_text("id,value\n1,a\n2,b\n")
        monkeypatch.setenv("CSV_FALLBACK_PATH", str(csv_path))

        called = {}

        class FakeCsv:
            def extract(self, ts, out_dir):
                called["yes"] = os.environ.get("CSV_SOURCE_PATH")
                return _Result()

        import pipeline.extract.csv_source as cs
        monkeypatch.setattr(cs, "extractor_from_env", lambda: FakeCsv())

        r, ex, attempts, fell = fixture.extract_with_fallback(
            datetime(2026, 1, 1), tmp_path)

        assert bq.calls == 3, "BigQuery must be tried exactly BQ_ATTEMPTS times"
        assert fell is True
        assert attempts == 3
        assert called["yes"] == str(csv_path)

    def test_attempt_count_is_configurable(self, monkeypatch, tmp_path):
        bq = FlakyBQ(fail_times=99)
        monkeypatch.setattr(fixture, "get_extractor", lambda: bq)
        monkeypatch.setenv("BQ_ATTEMPTS", "5")
        csv_path = tmp_path / "f.csv"
        csv_path.write_text("id\n1\n")
        monkeypatch.setenv("CSV_FALLBACK_PATH", str(csv_path))

        import pipeline.extract.csv_source as cs
        monkeypatch.setattr(cs, "extractor_from_env",
                            lambda: type("F", (), {"extract": lambda s, t, o: _Result()})())

        fixture.extract_with_fallback(datetime(2026, 1, 1), tmp_path)
        assert bq.calls == 5

    def test_raises_when_there_is_nothing_to_fall_back_to(self, monkeypatch):
        """A fallback with no configured CSV must fail loudly, not silently."""
        bq = FlakyBQ(fail_times=99)
        monkeypatch.setattr(fixture, "get_extractor", lambda: bq)
        monkeypatch.setenv("BQ_ATTEMPTS", "2")
        monkeypatch.delenv("CSV_FALLBACK_PATH", raising=False)
        monkeypatch.delenv("CSV_SOURCE_PATH", raising=False)
        with pytest.raises(RuntimeError, match="simulated BigQuery failure"):
            fixture.extract_with_fallback(datetime(2026, 1, 1), "/tmp")


class TestNonBigQuerySources:
    def test_a_csv_source_is_not_retried(self, monkeypatch):
        """Retrying a local file that failed would only delay a real failure."""
        class Csv:
            calls = 0
            def extract(self, ts, out_dir):
                Csv.calls += 1
                return _Result()
        Csv.__name__ = "CsvSourceExtractor"

        monkeypatch.setattr(fixture, "get_extractor", lambda: Csv())
        r, ex, attempts, fell = fixture.extract_with_fallback(
            datetime(2026, 1, 1), "/tmp")
        assert (attempts, fell) == (0, False)
