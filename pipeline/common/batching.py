"""Streaming helpers: the data path never holds a whole file in memory.

Uploads are uncapped (docs/DECISIONS.md D16), so a run may be a hundred rows
or ten million. Every stage that touches rows walks them in chunks of
PIPELINE_BATCH_ROWS through the helpers here, and every crypto call is one
chunk wide. The 100-row scheduled run takes exactly the same path with one
chunk; there is no small-file code and large-file code to drift apart.

Parquet artifacts stay single files, written incrementally with a fixed
all-string schema so every chunk has the same shape. Types are applied at the
warehouse boundary, as before.
"""
from __future__ import annotations

import csv as csvmod
import os
from collections.abc import Iterable, Iterator
from pathlib import Path


def batch_rows() -> int:
    """Chunk size, read at call time so a test or a run can set it."""
    return max(1, int(os.environ.get("PIPELINE_BATCH_ROWS", "5000")))


def chunked(items: Iterable, size: int) -> Iterator[list]:
    buf: list = []
    for it in items:
        buf.append(it)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def iter_csv_chunks(path: Path, size: int | None = None) -> Iterator[list[dict]]:
    """Dicts keyed by the CSV's own header, `size` rows at a time."""
    size = size or batch_rows()
    with Path(path).open(newline="", encoding="utf-8-sig") as fh:
        yield from chunked(csvmod.DictReader(fh), size)


def iter_parquet_batches(path: Path, size: int | None = None) -> Iterator[list[dict]]:
    """Dicts from a Parquet file, `size` rows at a time, without loading it."""
    import pyarrow.parquet as pq
    size = size or batch_rows()
    pf = pq.ParquetFile(str(path))
    for batch in pf.iter_batches(batch_size=size):
        yield batch.to_pylist()


def parquet_columns(path: Path) -> list[str]:
    import pyarrow.parquet as pq
    return list(pq.ParquetFile(str(path)).schema_arrow.names)


def parquet_row_count(path: Path) -> int:
    import pyarrow.parquet as pq
    return int(pq.read_metadata(str(path)).num_rows)


class ParquetSink:
    """Append chunks of dicts to ONE Parquet file with a fixed string schema.

    Values are stringified on the way in (None stays None), which is what the
    csv-derived rows already were; a number typed in the warehouse DDL is cast
    by Postgres on load, as it always has been.
    """

    def __init__(self, path: Path, columns: list[str]):
        import pyarrow as pa
        self.path = Path(path)
        self.columns = list(columns)
        self.schema = pa.schema([(c, pa.string()) for c in self.columns])
        self.rows = 0
        self._writer = None

    def write(self, rows: list[dict]) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq
        if not rows:
            return
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(str(self.path), self.schema)
        cols = {c: [None if r.get(c) is None else str(r.get(c)) for r in rows]
                for c in self.columns}
        self._writer.write_table(pa.table(cols, schema=self.schema))
        self.rows += len(rows)

    def close(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq
        if self._writer is None:
            # Nothing written: still leave a valid, empty file with the schema.
            self.path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table({c: [] for c in self.columns}, schema=self.schema),
                           str(self.path))
            return
        self._writer.close()
        self._writer = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
