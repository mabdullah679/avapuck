"""Canonical period arithmetic.

The canonical period is ALWAYS a calendar quarter. Fiscal labels are a
per-service presentation concern, translated inbound and re-derived outbound.
Four jurisdictions with two different fiscal calendars cannot share a fiscal
period without one of them being wrong, so the canonical layer refuses to
carry one.

Fiscal-year naming convention (declared, not assumed): a fiscal year is named
for the CALENDAR YEAR IN WHICH IT STARTS. Apr-2026 through Mar-2027 is FY26.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date

_Q_START_MONTH = {1: 1, 2: 4, 3: 7, 4: 10}
_Q_END = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}


@dataclass(frozen=True)
class Period:
    year: int
    quarter: int

    @property
    def period_id(self) -> str:
        return f"{self.year}CQ{self.quarter}"

    @property
    def start_date(self) -> date:
        return date(self.year, _Q_START_MONTH[self.quarter], 1)

    @property
    def end_date(self) -> date:
        m, d = _Q_END[self.quarter]
        return date(self.year, m, d)

    def is_closed(self, as_of: date) -> bool:
        return self.end_date < as_of

    def next(self) -> "Period":
        return Period(self.year + 1, 1) if self.quarter == 4 else Period(self.year, self.quarter + 1)

    @staticmethod
    def parse(period_id: str) -> "Period":
        y, q = period_id.split("CQ")
        return Period(int(y), int(q))

    @staticmethod
    def containing(d: date) -> "Period":
        return Period(d.year, (d.month - 1) // 3 + 1)


def to_fiscal_label(p: Period, fiscal_start_month: int) -> str:
    """Render a canonical calendar quarter as a service's own fiscal label.

    This is the seeded period conflict, generated. With fiscal_start_month=1
    (US, BH) the calendar quarter Jul-Sep 2026 is FY26Q3. With
    fiscal_start_month=4 (UK, JP) the SAME three months are FY26Q2, and that
    service's "FY26Q3" is a different three months entirely. Nothing in the
    string reveals which is meant -- only the jurisdiction does.
    """
    start = p.start_date
    months_in = (start.month - fiscal_start_month) % 12
    fq = months_in // 3 + 1
    fy = start.year if start.month >= fiscal_start_month else start.year - 1
    return f"FY{fy % 100:02d}Q{fq}"


def from_fiscal_label(label: str, fiscal_start_month: int) -> Period:
    """Inverse of to_fiscal_label. Used by the fiscal_to_calendar_quarter operator."""
    body = label.upper().removeprefix("FY")
    fy_s, fq_s = body.split("Q")
    fy = 2000 + int(fy_s)
    fq = int(fq_s)
    month = (fiscal_start_month - 1 + (fq - 1) * 3) % 12 + 1
    year = fy + (1 if fiscal_start_month - 1 + (fq - 1) * 3 >= 12 else 0)
    return Period(year, (month - 1) // 3 + 1)


def quarters_between(first: Period, last: Period) -> list[Period]:
    out, cur = [], first
    while (cur.year, cur.quarter) <= (last.year, last.quarter):
        out.append(cur)
        cur = cur.next()
    return out
