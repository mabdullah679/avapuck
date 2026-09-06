"""PDF report with charts, built from the Postgres warehouse. One per run.

Every figure on the page comes from MASKED warehouse data. The report is a
consumer of the warehouse like any other, with no privileged path back to
plaintext -- which is why a chart of "top stations" shows masked station names.
That is the system working, not a rendering bug, and the report says so on the
page rather than leaving the reader to wonder.

DATES (docs/DECISIONS.md D13)
The page is scoped to ONE run slot, never a whole day, so it pairs with exactly
one verdict message. Three timestamps appear in the header and all three are
UTC with a Z suffix: the run slot, the moment the page was generated, and the
data window the extract actually read -- which for a frozen public archive is
years before the run slot, and for a CSV is whatever its declared event-time
column says. Printing the run date as if it were the data's date was the gap
TRUST-BOUNDARY 1.9 recorded; this closes it.

Charts use a validated colourblind-safe palette and label values directly, so
the page reads correctly in greyscale and to a colourblind reader.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path

log = logging.getLogger(__name__)

# Validated categorical palette: adjacent pairs clear CVD and normal-vision
# separation floors.
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, MUTED, GRID = "#0b0b0b", "#6f6e6a", "#e6e5e1"
STATUS_COLOUR = {"complete": "#1f7a4d", "degraded": "#a15c00", "failed": "#b23a2e"}


def _utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _iso_z(ts: datetime) -> str:
    return _utc(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


def _slot_label(ts: datetime) -> str:
    return _utc(ts).strftime("%Y%m%dT%H%MZ")


def _fetch(conn, scope: tuple[str, object], dataset: str, primary_key: str) -> dict:
    """Collect the numbers for the page, for whatever columns this table has.

    The queries are built from the chart specs the profiler chose rather than
    written out per column, so a dataset the pipeline has never seen still
    produces a populated page.
    """
    from pipeline.report import profile

    where, params = profile._where(scope)
    with conn.cursor() as cur:
        types = profile._column_types(cur, dataset)
        if not types:
            raise RuntimeError(
                f"warehouse.{dataset} does not exist; run the load stage first.")

        tiles = profile.headline_metrics(cur, dataset, scope, primary_key, types)
        specs = profile.choose_charts(cur, dataset, scope, primary_key)

        cur.execute(f"SELECT count(*) FROM warehouse.{dataset} WHERE {where}", params)
        count = cur.fetchone()[0]

        charts = []
        for spec in specs:
            col = spec["column"]
            if spec["kind"] == "by_hour":
                cur.execute(
                    f"SELECT extract(hour from {col})::int AS hr, count(*) "
                    f"FROM warehouse.{dataset} WHERE {where} "
                    f"AND {col} IS NOT NULL GROUP BY hr ORDER BY hr", params)
                spec["rows"] = cur.fetchall()
            elif spec["kind"] == "histogram":
                lo, hi = spec["min"], spec["max"]
                cur.execute(
                    f"SELECT width_bucket({col}, %s, %s, 6) AS b, count(*) "
                    f"FROM warehouse.{dataset} WHERE {where} "
                    f"AND {col} IS NOT NULL GROUP BY b ORDER BY b",
                    (lo, hi, *params))
                spec["rows"] = cur.fetchall()
            else:  # categories
                cur.execute(
                    f"SELECT {col}, count(*) AS c FROM warehouse.{dataset} "
                    f"WHERE {where} AND {col} IS NOT NULL "
                    f"GROUP BY 1 ORDER BY c DESC LIMIT 6", params)
                spec["rows"] = cur.fetchall()
            charts.append(spec)

        cur.execute(f"SELECT masked_by, count(*) FROM warehouse.{dataset} "
                    f"WHERE {where} GROUP BY 1", params)
        provenance = cur.fetchall()

    return {"count": count, "tiles": tiles, "charts": charts,
            "provenance": provenance}


def _new_axes(height: float):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt, plt.subplots(figsize=(7.2, height), dpi=150)


def _finish(plt, fig, ax, xlabel: str, ylabel: str = "records",
            grid_axis: str = "y") -> BytesIO:
    """Shared chart furniture: grid behind, no top/right spines, muted ticks."""
    ax.set_xlabel(xlabel, fontsize=8.5, color=MUTED)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=8.5, color=MUTED)
    ax.grid(axis=grid_axis, color=GRID, lw=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8, colors=MUTED)
    fig.tight_layout()
    buf = BytesIO(); fig.savefig(buf, format="png"); plt.close(fig); buf.seek(0)
    return buf


def _chart_by_hour(spec) -> BytesIO:
    plt, (fig, ax) = _new_axes(2.9)
    hours = [h for h, _ in spec["rows"]]
    counts = [c for _, c in spec["rows"]]
    ax.bar(hours, counts, color=BLUE, width=0.72, zorder=3)
    for h, c in zip(hours, counts):
        if c:
            ax.text(h, c, str(c), ha="center", va="bottom", fontsize=7.5, color=INK)
    return _finish(plt, fig, ax, "hour of day (UTC)")


def _chart_histogram(spec) -> BytesIO:
    """Six equal buckets across the column's observed range, plus an overflow.

    Labels are computed from the real min/max rather than assuming a 0-60
    minute scale, so the same renderer serves trip durations and lease acreage.
    """
    plt, (fig, ax) = _new_axes(2.6)
    lo, hi = spec["min"], spec["max"]
    step = (hi - lo) / 6 if hi > lo else 1.0

    def _fmt(v: float) -> str:
        return f"{v:,.0f}" if abs(v) >= 100 or v == int(v) else f"{v:,.1f}"

    labels = [f"{_fmt(lo + i * step)}–{_fmt(lo + (i + 1) * step)}" for i in range(6)]
    labels.append(f"{_fmt(hi)}+")
    counts = [0] * 7
    for b, c in spec["rows"]:
        if b is None:
            continue
        # width_bucket returns 1..6 inside the range and 7 at/above the top.
        counts[min(max(int(b) - 1, 0), 6)] += c
    ax.bar(labels, counts, color=ORANGE, width=0.68, zorder=3)
    for i, c in enumerate(counts):
        if c:
            ax.text(i, c, str(c), ha="center", va="bottom", fontsize=7.5, color=INK)
    ax.tick_params(axis="x", labelrotation=20)
    return _finish(plt, fig, ax, spec["column"].replace("_", " "))


def _chart_categories(spec) -> BytesIO:
    plt, (fig, ax) = _new_axes(2.05)
    rows = list(reversed(spec["rows"]))
    names = [("(blank)" if r[0] in (None, "") else str(r[0]))[:38] for r in rows]
    counts = [r[1] for r in rows]
    ax.barh(range(len(rows)), counts, color=AQUA, height=0.62, zorder=3)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels(names, fontsize=7.5)
    for i, c in enumerate(counts):
        ax.text(c, i, f" {c}", va="center", fontsize=7.5, color=INK)
    note = (" (value is masked per Ranger policy)"
            if spec["column"].endswith("_masked") else "")
    return _finish(plt, fig, ax, f"records{note}", ylabel="", grid_axis="x")


_RENDERERS = {
    "by_hour": (_chart_by_hour, 68.0),
    "histogram": (_chart_histogram, 61.0),
    "categories": (_chart_categories, 170.0 * (2.05 / 7.2)),
}


def _window_text(data_window: dict | None) -> str:
    """The data window line, honest about where the dates came from."""
    if not data_window or not data_window.get("start"):
        return "data window: not recorded"
    start, end, source = data_window["start"], data_window.get("end"), data_window.get("source")
    span = start if not end or end == start else f"{start} .. {end}"
    how = {
        "archive_mapping": "archive window mapped from the run slot",
        "event_time_column": "from the dataset's declared event-time column",
        "logical_date": "the source carries no date of its own; this is the run date",
    }.get(source, source or "unknown")
    return f"data window: {span} ({how})"


def run(run_slot: datetime | date, out_dir: Path, conn=None,
        dataset: str = "trips", primary_key: str = "trip_id",
        title: str | None = None, *, trace_id: str | None = None,
        status: str | None = None, reasons: list[str] | None = None,
        data_window: dict | None = None) -> dict:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import (Image, Paragraph, SimpleDocTemplate, Spacer,
                                    Table, TableStyle)

    # A bare date (a hand run) means the midnight slot of that day.
    if not isinstance(run_slot, datetime):
        run_slot = datetime.combine(run_slot, datetime.min.time(), tzinfo=timezone.utc)
    run_slot = _utc(run_slot)
    scope = ("run_slot", run_slot)

    owns = conn is None
    if owns:
        from pipeline.load.postgres_load import connection_from_env
        conn = connection_from_env()
    try:
        data = _fetch(conn, scope, dataset, primary_key)
    finally:
        if owns:
            conn.close()

    if data["count"] == 0:
        raise RuntimeError(
            f"no rows in warehouse.{dataset} for run slot {_iso_z(run_slot)}; nothing "
            f"to report. Run the load stage for this slot first.")

    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = out_dir / f"{dataset}_report_{_slot_label(run_slot)}.pdf"
    heading = title or f"{dataset.replace('_', ' ').title()} — Run Report"
    generated = datetime.now(timezone.utc)

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=17,
                        textColor=colors.HexColor(INK), spaceAfter=2)
    sub = ParagraphStyle("sub", parent=styles["Normal"], fontSize=9,
                         textColor=colors.HexColor(MUTED), spaceAfter=3)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=11.5,
                        textColor=colors.HexColor(INK), spaceBefore=10, spaceAfter=4)
    body = ParagraphStyle("body", parent=styles["Normal"], fontSize=8.6,
                          textColor=colors.HexColor(MUTED), leading=12)

    doc = SimpleDocTemplate(str(pdf_path), pagesize=A4,
                            leftMargin=18 * mm, rightMargin=18 * mm,
                            topMargin=16 * mm, bottomMargin=16 * mm,
                            title=f"{dataset} report {_iso_z(run_slot)}")
    story = [
        Paragraph(heading, h1),
        Paragraph(
            f"Run slot {_iso_z(run_slot)} &nbsp;·&nbsp; "
            f"generated {_iso_z(generated)} &nbsp;·&nbsp; "
            f"source: masked warehouse &nbsp;·&nbsp; all times UTC", sub),
        Paragraph(_window_text(data_window)
                  + (f" &nbsp;·&nbsp; trace {trace_id}" if trace_id else ""), sub),
        Spacer(1, 6),
    ]

    tiles = [[f"{value}\n{label}" for value, label in data["tiles"]]]
    width = 172.0 / max(1, len(tiles[0]))
    t = Table(tiles, colWidths=[width * mm] * len(tiles[0]), rowHeights=[17 * mm])
    t.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(GRID)),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor(GRID)),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor(INK)),
    ]))
    story += [t, Spacer(1, 6)]

    # Heights preserve each figure's aspect ratio (all are 7.2in wide).
    for spec in data["charts"]:
        render, height = _RENDERERS[spec["kind"]]
        story += [Paragraph(spec["title"], h2),
                  Image(render(spec), width=170 * mm, height=height * mm)]

    # ── Run status: the same verdict downstream receives, in words ──
    if status:
        colour = STATUS_COLOUR.get(status, MUTED)
        status_line = (f"<font color='{colour}'><b>{status.upper()}</b></font> as of the "
                       f"report stage. The signed verdict on the control topic is computed "
                       f"after this page is filed and adds the report itself.")
        story += [Paragraph("Run status", h2), Paragraph(status_line, body)]
        for r in (reasons or []):
            story.append(Paragraph(f"&bull; {r}", body))
        if not reasons:
            story.append(Paragraph("No degradations recorded.", body))

    masked_by = ", ".join(f"{m} ({c})" for m, c in data["provenance"]) or "n/a"
    masked_cols = [s["column"] for s in data["charts"]
                   if s["column"].endswith("_masked")]
    masked_note = (
        f" The masked values shown here (<b>{', '.join(masked_cols)}</b>) are "
        f"the policy working as configured, not a rendering fault."
        if masked_cols else "")
    story += [
        Paragraph("Data handling", h2),
        Paragraph(
            "Columns classified as sensitive were AES-256-GCM encrypted before "
            "landing in Parquet, decrypted only inside the Hive stage, and "
            "written to the warehouse already masked under Apache Ranger "
            "policy — so no plaintext value exists in the warehouse this "
            f"report reads. Masking applied by: <b>{masked_by}</b>.{masked_note}",
            body),
    ]

    doc.build(story)
    log.info("wrote %s", pdf_path)
    return {"pdf_path": str(pdf_path), "rows": data["count"],
            "run_slot": _iso_z(run_slot), "generated_at": _iso_z(generated)}
