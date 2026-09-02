"""PDF report with charts, built from the Postgres warehouse.

Every figure on the page comes from MASKED warehouse data. The report is a
consumer of the warehouse like any other, with no privileged path back to
plaintext -- which is why a chart of "top stations" shows masked station names.
That is the system working, not a rendering bug, and the report says so on the
page rather than leaving the reader to wonder.

Charts use a validated colourblind-safe palette and label values directly, so
the page reads correctly in greyscale and to a colourblind reader.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from io import BytesIO
from pathlib import Path

log = logging.getLogger(__name__)

# Validated categorical palette: adjacent pairs clear CVD and normal-vision
# separation floors.
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, MUTED, GRID = "#0b0b0b", "#6f6e6a", "#e6e5e1"


def _fetch(conn, logical_date: date, dataset: str, primary_key: str) -> dict:
    """Collect the numbers for the page, for whatever columns this table has.

    The queries are built from the chart specs the profiler chose rather than
    written out per column, so a dataset the pipeline has never seen still
    produces a populated page.
    """
    from pipeline.report import profile

    with conn.cursor() as cur:
        types = profile._column_types(cur, dataset)
        if not types:
            raise RuntimeError(
                f"warehouse.{dataset} does not exist; run the load stage for "
                f"{logical_date} first.")

        tiles = profile.headline_metrics(cur, dataset, logical_date,
                                         primary_key, types)
        specs = profile.choose_charts(cur, dataset, logical_date, primary_key)

        cur.execute(f"SELECT count(*) FROM warehouse.{dataset} "
                    f"WHERE logical_date = %s", (logical_date,))
        count = cur.fetchone()[0]

        charts = []
        for spec in specs:
            col = spec["column"]
            if spec["kind"] == "by_hour":
                cur.execute(
                    f"SELECT extract(hour from {col})::int AS hr, count(*) "
                    f"FROM warehouse.{dataset} WHERE logical_date = %s "
                    f"AND {col} IS NOT NULL GROUP BY hr ORDER BY hr",
                    (logical_date,))
                spec["rows"] = cur.fetchall()
            elif spec["kind"] == "histogram":
                lo, hi = spec["min"], spec["max"]
                cur.execute(
                    f"SELECT width_bucket({col}, %s, %s, 6) AS b, count(*) "
                    f"FROM warehouse.{dataset} WHERE logical_date = %s "
                    f"AND {col} IS NOT NULL GROUP BY b ORDER BY b",
                    (lo, hi, logical_date))
                spec["rows"] = cur.fetchall()
            else:  # categories
                cur.execute(
                    f"SELECT {col}, count(*) AS c FROM warehouse.{dataset} "
                    f"WHERE logical_date = %s AND {col} IS NOT NULL "
                    f"GROUP BY 1 ORDER BY c DESC LIMIT 6", (logical_date,))
                spec["rows"] = cur.fetchall()
            charts.append(spec)

        cur.execute(f"SELECT masked_by, count(*) FROM warehouse.{dataset} "
                    f"WHERE logical_date = %s GROUP BY 1", (logical_date,))
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
    return _finish(plt, fig, ax, "hour of day")


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


def run(logical_date: date, out_dir: Path, conn=None,
        dataset: str = "trips", primary_key: str = "trip_id",
        title: str | None = None) -> dict:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import (Image, Paragraph, SimpleDocTemplate, Spacer,
                                    Table, TableStyle)

    owns = conn is None
    if owns:
        from pipeline.load.postgres_load import connection_from_env
        conn = connection_from_env()
    try:
        data = _fetch(conn, logical_date, dataset, primary_key)
    finally:
        if owns:
            conn.close()

    if data["count"] == 0:
        raise RuntimeError(
            f"no rows in warehouse.{dataset} for {logical_date}; nothing to "
            f"report. Run the load stage first.")

    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = out_dir / f"{dataset}_report_{logical_date.isoformat()}.pdf"
    heading = title or f"{dataset.replace('_', ' ').title()} — Daily Report"

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=17,
                        textColor=colors.HexColor(INK), spaceAfter=2)
    sub = ParagraphStyle("sub", parent=styles["Normal"], fontSize=9,
                         textColor=colors.HexColor(MUTED), spaceAfter=10)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=11.5,
                        textColor=colors.HexColor(INK), spaceBefore=10, spaceAfter=4)
    body = ParagraphStyle("body", parent=styles["Normal"], fontSize=8.6,
                          textColor=colors.HexColor(MUTED), leading=12)

    doc = SimpleDocTemplate(str(pdf_path), pagesize=A4,
                            leftMargin=18 * mm, rightMargin=18 * mm,
                            topMargin=16 * mm, bottomMargin=16 * mm,
                            title=f"{dataset} report {logical_date}")
    story = [
        Paragraph(heading, h1),
        Paragraph(
            f"Reporting date {logical_date.isoformat()} &nbsp;·&nbsp; "
            f"generated {datetime.now().strftime('%Y-%m-%d %H:%M')} &nbsp;·&nbsp; "
            f"source: masked warehouse", sub),
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
    return {"pdf_path": str(pdf_path), "rows": data["count"]}
