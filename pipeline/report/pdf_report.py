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


def _fetch(conn, logical_date: date) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*), coalesce(sum(duration_minutes),0),
                   coalesce(round(avg(duration_minutes)::numeric,1),0),
                   coalesce(max(duration_minutes),0)
            FROM warehouse.trips WHERE logical_date = %s""", (logical_date,))
        n, total, avg, longest = cur.fetchone()

        cur.execute("""
            SELECT extract(hour from start_time)::int AS hr, count(*)
            FROM warehouse.trips WHERE logical_date = %s
            GROUP BY hr ORDER BY hr""", (logical_date,))
        by_hour = cur.fetchall()

        cur.execute("""
            SELECT start_station_masked, count(*) AS c
            FROM warehouse.trips WHERE logical_date = %s
              AND start_station_masked IS NOT NULL
            GROUP BY 1 ORDER BY c DESC LIMIT 6""", (logical_date,))
        top_stations = cur.fetchall()

        cur.execute("""
            SELECT width_bucket(duration_minutes, 0, 60, 6) AS b, count(*)
            FROM warehouse.trips WHERE logical_date = %s
            GROUP BY b ORDER BY b""", (logical_date,))
        durations = cur.fetchall()

        cur.execute("""
            SELECT masked_by, count(*) FROM warehouse.trips
            WHERE logical_date = %s GROUP BY 1""", (logical_date,))
        provenance = cur.fetchall()

    return {"count": n, "total_minutes": total, "avg_minutes": float(avg),
            "longest": longest, "by_hour": by_hour, "top_stations": top_stations,
            "durations": durations, "provenance": provenance}


def _chart_trips_by_hour(data) -> BytesIO:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    hours = [h for h, _ in data["by_hour"]]
    counts = [c for _, c in data["by_hour"]]
    fig, ax = plt.subplots(figsize=(7.2, 2.9), dpi=150)
    ax.bar(hours, counts, color=BLUE, width=0.72, zorder=3)
    for h, c in zip(hours, counts):
        if c:
            ax.text(h, c, str(c), ha="center", va="bottom", fontsize=7.5, color=INK)
    ax.set_xlabel("hour of day", fontsize=8.5, color=MUTED)
    ax.set_ylabel("trips", fontsize=8.5, color=MUTED)
    ax.grid(axis="y", color=GRID, lw=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8, colors=MUTED)
    fig.tight_layout()
    buf = BytesIO(); fig.savefig(buf, format="png"); plt.close(fig); buf.seek(0)
    return buf


def _chart_duration_hist(data) -> BytesIO:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = ["0-10", "10-20", "20-30", "30-40", "40-50", "50-60", "60+"]
    counts = [0] * 7
    for b, c in data["durations"]:
        # width_bucket(v, 0, 60, 6) returns 1..6 for values inside the range
        # and 7 at or above the upper bound. Index 0 is the 0-10 bucket, so
        # bucket b maps to index b-1.
        if b is None:
            continue
        counts[min(max(int(b) - 1, 0), 6)] += c
    fig, ax = plt.subplots(figsize=(7.2, 2.6), dpi=150)
    ax.bar(labels, counts, color=ORANGE, width=0.68, zorder=3)
    for i, c in enumerate(counts):
        if c:
            ax.text(i, c, str(c), ha="center", va="bottom", fontsize=7.5, color=INK)
    ax.set_xlabel("trip duration (minutes)", fontsize=8.5, color=MUTED)
    ax.set_ylabel("trips", fontsize=8.5, color=MUTED)
    ax.grid(axis="y", color=GRID, lw=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8, colors=MUTED)
    fig.tight_layout()
    buf = BytesIO(); fig.savefig(buf, format="png"); plt.close(fig); buf.seek(0)
    return buf


def _chart_top_stations(data) -> BytesIO:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = list(reversed(data["top_stations"]))
    names = [r[0] for r in rows]
    counts = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(7.2, 2.05), dpi=150)
    ax.barh(range(len(rows)), counts, color=AQUA, height=0.62, zorder=3)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels(names, fontsize=7.5)
    for i, c in enumerate(counts):
        ax.text(c, i, f" {c}", va="center", fontsize=7.5, color=INK)
    ax.set_xlabel("trips started (station name is masked per Ranger policy)",
                  fontsize=8, color=MUTED)
    ax.grid(axis="x", color=GRID, lw=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8, colors=MUTED)
    fig.tight_layout()
    buf = BytesIO(); fig.savefig(buf, format="png"); plt.close(fig); buf.seek(0)
    return buf


def run(logical_date: date, out_dir: Path, conn=None) -> dict:
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
        data = _fetch(conn, logical_date)
    finally:
        if owns:
            conn.close()

    if data["count"] == 0:
        raise RuntimeError(
            f"no rows in the warehouse for {logical_date}; nothing to report. "
            f"Run the load stage first.")

    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = out_dir / f"trips_report_{logical_date.isoformat()}.pdf"

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
                            title=f"Trips report {logical_date}")
    story = [
        Paragraph("Bikeshare Trips — Daily Report", h1),
        Paragraph(
            f"Reporting date {logical_date.isoformat()} &nbsp;·&nbsp; "
            f"generated {datetime.now().strftime('%Y-%m-%d %H:%M')} &nbsp;·&nbsp; "
            f"source: masked warehouse", sub),
    ]

    tiles = [[
        f"{data['count']:,}\nTRIPS",
        f"{data['avg_minutes']:.1f} min\nAVERAGE",
        f"{data['longest']:,} min\nLONGEST",
        f"{data['total_minutes']:,} min\nTOTAL RIDDEN",
    ]]
    t = Table(tiles, colWidths=[43 * mm] * 4, rowHeights=[17 * mm])
    t.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(GRID)),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor(GRID)),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor(INK)),
    ]))
    story += [t, Spacer(1, 6)]

    story += [Paragraph("Trips by hour of day", h2),
              Image(_chart_trips_by_hour(data), width=170 * mm, height=68 * mm)]
    story += [Paragraph("Trip duration distribution", h2),
              Image(_chart_duration_hist(data), width=170 * mm, height=61 * mm)]
    # Aspect ratio preserved (figure is 7.2 x 2.05 in), sized to leave room
    # for the data-handling note on the same page.
    story += [Paragraph("Busiest departure stations", h2),
              Image(_chart_top_stations(data), width=170 * mm,
                    height=170 * mm * (2.05 / 7.2))]

    masked_by = ", ".join(f"{m} ({c})" for m, c in data["provenance"])
    story += [
        Paragraph("Data handling", h2),
        Paragraph(
            "Station names, bike identifiers and membership tiers are shown "
            "<b>masked</b>. Those fields were AES-256-GCM encrypted before "
            "landing in Parquet, decrypted only inside the Hive stage, and "
            "written to the warehouse already masked under Apache Ranger "
            f"policy — so no plaintext value exists in the warehouse this "
            f"report reads. Masking applied by: <b>{masked_by}</b>. "
            "A partially-masked station name is the policy working as "
            "configured, not a rendering fault.", body),
    ]

    doc.build(story)
    log.info("wrote %s", pdf_path)
    return {"pdf_path": str(pdf_path), "rows": data["count"]}
