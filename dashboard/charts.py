"""Inline-SVG chart primitives.

Written by hand rather than pulled from a charting library for one reason: the
hardest requirement on this dashboard is that a PROJECTION must never read as a
fact. Off-the-shelf charts fight you on that -- they will happily draw an
estimate in the same ink as a measurement. Here the distinction is structural:
projected segments are a different stroke, carry a variability band, and are
labelled with an asterisk.

Palette is the validated categorical default (see the dataviz reference); the
four slots pass adjacent CVD and normal-vision separation in both light and
dark. Two light-mode slots sit below 3:1 against the surface, so every chart
here ships direct labels AND a table view -- the declared relief.
"""
from __future__ import annotations

import html
from dataclasses import dataclass

SERIES = ["var(--series-1)", "var(--series-2)", "var(--series-3)", "var(--series-4)"]

CSS = """
<style>
html,body { margin:0; padding:0; background:transparent; }
.viz-root {
  --surface-1:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e;
  --text-muted:#6f6e6a; --grid:#e6e5e1; --axis:#c9c8c3;
  --series-1:#2a78d6; --series-2:#eb6834; --series-3:#1baf7a; --series-4:#eda100;
  --status-good:#0ca30c; --status-warning:#fab219; --status-critical:#d03b3b;
  --proj-fill:rgba(42,120,214,0.14);
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) .viz-root {
    --surface-1:#0e1117; --text-primary:#fafafa; --text-secondary:#c3c2b7;
    --text-muted:#9d9c94; --grid:#2e2e2c; --axis:#4a4a46;
    --series-1:#3987e5; --series-2:#d95926; --series-3:#199e70; --series-4:#c98500;
    --proj-fill:rgba(57,135,229,0.20);
  }
}
.viz-root text { fill: var(--text-secondary); }
.viz-title { font-size:15px; font-weight:600; color:var(--text-primary); margin:0 0 2px; }
.viz-sub { font-size:12px; color:var(--text-muted); margin:0 0 10px; }
.viz-legend { display:flex; gap:16px; flex-wrap:wrap; font-size:12px;
  color:var(--text-secondary); margin:8px 0 0; align-items:center; }
.viz-legend span.k { display:inline-flex; align-items:center; gap:6px; }
.viz-swatch { width:14px; height:3px; border-radius:2px; display:inline-block; }
.viz-note { font-size:11.5px; color:var(--text-muted); margin-top:8px; line-height:1.5; }
.viz-root { background:var(--surface-1); }
.est { color:var(--status-warning); font-weight:600; }
</style>
"""


def _fmt_money(minor: float, decimals: int = 1) -> str:
    v = minor / 100.0
    for unit, div in (("bn", 1e9), ("m", 1e6), ("k", 1e3)):
        if abs(v) >= div:
            return f"${v / div:,.{decimals}f}{unit}"
    return f"${v:,.0f}"


@dataclass
class Point:
    label: str
    value: float
    low: float | None = None
    high: float | None = None
    projected: bool = False


TREND_CHROME = 132     # title + subtitle + legend around the plot
BARS_CHROME = 92


def trend_chart(points: list[Point], title: str, subtitle: str,
                width: int = 1180, height: int = 320) -> str:
    """Quarterly trend where projected quarters are structurally distinct:
    dashed stroke, hollow markers, a shaded variability band, and an asterisk
    on the label. Actuals are solid with filled markers."""
    if not points:
        return "<p>no data</p>"

    pad_l, pad_r, pad_t, pad_b = 74, 24, 18, 46
    pw, ph = width - pad_l - pad_r, height - pad_t - pad_b

    lows = [p.low if p.low is not None else p.value for p in points]
    highs = [p.high if p.high is not None else p.value for p in points]
    vmax = max(highs) * 1.08
    vmin = min(min(lows) * 0.92, 0)
    span = vmax - vmin or 1

    def x(i): return pad_l + (pw * i / max(1, len(points) - 1))
    def y(v): return pad_t + ph - (ph * (v - vmin) / span)

    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" '
             f'role="img" aria-label="{html.escape(title)}" '
             f'style="max-width:{width}px;overflow:visible">']

    # recessive gridlines + y labels
    for f in (0, 0.25, 0.5, 0.75, 1.0):
        v = vmin + span * f
        yy = y(v)
        parts.append(f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{width - pad_r}" y2="{yy:.1f}" '
                     f'stroke="var(--grid)" stroke-width="1"/>')
        parts.append(f'<text x="{pad_l - 10}" y="{yy + 4:.1f}" text-anchor="end" '
                     f'font-size="11">{_fmt_money(v, 1)}</text>')

    # variability band, projected segment only
    band = [p for p in points if p.projected and p.low is not None]
    if band:
        first_proj = points.index(band[0])
        anchor = max(0, first_proj - 1)
        seq = points[anchor:]
        top = " ".join(f"{x(anchor + i):.1f},{y(p.high if p.high is not None else p.value):.1f}"
                       for i, p in enumerate(seq))
        bot = " ".join(f"{x(anchor + i):.1f},{y(p.low if p.low is not None else p.value):.1f}"
                       for i, p in reversed(list(enumerate(seq))))
        parts.append(f'<polygon points="{top} {bot}" fill="var(--proj-fill)" '
                     f'stroke="none"/>')

    # actual segment (solid) and projected segment (dashed), 2px
    def polyline(sub, dashed):
        if len(sub) < 2:
            return ""
        pts = " ".join(f"{x(i):.1f},{y(p.value):.1f}" for i, p in sub)
        dash = ' stroke-dasharray="7 5"' if dashed else ""
        return (f'<polyline points="{pts}" fill="none" stroke="var(--series-1)" '
                f'stroke-width="2" stroke-linejoin="round" stroke-linecap="round"{dash}/>')

    idx = list(enumerate(points))
    actual = [(i, p) for i, p in idx if not p.projected]
    proj_start = actual[-1][0] if actual else 0
    projected = [(i, p) for i, p in idx if i >= proj_start and (p.projected or i == proj_start)]
    parts.append(polyline(actual, False))
    parts.append(polyline(projected, True))

    # markers: filled for measured, hollow for estimated (>=8px)
    for i, p in idx:
        cx, cy = x(i), y(p.value)
        if p.projected:
            parts.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="4.5" '
                         f'fill="var(--surface-1)" stroke="var(--series-1)" stroke-width="2"/>')
        else:
            parts.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="4.5" fill="var(--series-1)" '
                         f'stroke="var(--surface-1)" stroke-width="2"/>')

    # selective direct labels: first, last actual, and every projection
    label_idx = {0, proj_start} | {i for i, p in idx if p.projected}
    for i, p in idx:
        if i not in label_idx:
            continue
        cx, cy = x(i), y(p.value)
        txt = _fmt_money(p.value) + ("*" if p.projected else "")
        fill = "var(--status-warning)" if p.projected else "var(--text-primary)"
        weight = "600"
        anchor = "middle"
        if i == 0:
            anchor = "start"
        elif i == len(points) - 1:
            anchor = "end"
        parts.append(f'<text x="{cx:.1f}" y="{cy - 12:.1f}" text-anchor="{anchor}" '
                     f'font-size="12" font-weight="{weight}" fill="{fill}">{txt}</text>')

    # x axis
    parts.append(f'<line x1="{pad_l}" y1="{pad_t + ph}" x2="{width - pad_r}" '
                 f'y2="{pad_t + ph}" stroke="var(--axis)" stroke-width="1"/>')
    for i, p in idx:
        lbl = p.label + ("*" if p.projected else "")
        fill = "var(--status-warning)" if p.projected else "var(--text-secondary)"
        parts.append(f'<text x="{x(i):.1f}" y="{pad_t + ph + 20:.1f}" text-anchor="middle" '
                     f'font-size="11.5" fill="{fill}">{lbl}</text>')

    parts.append("</svg>")

    legend = (
        '<div class="viz-legend">'
        '<span class="k"><span class="viz-swatch" style="background:var(--series-1)"></span>'
        'Measured (closed quarters)</span>'
        '<span class="k"><span class="viz-swatch" style="background:repeating-linear-gradient('
        '90deg,var(--series-1) 0 7px,transparent 7px 12px)"></span>'
        '<span class="est">Projected *</span></span>'
        '<span class="k"><span class="viz-swatch" style="background:var(--proj-fill);height:10px;'
        'border-radius:2px"></span>Variability range</span>'
        '</div>')

    return (f'<div class="viz-root">{CSS}'
            f'<p class="viz-title">{html.escape(title)}</p>'
            f'<p class="viz-sub">{html.escape(subtitle)}</p>'
            f'{"".join(parts)}{legend}</div>')


def reconciliation_bars(rows: list[tuple[str, str, float | None]], canonical: float | None,
                        title: str, subtitle: str, unit: str = "") -> str:
    """One bar per reporting service, each labelled with the NAMED RULE that
    produced it. The canonical value, when derivable, is a reference line.

    Direct value labels are always on -- two of the four light-mode slots sit
    below 3:1 against the surface, so labels are the declared relief, not a
    stylistic choice."""
    rows = [r for r in rows if r[2] is not None]
    if not rows:
        return '<div class="viz-root">' + CSS + '<p class="viz-sub">No service reported this metric.</p></div>'

    width, bar_h, gap = 1180, 30, 14
    pad_l, pad_r, pad_t = 150, 130, 10
    height = pad_t + len(rows) * (bar_h + gap) + 34
    vmax = max([r[2] for r in rows] + ([canonical] if canonical else [])) * 1.12 or 1
    pw = width - pad_l - pad_r

    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" '
             f'aria-label="{html.escape(title)}" style="max-width:{width}px;overflow:visible">']

    if canonical:
        cx = pad_l + pw * canonical / vmax
        parts.append(f'<line x1="{cx:.1f}" y1="{pad_t - 2}" x2="{cx:.1f}" '
                     f'y2="{height - 30}" stroke="var(--text-primary)" stroke-width="1.5" '
                     f'stroke-dasharray="4 4"/>')
        parts.append(f'<text x="{cx:.1f}" y="{height - 14}" text-anchor="middle" font-size="11" '
                     f'fill="var(--text-primary)" font-weight="600">canonical</text>')

    for i, (service, rule, value) in enumerate(rows):
        yy = pad_t + i * (bar_h + gap)
        w = max(2.0, pw * value / vmax)
        color = SERIES[i % len(SERIES)]
        # 2px surface gap between adjacent fills
        parts.append(f'<rect x="{pad_l}" y="{yy}" width="{w:.1f}" height="{bar_h}" rx="4" '
                     f'fill="{color}" stroke="var(--surface-1)" stroke-width="2"/>')
        parts.append(f'<text x="{pad_l - 12}" y="{yy + bar_h / 2 + 4:.0f}" text-anchor="end" '
                     f'font-size="12" font-weight="600" fill="var(--text-primary)">'
                     f'{html.escape(service)}</text>')
        parts.append(f'<text x="{pad_l + w + 10:.1f}" y="{yy + bar_h / 2 - 2:.0f}" '
                     f'font-size="12.5" font-weight="600" fill="var(--text-primary)">'
                     f'{value:,.0f}{html.escape(unit)}</text>')
        parts.append(f'<text x="{pad_l + w + 10:.1f}" y="{yy + bar_h / 2 + 12:.0f}" '
                     f'font-size="10.5" fill="var(--text-muted)">rule: {html.escape(rule or "-")}</text>')

    parts.append("</svg>")
    return (f'<div class="viz-root">{CSS}'
            f'<p class="viz-title">{html.escape(title)}</p>'
            f'<p class="viz-sub">{html.escape(subtitle)}</p>{"".join(parts)}</div>')


def stat_tiles(tiles: list[dict]) -> str:
    """Hero figures. A projected tile is marked with an asterisk, coloured with
    the warning status token, and carries its range -- never a bare number."""
    cells = []
    for t in tiles:
        est = t.get("projected")
        val_color = "var(--status-warning)" if est else "var(--text-primary)"
        star = "*" if est else ""
        sub = html.escape(t.get("sub", ""))
        cells.append(
            f'<div style="flex:1;min-width:170px;padding:14px 16px;border:1px solid var(--grid);'
            f'border-radius:10px;background:var(--surface-1)">'
            f'<div style="font-size:11.5px;color:var(--text-muted);text-transform:uppercase;'
            f'letter-spacing:.4px;margin-bottom:6px">{html.escape(t["label"])}</div>'
            f'<div style="font-size:26px;font-weight:650;color:{val_color};line-height:1.1">'
            f'{html.escape(t["value"])}{star}</div>'
            f'<div style="font-size:11.5px;color:var(--text-muted);margin-top:5px">{sub}</div>'
            f'</div>')
    return (f'<div class="viz-root">{CSS}'
            f'<div style="display:flex;gap:12px;flex-wrap:wrap">{"".join(cells)}</div></div>')
